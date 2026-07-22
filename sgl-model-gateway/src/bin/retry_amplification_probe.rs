//! Deterministic fault-injection probe for retry amplification.
//!
//! The probe starts a burst of concurrent origin requests against one shared
//! worker. Every first-wave upstream attempt reaches an injected 503 only after
//! all origins are in flight, modeling a concurrent brownout. Once those
//! failures are recorded, the worker circuit opens and later retry-loop calls
//! receive a local 503 rather than making another downstream attempt.
//!
//! It emits one proof JSONL metric: downstream attempts divided by origin
//! requests. The normal configuration should produce 1.0 for the configured
//! burst; disabling the circuit gate produces the retry budget (5.0 by
//! default), which makes this a calibration-sensitive fault-injection probe.

use std::{
    env,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::future::join_all;
use smg::{
    config::types::RetryConfig,
    core::{BasicWorker, BasicWorkerBuilder, CircuitBreakerConfig, CircuitState, RetryExecutor, Worker},
};
use tokio::sync::Barrier;

const DEFAULT_ORIGINS: usize = 32;
const FAILURE_THRESHOLD: u32 = 10;

#[derive(Clone)]
struct Scenario {
    worker: Arc<BasicWorker>,
    downstream_attempts: Arc<AtomicU64>,
    first_wave: Arc<AtomicUsize>,
    first_wave_barrier: Arc<Barrier>,
    origins: usize,
}

fn origin_count() -> usize {
    let origins = env::var("RETRY_PROBE_ORIGINS")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("RETRY_PROBE_ORIGINS must be a positive integer")
        })
        .unwrap_or(DEFAULT_ORIGINS);

    assert!(
        origins > FAILURE_THRESHOLD as usize,
        "the fault-injection burst must exceed the circuit-breaker threshold"
    );
    origins
}

fn retry_config() -> RetryConfig {
    RetryConfig {
        // Keep the run inexpensive while retaining the router's default retry
        // budget and jitter. The metric is an attempt count, not a delay metric.
        initial_backoff_ms: 1,
        max_backoff_ms: 1,
        backoff_multiplier: 1.0,
        ..RetryConfig::default()
    }
}

async fn injected_downstream_attempt(scenario: Scenario) -> Response {
    if !scenario.worker.is_available() {
        // This mirrors route selection when all workers have opened circuits:
        // the retry loop sees a retryable local response, but no request is
        // sent to the shared downstream.
        return (StatusCode::SERVICE_UNAVAILABLE, "shared worker circuit is open").into_response();
    }

    scenario.downstream_attempts.fetch_add(1, Ordering::SeqCst);
    let wave_index = scenario.first_wave.fetch_add(1, Ordering::SeqCst);

    if wave_index < scenario.origins {
        // Hold every first attempt until the entire concurrent burst has
        // reached the injected downstream. This prevents an artificial
        // scheduler ordering from hiding the in-flight failure wave.
        scenario.first_wave_barrier.wait().await;
    }

    // The injected downstream has returned a retryable 503. Router routes
    // record this per-attempt outcome on the selected worker in the same way.
    scenario.worker.record_outcome(false);

    if wave_index < scenario.origins {
        // Do not release any retry loop until every first-wave outcome has
        // updated the shared breaker, so later attempts observe its open state.
        scenario.first_wave_barrier.wait().await;
    }

    (StatusCode::SERVICE_UNAVAILABLE, "injected downstream brownout").into_response()
}

async fn run_origin(scenario: Scenario) -> Response {
    let config = retry_config();
    RetryExecutor::execute_response_with_retry(
        &config,
        move |_attempt| {
            let scenario = scenario.clone();
            async move { injected_downstream_attempt(scenario).await }
        },
        |response, _attempt| response.status() == StatusCode::SERVICE_UNAVAILABLE,
        |_delay, _attempt| {},
        || {},
    )
    .await
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let origins = origin_count();
    let worker = Arc::new(
        BasicWorkerBuilder::new("http://injected-shared-downstream")
            .circuit_breaker_config(CircuitBreakerConfig {
                failure_threshold: FAILURE_THRESHOLD,
                success_threshold: 3,
                timeout_duration: Duration::from_secs(60),
                window_duration: Duration::from_secs(120),
            })
            .build(),
    );
    let scenario = Scenario {
        worker: Arc::clone(&worker),
        downstream_attempts: Arc::new(AtomicU64::new(0)),
        first_wave: Arc::new(AtomicUsize::new(0)),
        first_wave_barrier: Arc::new(Barrier::new(origins)),
        origins,
    };

    let responses = join_all((0..origins).map(|_| {
        let scenario = scenario.clone();
        tokio::spawn(async move { run_origin(scenario).await })
    }))
    .await
    .into_iter()
    .map(|result| result.expect("origin task must not panic"))
    .collect::<Vec<_>>();

    assert!(
        responses
            .iter()
            .all(|response| response.status() == StatusCode::SERVICE_UNAVAILABLE),
        "every origin must observe the injected failure"
    );
    assert_eq!(
        worker.circuit_breaker().state(),
        CircuitState::Open,
        "the shared worker breaker must open during the failure burst"
    );

    let attempts = scenario.downstream_attempts.load(Ordering::SeqCst);
    let max_attempts = origins as u64 * retry_config().max_retries as u64;
    assert!(
        attempts >= origins as u64 && attempts <= max_attempts,
        "every first-wave request must reach the downstream once, and retries cannot exceed the retry budget"
    );

    println!(
        r#"{{"metric":"downstream_attempts_per_origin","value":{:.6}}}"#,
        attempts as f64 / origins as f64
    );
}
