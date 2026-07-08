use std::{
    collections::HashMap,
    sync::{Arc, Barrier},
    thread,
    time::Instant,
};

use smg::core::{
    BasicWorkerBuilder, CircuitBreakerConfig, ConnectionMode, WorkerRegistry, WorkerType,
};

fn main() {
    // 1. Setup registry with 200 workers
    let registry = Arc::new(WorkerRegistry::new());
    let num_workers = 200;

    for i in 0..num_workers {
        let mut labels = HashMap::new();
        labels.insert("model_id".to_string(), "benchmark-model".to_string());

        // Alternate regular and decode to have some variety
        let worker_type = if i % 2 == 0 {
            WorkerType::Regular
        } else {
            WorkerType::Decode
        };

        let worker = BasicWorkerBuilder::new(format!("http://worker-{}:8000", i))
            .worker_type(worker_type)
            .labels(labels)
            .connection_mode(ConnectionMode::Grpc { port: None })
            .circuit_breaker_config(CircuitBreakerConfig::default())
            .build();

        registry.register(Arc::from(worker));
    }

    // Warm up
    for j in 0..100 {
        let model_id = if j % 2 == 0 {
            Some("benchmark-model")
        } else {
            Some("other-model")
        };
        let worker_type = if j % 3 == 0 {
            Some(WorkerType::Regular)
        } else {
            Some(WorkerType::Decode)
        };
        let _ = registry.get_workers_filtered(
            model_id,
            worker_type,
            Some(ConnectionMode::Grpc { port: None }),
            None,
            false,
        );
    }

    // 2. Spawn concurrent threads
    let num_threads = 8;
    let iterations_per_thread = 5000;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::new();

    let start = Instant::now();

    for _ in 0..num_threads {
        let registry = Arc::clone(&registry);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            // Synchronize threads to start at the exact same time
            barrier.wait();

            for j in 0..iterations_per_thread {
                let model_id = if j % 2 == 0 {
                    Some("benchmark-model")
                } else {
                    Some("other-model")
                };
                let worker_type = if j % 3 == 0 {
                    Some(WorkerType::Regular)
                } else if j % 3 == 1 {
                    Some(WorkerType::Decode)
                } else {
                    None
                };
                let healthy_only = j % 5 == 0;

                let res = registry.get_workers_filtered(
                    model_id,
                    worker_type,
                    Some(ConnectionMode::Grpc { port: None }),
                    None,
                    healthy_only,
                );
                std::hint::black_box(res);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_iterations = num_threads * iterations_per_thread;

    // Average nanoseconds per operation across all threads/operations
    let ns_per_op = elapsed.as_nanos() as f64 / total_iterations as f64;
    println!("{{\"metric\": \"ns/op\", \"value\": {:.2}}}", ns_per_op);
}
