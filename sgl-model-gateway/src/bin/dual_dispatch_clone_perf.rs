#![allow(dead_code)]

#[path = "../routers/grpc/proto_wrapper.rs"]
mod proto_wrapper;

use std::time::Instant;

use proto_wrapper::ProtoGenerateRequest;
use smg_grpc_client::sglang_proto as sglang;

fn create_large_sglang_request() -> sglang::GenerateRequest {
    sglang::GenerateRequest {
        request_id: "test-req-123".to_string(),
        tokenized: Some(sglang::TokenizedInput {
            original_text: "Lorem ipsum dolor sit amet ".repeat(1000), // very large text to make cloning expensive!
            input_ids: vec![123; 10000],                               // large input_ids vector!
        }),
        sampling_params: Some(sglang::SamplingParams {
            temperature: 0.7,
            max_new_tokens: Some(100),
            stop: vec!["</s>".to_string(); 50], // lots of stop tokens
            stop_token_ids: vec![1, 2, 3, 4, 5],
            ..Default::default()
        }),
        return_logprob: true,
        logprob_start_len: 0,
        top_logprobs_num: 5,
        token_ids_logprob: vec![1, 2, 3],
        return_hidden_states: false,
        stream: true,
        log_metrics: true,
        ..Default::default()
    }
}

fn main() {
    let sglang_req = create_large_sglang_request();
    let proto_request = ProtoGenerateRequest::Sglang(std::sync::Arc::new(sglang_req));

    // Warmup
    for _ in 0..1000 {
        let cloned = std::hint::black_box(&proto_request).clone_inner();
        std::hint::black_box(cloned);
    }

    let iterations = 20_000;
    let start = Instant::now();
    for _ in 0..iterations {
        let cloned = std::hint::black_box(&proto_request).clone_inner();
        std::hint::black_box(cloned);
    }
    let duration = start.elapsed();
    let ns_per_op = (duration.as_nanos() as f64) / (iterations as f64);

    println!(r#"{{"metric":"ns/op","value":{}}}"#, ns_per_op);
}
