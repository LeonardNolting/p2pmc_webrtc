use rcgen::generate_simple_self_signed;

fn register() {
    generate_simple_self_signed(vec![
        "peer_id".to_string(),
        "localhost".to_string(),
    ])
    .unwrap();
}
