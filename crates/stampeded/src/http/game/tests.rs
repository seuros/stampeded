use super::*;

#[test]
fn the_game_page_loads_no_external_assets() {
    let html = INDEX;

    assert_eq!(
        crate::http::test_support::network_fetches(html),
        Vec::<String>::new(),
        "the game page must serve its own assets"
    );

    assert!(
        html.contains("/game/three.module.min.js"),
        "the game page does not load Three.js"
    );

    assert!(
        html.contains("/game/loaders/gltf-loader.js"),
        "the game page does not load GLTFLoader"
    );

    assert!(
        html.contains("/game/stream"),
        "the game page does not connect to the game stream"
    );
}

#[test]
fn game_world_serializes_to_json() {
    let world = GameWorld {
        queues: vec![QueueConveyor {
            name: "default".to_string(),
            available: 10,
            executing: 5,
            completed: 100,
            retryable: 2,
            discarded: 1,
            cancelled: 0,
            position: [0.0, 0.0, 0.0],
            throughput_per_sec: 0.0,
            oldest_age_secs: Some(30),
        }],
        workers: vec![WorkerEntity {
            id: "worker-1".to_string(),
            name: "host-a:4242".to_string(),
            queue: "default".to_string(),
            alive: true,
            last_heartbeat_secs_ago: 5,
            position: [1.0, 1.0, 1.0],
        }],
        destinations: vec![DestinationEntity {
            name: "api.example.com".to_string(),
            waiting: 3,
            failed_recently: 0,
            breaker_state: "closed".to_string(),
            position: [5.0, 1.0, -5.0],
        }],
        timestamp_ms: 1_234_567_890,
    };

    let json = serde_json::to_string(&world).expect("failed to serialize");
    assert!(json.contains("default"));
    assert!(json.contains("worker-1"));
    assert!(
        json.contains("host-a:4242"),
        "the HUD names workers by host:pid"
    );
    assert!(json.contains("api.example.com"));
    assert!(json.contains("1234567890"));
}
