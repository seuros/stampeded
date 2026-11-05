use super::*;

#[test]
fn the_game_page_loads_no_external_assets() {
    let html = PAGE.as_str();

    assert_eq!(
        crate::http::test_support::network_fetches(html),
        Vec::<String>::new(),
        "the game page must serve its own assets"
    );

    assert!(
        html.contains(r#"<script type="importmap">"#),
        "the game page has no import map, so `three` cannot resolve"
    );

    assert!(
        html.contains(r#"data-three:world="/game/game.js""#),
        "the canvas does not name its scene module"
    );

    assert!(
        html.contains(r#"import "rama-x-threejs""#),
        "the game page never loads the hook that mounts the scene"
    );

    assert!(
        html.contains("/game/stream"),
        "the game page does not connect to the game stream"
    );

    assert!(!html.contains("__"), "a placeholder was left unfilled");
}

/// The hook imports Datastar through the import map; a second copy loaded by
/// its own <script> would run two Datastars on one page.
#[test]
fn the_page_loads_datastar_only_through_the_hook() {
    let html = PAGE.as_str();
    assert!(
        !html.contains("<script type=\"module\" src="),
        "the page loads a script of its own, e.g. a second Datastar"
    );
    let importmap = html
        .find(r#"<script type="importmap">"#)
        .expect("import map");
    let hook = html.find(r#"import "rama-x-threejs""#).expect("hook");
    assert!(
        importmap < hook,
        "the import map must precede the module that uses it"
    );
}

#[tokio::test]
async fn every_embedded_model_is_served_and_others_are_not() {
    for (name, bytes) in MODELS {
        let res = model(name);
        assert_eq!(res.status(), StatusCode::OK, "{name}");
        assert_eq!(res.headers()[header::CONTENT_TYPE], "model/gltf-binary");
        assert!(bytes.starts_with(b"glTF"), "{name} is not a GLB");
    }
    assert_eq!(model("nope.glb").status(), StatusCode::NOT_FOUND);
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
