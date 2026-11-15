//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042), with requests (ADR-056): it starts, speaks the protocol's version, opens
//! sessions, returns results in the order the contract says, fails without hanging and ends
//! when its input closes. What the engine's values look like is `tests/neo4j.rs`. It needs
//! `SPELAION_TEST_NEO4J` (`ci/services.sh`); without it the test is skipped, saying so.

use serde_json::json;
use spelaion_driver_api::{ConnectParams, Secret};
use spelaion_plugin_host::conformance::{RequestTarget, assert_conforms_requests};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-neo4j");

#[tokio::test]
async fn the_plugin_conforms() {
    let Ok(addr) = std::env::var("SPELAION_TEST_NEO4J") else {
        eprintln!("SPELAION_TEST_NEO4J not set: test skipped");
        return;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let target = RequestTarget {
        program: PROGRAM.into(),
        engine: "neo4j".into(),
        params: ConnectParams {
            host: host.into(),
            port: port.parse().expect("port"),
            user: "neo4j".into(),
            password: Some(Secret::new("spelaion-test")),
            tls: spelaion_driver_api::TlsMode::Disable,
            ..Default::default()
        },
        one_row: json!({"database": "neo4j", "statement": "RETURN 1 AS n"}),
        refused: json!({"database": "neo4j", "statement": "RETRUN 1"}),
        // Statements cannot be cancelled through the Query API.
        slow: None,
    };
    assert_conforms_requests(&target).await;
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself.
#[tokio::test]
async fn the_manifest_is_what_the_program_says() {
    let manifest = include_str!("../plugin.toml");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} = ")))
            .unwrap_or_else(|| panic!("plugin.toml has no `{key}`"))
            .trim()
            .to_string()
    };
    let plugin = spelaion_plugin_host::Plugin::new(PROGRAM);
    let info = plugin.describe().await.expect("the program starts");

    assert_eq!(value("protocol"), format!("\"{}\"", info.protocol));
    let engines: Vec<String> = info.engines.iter().map(|e| format!("\"{e}\"")).collect();
    assert_eq!(value("engines"), format!("[{}]", engines.join(", ")));
    assert_eq!(info.plugin.name, env!("CARGO_PKG_NAME"));
    assert_eq!(info.plugin.version, env!("CARGO_PKG_VERSION"));
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
