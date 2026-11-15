//! Tests against a real Neo4j (the community image, 5.26).
//!
//! They need `SPELAION_TEST_NEO4J=host:port` (the HTTP Query API) and, for TLS,
//! `SPELAION_TEST_NEO4J_TLS=host:port` (HTTPS with a certificate for `neo4j.spelaion.test`
//! signed by `testing/keys/ca.crt`). User `neo4j`, password `spelaion-test`. Locally:
//! `ci/services.sh` and
//! `SPELAION_TEST_NEO4J=127.0.0.1:17474 SPELAION_TEST_NEO4J_TLS=127.0.0.1:17473 cargo test`.
//! Without the variables they are skipped with a notice, which does not count as tested.

use std::path::PathBuf;

use futures_util::TryStreamExt;
use serde_json::json;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType,
    ObjectKind, Secret, TlsMode, Value,
};
use spelaion_driver_neo4j::Neo4jDriver;

fn addr(variable: &str) -> Option<(String, u16)> {
    let Ok(addr) = std::env::var(variable) else {
        eprintln!("{variable} not set: test skipped");
        return None;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Some((host.to_string(), port.parse().expect("port")))
}

fn params(host: String, port: u16) -> ConnectParams {
    ConnectParams {
        host,
        port,
        user: "neo4j".into(),
        password: Some(Secret::new("spelaion-test")),
        tls: TlsMode::Disable,
        ..Default::default()
    }
}

async fn connect() -> Option<Box<dyn Connection>> {
    let (host, port) = addr("SPELAION_TEST_NEO4J")?;
    Some(
        Neo4jDriver
            .connect(&params(host, port))
            .await
            .expect("connects"),
    )
}

async fn run(conn: &dyn Connection, statement: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute_request(
        json!({"database": "neo4j", "statement": statement}),
        &ExecOptions::default(),
    )
    .await?
    .try_collect()
    .await
}

/// Header (name, logical type, native type) and the rows as text.
async fn table(
    conn: &dyn Connection,
    statement: &str,
) -> (Vec<(String, LogicalType, String)>, Vec<Vec<String>>) {
    let events = run(conn, statement)
        .await
        .unwrap_or_else(|e| panic!("{statement}: {e}"));
    let mut header = Vec::new();
    let mut rows = Vec::new();
    for event in events {
        match event {
            ExecEvent::Columns(schema) => {
                header = schema
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.logical, c.native_type.clone()))
                    .collect();
            }
            ExecEvent::Batch(batch) => {
                for r in 0..batch.num_rows() {
                    rows.push(
                        header
                            .iter()
                            .enumerate()
                            .map(|(i, (_, logical, _))| {
                                Value::from_array(batch.column(i).as_ref(), r, *logical)
                                    .unwrap()
                                    .to_string()
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    (header, rows)
}

/// Every kind of Cypher value, its logical type, native type and cell.
#[tokio::test]
async fn value_zoo() {
    let Some(conn) = connect().await else { return };
    let (header, rows) = table(
        conn.as_ref(),
        "RETURN 9223372036854775807 AS big, 1.5 AS f, 'texto ñ' AS s, true AS b, null AS z, \
         [1, 'a'] AS l, {a: 1} AS m, date('2026-11-15') AS d, \
         datetime('2026-11-15T10:20:30Z') AS dt, localdatetime('2026-11-15T10:20:30') AS ldt, \
         duration('P1DT2H') AS dur, point({x: 1, y: 2}) AS p",
    )
    .await;
    let cells: Vec<(&str, LogicalType, &str, &str)> = header
        .iter()
        .zip(&rows[0])
        .map(|((n, l, native), v)| (n.as_str(), *l, native.as_str(), v.as_str()))
        .collect();
    assert_eq!(
        cells,
        [
            ("big", LogicalType::Int, "Integer", "9223372036854775807"),
            ("f", LogicalType::Float, "Float", "1.5"),
            ("s", LogicalType::Text, "String", "texto ñ"),
            ("b", LogicalType::Bool, "Boolean", "true"),
            ("z", LogicalType::Text, "Null", "NULL"),
            ("l", LogicalType::Json, "list", "[1,\"a\"]"),
            ("m", LogicalType::Json, "map", "{\"a\":1}"),
            ("d", LogicalType::Text, "Date", "2026-11-15"),
            (
                "dt",
                LogicalType::Text,
                "OffsetDateTime",
                "2026-11-15T10:20:30Z"
            ),
            (
                "ldt",
                LogicalType::Text,
                "LocalDateTime",
                "2026-11-15T10:20:30"
            ),
            ("dur", LogicalType::Text, "Duration", "P1DT2H"),
            ("p", LogicalType::Text, "Point", "SRID=7203;POINT (1.0 2.0)"),
        ]
    );
}

/// Nodes, relationships and paths are JSON cells (ADR-056, point 3).
#[tokio::test]
async fn graphs_are_json_cells() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    let tag = format!("t{}", std::process::id());
    table(
        conn,
        &format!(
            "CREATE (:SpelaionPerson {{name: 'Ana', tag: '{tag}'}})-[:SPELAION_KNOWS {{since: 2020}}]->(:SpelaionPerson {{name: 'Luis', tag: '{tag}'}})"
        ),
    )
    .await;
    let (header, rows) = table(
        conn,
        &format!(
            "MATCH p = (a:SpelaionPerson {{tag: '{tag}'}})-[r:SPELAION_KNOWS]->(b) RETURN a, r, p"
        ),
    )
    .await;
    let natives: Vec<&str> = header.iter().map(|(_, _, n)| n.as_str()).collect();
    assert_eq!(natives, ["node", "relationship", "path"]);
    assert!(header.iter().all(|(_, l, _)| *l == LogicalType::Json));
    let node: serde_json::Value = serde_json::from_str(&rows[0][0]).unwrap();
    assert_eq!(node["labels"], json!(["SpelaionPerson"]));
    assert_eq!(node["properties"]["name"], json!("Ana"));
    let rel: serde_json::Value = serde_json::from_str(&rows[0][1]).unwrap();
    assert_eq!(rel["type"], json!("SPELAION_KNOWS"));
    assert_eq!(rel["properties"]["since"], json!(2020));
    let path: serde_json::Value = serde_json::from_str(&rows[0][2]).unwrap();
    assert_eq!(path.as_array().unwrap().len(), 3);
    table(
        conn,
        &format!("MATCH (n:SpelaionPerson {{tag: '{tag}'}}) DETACH DELETE n"),
    )
    .await;
}

#[tokio::test]
async fn errors_are_the_servers_and_the_session_survives() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    match run(conn, "RETRUN 1").await {
        Err(DriverError::Database { code, message, .. }) => {
            assert_eq!(
                code.as_deref(),
                Some("Neo.ClientError.Statement.SyntaxError")
            );
            assert!(message.contains("RETRUN"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    let (_, rows) = table(conn, "RETURN 'still here' AS s").await;
    assert_eq!(rows[0][0], "still here");
}

#[tokio::test]
async fn batches_respect_the_batch_size() {
    let Some(conn) = connect().await else { return };
    let events: Vec<ExecEvent> = conn
        .execute_request(
            json!({"database": "neo4j", "statement": "UNWIND range(1, 2500) AS i RETURN i"}),
            &ExecOptions {
                batch_size: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let sizes: Vec<usize> = events
        .iter()
        .filter_map(|e| match e {
            ExecEvent::Batch(b) => Some(b.num_rows()),
            _ => None,
        })
        .collect();
    assert_eq!(sizes, [1000, 1000, 500]);
}

#[tokio::test]
async fn the_explorer_lists_databases_labels_and_relationship_types() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    let tag = format!("e{}", std::process::id());
    table(
        conn,
        &format!("CREATE (:SpelaionLabel {{tag: '{tag}'}})-[:SPELAION_REL]->(:SpelaionLabel {{tag: '{tag}'}})"),
    )
    .await;
    let schemas = conn.list_schemas().await.unwrap();
    assert!(
        schemas.iter().any(|s| s.name == "neo4j" && !s.system),
        "{schemas:?}"
    );
    assert!(schemas.iter().any(|s| s.name == "system" && s.system));
    let objects = conn.list_objects("neo4j").await.unwrap();
    assert!(
        objects
            .iter()
            .any(|o| o.name == "SpelaionLabel" && o.kind == ObjectKind::NodeLabel)
    );
    assert!(
        objects
            .iter()
            .any(|o| o.name == "SPELAION_REL" && o.kind == ObjectKind::RelationshipType)
    );
    assert_eq!(
        conn.current_database().await.unwrap().as_deref(),
        Some("neo4j")
    );
    assert!(
        conn.server_version()
            .await
            .unwrap()
            .starts_with("Neo4j Kernel 5.")
    );
    assert!(conn.select_database("../other").await.is_err());
    table(
        conn,
        &format!("MATCH (n:SpelaionLabel {{tag: '{tag}'}}) DETACH DELETE n"),
    )
    .await;
}

#[tokio::test]
async fn a_wrong_password_is_refused_and_never_shown() {
    let Some((host, port)) = addr("SPELAION_TEST_NEO4J") else {
        return;
    };
    let mut wrong = params(host, port);
    wrong.password = Some(Secret::new("not-the-Password-42"));
    let error = Neo4jDriver.connect(&wrong).await.err().expect("refused");
    let text = error.to_string();
    assert!(text.contains("401"), "{text}");
    assert!(!text.contains("not-the-Password-42"), "{text}");
}

fn keys() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testing/keys")
}

/// TLS to 127.0.0.1, the address a tunnel gives, with the certificate checked against the
/// connection's host name: that name works, any other is refused.
#[tokio::test]
async fn tls_is_verified_against_the_host_name_not_the_address() {
    let Some((host, port)) = addr("SPELAION_TEST_NEO4J_TLS") else {
        return;
    };
    let tls = |name: &str| ConnectParams {
        host: name.into(),
        port,
        connect_addr: Some(format!("{host}:{port}").parse().unwrap()),
        tls: TlsMode::VerifyFull,
        ca_certificate: Some(keys().join("ca.crt")),
        ..params(name.into(), port)
    };
    let conn = Neo4jDriver
        .connect(&tls("neo4j.spelaion.test"))
        .await
        .expect("verify-full against the name");
    let (_, rows) = table(conn.as_ref(), "RETURN 'over tls' AS s").await;
    assert_eq!(rows[0][0], "over tls");
    let refused = Neo4jDriver
        .connect(&tls("another.spelaion.test"))
        .await
        .err()
        .expect("another name is refused");
    assert!(refused.to_string().contains("TLS"), "{refused}");
}
