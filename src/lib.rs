//! Neo4j for SpelaionDB (ADR-056 there): Cypher, read by SpelaionDB's core and sent here as a
//! request (`{"database", "statement"}`), run through Neo4j's HTTP Query API.
//!
//! - `http.rs` opens the plugin's own connections to `connect_addr` when it is given (the local
//!   end of SpelaionDB's tunnel or proxy), never through a system proxy, with TLS verified
//!   against `host` (`tls.rs`, libpq's modes). The user and password travel in HTTP Basic.
//! - `query.rs` turns Neo4j's typed JSON into rows: simple types as themselves, temporal values
//!   as text, nodes, relationships and paths as JSON cells.
//! - The explorer: a schema is a database (`SHOW DATABASES`), its objects are node labels and
//!   relationship types (ADR-056's `node_label` and `relationship_type`).
//!
//! Each statement is a transaction of its own (the Query API's implicit transactions): no
//! transaction spans statements, and none can be cancelled. Requests go one at a time through
//! an actor (a tokio task).

mod http;
mod query;
mod tls;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ConnectParams, Connection, Driver, DriverError, EventStream,
    ExecEvent, ExecOptions, ObjectInfo, ObjectKind, Result, SchemaInfo, TableInfo,
};
use tokio::sync::{mpsc, oneshot};

/// The database a connection starts in when it names none.
const DEFAULT_DATABASE: &str = "neo4j";

/// The Neo4j driver. `user` and `password` log in; `database` is the one to start in.
#[derive(Debug, Default, Clone)]
pub struct Neo4jDriver;

#[async_trait]
impl Driver for Neo4jDriver {
    fn engine(&self) -> &str {
        "neo4j"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        if let Some(key) = params.options.keys().next() {
            return Err(DriverError::Connection(format!(
                "{key} is not a setting of a Neo4j connection"
            )));
        }
        let identity = match (&params.client_certificate, &params.client_key) {
            (Some(cert), Some(key)) => Some(tls::ClientIdentity { cert, key }),
            _ => None,
        };
        let tls = tls::configure(params.tls, params.ca_certificate.as_deref(), identity)?;
        let endpoint = Arc::new(http::Endpoint::new(
            params.host.clone(),
            params.port,
            params.connect_addr,
            tls,
            params.connect_timeout(),
            params.user.clone(),
            params.password.clone(),
        ));
        let database = if params.database.trim().is_empty() {
            DEFAULT_DATABASE.to_string()
        } else {
            params.database.trim().to_string()
        };
        let mut client = http::Client::new(endpoint.clone());
        // The first request checks the server, the TLS and the login, within the connect
        // timeout.
        match tokio::time::timeout(
            params.connect_timeout(),
            run(&mut client, &database, "RETURN 1"),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(scrubbed(e, &endpoint)),
            Err(_) => {
                return Err(DriverError::Connection(format!(
                    "the server did not answer within {} s",
                    params.connect_timeout().as_secs()
                )));
            }
        }
        let (commands, inbox) = mpsc::unbounded_channel();
        tokio::spawn(actor(client, endpoint, inbox));
        Ok(Box::new(Neo4jConnection {
            commands,
            database: Mutex::new(database),
            cancel: Arc::new(NoCancel),
            capabilities: Capabilities {
                schemas: true,
                ..Capabilities::default()
            },
        }))
    }
}

/// A database name as it goes into the path: Neo4j's names are letters, digits, dots and
/// dashes; anything else is refused rather than encoded.
fn database_path(database: &str) -> Result<String> {
    if database.is_empty()
        || !database
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
    {
        return Err(DriverError::Other(format!(
            "{database:?} is not a Neo4j database name"
        )));
    }
    Ok(format!("/db/{database}/query/v2"))
}

/// One statement in `database`.
async fn run(client: &mut http::Client, database: &str, statement: &str) -> Result<query::Answer> {
    let body = client
        .post(&database_path(database)?, query::body(statement, &[]))
        .await?;
    query::answer(&body)
}

/// The first column of every row, as text.
async fn names(client: &mut http::Client, database: &str, statement: &str) -> Result<Vec<String>> {
    Ok(run(client, database, statement)
        .await?
        .rows
        .iter()
        .filter_map(|row| {
            row.first()
                .and_then(|v| v["_value"].as_str())
                .map(str::to_string)
        })
        .collect())
}

fn scrubbed(e: DriverError, endpoint: &http::Endpoint) -> DriverError {
    match e {
        DriverError::Connection(m) => DriverError::Connection(endpoint.scrub(&m)),
        DriverError::Other(m) => DriverError::Other(endpoint.scrub(&m)),
        DriverError::Database {
            code,
            message,
            detail,
            hint,
            position,
        } => DriverError::Database {
            code,
            message: endpoint.scrub(&message),
            detail,
            hint,
            position,
        },
        other => other,
    }
}

type Reply<T> = oneshot::Sender<Result<T>>;

enum Command {
    Request {
        database: String,
        statement: String,
        batch_size: usize,
        events: mpsc::Sender<Result<ExecEvent>>,
    },
    Version(Reply<String>),
    Databases(Reply<Vec<String>>),
    Objects(String, Reply<Vec<ObjectInfo>>),
}

async fn actor(
    mut client: http::Client,
    endpoint: Arc<http::Endpoint>,
    mut inbox: mpsc::UnboundedReceiver<Command>,
) {
    while let Some(command) = inbox.recv().await {
        match command {
            Command::Request {
                database,
                statement,
                batch_size,
                events,
            } => {
                let result = async {
                    let answer = run(&mut client, &database, &statement).await?;
                    query::batches(&answer, batch_size)
                }
                .await;
                match result {
                    Ok((schema, batches)) => {
                        if events.send(Ok(ExecEvent::Columns(schema))).await.is_err() {
                            continue;
                        }
                        for batch in batches {
                            if events.send(Ok(ExecEvent::Batch(batch))).await.is_err() {
                                break;
                            }
                        }
                        let _ = events
                            .send(Ok(ExecEvent::Done {
                                rows_affected: None,
                            }))
                            .await;
                    }
                    Err(e) => {
                        let _ = events.send(Err(scrubbed(e, &endpoint))).await;
                    }
                }
            }
            Command::Version(reply) => {
                let result = run(
                    &mut client,
                    "system",
                    "CALL dbms.components() YIELD name, versions, edition RETURN name, versions[0], edition",
                )
                .await
                .map(|answer| {
                    let row = answer.rows.first().cloned().unwrap_or_default();
                    let text = |i: usize| {
                        row.get(i)
                            .and_then(|v| v["_value"].as_str())
                            .unwrap_or_default()
                            .to_string()
                    };
                    format!("{} {} ({})", text(0), text(1), text(2))
                });
                let _ = reply.send(result);
            }
            Command::Databases(reply) => {
                let result = names(
                    &mut client,
                    "system",
                    "SHOW DATABASES YIELD name RETURN DISTINCT name ORDER BY name",
                )
                .await;
                let _ = reply.send(result);
            }
            Command::Objects(database, reply) => {
                let result = async {
                    let mut out: Vec<ObjectInfo> = names(
                        &mut client,
                        &database,
                        "CALL db.labels() YIELD label RETURN label ORDER BY label",
                    )
                    .await?
                    .into_iter()
                    .map(|name| ObjectInfo {
                        schema: database.clone(),
                        name,
                        kind: ObjectKind::NodeLabel,
                        estimated_rows: None,
                        comment: None,
                    })
                    .collect();
                    out.extend(
                        names(
                            &mut client,
                            &database,
                            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType",
                        )
                        .await?
                        .into_iter()
                        .map(|name| ObjectInfo {
                            schema: database.clone(),
                            name,
                            kind: ObjectKind::RelationshipType,
                            estimated_rows: None,
                            comment: None,
                        }),
                    );
                    Ok(out)
                }
                .await;
                let _ = reply.send(result);
            }
        }
    }
}

struct Neo4jConnection {
    commands: mpsc::UnboundedSender<Command>,
    /// The database statements run in when a request names none, and the explorer starts in.
    database: Mutex<String>,
    cancel: Arc<NoCancel>,
    capabilities: Capabilities,
}

fn gone() -> DriverError {
    DriverError::Connection("the Neo4j connection is closed".into())
}

impl Neo4jConnection {
    async fn ask<T>(&self, make: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(make(tx)).map_err(|_| gone())?;
        rx.await.map_err(|_| gone())?
    }
}

#[async_trait]
impl Connection for Neo4jConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, _sql: &str, _opts: &ExecOptions) -> Result<EventStream> {
        Err(DriverError::Other(
            "Neo4j runs requests, not statements: SpelaionDB's core sends each Cypher statement as a request".into(),
        ))
    }

    async fn execute_request(&self, request: Value, opts: &ExecOptions) -> Result<EventStream> {
        let statement = request["statement"]
            .as_str()
            .ok_or_else(|| DriverError::Other("the request has no statement".into()))?
            .to_string();
        let database = match request["database"].as_str() {
            Some(d) if !d.is_empty() => d.to_string(),
            _ => self.database.lock().expect("lock poisoned").clone(),
        };
        database_path(&database)?;
        let (events, rx) = mpsc::channel(4);
        self.commands
            .send(Command::Request {
                database,
                statement,
                batch_size: opts.batch_size.max(1),
                events,
            })
            .map_err(|_| gone())?;
        Ok(Box::pin(futures_util::stream::unfold(
            rx,
            |mut rx| async move { rx.recv().await.map(|event| (event, rx)) },
        )))
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        self.cancel.clone()
    }

    async fn server_version(&self) -> Result<String> {
        self.ask(Command::Version).await
    }

    async fn current_database(&self) -> Result<Option<String>> {
        Ok(Some(self.database.lock().expect("lock poisoned").clone()))
    }

    async fn database_context(&self) -> Result<Option<String>> {
        self.current_database().await
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        database_path(database)?;
        *self.database.lock().expect("lock poisoned") = database.to_string();
        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        self.ask(Command::Databases).await
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(self
            .list_databases()
            .await?
            .into_iter()
            .map(|name| SchemaInfo {
                system: name == "system",
                name,
            })
            .collect())
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        let schema = schema.to_string();
        self.ask(|reply| Command::Objects(schema, reply)).await
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        Err(DriverError::Other(format!(
            "{schema}.{name} is a label or a relationship type: Neo4j has no tables to describe"
        )))
    }
}

/// Neo4j's statements cannot be cancelled here (`Capabilities::cancel` is false).
struct NoCancel;

#[async_trait]
impl CancelHandle for NoCancel {
    async fn cancel(&self) -> Result<()> {
        Err(DriverError::Other(
            "a Neo4j statement cannot be cancelled".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::database_path;

    #[test]
    fn a_database_name_cannot_change_the_path() {
        assert_eq!(database_path("neo4j").unwrap(), "/db/neo4j/query/v2");
        assert_eq!(database_path("my-db.v2").unwrap(), "/db/my-db.v2/query/v2");
        for bad in ["", "a/b", "../x", "a b", "a?x=1", "a%2F"] {
            assert!(database_path(bad).is_err(), "{bad:?}");
        }
    }
}
