//! Neo4j's Query API (`POST /db/{database}/query/v2`) and its typed JSON, as SpelaionDB's
//! results (ADR-056, point 3).
//!
//! Every value says its Cypher type (`{"$type": "Integer", "_value": "1"}`). A column whose
//! non-null values are all of one simple type is that type: `Integer` an integer (it travels
//! as a string, so nothing is rounded), `Float` a float, `String` text, `Boolean` a boolean.
//! Temporal values, durations and points are text as Neo4j writes them, with their Cypher type
//! as the native type. A node, a relationship, a path, a list or a map is a JSON cell with
//! native type `node`, `relationship`, `path`, `list` or `map`; a column whose values mix
//! types is JSON too (`mixed`), so a column never changes type halfway.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use serde_json::{Map, Value, json};
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// A statement's answer: its columns' names and rows of typed values.
pub struct Answer {
    pub fields: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

/// The body of a query: the statement, with its parameters as text when the core binds some.
pub fn body(statement: &str, parameters: &[(String, String)]) -> String {
    let mut params = Map::new();
    for (name, value) in parameters {
        params.insert(name.clone(), Value::String(value.clone()));
    }
    json!({ "statement": statement, "parameters": params }).to_string()
}

/// Reads an answer, or the server's error (`{"errors": [{"code", "message"}]}`).
pub fn answer(body: &[u8]) -> Result<Answer> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| DriverError::Other(format!("the server's answer is not JSON: {e}")))?;
    if let Some(error) = value["errors"].as_array().and_then(|e| e.first()) {
        return Err(DriverError::Database {
            code: error["code"].as_str().map(str::to_string),
            message: error["message"]
                .as_str()
                .unwrap_or("the statement failed")
                .to_string(),
            detail: None,
            hint: None,
            position: None,
        });
    }
    let data = &value["data"];
    Ok(Answer {
        fields: data["fields"]
            .as_array()
            .map(|f| {
                f.iter()
                    .filter_map(|n| n.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        rows: data["values"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .map(|r| r.as_array().cloned().unwrap_or_default())
                    .collect()
            })
            .unwrap_or_default(),
    })
}

fn cypher_type(v: &Value) -> &str {
    v["$type"].as_str().unwrap_or("Null")
}

/// How a column is read, from its values' Cypher types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reader {
    Int,
    Float,
    Text,
    Bool,
    Json,
}

/// A column's reader and native type.
fn column(rows: &[Vec<Value>], i: usize) -> (Reader, String) {
    let mut seen: Option<String> = None;
    for row in rows {
        let t = row.get(i).map_or("Null", cypher_type);
        if t == "Null" {
            continue;
        }
        match &seen {
            None => seen = Some(t.to_string()),
            Some(s) if s == t => {}
            Some(_) => return (Reader::Json, "mixed".into()),
        }
    }
    match seen.as_deref() {
        None => (Reader::Text, "Null".into()),
        Some("Integer") => (Reader::Int, "Integer".into()),
        Some("Float") => (Reader::Float, "Float".into()),
        Some("String") => (Reader::Text, "String".into()),
        Some("Boolean") => (Reader::Bool, "Boolean".into()),
        Some(
            t @ ("Date" | "Time" | "LocalTime" | "LocalDateTime" | "OffsetDateTime"
            | "ZonedDateTime" | "Duration" | "Point" | "Base64"),
        ) => (Reader::Text, t.to_string()),
        Some(t) => (Reader::Json, t.to_lowercase()),
    }
}

/// A typed value as plain JSON: numbers as numbers (an integer past 2^53 as a string), a
/// node as `{"elementId", "labels", "properties"}`, a relationship with its `type`, `start`
/// and `end`, a path as its list of nodes and relationships.
pub fn plain(v: &Value) -> Value {
    let inner = &v["_value"];
    match cypher_type(v) {
        "Null" => Value::Null,
        "Integer" => inner
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|n| n.unsigned_abs() <= 1 << 53)
            .map_or_else(|| inner.clone(), |n| json!(n)),
        "Float" => inner
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|f| f.is_finite())
            .map_or_else(|| inner.clone(), |f| json!(f)),
        "List" => Value::Array(
            inner
                .as_array()
                .map(|a| a.iter().map(plain).collect())
                .unwrap_or_default(),
        ),
        "Map" => Value::Object(
            inner
                .as_object()
                .map(|m| m.iter().map(|(k, v)| (k.clone(), plain(v))).collect())
                .unwrap_or_default(),
        ),
        "Node" => json!({
            "elementId": inner["_element_id"],
            "labels": inner["_labels"],
            "properties": properties(&inner["_properties"]),
        }),
        "Relationship" => json!({
            "elementId": inner["_element_id"],
            "type": inner["_type"],
            "start": inner["_start_node_element_id"],
            "end": inner["_end_node_element_id"],
            "properties": properties(&inner["_properties"]),
        }),
        "Path" => Value::Array(
            inner
                .as_array()
                .map(|a| a.iter().map(plain).collect())
                .unwrap_or_default(),
        ),
        _ => inner.clone(),
    }
}

fn properties(v: &Value) -> Value {
    Value::Object(
        v.as_object()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), plain(v))).collect())
            .unwrap_or_default(),
    )
}

/// The answer's schema and its rows as batches of `batch_size`.
pub fn batches(
    answer: &Answer,
    batch_size: usize,
) -> Result<(Arc<ResultSchema>, Vec<RecordBatch>)> {
    let columns: Vec<(Reader, String)> = (0..answer.fields.len())
        .map(|i| column(&answer.rows, i))
        .collect();
    let schema = Arc::new(ResultSchema::new(
        answer
            .fields
            .iter()
            .zip(&columns)
            .map(|(name, (reader, native))| ColumnMeta {
                name: name.clone(),
                logical: match reader {
                    Reader::Int => LogicalType::Int,
                    Reader::Float => LogicalType::Float,
                    Reader::Text => LogicalType::Text,
                    Reader::Bool => LogicalType::Bool,
                    Reader::Json => LogicalType::Json,
                },
                native_type: native.clone(),
                native_id: None,
                origin: None,
            })
            .collect(),
    ));
    let mut out = Vec::new();
    for chunk in answer.rows.chunks(batch_size.max(1)) {
        let arrays: Vec<ArrayRef> = columns
            .iter()
            .enumerate()
            .map(|(i, (reader, _))| array(*reader, chunk, i))
            .collect::<Result<_>>()?;
        out.push(
            RecordBatch::try_new(schema.arrow.clone(), arrays)
                .map_err(|e| DriverError::Other(format!("building a batch: {e}")))?,
        );
    }
    Ok((schema, out))
}

fn array(reader: Reader, rows: &[Vec<Value>], i: usize) -> Result<ArrayRef> {
    let cell = |row: &Vec<Value>| row.get(i).cloned().unwrap_or(Value::Null);
    let null = |v: &Value| cypher_type(v) == "Null";
    Ok(match reader {
        Reader::Int => {
            let mut b = Int64Builder::new();
            for row in rows {
                let v = cell(row);
                if null(&v) {
                    b.append_null();
                } else {
                    let text = v["_value"].as_str().unwrap_or_default();
                    b.append_value(text.parse().map_err(|_| DriverError::Decode {
                        type_name: "Integer".into(),
                        message: format!("{text} does not fit in a 64-bit integer"),
                    })?);
                }
            }
            Arc::new(b.finish())
        }
        Reader::Float => {
            let mut b = Float64Builder::new();
            for row in rows {
                let v = cell(row);
                let x = match &v["_value"] {
                    Value::String(s) => s.parse::<f64>().ok(),
                    Value::Number(n) => n.as_f64(),
                    _ => None,
                };
                b.append_option(if null(&v) { None } else { x });
            }
            Arc::new(b.finish())
        }
        Reader::Bool => {
            let mut b = BooleanBuilder::new();
            for row in rows {
                let v = cell(row);
                b.append_option(if null(&v) {
                    None
                } else {
                    v["_value"].as_bool()
                });
            }
            Arc::new(b.finish())
        }
        Reader::Text => {
            let mut b = StringBuilder::new();
            for row in rows {
                let v = cell(row);
                if null(&v) {
                    b.append_null();
                } else {
                    match &v["_value"] {
                        Value::String(s) => b.append_value(s),
                        other => b.append_value(other.to_string()),
                    }
                }
            }
            Arc::new(b.finish())
        }
        Reader::Json => {
            let mut b = StringBuilder::new();
            for row in rows {
                let v = cell(row);
                if null(&v) {
                    b.append_null();
                } else {
                    b.append_value(plain(&v).to_string());
                }
            }
            Arc::new(b.finish())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed(t: &str, v: Value) -> Value {
        json!({"$type": t, "_value": v})
    }

    #[test]
    fn a_column_of_one_type_is_that_type_and_a_mix_is_json() {
        let rows = vec![
            vec![
                typed("Integer", json!("1")),
                typed("String", json!("a")),
                typed("Integer", json!("1")),
            ],
            vec![
                typed("Null", Value::Null),
                typed("String", json!("b")),
                typed("Float", json!("1.5")),
            ],
        ];
        assert_eq!(column(&rows, 0), (Reader::Int, "Integer".into()));
        assert_eq!(column(&rows, 1), (Reader::Text, "String".into()));
        assert_eq!(column(&rows, 2), (Reader::Json, "mixed".into()));
    }

    #[test]
    fn a_node_is_its_id_labels_and_plain_properties() {
        let node = typed(
            "Node",
            json!({"_element_id": "4:x:0", "_labels": ["Person"], "_properties": {"name": {"$type": "String", "_value": "Ana"}, "age": {"$type": "Integer", "_value": "30"}}}),
        );
        assert_eq!(
            plain(&node),
            json!({"elementId": "4:x:0", "labels": ["Person"], "properties": {"name": "Ana", "age": 30}})
        );
        // Past 2^53 an integer stays a string, so it is not rounded.
        assert_eq!(
            plain(&typed("Integer", json!("9223372036854775807"))),
            json!("9223372036854775807")
        );
    }

    #[test]
    fn errors_are_the_servers() {
        let error = answer(br#"{"errors":[{"code":"Neo.ClientError.Statement.SyntaxError","message":"Invalid input 'RETRUN'"}]}"#)
            .err()
            .unwrap();
        match error {
            DriverError::Database { code, message, .. } => {
                assert_eq!(
                    code.as_deref(),
                    Some("Neo.ClientError.Statement.SyntaxError")
                );
                assert!(message.contains("RETRUN"));
            }
            other => panic!("{other:?}"),
        }
    }
}
