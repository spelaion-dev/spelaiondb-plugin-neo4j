# spelaiondb-plugin-neo4j

Neo4j for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop database
client. The engine's driver, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This is the one for Neo4j, a database whose language is not SQL (its ADR-056).
It is built here, in its own repository, and SpelaionDB pins by hash the exact release it was
tested with (its ADR-042, ADR-055).

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.1 (requests). Experimental until SpelaionDB 1.0.
- **Engine:** `neo4j`, over Neo4j's HTTP Query API (Neo4j 5.19 and later), with `hyper` and
  rustls, user and password in HTTP Basic. SpelaionDB's core splits Cypher and sends each
  statement as a request, `{"database": …, "statement": …}`; each statement is its own
  transaction. Numbers, text and booleans are typed columns; dates, times, durations and points
  are text with Neo4j's type as the native type; nodes, relationships, paths, lists and maps are
  JSON cells. The explorer shows the databases, and in each its node labels and relationship
  types.
- **What it does not do yet:** cancel a statement (the Query API has no way to), transactions
  across statements, read-only connections and agents, bulk imports, the Bolt protocol.
- **License:** Apache-2.0.

## Working on it

```bash
ci/services.sh
```

starts the test servers (Neo4j 5.26's community image in Docker, and the same behind a TLS
proxy, with their keys). Then

```bash
SPELAION_TEST_NEO4J=127.0.0.1:17474 SPELAION_TEST_NEO4J_TLS=127.0.0.1:17473 cargo test
```

runs the value zoo, graphs, errors, the explorer, the password, TLS and the protocol's
conformance set against this plugin's own program. Without the variables the tests that need a
server are skipped, which does not count as tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_NEO4J=/path/to/target/debug/spelaiondb-plugin-neo4j`, in SpelaionDB's
repository; a release build ignores that variable).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
