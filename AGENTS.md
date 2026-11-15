# SpelaionDB's Neo4j plugin — rules for agents

Neo4j for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): the engine's driver, and a
small program that serves it to SpelaionDB over its plugin protocol. Apache-2.0.

**The rules of SpelaionDB's repository apply here** (its `AGENTS.md`: all the code is written
by an agent, the compiler, the tests and CI are the only reviewer; everything in English;
sign-off; one task, one short branch; commit subjects that read well in a changelog). This file
says only what is different in this repository.

## What this is
- `src/`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005) over Neo4j's HTTP Query API (`POST /db/<database>/query/v2`), as a
  language that is not SQL (SpelaionDB's ADR-056): it runs requests (`execute_request`,
  `{"database", "statement"}`), never text (`execute` refuses).
  - `http.rs` opens its own connections with `hyper`: to `connect_addr` when it is given (the
    local end of SpelaionDB's tunnel or proxy), never through a system proxy, with TLS verified
    against `host` (`tls.rs`, libpq's modes, as in the other plugins). The password travels in
    the `Authorization` header (HTTP Basic), never in the address, and it and its Base64 form
    are taken out of every error.
  - `query.rs`: the Query API's typed JSON (`{"$type", "_value"}`) into Arrow columns. Integer,
    Float, String and Boolean columns are typed; temporal values, durations, points and bytes
    are text with the `$type` as native type; nodes, relationships, paths, lists, maps and
    mixed columns are JSON cells, nodes as `{elementId, labels, properties}` and relationships
    as `{elementId, type, start, end, properties}`. An integer past 2^53 inside JSON stays a
    string, so no client rounds it.
  - `lib.rs`: each connection's requests go one at a time through an actor (a tokio task). A
    database name is checked before it goes into a path. The explorer: databases
    (`SHOW DATABASES`, `system` as a system one), and in each `db.labels()` and
    `db.relationshipTypes()` as node labels and relationship types. No cancel, no
    transactions across requests, no `insert_rows`.
- `src/bin/spelaiondb-plugin-neo4j.rs`: the program. It hands the driver to
  `spelaion_plugin_sdk::run` and nothing else.
- `tests/neo4j.rs`: the value zoo, graphs as JSON, errors, batches, the explorer, a wrong
  password (refused, never shown) and TLS checked against the host name, against real
  servers. `tests/conformance.rs`: SpelaionDB's conformance set for requests against this
  program, and `plugin.toml` checked against what the program says of itself.
- `testing/` and `ci/services.sh`: the test servers, Neo4j 5.26's community image (user
  `neo4j`, password `spelaion-test`) on 17474, and behind a TLS proxy on 17473 with a
  certificate for `neo4j.spelaion.test` (`testing/gen-keys.sh`).
- `plugin.toml`: the protocol version, the engines and the lowest SpelaionDB of this plugin.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). This plugin connects, runs what it is sent and introspects.
So, and these are not negotiable (`docs/plugin-protocol.md` in SpelaionDB's repository, "What a
plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`.
- **The password arrives in `connect` and is written nowhere**: not to a log, an error, a
  file, the environment or a request's address.
- **It runs what it is sent.** It never decides whether a statement is a read, and never
  changes one.
- **Nothing is run again** after a session is lost.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, opens no tunnel, and
  reads no proxy from the environment.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-plugin-sdk` and, for the tests, `spelaion-plugin-host` are git
dependencies on one tag of SpelaionDB's repository (`plugin-sdk-vX.Y.Z`).

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the release every plugin shares
(the template's repository, called by commit). **Nothing is signed and no secret lives here**:
a release reaches users only when SpelaionDB's repository pins its hashes in a pull request of
its own (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` waits for
an owner. The work is tracked in SpelaionDB's tracker (Plane, project `SDB`): write the item's
identifier in the pull request's description, never in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`.
- The tests against real servers (without the variables they are skipped, and that does
  **not** count as tested): `ci/services.sh` and
  `SPELAION_TEST_NEO4J=127.0.0.1:17474 SPELAION_TEST_NEO4J_TLS=127.0.0.1:17473 cargo test`.
- `cargo deny check` if dependencies change.
- If something could not be tested, say so.
