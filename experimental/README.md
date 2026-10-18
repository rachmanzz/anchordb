# Experimental logs

This folder contains historical, revision-specific experiment logs. Currently
it holds the AnchorDB vs. Neo4j Podman history; add future experiments as
separate files using the same header rules.

These logs are not benchmarks of the current release and not database
rankings. Entries summarize local runs whose raw logs and harnesses are not
published, so the results are not independently reproducible from this
repository. Treat them as historical observations, not verified release
benchmarks.

The history also records experiments and investigations that helped identify
AnchorDB issues and track behavior across revisions. It provides context for
the engineering work; inclusion of a finding does not mean that the issue was
fixed or that the fix was independently verified.

- Read each file's header before comparing numbers.
- Rows are only comparable when source revision, transport, write
  granularity, cgroup limit, resident databases, storage medium, and
  completion status match.
- Raw logs and harnesses are not stored in this repository unless
  explicitly linked.
- Do not use results without public supporting artifacts as release claims
  or reproducible benchmark evidence.

Index:
- [AnchorDB vs Neo4j on Podman — history](./anchordb_neo4j_podman_test_history.md)
