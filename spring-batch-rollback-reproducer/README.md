# Confirmed rollback reproducer for Spring Batch 6.0.5

Requirements: Java 17 or later and Docker.

From the repository root, run:

```sh
./mvnw -B -f spring-batch-rollback-reproducer/pom.xml test
```

This standalone project pins Spring Batch 6.0.5 and Spring Framework 7.0.9.
It uses only framework APIs, the JDBC JobRepository and PostgreSQL, with the
same data source and transaction manager for business writes and metadata.
The context serializer is Batch's default.

The parameterized test commits the first record, then throws from a transaction
synchronization's `beforeCommit` for the second record. It verifies
`STATUS_ROLLED_BACK` and that only the first business update remains committed.
It then checks `FAILED` status and a successful restart of the same job instance.

Expected on stock 6.0.5: two invocations, compatibility passes and native
`ChunkOrientedStep` fails with `expected FAILED but was UNKNOWN`. The native
failure reports an optimistic-locking mismatch: in-memory step version 3,
database version 2.

The proposed framework fix lives on the separate `fix/chunk-rollback-metadata`
branch. This reproduction branch retains the released, unpatched dependency
so maintainers can observe the original failure.
