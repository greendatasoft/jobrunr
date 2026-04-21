# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
./gradlew build                  # Full build with all tests
./gradlew test                   # Run all unit tests
./gradlew <module>:test          # Run tests for a specific module (e.g., core:test)
./gradlew test --tests "org.jobrunr.SomeTest"  # Run a single test class
./gradlew test --tests "org.jobrunr.SomeTest.someMethod"  # Run a single test method
./gradlew test17                 # Java 17 multirelease tests
./gradlew test25                 # Java 25 multirelease tests
./gradlew publishToMavenLocal    # Publish to local Maven repo
```

Frontend (in `core/src/main/resources/org/jobrunr/dashboard/frontend`):
```bash
npm ci && npm run build          # Install and build React dashboard
npm run test                     # Frontend tests
```

## Module Structure

```
core/                            # Main library — scheduling, execution, storage, dashboard
platform/                        # BOM for dependency version management
framework-support/
  jobrunr-spring-boot-3-starter/ # Spring Boot 3 auto-configuration
  jobrunr-spring-boot-4-starter/ # Spring Boot 4 auto-configuration
  jobrunr-quarkus-extension/     # Quarkus integration
  jobrunr-micronaut-feature/     # Micronaut integration
language-support/
  jobrunr-kotlin-support/        # Kotlin coroutine support
tests/
  e2e-ui/                        # Playwright UI tests
  e2e-vm-jdk/                    # JVM/JDK compatibility tests
```

## Architecture

### Job Lifecycle

Jobs move through a state machine: `Enqueued → Processing → Succeeded` (or `Failed`, `Deleted`). `Scheduled` and `CarbonAwareAwaiting` are pre-enqueue states. The `Job` class is the central entity — it carries `JobDetails` (class + method + parameters), a version counter for optimistic locking, and a full history of `JobState` transitions.

### Lambda to Job: how scheduling works

When a user writes `BackgroundJob.enqueue(() -> myService.doWork(id))`, the lambda is passed to `JobScheduler`, which uses `JobDetailsGenerator` (ASM bytecode analysis) to extract the method reference, class, and arguments into a `JobDetails` object. This is serialized and stored by the `StorageProvider`. On execution, the worker deserializes `JobDetails` and reflectively invokes the method.

### Key abstractions in `core/`

| Class/Interface | Package | Role |
|---|---|---|
| `BackgroundJob` | `scheduling` | Static facade for scheduling jobs |
| `JobScheduler` | `scheduling` | Core scheduling logic, applies `JobFilter`s |
| `StorageProvider` | `storage` | Persistence interface — SQL, NoSQL, in-memory |
| `BackgroundJobServer` | `server` | Picks up enqueued jobs and executes them |
| `JobActivator` | `server` | IoC integration hook (`T activate(Class<T>)`) |
| `JobZooKeeper` | `server` | Coordinates job processing within one server |
| `ServerZooKeeper` | `server` | Manages cluster heartbeats and server coordination |
| `Job` | `jobs` | Immutable job representation with versioning |
| `RecurringJob` | `jobs` | Recurring job definition (cron/interval) |
| `JobDetails` | `jobs` | Method invocation descriptor (class, method, params) |
| `JobFilter` | `jobs.filters` | Extension points: `JobClientFilter`, `JobServerFilter`, `ElectStateFilter`, `ApplyStateFilter` |

### Storage layer

`StorageProvider` has implementations for all major SQL databases (PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, DB2, H2, SQLite) and MongoDB/DocumentDB, plus an `InMemoryStorageProvider`. SQL implementations share an `AbstractStorageProvider` with database-specific SQL scripts in resources.

### Dashboard

The web dashboard is a React SPA served embedded in the JAR. The Java side exposes a REST API via `JobRunrApiHandler` and pushes live updates via SSE. The frontend source is in `core/src/main/resources/org/jobrunr/dashboard/frontend` and is built by Gradle via the npm tasks.

### JSON mapping

JobRunr supports Jackson (primary), Gson, and Yasson for serializing `JobDetails`. The mapper is auto-detected at startup based on what's on the classpath.

### Multirelease JARs

The build produces a multirelease JAR. Java 17-specific code lives under `src/main/java17/` and Java 25-specific code under `src/main/java25/` (or equivalent Gradle source sets). Tests for those versions are in separate source sets and run via `test17`/`test25` tasks.

## Technology Stack

- **Java 8** source compatibility for core library; tests compile at Java 11
- **Gradle** build system
- **ASM 9.x** — bytecode analysis for lambda job extraction
- **JUnit 5, Mockito, AssertJ, Awaitility** — testing
- **Testcontainers** — database integration tests
- **ArchUnit** — architecture constraint tests
- **Micrometer** — optional metrics integration

## Contributing Notes

- Rebase branches onto `master` before PRs
- Commit messages should reference issues: `Fixes #XXX`
- Unit tests are required for new features and important bug fixes
- A CLA is required for external contributions
