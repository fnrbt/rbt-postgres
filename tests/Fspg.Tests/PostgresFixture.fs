namespace Fspg.Tests

open Xunit
open Fspg

/// Shared fixture: probes the configured PostgreSQL server once. If it is not
/// reachable, tests are skipped (never silently passed) with a message pointing
/// at the provisioning script.
type PostgresFixture() =
    let config = TestConfig.fromEnv ()
    let available, reason =
        try
            use c = new Connection(config)
            c.Open()
            c.Query("SELECT 1") |> ignore
            true, ""
        with ex ->
            false,
            sprintf
                "PostgreSQL not reachable (%s): %s. Run tests/setup-postgres.sh first."
                (TestConfig.describe config) ex.Message

    member _.Config = config
    member _.IsAvailable = available
    member _.SkipReason = reason

    /// Guard a test: fail loudly (never silently pass) when the server is down.
    member _.Require() =
        if not available then failwith reason

    /// Open a fresh authenticated connection (caller disposes).
    member _.Connect() =
        let c = new Connection(config)
        c.Open()
        c

    /// A new, *unopened* connection so the caller can set options (e.g. SslMode)
    /// before opening. Caller disposes.
    member _.NewConnection() = new Connection(config)

[<CollectionDefinition("postgres")>]
type PostgresCollection() =
    interface ICollectionFixture<PostgresFixture>
