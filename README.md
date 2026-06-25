# fspg — an F# PostgreSQL wire-protocol client

A from-scratch implementation of the PostgreSQL frontend/backend protocol (v3.0)
in F#, with **no dependency on Npgsql or libpq**. It speaks the raw wire protocol
over TCP or a Unix-domain socket, and is verified end-to-end against PostgreSQL
running in Podman.

```
fspg.slnx
├─ src/Fspg/            # the client library
├─ samples/Fspg.Sample/ # a runnable console demo
└─ tests/Fspg.Tests/    # xUnit integration tests (run against Podman)
```

## Feature matrix

| Area | Support |
|------|---------|
| **Transport** | TCP and Unix-domain sockets; async, buffered, pooled I/O (`ArrayPool`), batched writes |
| **TLS** | `SSLRequest` + `SslStream`; `sslmode` = disable/allow/prefer/require/verify-ca/verify-full; custom CA; client certs |
| **Authentication** | trust, cleartext, MD5, **SCRAM-SHA-256**, **SCRAM-SHA-256-PLUS** (`tls-server-end-point` channel binding), **GSSAPI/SSPI**¹ |
| **Errors** | typed `PostgresException` exposing every ErrorResponse field (SQLSTATE, detail, hint, position, constraint, schema/table/column, …); notices |
| **Simple query** | multi-statement results, full message handling, error recovery |
| **Extended query** | named prepared statements + portals, statement cache, Describe metadata, `Close`/`Flush`, explicit/inferred param types |
| **Typed decoding** | binary format for fixed-width types, text for the rest; native .NET values (int/bool/float/decimal/`Guid`/`DateOnly`/`DateTime`/`DateTimeOffset`/`byte[]`/`interval`→`PgInterval`/ranges→`PgRange`/nested & multi-dimensional arrays); NULLs. Types with no canonical .NET shape (inet, geometric, …) decode to faithful text |
| **Parameters** | sent in **binary** for types with a binary encoder (int/bool/float/`bytea`/`uuid`/date-time), text otherwise — chosen per the parameter's type OID |
| **Encodings** | `client_encoding` resolved from the server and applied in both directions — SQL text, value decoding, and text params (UTF8, LATIN1, …) |
| **Streaming** | row-limited `IAsyncEnumerable<PgRow>` cursor (bounded memory) |
| **COPY** | `COPY FROM/TO STDIN` in text and binary formats |
| **Cancellation** | graceful: a fired `CancellationToken`/timeout sends a server `CancelRequest` and drains the query cleanly, so the connection stays usable; explicit `CancelAsync()` too |
| **LISTEN/NOTIFY** | async notifications via event + `WaitForNotificationAsync` |
| **Liveness** | cheap no-round-trip `Connection.IsHealthy` check |
| **Function call** | legacy `FunctionCall`/`FunctionCallResponse` protocol |
| **Replication** | logical + physical: `IDENTIFY_SYSTEM`, slots, `START_REPLICATION`, CopyBoth, XLogData, keepalives, standby status feedback; **pgoutput** decoding (Begin/Relation/Insert/Update/Delete/Commit) + test_decoding |
| **Negotiation** | `NegotiateProtocolVersion` handled |

¹ GSSAPI/SSPI is **verified end-to-end** against a Kerberos KDC by
`tests/setup-kerberos.sh`, which stands up a KDC + a GSS-enabled PostgreSQL +
the client in one Podman container and confirms `mechanism=GSS`. The host test
suite additionally unit-tests the GSS message framing.

## Library layout

| File | Responsibility |
|------|----------------|
| `Wire.fs` | big-endian I/O, message framing, async buffered `Transport` |
| `Messages.fs` | typed backend parsers + frontend message constructors |
| `Errors.fs` | full ErrorResponse fields → `PostgresException` |
| `Tls.fs` | `SSLRequest`/`SslStream`, `sslmode`, `tls-server-end-point` hash |
| `Scram.fs` | SCRAM-SHA-256 (+ `-PLUS` channel binding) |
| `Oids.fs`, `Codecs.fs` | type OIDs, text/binary codecs, typed `PgRow` |
| `Connection.fs` | connect, auth, queries, prepared statements, streaming, COPY, cancel, LISTEN/NOTIFY, function call, replication |
| `Replication.fs` | replication helpers |

## Build, test, run

```bash
# 1. Provision a fully-configured Postgres 18 container in Podman
#    (TCP + UDS, TLS on, wal_level=logical, md5/cleartext/replication roles)
./tests/setup-postgres.sh

# 2. Build everything
dotnet build -c Release

# 3. Run the integration tests against the container (38 tests)
dotnet test -c Release

# 4. Run the console demo
dotnet run -c Release --project samples/Fspg.Sample -- \
    --host 127.0.0.1 --port 55432 --user tester --password secret --db testdb \
    --sslmode verify-full --sslrootcert tests/certs/root.crt
```

The demo also runs over a Unix socket:

```bash
dotnet run -c Release --project samples/Fspg.Sample -- \
    --socket ./run --port 5432 --user tester --password secret --db testdb
```

Connection settings can come from CLI flags (`--host/--port/--socket/--user/
--password/--db/--sslmode/--sslrootcert`) or env vars (`FSPG_HOST`, `FSPG_PORT`,
`FSPG_SOCKET`, `FSPG_USER`, `FSPG_PASSWORD`, `FSPG_DB`).

## API sketch

```fsharp
open Fspg

// Simple + typed queries
use conn = new Connection({ Endpoint = Tcp("127.0.0.1", 55432)
                            User = "tester"; Password = "secret"; Database = "testdb" })
conn.SslMode <- Require
do! conn.OpenAsync()

let! rows = conn.QueryAsync("SELECT 1")
let typed = conn.QueryTyped("SELECT now()::date AS d")
let d = (typed.Rows |> List.head).Get<System.DateOnly>(0)

// Prepared + streaming
let stmt = conn.Prepare("SELECT $1::int4 * 2 AS x")
let! r = conn.ExecutePreparedAsync(stmt, [ Some (box 21) ])
for await row in conn.Stream("SELECT id FROM big_table", batchSize = 500) do
    ...

// COPY, LISTEN/NOTIFY, cancel
let! n = conn.CopyInTextAsync("COPY t FROM STDIN", [ "1\ta"; "2\tb" ])
conn.Listen("chan");  let! note = conn.WaitForNotificationAsync(TimeSpan.FromSeconds 5.)
do! conn.CancelAsync()

// Register a custom codec for any type fspg doesn't model natively
Codecs.registerText 869 (fun s -> box (System.Net.IPAddress.Parse s)) // inet -> IPAddress
```

This is a **protocol driver**: it implements the wire protocol over a single
`Connection`. Higher-level concerns that aren't part of the protocol — connection
pooling, connection-string parsing, multi-host/failover — are deliberately left
out; build them on top of `Connection` (a pool is a generic object pool whose
only PostgreSQL-specific touch is a `DISCARD ALL` reset).

## Limitations / known gaps

Honest boundaries. None affect the supported feature set; they're scope edges.

- **GSS transport encryption** (`gssencmode` / `GSSENCRequest` + `gss_wrap`):
  not implemented. GSS/Kerberos *authentication* is implemented and verified
  end-to-end (see `tests/setup-kerberos.sh`); encrypting the transport with the
  GSS security context is a separate, niche feature (most GSS deployments use
  TLS or plaintext+gss-auth). This is the one item that's simply not built.
- **Types without a canonical .NET shape** — `inet`/`cidr`/`macaddr`, `bit`/
  `varbit`, geometric types, `tsvector`/`tsquery`, `money`, `xml`, and
  composite/enum/domain types — decode to their **faithful text** form rather
  than a bespoke .NET type (`interval`, ranges and arrays *do* get typed values).
  For any of these, register your own codec — `Codecs.registerText 869 (fun s ->
  box (IPAddress.Parse s))` — and `QueryTyped` will return your type. This escape
  hatch covers every type without fspg having to model all of them.
- **`numeric`** decodes via text → `decimal` (lossless), not the binary format.
- **Binary parameters** cover the core scalar types (int/bool/float/`bytea`/
  `uuid`/date-time); other parameter types are sent as text (also lossless).
- **`client_encoding`** decoding covers UTF8, LATIN1, ISO-8859-15, and any name
  .NET's `Encoding.GetEncoding` resolves; exotic legacy server encodings that
  need the code-pages provider fall back to UTF-8.
- **A single `Connection` is not safe for concurrent use** — the wire protocol
  is a serial request/response stream (no multiplexing), so concurrency means a
  connection per concurrent operation. Pooling is intentionally *not* part of
  this library (it's resource management, not protocol); `Connection.IsHealthy`
  gives a pool the cheap liveness check it needs.

## Test container

`tests/setup-postgres.sh` (re)creates the `fspg-test` container with everything
the suite needs: TCP on `55432`, a bind-mounted Unix socket at `./run`, TLS
(`ssl=on`, generated CA at `tests/certs/root.crt`), `wal_level=logical`, and
`pg_hba` rules + roles for the md5/cleartext/replication tests. Teardown:
`podman rm -f fspg-test`.

`tests/setup-kerberos.sh` is a separate, self-contained verification of the
GSSAPI path: it builds a Kerberos KDC + GSS-enabled PostgreSQL + the .NET client
in a single container and runs the client through a real Kerberos login
(`mechanism=GSS`). Teardown: `podman rm -f fspg-krb`.
```
