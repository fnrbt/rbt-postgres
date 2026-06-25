namespace Fspg.Tests

open System
open System.Threading.Tasks
open Xunit
open Fspg

[<Collection("postgres")>]
type Phase7Tests(fixture: PostgresFixture) =

    [<Fact>]
    member _.``CancelAsync aborts an in-flight query (57014)``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            let queryTask = conn.QueryAsync("SELECT pg_sleep(30)")
            do! Task.Delay 500
            do! conn.CancelAsync()
            let mutable caught : exn option = None
            try
                let! _ = queryTask
                ()
            with ex ->
                caught <- Some ex
            match caught with
            | Some(:? PostgresException as pe) -> Assert.Equal("57014", pe.SqlState) // query_canceled
            | other -> failwithf "expected query_canceled, got %A" other
        }

    [<Fact>]
    member _.``a NOTIFY on one connection is delivered to a LISTENing connection``() : Task =
        fixture.Require()
        task {
            use listener = fixture.Connect()
            use notifier = fixture.Connect()
            listener.Listen("fspg_chan")
            // Give the LISTEN a moment to register, then notify from the other conn.
            notifier.Query("NOTIFY fspg_chan, 'hello world'") |> ignore
            let! got = listener.WaitForNotificationAsync(TimeSpan.FromSeconds 5.0)
            match got with
            | Some n ->
                Assert.Equal("fspg_chan", n.Channel)
                Assert.Equal("hello world", n.Payload)
            | None -> failwith "no notification received within timeout"
        }

    [<Fact>]
    member _.``WaitForNotificationAsync times out cleanly when nothing arrives``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            conn.Listen("fspg_quiet")
            let! got = conn.WaitForNotificationAsync(TimeSpan.FromMilliseconds 300.0)
            Assert.True(got.IsNone)
            // connection still usable after the timed-out wait
            let rs = conn.Query("SELECT 1 AS x") |> List.exactlyOne
            Assert.Equal(Some "1", rs.Rows |> List.head |> Array.head)
        }
