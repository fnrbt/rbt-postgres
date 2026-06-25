namespace Fspg.Tests

open System
open System.Collections.Generic
open System.Text
open System.Threading.Tasks
open Xunit
open Fspg

[<Collection("postgres")>]
type Phase10Tests(fixture: PostgresFixture) =

    [<Fact>]
    member _.``IDENTIFY_SYSTEM works on a physical replication connection``() =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "needs TCP")
        use conn = fixture.NewConnection()
        conn.ReplicationMode <- Some "true" // physical replication
        conn.Open()
        let sys = Replication.identifySystem conn
        Assert.True(sys.SystemId.Length > 0)
        Assert.True(sys.Timeline >= 1)

    [<Fact>]
    member _.``logical replication streams decoded INSERT/UPDATE/DELETE changes``() : Task =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "needs TCP")
        task {
            let slot = "fspg_slot"
            use repl = fixture.NewConnection()
            repl.ReplicationMode <- Some "database" // logical replication
            repl.Open()
            Replication.dropSlot repl slot // clean any leftover
            Replication.createLogicalSlot repl slot "test_decoding"

            // Make changes on a separate, normal connection.
            use work = fixture.Connect()
            work.Query("DROP TABLE IF EXISTS fspg_repl") |> ignore
            work.Query("CREATE TABLE fspg_repl (id int primary key, name text)") |> ignore
            work.Query("INSERT INTO fspg_repl VALUES (1,'alice'),(2,'bob')") |> ignore
            work.Query("UPDATE fspg_repl SET name='ALICE' WHERE id=1") |> ignore
            work.Query("DELETE FROM fspg_repl WHERE id=2") |> ignore

            // Stream and collect decoded text until we observe the DELETE.
            let collected = List<string>()
            do!
                Replication.streamLogical repl slot "0/0" (fun (_, data) ->
                    let s = Encoding.UTF8.GetString data
                    collected.Add s
                    not (s.Contains "DELETE"))

            let all = String.Join("\n", collected)
            Assert.Contains("table public.fspg_repl", all)
            Assert.Contains("INSERT", all)
            Assert.Contains("UPDATE", all)
            Assert.Contains("DELETE", all)

            Replication.dropSlot repl slot
            work.Query("DROP TABLE IF EXISTS fspg_repl") |> ignore
        }

    [<Fact>]
    member _.``pgoutput streams structured (decoded) change messages``() : Task =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "needs TCP")
        task {
            let slot = "fspg_pgo_slot"
            let pub = "fspg_pub"
            use repl = fixture.NewConnection()
            repl.ReplicationMode <- Some "database"
            repl.Open()

            use work = fixture.Connect()
            work.Query("DROP TABLE IF EXISTS fspg_pgo CASCADE") |> ignore
            work.Query("CREATE TABLE fspg_pgo (id int primary key, name text)") |> ignore
            work.Query(sprintf "DROP PUBLICATION IF EXISTS %s" pub) |> ignore
            work.Query(sprintf "CREATE PUBLICATION %s FOR TABLE fspg_pgo" pub) |> ignore

            Replication.dropSlot repl slot
            Replication.createLogicalSlot repl slot "pgoutput"

            work.Query("INSERT INTO fspg_pgo VALUES (1,'alice'),(2,'bob')") |> ignore
            work.Query("UPDATE fspg_pgo SET name='ALICE' WHERE id=1") |> ignore
            work.Query("DELETE FROM fspg_pgo WHERE id=2") |> ignore

            let msgs = List<PgOutput.Message>()
            do!
                repl.StreamReplicationAsync(
                    sprintf "START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '1', publication_names '%s')" slot pub,
                    (fun (_, data) ->
                        let m = PgOutput.parse data
                        msgs.Add m
                        match m with
                        | PgOutput.Delete _ -> false // stop once the DELETE arrives
                        | _ -> true))

            // a Relation message describes the table + its columns
            let rel = msgs |> Seq.tryPick (function PgOutput.Relation(_, _, n, cols) -> Some(n, cols) | _ -> None)
            Assert.True(rel.IsSome)
            let relName, cols = rel.Value
            Assert.Equal("fspg_pgo", relName)
            Assert.True([| "id"; "name" |] = cols)
            // INSERTs carry the new tuples (decoded per column)
            let inserts =
                msgs |> Seq.choose (function PgOutput.Insert(_, t) -> Some t | _ -> None) |> Seq.toList
            Assert.True(inserts.Length >= 2)
            Assert.Equal(Some "1", inserts.[0].[0])
            Assert.Equal(Some "alice", inserts.[0].[1])
            // UPDATE + DELETE were observed
            Assert.True(msgs |> Seq.exists (function PgOutput.Update _ -> true | _ -> false))
            Assert.True(msgs |> Seq.exists (function PgOutput.Delete _ -> true | _ -> false))

            Replication.dropSlot repl slot
            work.Query(sprintf "DROP PUBLICATION IF EXISTS %s" pub) |> ignore
            work.Query("DROP TABLE IF EXISTS fspg_pgo CASCADE") |> ignore
        }
