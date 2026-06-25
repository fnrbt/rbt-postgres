namespace Fspg.Tests

open System.Threading.Tasks
open Xunit
open Fspg

[<Collection("postgres")>]
type Phase6Tests(fixture: PostgresFixture) =

    [<Fact>]
    member _.``COPY FROM/TO STDIN round-trips text rows``() =
        fixture.Require()
        use conn = fixture.Connect()
        conn.Query("DROP TABLE IF EXISTS fspg_copy") |> ignore
        conn.Query("CREATE TABLE fspg_copy (id int, name text)") |> ignore
        let n = conn.CopyInText("COPY fspg_copy FROM STDIN", [ "1\talice"; "2\tbob"; "3\tcarol" ])
        Assert.Equal(3, n)
        let cnt = conn.Query("SELECT count(*) FROM fspg_copy") |> List.exactlyOne
        Assert.Equal(Some "3", cnt.Rows |> List.head |> Array.head)
        let out = conn.CopyOutText("COPY fspg_copy TO STDOUT")
        Assert.Equal(3, out.Length)
        Assert.Contains("2\tbob", out)
        conn.Query("DROP TABLE IF EXISTS fspg_copy") |> ignore

    [<Fact>]
    member _.``COPY binary format round-trips via raw chunks``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            conn.Query("DROP TABLE IF EXISTS fspg_copyb") |> ignore
            conn.Query("CREATE TABLE fspg_copyb (id int, name text)") |> ignore
            conn.CopyInText("COPY fspg_copyb FROM STDIN", [ "10\tx"; "20\ty" ]) |> ignore
            // Dump as binary, then reload from the captured binary stream.
            let! rawBin = conn.CopyOutRawAsync("COPY fspg_copyb TO STDOUT WITH (FORMAT binary)")
            conn.Query("TRUNCATE fspg_copyb") |> ignore
            let! n = conn.CopyInRawAsync("COPY fspg_copyb FROM STDIN WITH (FORMAT binary)", rawBin)
            Assert.Equal(2, n)
            let cnt = conn.Query("SELECT count(*) FROM fspg_copyb") |> List.exactlyOne
            Assert.Equal(Some "2", cnt.Rows |> List.head |> Array.head)
            conn.Query("DROP TABLE IF EXISTS fspg_copyb") |> ignore
        }

    [<Fact>]
    member _.``COPY FROM a bad relation raises and leaves the connection usable``() =
        fixture.Require()
        use conn = fixture.Connect()
        Assert.Throws<PostgresException>(fun () ->
            conn.CopyInText("COPY no_such_table FROM STDIN", [ "1" ]) |> ignore)
        |> ignore
        let rs = conn.Query("SELECT 1 AS x") |> List.exactlyOne
        Assert.Equal(Some "1", rs.Rows |> List.head |> Array.head)
