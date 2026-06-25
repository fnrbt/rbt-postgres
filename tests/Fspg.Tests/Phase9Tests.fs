namespace Fspg.Tests

open System.Text
open System.Threading.Tasks
open Xunit
open Fspg
open Fspg.Wire
open Fspg.Messages

[<Collection("postgres")>]
type Phase9Tests(fixture: PostgresFixture) =

    [<Fact>]
    member _.``the function-call protocol invokes int4pl(int4,int4)``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            let oid =
                (conn.Query("SELECT 'int4pl'::regproc::oid") |> List.exactlyOne)
                    .Rows
                |> List.head
                |> Array.head
                |> Option.get
                |> int
            // args sent as text; server parses them as int4 and returns 40 + 2.
            let! res =
                conn.CallFunctionAsync(oid, [ Some(Encoding.UTF8.GetBytes "40"); Some(Encoding.UTF8.GetBytes "2") ])
            Assert.Equal(Some "42", res |> Option.map Encoding.UTF8.GetString)
        }

    [<Fact>]
    member _.``a non-UTF8 client_encoding is honored and decoded correctly``() =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "needs TCP")
        use conn = fixture.NewConnection()
        conn.ClientEncoding <- "LATIN1"
        conn.Open()
        Assert.Equal("LATIN1", (conn.Query("SHOW client_encoding") |> List.exactlyOne).Rows.Head.[0].Value)
        Assert.Equal(System.Text.Encoding.Latin1, conn.DataEncoding)
        // chr(233) = 'é' (U+00E9). On the wire under LATIN1 it is the single
        // byte 0xE9, which is invalid UTF-8 — decoding it correctly proves the
        // connection's resolved encoding (Latin1) is used, not hardcoded UTF-8.
        let rs = conn.Query("SELECT chr(233) AS c") |> List.exactlyOne
        Assert.Equal(Some "é", rs.Rows |> List.head |> Array.head)
        // And it round-trips through the typed path too.
        let t = conn.QueryTyped("SELECT chr(233) AS c")
        Assert.Equal("é", (t.Rows |> List.head).GetString 0)
        // An inline non-ASCII SQL *literal* must be encoded with the connection
        // encoding on the way out (not hardcoded UTF-8), else the server reads
        // mojibake. 'é' here goes out as the single LATIN1 byte 0xE9.
        let lit = conn.Query("SELECT 'é' AS x") |> List.exactlyOne
        Assert.Equal(Some "é", lit.Rows |> List.head |> Array.head)

    // The full GSSAPI handshake is verified end-to-end by tests/setup-kerberos.sh
    // (a KDC + GSS Postgres + the client in one container). Here we only unit-test
    // that the auth-request framing is parsed correctly.
    [<Fact>]
    member _.``parseAuth recognizes GSS messages (framing)``() =
        // AuthenticationGSS: 'R' body = Int32 7
        let m7 = IncomingMessage('R', [| 0uy; 0uy; 0uy; 7uy |])
        match parseAuth m7 with
        | AuthGSS -> ()
        | other -> failwithf "expected AuthGSS, got %A" other
        // AuthenticationGSSContinue: Int32 8 followed by a token
        let m8 = IncomingMessage('R', Array.append [| 0uy; 0uy; 0uy; 8uy |] [| 1uy; 2uy; 3uy |])
        match parseAuth m8 with
        | AuthGSSContinue t -> Assert.True([| 1uy; 2uy; 3uy |] = t)
        | other -> failwithf "expected AuthGSSContinue, got %A" other
        // AuthenticationSSPI: Int32 9
        match parseAuth (IncomingMessage('R', [| 0uy; 0uy; 0uy; 9uy |])) with
        | AuthSSPI -> ()
        | other -> failwithf "expected AuthSSPI, got %A" other
