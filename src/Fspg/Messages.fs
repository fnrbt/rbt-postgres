namespace Fspg

open Fspg.Wire

/// Typed views over the PostgreSQL message set we care about.
module Messages =

    // ---- Authentication request sub-types (body of an 'R' message) ----------

    type AuthRequest =
        | AuthOk
        | AuthCleartextPassword
        | AuthMD5Password of salt: byte[]
        | AuthGSS
        | AuthGSSContinue of data: byte[]
        | AuthSSPI
        | AuthSASL of mechanisms: string list
        | AuthSASLContinue of data: byte[]
        | AuthSASLFinal of data: byte[]
        | AuthOther of int

    let parseAuth (m: IncomingMessage) : AuthRequest =
        match m.Int32() with
        | 0 -> AuthOk
        | 3 -> AuthCleartextPassword
        | 5 -> AuthMD5Password(m.Bytes 4)
        | 7 -> AuthGSS
        | 8 -> AuthGSSContinue(m.RestBytes())
        | 9 -> AuthSSPI
        | 10 ->
            // List of NUL-terminated mechanism names, ended by an empty string.
            let rec loop acc =
                let name = m.CString()
                if name = "" then List.rev acc else loop (name :: acc)
            AuthSASL(loop [])
        | 11 -> AuthSASLContinue(m.RestBytes())
        | 12 -> AuthSASLFinal(m.RestBytes())
        | other -> AuthOther other

    // ---- Field descriptions (RowDescription 'T') ----------------------------

    type FieldDescription =
        { Name: string
          TableOid: int
          ColumnAttr: int
          TypeOid: int
          TypeSize: int
          TypeModifier: int
          FormatCode: int }

    let parseRowDescription (m: IncomingMessage) : FieldDescription[] =
        let count = m.Int16()
        Array.init count (fun _ ->
            { Name = m.CString()
              TableOid = m.Int32()
              ColumnAttr = m.Int16()
              TypeOid = m.Int32()
              TypeSize = m.Int16()
              TypeModifier = m.Int32()
              FormatCode = m.Int16() })

    /// ParameterDescription ('t'): the OIDs of a prepared statement's parameters.
    let parseParameterDescription (m: IncomingMessage) : int[] =
        let count = m.Int16()
        Array.init count (fun _ -> m.Int32())

    /// One DataRow ('D'): an array of nullable column values (raw bytes).
    let parseDataRow (m: IncomingMessage) : byte[] option [] =
        let count = m.Int16()
        Array.init count (fun _ ->
            let len = m.Int32()
            if len < 0 then None else Some(m.Bytes len))

    // ErrorResponse / NoticeResponse ('E' / 'N') parsing lives in Errors.fs
    // (ErrorFields.parse), which produces the full typed PostgresErrorFields.

    // ---- Frontend message constructors --------------------------------------

    let protocolVersion = (3 <<< 16) ||| 0 // 3.0 == 196608

    let startupMessage (parameters: (string * string) list) =
        // No type tag for the startup message.
        let m = OutgoingMessage()
        m.Int32(protocolVersion)
        for (k, v) in parameters do
            m.CString(k)
            m.CString(v)
        m.Byte(0uy) // terminating empty key
        m

    /// SSLRequest: special startup-style message asking whether the server
    /// supports TLS. Reply is a single byte 'S' (yes) or 'N' (no).
    let sslRequest () =
        let m = OutgoingMessage()
        m.Int32(80877103) // magic 1234,5679
        m

    /// CancelRequest: a startup-style message (no type tag) sent on a *separate*
    /// connection to cancel the query running on the backend identified by pid.
    let cancelRequest (pid: int) (secretKey: int) =
        let m = OutgoingMessage()
        m.Int32(80877102) // magic 1234,5678
        m.Int32(pid)
        m.Int32(secretKey)
        m

    /// NotificationResponse ('A'): an async LISTEN/NOTIFY message.
    let parseNotification (m: IncomingMessage) =
        let pid = m.Int32()
        let channel = m.CString()
        let payload = m.CString()
        pid, channel, payload

    /// Cleartext or MD5 password response: a single null-terminated string.
    let passwordMessage (password: string) =
        let m = OutgoingMessage('p')
        m.CString(password)
        m

    let saslInitialResponse (mechanism: string) (clientFirst: byte[]) =
        let m = OutgoingMessage('p')
        m.CString(mechanism)
        m.Int32(clientFirst.Length)
        m.Bytes(clientFirst)
        m

    let saslResponse (clientFinal: byte[]) =
        let m = OutgoingMessage('p')
        m.Bytes(clientFinal)
        m

    /// A GSSAPI/SSPI token carried in a PasswordMessage ('p').
    let gssResponse (token: byte[]) =
        let m = OutgoingMessage('p')
        m.Bytes(token)
        m

    // ---- Legacy function-call protocol ('F' / 'V') --------------------------

    /// FunctionCall ('F'): invoke a function by OID with args sent as text.
    let functionCall (oid: int) (args: byte[] option list) (resultBinary: bool) =
        let m = OutgoingMessage('F')
        m.Int32(oid)
        m.Int16(0) // 0 arg format codes => all args text
        m.Int16(List.length args)
        for a in args do
            match a with
            | None -> m.Int32(-1)
            | Some b ->
                m.Int32(b.Length)
                m.Bytes(b)
        m.Int16(if resultBinary then 1 else 0)
        m

    /// FunctionCallResponse ('V'): the (nullable) result value bytes.
    let parseFunctionResult (m: IncomingMessage) : byte[] option =
        let len = m.Int32()
        if len < 0 then None else Some(m.Bytes len)

    let query (encoding: System.Text.Encoding) (sql: string) =
        let m = OutgoingMessage('Q', encoding)
        m.CString(sql)
        m

    // ---- Extended query protocol --------------------------------------------

    let parse (encoding: System.Text.Encoding) (statementName: string) (sql: string) (paramTypeOids: int list) =
        let m = OutgoingMessage('P', encoding)
        m.CString(statementName)
        m.CString(sql)
        m.Int16(List.length paramTypeOids)
        for oid in paramTypeOids do
            m.Int32(oid)
        m

    /// Bind with all parameters sent in text format and results requested in text.
    let bind (portal: string) (statementName: string) (paramValues: string option list) =
        let m = OutgoingMessage('B')
        m.CString(portal)
        m.CString(statementName)
        m.Int16(0) // 0 parameter format codes => all text
        m.Int16(List.length paramValues)
        for p in paramValues do
            match p with
            | None -> m.Int32(-1) // SQL NULL
            | Some s ->
                let bytes = utf8.GetBytes(s)
                m.Int32(bytes.Length)
                m.Bytes(bytes)
        m.Int16(0) // 0 result format codes => all text
        m

    /// Bind with per-parameter format codes: each param is Some(formatCode, bytes)
    /// (0 = text, 1 = binary) or None for SQL NULL. Each result column's format
    /// is given by `resultFormats`.
    let bindParams
        (portal: string)
        (statementName: string)
        (paramValues: (int * byte[]) option list)
        (resultFormats: int[])
        =
        let m = OutgoingMessage('B')
        m.CString(portal)
        m.CString(statementName)
        m.Int16(List.length paramValues) // one format code per parameter
        for p in paramValues do
            match p with
            | Some(fmt, _) -> m.Int16(fmt)
            | None -> m.Int16(0)
        m.Int16(List.length paramValues)
        for p in paramValues do
            match p with
            | None -> m.Int32(-1)
            | Some(_, bytes) ->
                m.Int32(bytes.Length)
                m.Bytes(bytes)
        m.Int16(resultFormats.Length)
        for f in resultFormats do
            m.Int16(f)
        m

    let describePortal (portal: string) =
        let m = OutgoingMessage('D')
        m.Byte(byte 'P')
        m.CString(portal)
        m

    let describeStatement (statementName: string) =
        let m = OutgoingMessage('D')
        m.Byte(byte 'S')
        m.CString(statementName)
        m

    let execute (portal: string) (maxRows: int) =
        let m = OutgoingMessage('E')
        m.CString(portal)
        m.Int32(maxRows)
        m

    let flush () = OutgoingMessage('H')

    // ---- COPY protocol (frontend messages) ----------------------------------

    let copyData (bytes: byte[]) =
        let m = OutgoingMessage('d')
        m.Bytes(bytes)
        m

    let copyDone () = OutgoingMessage('c')

    let copyFail (reason: string) =
        let m = OutgoingMessage('f')
        m.CString(reason)
        m

    /// Parse the row count out of a "COPY n" CommandComplete tag.
    let copyCount (commandTag: string) =
        match commandTag.Split(' ') with
        | [| "COPY"; n |] ->
            match System.Int32.TryParse n with
            | true, v -> v
            | _ -> 0
        | _ -> 0

    let closeStatement (name: string) =
        let m = OutgoingMessage('C')
        m.Byte(byte 'S')
        m.CString(name)
        m

    let closePortal (name: string) =
        let m = OutgoingMessage('C')
        m.Byte(byte 'P')
        m.CString(name)
        m

    let sync () = OutgoingMessage('S')

    let terminate () = OutgoingMessage('X')
