namespace Fspg

open System
open Fspg.Wire

/// Every field PostgreSQL can put in an ErrorResponse / NoticeResponse message
/// (see protocol "Error and Notice Message Fields"). Optional fields are
/// `None` when the server omitted them.
type PostgresErrorFields =
    { Severity: string // localized ('S'); falls back to 'V' when only that is present
      SeverityUnlocalized: string // 'V' (always English: ERROR/FATAL/...)
      SqlState: string // 'C'
      MessageText: string // 'M'
      Detail: string option // 'D'
      Hint: string option // 'H'
      Position: int option // 'P'
      InternalPosition: int option // 'p'
      InternalQuery: string option // 'q'
      Where: string option // 'W'
      SchemaName: string option // 's'
      TableName: string option // 't'
      ColumnName: string option // 'c'
      DataTypeName: string option // 'd'
      ConstraintName: string option // 'n'
      File: string option // 'F'
      Line: string option // 'L'
      Routine: string option // 'R'
      Raw: Map<char, string> }

module ErrorFields =

    /// Parse the body of an 'E' (error) or 'N' (notice) message.
    let parse (m: IncomingMessage) : PostgresErrorFields =
        let rec loop acc =
            let code = m.Byte()
            if code = 0uy then acc else loop (Map.add (char code) (m.CString()) acc)
        let f = loop Map.empty
        let g k = Map.tryFind k f
        let gd k = g k |> Option.defaultValue ""
        let gi k =
            g k
            |> Option.bind (fun s ->
                match Int32.TryParse s with
                | true, v -> Some v
                | _ -> None)
        { Severity = (g 'S' |> Option.orElse (g 'V') |> Option.defaultValue "")
          SeverityUnlocalized = gd 'V'
          SqlState = gd 'C'
          MessageText = gd 'M'
          Detail = g 'D'
          Hint = g 'H'
          Position = gi 'P'
          InternalPosition = gi 'p'
          InternalQuery = g 'q'
          Where = g 'W'
          SchemaName = g 's'
          TableName = g 't'
          ColumnName = g 'c'
          DataTypeName = g 'd'
          ConstraintName = g 'n'
          File = g 'F'
          Line = g 'L'
          Routine = g 'R'
          Raw = f }

    /// A compact single-line rendering for messages and notices.
    let format (f: PostgresErrorFields) =
        let detail =
            match f.Detail with
            | Some d -> $" (detail: {d})"
            | None -> ""
        $"{f.Severity} [{f.SqlState}] {f.MessageText}{detail}"

/// Raised for any backend ErrorResponse. Exposes every error field.
type PostgresException(fields: PostgresErrorFields) =
    inherit Exception(ErrorFields.format fields)
    member _.Fields = fields
    member _.SqlState = fields.SqlState
    member _.Severity = fields.Severity
    member _.MessageText = fields.MessageText
    member _.Detail = fields.Detail
    member _.Hint = fields.Hint
    member _.Position = fields.Position
    member _.ConstraintName = fields.ConstraintName
    member _.TableName = fields.TableName
    member _.ColumnName = fields.ColumnName
    member _.SchemaName = fields.SchemaName
    member _.Routine = fields.Routine
