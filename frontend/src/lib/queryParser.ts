import { parse } from './query.peggy'

export type QueryStatement =
  | { type: 'CREATE_ROLE'; name: string }
  | { type: 'DROP_ROLE'; name: string }
  | { type: 'GRANT_ROLE'; roleName: string; userIdentifier: string }
  | { type: 'REVOKE_ROLE'; roleName: string; userIdentifier: string }
  | { type: 'GRANT_ACCESS'; columnName: string; roleName: string }
  | { type: 'REVOKE_ACCESS'; columnName: string; roleName: string }

export type ParseResult =
  | { ok: true; stmt: QueryStatement; raw: string }
  | { ok: false; raw: string; error: string }

// Splits input on semicolons so each statement is parsed independently.
// This preserves per-statement error recovery: one bad statement does not
// prevent the rest from being parsed and executed.
// -- comments are stripped before parsing but kept in `raw` for display.
export function parseQuery(input: string): ParseResult[] {
  const results: ParseResult[] = []

  for (const chunk of input.split(';')) {
    const raw = chunk.trim()

    // Strip -- comments from each line before handing to the grammar
    const cleaned = raw
      .split('\n')
      .map(line => line.replace(/--.*$/, ''))
      .join('\n')
      .trim()

    if (!cleaned) continue

    try {
      const stmt = parse(cleaned) as QueryStatement
      results.push({ ok: true, stmt, raw })
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Syntax error'
      results.push({ ok: false, raw, error: msg })
    }
  }

  return results
}
