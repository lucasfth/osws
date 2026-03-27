declare module '*.peggy' {
  import type { QueryStatement } from './queryParser'
  export function parse(input: string): QueryStatement
}
