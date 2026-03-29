import type { ColumnWithRoles, Role, UserWithRoles } from '../api/types'
import {
  assignRole,
  createRole,
  deleteRole,
  grantColumnAccess,
  removeRole,
  revokeColumnAccess,
} from '../api/admin'
import type { QueryStatement } from './queryParser'

type ApiCall = <T>(path: string, options?: RequestInit) => Promise<T>

export interface ExecutionContext {
  users: UserWithRoles[]
  roles: Role[]
  columns: ColumnWithRoles[]
}

export interface StatementResult {
  ok: boolean
  message: string
}

// Executes a single parsed statement against the API.
// ctx is mutated in-place so that subsequent statements in the same batch
// can see roles created/dropped earlier in the same batch.
export async function executeStatement(
  api: ApiCall,
  ctx: ExecutionContext,
  stmt: QueryStatement
): Promise<StatementResult> {
  try {
    switch (stmt.type) {
      case 'CREATE_ROLE': {
        const role = await createRole(api, stmt.name)
        ctx.roles.push(role)
        return { ok: true, message: `Role "${stmt.name}" created` }
      }
      case 'DROP_ROLE': {
        const role = ctx.roles.find(r => r.name.toLowerCase() === stmt.name.toLowerCase())
        if (!role) return { ok: false, message: `Role "${stmt.name}" not found` }
        await deleteRole(api, role.id)
        ctx.roles.splice(ctx.roles.indexOf(role), 1)
        return { ok: true, message: `Role "${stmt.name}" dropped` }
      }
      case 'GRANT_ROLE': {
        const role = ctx.roles.find(r => r.name.toLowerCase() === stmt.roleName.toLowerCase())
        if (!role) return { ok: false, message: `Role "${stmt.roleName}" not found` }
        const user = ctx.users.find(
          u =>
            u.email?.toLowerCase() === stmt.userIdentifier.toLowerCase() ||
            u.name.toLowerCase() === stmt.userIdentifier.toLowerCase()
        )
        if (!user) return { ok: false, message: `User "${stmt.userIdentifier}" not found` }
        await assignRole(api, user.id, role.id)
        return { ok: true, message: `Role "${stmt.roleName}" granted to "${stmt.userIdentifier}"` }
      }
      case 'REVOKE_ROLE': {
        const role = ctx.roles.find(r => r.name.toLowerCase() === stmt.roleName.toLowerCase())
        if (!role) return { ok: false, message: `Role "${stmt.roleName}" not found` }
        const user = ctx.users.find(
          u =>
            u.email?.toLowerCase() === stmt.userIdentifier.toLowerCase() ||
            u.name.toLowerCase() === stmt.userIdentifier.toLowerCase()
        )
        if (!user) return { ok: false, message: `User "${stmt.userIdentifier}" not found` }
        await removeRole(api, user.id, role.id)
        return { ok: true, message: `Role "${stmt.roleName}" revoked from "${stmt.userIdentifier}"` }
      }
      case 'GRANT_ACCESS': {
        const column = ctx.columns.find(c => c.name.toLowerCase() === stmt.columnName.toLowerCase())
        if (!column) return { ok: false, message: `Column "${stmt.columnName}" not found` }
        const role = ctx.roles.find(r => r.name.toLowerCase() === stmt.roleName.toLowerCase())
        if (!role) return { ok: false, message: `Role "${stmt.roleName}" not found` }
        await grantColumnAccess(api, column.id, role.id)
        return { ok: true, message: `Access to "${stmt.columnName}" granted to role "${stmt.roleName}"` }
      }
      case 'REVOKE_ACCESS': {
        const column = ctx.columns.find(c => c.name.toLowerCase() === stmt.columnName.toLowerCase())
        if (!column) return { ok: false, message: `Column "${stmt.columnName}" not found` }
        const role = ctx.roles.find(r => r.name.toLowerCase() === stmt.roleName.toLowerCase())
        if (!role) return { ok: false, message: `Role "${stmt.roleName}" not found` }
        await revokeColumnAccess(api, column.id, role.id)
        return { ok: true, message: `Access to "${stmt.columnName}" revoked from role "${stmt.roleName}"` }
      }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : 'Unknown error'
    return { ok: false, message: msg }
  }
}
