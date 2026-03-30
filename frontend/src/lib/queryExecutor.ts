import type { ColumnWithRoles, Role, UserWithRoles } from '../api/types'
import {
  assignRole,
  createRole,
  deleteRole,
  grantColumnAccess,
  inheritRole,
  removeRole,
  revokeColumnAccess,
  uninheritRole,
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
        for (const name of stmt.names) {
          const role = await createRole(api, name)
          ctx.roles.push(role)
        }
        return { ok: true, message: `Role(s) created: ${stmt.names.join(', ')}` }
      }
      case 'DROP_ROLE': {
        for (const name of stmt.names) {
          const role = ctx.roles.find(r => r.name.toLowerCase() === name.toLowerCase())
          if (!role) return { ok: false, message: `Role "${name}" not found` }
          await deleteRole(api, role.id)
          ctx.roles.splice(ctx.roles.indexOf(role), 1)
        }
        return { ok: true, message: `Role(s) dropped: ${stmt.names.join(', ')}` }
      }
      case 'GRANT_ROLE_TO_USER': {
        for (const userName of stmt.userNames) {
          const user = ctx.users.find(
            u =>
              u.email?.toLowerCase() === userName.toLowerCase() ||
              u.name.toLowerCase() === userName.toLowerCase()
          )
          if (!user) return { ok: false, message: `User "${userName}" not found` }
          for (const roleName of stmt.roleNames) {
            const role = ctx.roles.find(r => r.name.toLowerCase() === roleName.toLowerCase())
            if (!role) return { ok: false, message: `Role "${roleName}" not found` }
            await assignRole(api, user.id, role.id)
          }
        }
        return { ok: true, message: `Role(s) ${stmt.roleNames.join(', ')} granted to user(s) ${stmt.userNames.join(', ')}` }
      }
      case 'REVOKE_ROLE_FROM_USER': {
        for (const userName of stmt.userNames) {
          const user = ctx.users.find(
            u =>
              u.email?.toLowerCase() === userName.toLowerCase() ||
              u.name.toLowerCase() === userName.toLowerCase()
          )
          if (!user) return { ok: false, message: `User "${userName}" not found` }
          for (const roleName of stmt.roleNames) {
            const role = ctx.roles.find(r => r.name.toLowerCase() === roleName.toLowerCase())
            if (!role) return { ok: false, message: `Role "${roleName}" not found` }
            await removeRole(api, user.id, role.id)
          }
        }
        return { ok: true, message: `Role(s) ${stmt.roleNames.join(', ')} revoked from user(s) ${stmt.userNames.join(', ')}` }
      }
      case 'GRANT_ROLE_TO_ROLE': {
        for (const parentName of stmt.parentRoleNames) {
          const parent = ctx.roles.find(r => r.name.toLowerCase() === parentName.toLowerCase())
          if (!parent) return { ok: false, message: `Role "${parentName}" not found` }
          for (const roleName of stmt.roleNames) {
            const child = ctx.roles.find(r => r.name.toLowerCase() === roleName.toLowerCase())
            if (!child) return { ok: false, message: `Role "${roleName}" not found` }
            await inheritRole(api, parent.id, child.id)
          }
        }
        return { ok: true, message: `Role(s) ${stmt.parentRoleNames.join(', ')} now inherit(s) ${stmt.roleNames.join(', ')}` }
      }
      case 'REVOKE_ROLE_FROM_ROLE': {
        for (const parentName of stmt.parentRoleNames) {
          const parent = ctx.roles.find(r => r.name.toLowerCase() === parentName.toLowerCase())
          if (!parent) return { ok: false, message: `Role "${parentName}" not found` }
          for (const roleName of stmt.roleNames) {
            const child = ctx.roles.find(r => r.name.toLowerCase() === roleName.toLowerCase())
            if (!child) return { ok: false, message: `Role "${roleName}" not found` }
            await uninheritRole(api, parent.id, child.id)
          }
        }
        return { ok: true, message: `Role(s) ${stmt.parentRoleNames.join(', ')} no longer inherit(s) ${stmt.roleNames.join(', ')}` }
      }
      case 'GRANT_ACCESS': {
        for (const roleName of stmt.roleNames) {
          const role = ctx.roles.find(r => r.name.toLowerCase() === roleName.toLowerCase())
          if (!role) return { ok: false, message: `Role "${roleName}" not found` }
          for (const columnName of stmt.columnNames) {
            const column = ctx.columns.find(c => c.name.toLowerCase() === columnName.toLowerCase())
            if (!column) return { ok: false, message: `Column "${columnName}" not found` }
            await grantColumnAccess(api, column.id, role.id)
          }
        }
        return { ok: true, message: `Access to ${stmt.columnNames.join(', ')} granted to role(s) ${stmt.roleNames.join(', ')}` }
      }
      case 'REVOKE_ACCESS': {
        for (const roleName of stmt.roleNames) {
          const role = ctx.roles.find(r => r.name.toLowerCase() === roleName.toLowerCase())
          if (!role) return { ok: false, message: `Role "${roleName}" not found` }
          for (const columnName of stmt.columnNames) {
            const column = ctx.columns.find(c => c.name.toLowerCase() === columnName.toLowerCase())
            if (!column) return { ok: false, message: `Column "${columnName}" not found` }
            await revokeColumnAccess(api, column.id, role.id)
          }
        }
        return { ok: true, message: `Access to ${stmt.columnNames.join(', ')} revoked from role(s) ${stmt.roleNames.join(', ')}` }
      }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : 'Unknown error'
    return { ok: false, message: msg }
  }
}
