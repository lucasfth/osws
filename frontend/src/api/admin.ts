import type { ColumnWithRoles, Role, UserWithRoles } from "./types";

type ApiCall = <T>(path: string, options?: RequestInit) => Promise<T>;

// Roles
export function listRoles(api: ApiCall): Promise<Role[]> {
  return api("/api/admin/roles");
}

export function createRole(api: ApiCall, name: string): Promise<Role> {
  return api("/api/admin/roles", {
    method: "POST",
    body: JSON.stringify({ name }),
  });
}

export function deleteRole(api: ApiCall, id: number): Promise<void> {
  return api(`/api/admin/roles/${id}`, { method: "DELETE" });
}

// Users
export function listUsers(api: ApiCall): Promise<UserWithRoles[]> {
  return api("/api/admin/users");
}

export function assignRole(
  api: ApiCall,
  userId: number,
  roleId: number
): Promise<void> {
  return api(`/api/admin/users/${userId}/roles/${roleId}`, { method: "POST" });
}

export function removeRole(
  api: ApiCall,
  userId: number,
  roleId: number
): Promise<void> {
  return api(`/api/admin/users/${userId}/roles/${roleId}`, {
    method: "DELETE",
  });
}

// Columns / Permissions
export function listColumns(api: ApiCall): Promise<ColumnWithRoles[]> {
  return api("/api/admin/columns");
}

export function grantColumnAccess(
  api: ApiCall,
  columnId: number,
  roleId: number
): Promise<void> {
  return api(`/api/admin/columns/${columnId}/roles/${roleId}`, {
    method: "POST",
  });
}

export function revokeColumnAccess(
  api: ApiCall,
  columnId: number,
  roleId: number
): Promise<void> {
  return api(`/api/admin/columns/${columnId}/roles/${roleId}`, {
    method: "DELETE",
  });
}

// Role inheritance
export function inheritRole(
  api: ApiCall,
  parentId: number,
  childId: number
): Promise<void> {
  return api(`/api/admin/roles/${parentId}/inherit/${childId}`, {
    method: "POST",
  });
}

export function uninheritRole(
  api: ApiCall,
  parentId: number,
  childId: number
): Promise<void> {
  return api(`/api/admin/roles/${parentId}/inherit/${childId}`, {
    method: "DELETE",
  });
}
