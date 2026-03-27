import type { NewS3Credential, S3Credential } from "./types";

type ApiCall = <T>(path: string, options?: RequestInit) => Promise<T>;

export function listCredentials(api: ApiCall): Promise<S3Credential[]> {
  return api("/api/credentials/");
}

export function createCredential(
  api: ApiCall,
  defaultRoleId?: number
): Promise<NewS3Credential> {
  return api("/api/credentials/", {
    method: "POST",
    body: JSON.stringify({ defaultRoleId: defaultRoleId ?? null }),
  });
}

export function revokeCredential(api: ApiCall, id: number): Promise<void> {
  return api(`/api/credentials/${id}`, { method: "DELETE" });
}
