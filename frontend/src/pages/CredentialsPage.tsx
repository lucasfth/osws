import { useEffect, useState } from "react";
import { useApi } from "../api/client";
import {
  createCredential,
  listCredentials,
  revokeCredential,
} from "../api/credentials";
import { listRoles } from "../api/admin";
import type { NewS3Credential, Role, S3Credential } from "../api/types";
import { SecretBanner } from "../components/SecretBanner";
import { useUser } from "../context/UserContext";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";

export function CredentialsPage() {
  const api = useApi();
  const { profile } = useUser();
  const [credentials, setCredentials] = useState<S3Credential[]>([]);
  const [allRoles, setAllRoles] = useState<Role[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [newCredential, setNewCredential] = useState<NewS3Credential | null>(null);
  const [selectedRoleId, setSelectedRoleId] = useState<string>("");
  const [creating, setCreating] = useState(false);

  useEffect(() => {
    Promise.all([
      listCredentials(api),
      profile?.isAdmin ? listRoles(api) : Promise.resolve([] as Role[]),
    ])
      .then(([creds, roles]) => {
        setCredentials(creds);
        // Admins see all roles; regular users see only their own roles
        if (profile?.isAdmin) {
          setAllRoles(roles);
        } else {
          setAllRoles(profile?.roles ?? []);
        }
      })
      .catch((e: unknown) =>
        setError(e instanceof Error ? e.message : "Failed to load")
      )
      .finally(() => setLoading(false));
  }, [profile]);

  async function handleCreate() {
    setCreating(true);
    setError(null);
    try {
      const roleId = selectedRoleId ? parseInt(selectedRoleId, 10) : undefined;
      const cred = await createCredential(api, roleId);
      setNewCredential(cred);
      setCredentials((prev) => [
        ...prev,
        {
          id: cred.id,
          accessKeyId: cred.accessKeyId,
          isActive: cred.isActive,
          createdAt: cred.createdAt,
          defaultRoleId: cred.defaultRoleId,
          defaultRoleName: cred.defaultRoleName,
        },
      ]);
      setSelectedRoleId("");
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to create credential");
    } finally {
      setCreating(false);
    }
  }

  async function handleRevoke(id: number) {
    try {
      await revokeCredential(api, id);
      setCredentials((prev) => prev.filter((c) => c.id !== id));
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to revoke credential");
    }
  }

  if (loading) {
    return (
      <div className="flex flex-col gap-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-32 w-full" />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6">
      <h2 className="text-2xl font-semibold">S3 Credentials</h2>

      {newCredential && (
        <SecretBanner
          credential={newCredential}
          onDismiss={() => setNewCredential(null)}
        />
      )}

      {error && <p className="text-sm text-destructive">{error}</p>}

      <Card>
        <CardHeader>
          <CardTitle>Generate New Credential</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-3 flex-wrap">
            <Label htmlFor="role-select">Default Role</Label>
            <Select
              value={selectedRoleId || null}
              onValueChange={(v) => setSelectedRoleId(v ?? "")}
            >
              <SelectTrigger id="role-select" className="w-48">
                <SelectValue placeholder="— None —" />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {allRoles.map((r) => (
                    <SelectItem key={r.id} value={String(r.id)}>
                      {r.name}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
            <Button onClick={handleCreate} disabled={creating}>
              {creating ? "Generating…" : "Generate"}
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Active Credentials</CardTitle>
        </CardHeader>
        <CardContent>
          {credentials.length === 0 ? (
            <p className="text-sm text-muted-foreground">No credentials yet.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Access Key ID</TableHead>
                  <TableHead>Default Role</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {credentials.map((c) => (
                  <TableRow key={c.id}>
                    <TableCell>
                      <code className="text-xs">{c.accessKeyId}</code>
                    </TableCell>
                    <TableCell>{c.defaultRoleName ?? "—"}</TableCell>
                    <TableCell>{new Date(c.createdAt).toLocaleDateString()}</TableCell>
                    <TableCell>
                      <Button
                        variant="destructive"
                        size="sm"
                        onClick={() => handleRevoke(c.id)}
                      >
                        Revoke
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
