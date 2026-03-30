import { useUser } from "../context/UserContext";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";

export function DashboardPage() {
  const { profile, loading, error } = useUser();

  if (loading) {
    return (
      <div className="flex flex-col gap-4">
        <Skeleton className="h-8 w-36" />
        <Skeleton className="h-40 w-full" />
      </div>
    );
  }
  if (error) return <p className="text-destructive">{error}</p>;
  if (!profile) return null;

  return (
    <div className="flex flex-col gap-6">
      <h2 className="text-2xl font-semibold">Dashboard</h2>

      <Card>
        <CardHeader>
          <CardTitle>Profile</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableBody>
              <TableRow>
                <TableHead className="w-32">Name</TableHead>
                <TableCell>{profile.name}</TableCell>
              </TableRow>
              <TableRow>
                <TableHead>Email</TableHead>
                <TableCell>{profile.email ?? "—"}</TableCell>
              </TableRow>
              <TableRow>
                <TableHead>Provider</TableHead>
                <TableCell>{profile.provider}</TableCell>
              </TableRow>
              <TableRow>
                <TableHead>Admin</TableHead>
                <TableCell>{profile.isRbacAdmin ? "Yes" : "No"}</TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Roles</CardTitle>
        </CardHeader>
        <CardContent>
          {profile.roles.length === 0 ? (
            <p className="text-sm text-muted-foreground">No roles assigned.</p>
          ) : (
            <div className="flex flex-wrap gap-2">
              {profile.roles.map((r) => (
                <Badge key={r.id} variant="secondary">
                  {r.name}
                </Badge>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
