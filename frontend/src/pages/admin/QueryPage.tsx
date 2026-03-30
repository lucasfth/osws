import { useEffect, useRef, useState } from "react";
import { useApi } from "@/api/client";
import { listColumns, listRoles, listUsers } from "@/api/admin";
import { parseQuery } from "@/lib/queryParser";
import { executeStatement, type ExecutionContext } from "@/lib/queryExecutor";
import { Button } from "@/components/ui/button";
import { Kbd, KbdGroup } from "@/components/ui/kbd";
import { QueryEditor } from "@/components/QueryEditor";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

const PLACEHOLDER = `-- Examples:
-- CREATE ROLE analyst;
-- DROP ROLE analyst;
-- GRANT analyst TO user@example.com;
-- REVOKE analyst FROM user@example.com;
-- GRANT ACCESS ON column_name TO analyst;
-- REVOKE ACCESS ON column_name FROM analyst;`;

interface OutputLine {
  raw: string;
  ok: boolean;
  message: string;
}

export function QueryPage() {
  const api = useApi();
  const [ctx, setCtx] = useState<ExecutionContext | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [input, setInput] = useState("");
  const [output, setOutput] = useState<OutputLine[]>([]);
  const [isRunning, setIsRunning] = useState(false);
  const outputRef = useRef<HTMLDivElement>(null);

  async function loadContext() {
    try {
      const [users, roles, columns] = await Promise.all([
        listUsers(api),
        listRoles(api),
        listColumns(api),
      ]);
      setCtx({ users, roles, columns });
      setLoadError(null);
    } catch {
      setLoadError("Failed to load RBAC context. Is the backend running?");
    }
  }

  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => { loadContext() }, []);

  async function runQuery() {
    if (!input.trim() || isRunning || !ctx) return;

    setIsRunning(true);

    const parsed = parseQuery(input);
    const results: OutputLine[] = [];

    const mutableCtx: ExecutionContext = {
      users: [...ctx.users],
      roles: [...ctx.roles],
      columns: [...ctx.columns],
    };

    for (const result of parsed) {
      if (!result.ok) {
        results.push({ raw: result.raw, ok: false, message: "Syntax error: unrecognized statement" });
        continue;
      }
      const out = await executeStatement(api, mutableCtx, result.stmt);
      results.push({ raw: result.raw, ok: out.ok, message: out.message });
    }

    setOutput(results);

    await loadContext();
    setIsRunning(false);

    setTimeout(() => outputRef.current?.scrollIntoView({ behavior: "smooth" }), 50);
  }

  const successCount = output.filter(l => l.ok).length;
  const errorCount = output.filter(l => !l.ok).length;

  return (
    <div className="flex flex-col gap-6 max-w-4xl">
      <div>
        <h1 className="text-2xl font-bold">Admin</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Manage roles, users, and column access using SQL-style GRANT/REVOKE statements.
        </p>
      </div>

      {loadError && (
        <p className="text-sm text-destructive">{loadError}</p>
      )}

      <Card>
        <CardContent className="pt-4 space-y-3">
          <QueryEditor
            value={input}
            onChange={setInput}
            onRun={runQuery}
            disabled={isRunning}
            placeholder={PLACEHOLDER}
          />
          <div className="flex items-center justify-between">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              Statements separated by <Kbd>;</Kbd>
              <span>&nbsp;·&nbsp;</span>
              <KbdGroup><Kbd>⌘</Kbd><Kbd>Enter</Kbd></KbdGroup> to run
            </span>
            <Button
              onClick={runQuery}
              disabled={isRunning || !ctx || !input.trim()}
            >
              {isRunning ? "Running…" : "Run"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {output.length > 0 && (
        <Card ref={outputRef}>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Output &mdash;{" "}
              <span className="text-green-600 dark:text-green-400">{successCount} ok</span>
              {errorCount > 0 && (
                <>, <span className="text-red-600 dark:text-red-400">{errorCount} error{errorCount > 1 ? "s" : ""}</span></>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="font-mono text-sm bg-muted/40 rounded p-3 space-y-2">
              {output.map((line, i) => (
                <div key={i}>
                  <div className="text-muted-foreground">{line.raw}</div>
                  <div className={line.ok ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}>
                    &nbsp;&nbsp;{line.ok ? "✓" : "✗"} {line.message}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {ctx === null && !loadError ? (
        <div className="flex flex-col gap-3">
          <Skeleton className="h-10 w-64" />
          <Skeleton className="h-48 w-full" />
        </div>
      ) : ctx !== null && (
        <Tabs defaultValue="roles">
          <TabsList>
            <TabsTrigger value="roles">Roles</TabsTrigger>
            <TabsTrigger value="users">Users</TabsTrigger>
            <TabsTrigger value="permissions">Permissions</TabsTrigger>
          </TabsList>

          <TabsContent value="roles">
            <Card>
              <CardContent className="pt-4">
                {ctx.roles.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No roles yet.</p>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-16">ID</TableHead>
                        <TableHead>Name</TableHead>
                        <TableHead>Child roles</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {ctx.roles.map((r) => (
                        <TableRow key={r.id}>
                          <TableCell>{r.id}</TableCell>
                          <TableCell>{r.name}</TableCell>
                          <TableCell>
                            <div className="flex flex-wrap gap-1">
                              {r.childRoles.length === 0 ? (
                                <span className="text-sm text-muted-foreground">None</span>
                              ) : (
                                r.childRoles.map((child) => (
                                  <Badge key={child.id} variant="secondary">
                                    {child.name}
                                  </Badge>
                                ))
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="users">
            <Card>
              <CardContent className="pt-4">
                {ctx.users.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No provisioned users.</p>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Name</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Roles</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {ctx.users.map((u) => (
                        <TableRow key={u.id}>
                          <TableCell>{u.name}</TableCell>
                          <TableCell>{u.email ?? "—"}</TableCell>
                          <TableCell>
                            <div className="flex flex-wrap gap-1">
                              {u.roles.length === 0 ? (
                                <span className="text-sm text-muted-foreground">None</span>
                              ) : (
                                u.roles.map((r) => (
                                  <Badge key={r.id} variant="secondary">{r.name}</Badge>
                                ))
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="permissions">
            <Card>
              <CardContent className="pt-4">
                {ctx.columns.length === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    No columns yet. Upload a Parquet file to generate columns.
                  </p>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Column</TableHead>
                        <TableHead>Roles with Access</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {ctx.columns.map((col) => (
                        <TableRow key={col.id}>
                          <TableCell>
                            <code className="text-xs font-mono">{col.name}</code>
                          </TableCell>
                          <TableCell>
                            <div className="flex flex-wrap gap-1">
                              {col.roles.length === 0 ? (
                                <span className="text-sm text-muted-foreground">None</span>
                              ) : (
                                col.roles.map((r) => (
                                  <Badge key={r.id} variant="secondary">{r.name}</Badge>
                                ))
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
}
