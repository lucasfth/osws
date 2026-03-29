import { useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "react-oidc-context";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";

export function LoginPage() {
  const auth = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    if (auth.isAuthenticated) {
      navigate("/dashboard", { replace: true });
    }
  }, [auth.isAuthenticated, navigate]);

  if (auth.isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <Skeleton className="h-8 w-48" />
      </div>
    );
  }

  if (auth.error) {
    return (
      <div className="flex min-h-screen flex-col items-center justify-center gap-6">
        <h1 className="text-4xl font-bold">OSWS</h1>
        <Alert variant="destructive" className="max-w-sm">
          <AlertDescription>Auth error: {auth.error.message}</AlertDescription>
        </Alert>
        <Button onClick={() => auth.signinRedirect()}>Try again</Button>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen flex-col items-center justify-center gap-6">
      <h1 className="text-4xl font-bold">OSWS</h1>
      <Button size="lg" onClick={() => auth.signinRedirect()}>
        Sign in
      </Button>
    </div>
  );
}
