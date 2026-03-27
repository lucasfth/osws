import { useAuth } from "react-oidc-context";
import { Navigate, Outlet } from "react-router-dom";
import { Skeleton } from "@/components/ui/skeleton";

export function ProtectedRoute() {
  const auth = useAuth();

  if (auth.isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <Skeleton className="h-8 w-48" />
      </div>
    );
  }

  if (!auth.isAuthenticated) {
    return <Navigate to="/" replace />;
  }

  return <Outlet />;
}
