import { Navigate, Outlet } from "react-router-dom";
import { useUser } from "../context/UserContext";
import { Skeleton } from "@/components/ui/skeleton";

export function AdminRoute() {
  const { profile, loading } = useUser();

  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <Skeleton className="h-8 w-48" />
      </div>
    );
  }

  if (!profile?.isAdmin) {
    return <Navigate to="/dashboard" replace />;
  }

  return <Outlet />;
}
