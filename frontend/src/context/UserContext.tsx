import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import { useAuth } from "react-oidc-context";
import { useApi } from "../api/client";
import type { UserProfile } from "../api/types";

interface UserContextValue {
  profile: UserProfile | null;
  loading: boolean;
  error: string | null;
  refresh: () => void;
}

const UserContext = createContext<UserContextValue | null>(null);

export function UserContextProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const auth = useAuth();
  const api = useApi();
  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchProfile = useCallback(() => {
    if (!auth.isAuthenticated) return;
    setLoading(true);
    setError(null);
    api<UserProfile>("/api/me", { method: "GET" })
      .then((p) => setProfile(p))
      .catch((e: unknown) =>
        setError(e instanceof Error ? e.message : "Failed to load profile")
      )
      .finally(() => setLoading(false));
  }, [auth.isAuthenticated, api]);

  useEffect(() => {
    fetchProfile();
  }, [fetchProfile]);

  return (
    <UserContext.Provider value={{ profile, loading, error, refresh: fetchProfile }}>
      {children}
    </UserContext.Provider>
  );
}

export function useUser(): UserContextValue {
  const ctx = useContext(UserContext);
  if (!ctx) throw new Error("useUser must be used within UserContextProvider");
  return ctx;
}
