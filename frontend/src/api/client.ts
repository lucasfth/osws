import { useCallback } from "react";
import { useAuth } from "react-oidc-context";

const API_BASE = import.meta.env.VITE_API_BASE_URL as string;

export function useApi() {
  const auth = useAuth();
  const token = auth.user?.access_token;

  return useCallback(
    async <T>(path: string, options?: RequestInit): Promise<T> => {
      const res = await fetch(`${API_BASE}${path}`, {
        ...options,
        headers: {
          "Content-Type": "application/json",
          ...options?.headers,
          Authorization: `Bearer ${token}`,
        },
      });
      if (!res.ok) throw new Error(`${res.status}: ${res.statusText}`);
      if (res.status === 204) return undefined as T;
      return res.json() as Promise<T>;
    },
    [token]
  );
}
