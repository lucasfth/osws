import { useEffect, useState } from "react";
import { useAuth } from "react-oidc-context";

const API_BASE = import.meta.env.VITE_API_BASE_URL as string;

function useApiCall(auth: ReturnType<typeof useAuth>) {
  return async (path: string, options?: RequestInit) => {
    const token = auth.user?.access_token;
    const res = await fetch(`${API_BASE}${path}`, {
      ...options,
      headers: {
        ...options?.headers,
        Authorization: `Bearer ${token}`,
      },
    });
    if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);
    return res;
  };
}

function App() {
  const auth = useAuth();
  const apiCall = useApiCall(auth);
  const [provisioned, setProvisioned] = useState(false);

  useEffect(() => {
    if (!auth.isAuthenticated) return;
    apiCall("/api/me", { method: "GET" })
      .then(() => setProvisioned(true))
      .catch((err) => console.error("Provisioning failed:", err));
  }, [auth.isAuthenticated]);

  if (auth.isLoading) {
    return <p>Loading…</p>;
  }

  if (auth.error) {
    return <p>Auth error: {auth.error.message}</p>;
  }

  if (auth.isAuthenticated) {
    return (
      <div>
        <h1>Welcome, {auth.user?.profile.name ?? "user"}</h1>
        <p>{auth.user?.profile.email}</p>
        {!provisioned && <p>Provisioning user…</p>}
        <button onClick={() => auth.removeUser()}>Sign out</button>
      </div>
    );
  }

  return (
    <div>
      <h1>OSWS</h1>
      <button onClick={() => auth.signinRedirect()}>Sign in</button>
    </div>
  );
}

export default App;
