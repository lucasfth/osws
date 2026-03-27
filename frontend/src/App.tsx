import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { AdminRoute } from "./components/AdminRoute";
import { Layout } from "./components/Layout";
import { ProtectedRoute } from "./components/ProtectedRoute";
import { UserContextProvider } from "./context/UserContext";
import { CredentialsPage } from "./pages/CredentialsPage";
import { DashboardPage } from "./pages/DashboardPage";
import { LoginPage } from "./pages/LoginPage";
import { QueryPage } from "./pages/admin/QueryPage";
import { TooltipProvider } from "@/components/ui/tooltip";

function App() {
  return (
    <TooltipProvider>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<LoginPage />} />

        <Route element={<ProtectedRoute />}>
          <Route element={<UserContextProvider><Layout /></UserContextProvider>}>
            <Route path="/dashboard" element={<DashboardPage />} />
            <Route path="/credentials" element={<CredentialsPage />} />

            <Route element={<AdminRoute />}>
              <Route path="/admin/query" element={<QueryPage />} />
            </Route>
          </Route>
        </Route>

        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
    </TooltipProvider>
  );
}

export default App;
