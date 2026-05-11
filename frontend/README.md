# OSWS Admin UI

Web frontend for OSWS — manage RBAC roles, users, column permissions, and S3 credentials.

## Tech stack

React 19, TypeScript, Vite 7, Tailwind CSS v4, shadcn/ui, CodeMirror 6, react-oidc-context, react-router-dom v7.

## Setup

```bash
bun install
cp .env.example .env
```

Edit `.env`:

```env
VITE_API_BASE_URL=http://localhost:5000
VITE_OIDC_AUTHORITY=https://your-oidc-provider
VITE_OIDC_CLIENT_ID=your-client-id
```

## Commands

| Command         | Description                        |
| --------------- | ---------------------------------- |
| `bun dev`       | Start dev server at :5173          |
| `bun build`     | Production build to `dist/`        |
| `bun test`      | Run tests (Vitest)                 |
| `bun lint`      | ESLint                             |
| `bun preview`   | Preview production build           |

## Pages

| Route              | Description                                               |
| ------------------ | --------------------------------------------------------- |
| `/`                | Login page — redirects to OIDC provider                   |
| `/dashboard`       | Profile info and assigned role badges                     |
| `/credentials`     | Create/revoke S3 credentials with optional default role   |
| `/admin/query`     | SQL-style admin editor for RBAC (admin only)              |

## Notes

- **Authentication** via react-oidc-context (OIDC PKCE flow). Users are JIT-provisioned in the backend on first login.
- **Admin detection**: the OIDC provider must set an `isRbacAdmin` claim. Admin users see the query editor; non-admins see only dashboard and credentials.
- **Admin query language**: a PEG grammar (`src/lib/query.peggy`) parses SQL-style `GRANT`/`REVOKE`/`CREATE ROLE`/`DROP ROLE` statements. The editor uses a custom CodeMirror language extension with syntax highlighting.
