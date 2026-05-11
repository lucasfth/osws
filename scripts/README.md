# Scripts

Utility scripts for OSWS key management.

## Prerequisites

Scripts require [Bun](https://bun.sh) or [Zig](https://ziglang.org/) depending on the script.

## removeKeysByDate.ts (Bun)

Removes keys from an Azure Key Vault older than a specified date.

```bash
bun removeKeysByDate.ts <vault-name> <concurrency>
```

Concurrency defaults to 10. Optionally pass a date filter:

```bash
bun removeKeysByDate.ts <vault-name> <concurrency> <days>
```

## listKeys.ts (Bun)

Lists all keys in an Azure Key Vault.

```bash
bun listKeys.ts <vault-name>
```

## check_architecture.zig (Zig)

Checks the project's .NET project references to verify the architecture dependency tree is consistent (layered architecture enforcement).

```bash
# From repo root
zig run scripts/check_architecture.zig -- .
```
