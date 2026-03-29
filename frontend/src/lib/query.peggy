// OSWS RBAC "Query Language"
// Compiled to a JS parser module at build time by the Vite plugin in vite.config.ts.
// Called per-line by queryParser.ts; this grammar parses a single statement.
//
// To test a statement manually:
//   bunx peggy --test "GRANT ACCESS ON ssn TO analyst" src/lib/query.peggy

Statement
  = _ s:Stmt _ { return s }

// PEG tries alternatives in order.
// GrantAccess / RevokeAccess must precede GrantRole / RevokeRole because both
// begin with the same keyword. The more specific pattern wins.
Stmt
  = GrantAccess
  / RevokeAccess
  / CreateRole
  / DropRole
  / GrantRole
  / RevokeRole

GrantAccess
  = GRANT _ ACCESS _ ON _ col:Ident _ TO _ role:Ident
    { return { type: "GRANT_ACCESS", columnName: col, roleName: role } }

RevokeAccess
  = REVOKE _ ACCESS _ ON _ col:Ident _ FROM _ role:Ident
    { return { type: "REVOKE_ACCESS", columnName: col, roleName: role } }

CreateRole
  = CREATE _ ROLE _ name:Ident
    { return { type: "CREATE_ROLE", name: name } }

DropRole
  = DROP _ ROLE _ name:Ident
    { return { type: "DROP_ROLE", name: name } }

GrantRole
  = GRANT _ role:Ident _ TO _ user:Ident
    { return { type: "GRANT_ROLE", roleName: role, userIdentifier: user } }

RevokeRole
  = REVOKE _ role:Ident _ FROM _ user:Ident
    { return { type: "REVOKE_ROLE", roleName: role, userIdentifier: user } }

// Keywords: !IdentTail prevents "grant_admin" from lexing as GRANT + "_admin"
GRANT  = "grant"i  !IdentTail
REVOKE = "revoke"i !IdentTail
CREATE = "create"i !IdentTail
DROP   = "drop"i   !IdentTail
ROLE   = "role"i   !IdentTail
ACCESS = "access"i !IdentTail
ON     = "on"i     !IdentTail
TO     = "to"i     !IdentTail
FROM   = "from"i   !IdentTail

// Identifiers: alphanumeric, dots, underscores, @ and hyphens (covers emails)
Ident     = chars:[a-zA-Z0-9_.@-]+ { return chars.join("") }
IdentTail = [a-zA-Z0-9_]
_         = [ \t\n\r]*
