// OSWS RBAC "Query Language"
// Compiled to a JS parser module at build time by the Vite plugin in vite.config.ts.
// Called per-line by queryParser.ts; this grammar parses a single statement.
//
// To test a statement manually:
//   bunx peggy --test "GRANT ACCESS ON ssn TO analyst" src/lib/query.peggy

Statement
  = _ s:Stmt _ { return s }

// PEG tries alternatives in order.
// GrantAccess / RevokeAccess must precede GrantRole* / RevokeRole* because both
// begin with the same keyword. The more specific pattern wins.
Stmt
  = GrantAccess
  / RevokeAccess
  / CreateRole
  / DropRole
  / GrantRoleToUser
  / GrantRoleToRole
  / RevokeRoleFromUser
  / RevokeRoleFromRole

GrantAccess
  = GRANT _ ACCESS _ ON _ cols:List _ TO _ roles:List
    { return { type: "GRANT_ACCESS", columnNames: cols, roleNames: roles } }

RevokeAccess
  = REVOKE _ ACCESS _ ON _ cols:List _ FROM _ roles:List
    { return { type: "REVOKE_ACCESS", columnNames: cols, roleNames: roles} }

CreateRole
  = CREATE _ ROLE _ names:List
    { return { type: "CREATE_ROLE", names: names } }

DropRole
  = DROP _ ROLE _ names:List
    { return { type: "DROP_ROLE", names: names } }

GrantRoleToUser
  = GRANT _ roles:List _ TO _ USER _ users:List
    { return { type: "GRANT_ROLE_TO_USER", roleNames: roles, userNames: users } }

GrantRoleToRole
  = GRANT _ roles:List _ TO _ ROLE _ parents:List
    { return { type: "GRANT_ROLE_TO_ROLE", roleNames: roles, parentRoleNames: parents } }

RevokeRoleFromUser
  = REVOKE _ roles:List _ FROM _ USER _ users:List
    { return { type: "REVOKE_ROLE_FROM_USER", roleNames: roles, userNames: users } }

RevokeRoleFromRole
  = REVOKE _ roles:List _ FROM _ ROLE _ parents:List
    { return { type: "REVOKE_ROLE_FROM_ROLE", roleNames: roles, parentRoleNames: parents } }

// Keywords: !IdentTail prevents "grant_admin" from lexing as GRANT + "_admin"
GRANT  = "grant"i  !IdentTail
REVOKE = "revoke"i !IdentTail
CREATE = "create"i !IdentTail
DROP   = "drop"i   !IdentTail
ROLE   = "role"i   !IdentTail
USER   = "user"i   !IdentTail
ACCESS = "access"i !IdentTail
ON     = "on"i     !IdentTail
TO     = "to"i     !IdentTail
FROM   = "from"i   !IdentTail

// Identifiers: alphanumeric, dots, underscores, @ and hyphens (covers emails)
Ident     = chars:[a-zA-Z0-9_.@-]+ { return chars.join("") }
IdentTail = [a-zA-Z0-9_]
_         = [ \t\n\r]*

List = Ident|1.., _"," _|

