import { describe, it, expect } from 'vitest'
import { parse } from './query.peggy'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Asserts the input parses successfully and returns the result. */
function ok(input: string) {
  return expect(() => parse(input)).not.toThrow()
}

/** Asserts the input fails to parse. */
function fails(input: string) {
  return expect(() => parse(input)).toThrow()
}

// ---------------------------------------------------------------------------
// GRANT ACCESS ON <cols> TO <roles>
// ---------------------------------------------------------------------------

describe('GrantAccess', () => {
  it('single column, single role', () => {
    expect(parse('GRANT ACCESS ON ssn TO analyst')).toEqual({
      type: 'GRANT_ACCESS',
      columnNames: ['ssn'],
      roleNames: ['analyst'],
    })
  })

  it('multiple columns, single role', () => {
    expect(parse('GRANT ACCESS ON ssn, dob TO analyst')).toEqual({
      type: 'GRANT_ACCESS',
      columnNames: ['ssn', 'dob'],
      roleNames: ['analyst'],
    })
  })

  it('single column, multiple roles', () => {
    expect(parse('GRANT ACCESS ON ssn TO analyst, admin')).toEqual({
      type: 'GRANT_ACCESS',
      columnNames: ['ssn'],
      roleNames: ['analyst', 'admin'],
    })
  })

  it('multiple columns, multiple roles', () => {
    expect(parse('GRANT ACCESS ON ssn, dob, salary TO analyst, admin, auditor')).toEqual({
      type: 'GRANT_ACCESS',
      columnNames: ['ssn', 'dob', 'salary'],
      roleNames: ['analyst', 'admin', 'auditor'],
    })
  })

  it('is case-insensitive', () => {
    expect(parse('grant access on ssn to analyst')).toMatchObject({ type: 'GRANT_ACCESS' })
    expect(parse('Grant Access On ssn To analyst')).toMatchObject({ type: 'GRANT_ACCESS' })
  })

  it('tolerates extra whitespace', () => {
    ok('  GRANT  ACCESS  ON  ssn  TO  analyst  ')
  })

  it('rejects missing ON clause', () => {
    fails('GRANT ACCESS ssn TO analyst')
  })

  it('rejects missing TO clause', () => {
    fails('GRANT ACCESS ON ssn analyst')
  })
})

// ---------------------------------------------------------------------------
// REVOKE ACCESS ON <cols> FROM <roles>
// ---------------------------------------------------------------------------

describe('RevokeAccess', () => {
  it('single column, single role', () => {
    expect(parse('REVOKE ACCESS ON ssn FROM analyst')).toEqual({
      type: 'REVOKE_ACCESS',
      columnNames: ['ssn'],
      roleNames: ['analyst'],
    })
  })

  it('multiple columns, single role', () => {
    expect(parse('REVOKE ACCESS ON ssn, dob FROM analyst')).toEqual({
      type: 'REVOKE_ACCESS',
      columnNames: ['ssn', 'dob'],
      roleNames: ['analyst'],
    })
  })

  it('single column, multiple roles', () => {
    expect(parse('REVOKE ACCESS ON ssn FROM analyst, admin')).toEqual({
      type: 'REVOKE_ACCESS',
      columnNames: ['ssn'],
      roleNames: ['analyst', 'admin'],
    })
  })

  it('multiple columns, multiple roles', () => {
    expect(parse('REVOKE ACCESS ON ssn, dob, salary FROM analyst, admin, auditor')).toEqual({
      type: 'REVOKE_ACCESS',
      columnNames: ['ssn', 'dob', 'salary'],
      roleNames: ['analyst', 'admin', 'auditor'],
    })
  })

  it('is case-insensitive', () => {
    expect(parse('revoke access on ssn from analyst')).toMatchObject({ type: 'REVOKE_ACCESS' })
  })

  it('rejects missing FROM clause', () => {
    fails('REVOKE ACCESS ON ssn analyst')
  })
})

// ---------------------------------------------------------------------------
// CREATE ROLE <names>
// ---------------------------------------------------------------------------

describe('CreateRole', () => {
  it('single name', () => {
    expect(parse('CREATE ROLE analyst')).toEqual({
      type: 'CREATE_ROLE',
      names: ['analyst'],
    })
  })

  it('multiple names', () => {
    expect(parse('CREATE ROLE analyst, admin, auditor')).toEqual({
      type: 'CREATE_ROLE',
      names: ['analyst', 'admin', 'auditor'],
    })
  })

  it('is case-insensitive', () => {
    expect(parse('create role analyst')).toMatchObject({ type: 'CREATE_ROLE' })
  })

  it('allows hyphens and underscores in names', () => {
    expect(parse('CREATE ROLE data-analyst, data_admin')).toEqual({
      type: 'CREATE_ROLE',
      names: ['data-analyst', 'data_admin'],
    })
  })

  it('rejects missing name', () => {
    fails('CREATE ROLE')
  })
})

// ---------------------------------------------------------------------------
// DROP ROLE <names>
// ---------------------------------------------------------------------------

describe('DropRole', () => {
  it('single name', () => {
    expect(parse('DROP ROLE analyst')).toEqual({
      type: 'DROP_ROLE',
      names: ['analyst'],
    })
  })

  it('multiple names', () => {
    expect(parse('DROP ROLE analyst, admin, auditor')).toEqual({
      type: 'DROP_ROLE',
      names: ['analyst', 'admin', 'auditor'],
    })
  })

  it('is case-insensitive', () => {
    expect(parse('drop role analyst')).toMatchObject({ type: 'DROP_ROLE' })
  })

  it('rejects missing name', () => {
    fails('DROP ROLE')
  })
})

// ---------------------------------------------------------------------------
// GRANT <roles> TO USER <users>
// ---------------------------------------------------------------------------

describe('GrantRoleToUser', () => {
  it('single role, single user', () => {
    expect(parse('GRANT analyst TO USER alice')).toEqual({
      type: 'GRANT_ROLE_TO_USER',
      roleNames: ['analyst'],
      userNames: ['alice'],
    })
  })

  it('multiple roles, single user', () => {
    expect(parse('GRANT analyst, admin TO USER alice')).toEqual({
      type: 'GRANT_ROLE_TO_USER',
      roleNames: ['analyst', 'admin'],
      userNames: ['alice'],
    })
  })

  it('single role, multiple users', () => {
    expect(parse('GRANT analyst TO USER alice, bob')).toEqual({
      type: 'GRANT_ROLE_TO_USER',
      roleNames: ['analyst'],
      userNames: ['alice', 'bob'],
    })
  })

  it('multiple roles, multiple users', () => {
    expect(parse('GRANT analyst, admin TO USER alice, bob, carol')).toEqual({
      type: 'GRANT_ROLE_TO_USER',
      roleNames: ['analyst', 'admin'],
      userNames: ['alice', 'bob', 'carol'],
    })
  })

  it('supports email-like usernames', () => {
    expect(parse('GRANT analyst TO USER alice@example.com')).toMatchObject({
      type: 'GRANT_ROLE_TO_USER',
      userNames: ['alice@example.com'],
    })
  })

  it('is case-insensitive', () => {
    expect(parse('grant analyst to user alice')).toMatchObject({ type: 'GRANT_ROLE_TO_USER' })
  })

  it('rejects missing USER keyword', () => {
    fails('GRANT analyst TO alice')
  })
})

// ---------------------------------------------------------------------------
// GRANT <roles> TO ROLE <parents>
// ---------------------------------------------------------------------------

describe('GrantRoleToRole', () => {
  it('single role, single parent', () => {
    expect(parse('GRANT analyst TO ROLE senior')).toEqual({
      type: 'GRANT_ROLE_TO_ROLE',
      roleNames: ['analyst'],
      parentRoleNames: ['senior'],
    })
  })

  it('multiple roles, single parent', () => {
    expect(parse('GRANT analyst, auditor TO ROLE senior')).toEqual({
      type: 'GRANT_ROLE_TO_ROLE',
      roleNames: ['analyst', 'auditor'],
      parentRoleNames: ['senior'],
    })
  })

  it('single role, multiple parents', () => {
    expect(parse('GRANT analyst TO ROLE senior, manager')).toEqual({
      type: 'GRANT_ROLE_TO_ROLE',
      roleNames: ['analyst'],
      parentRoleNames: ['senior', 'manager'],
    })
  })

  it('multiple roles, multiple parents', () => {
    expect(parse('GRANT analyst, auditor TO ROLE senior, manager, director')).toEqual({
      type: 'GRANT_ROLE_TO_ROLE',
      roleNames: ['analyst', 'auditor'],
      parentRoleNames: ['senior', 'manager', 'director'],
    })
  })

  it('is case-insensitive', () => {
    expect(parse('grant analyst to role senior')).toMatchObject({ type: 'GRANT_ROLE_TO_ROLE' })
  })

  it('rejects missing ROLE keyword', () => {
    fails('GRANT analyst TO senior')
  })
})

// ---------------------------------------------------------------------------
// REVOKE <roles> FROM USER <users>
// ---------------------------------------------------------------------------

describe('RevokeRoleFromUser', () => {
  it('single role, single user', () => {
    expect(parse('REVOKE analyst FROM USER alice')).toEqual({
      type: 'REVOKE_ROLE_FROM_USER',
      roleNames: ['analyst'],
      userNames: ['alice'],
    })
  })

  it('multiple roles, single user', () => {
    expect(parse('REVOKE analyst, admin FROM USER alice')).toEqual({
      type: 'REVOKE_ROLE_FROM_USER',
      roleNames: ['analyst', 'admin'],
      userNames: ['alice'],
    })
  })

  it('single role, multiple users', () => {
    expect(parse('REVOKE analyst FROM USER alice, bob')).toEqual({
      type: 'REVOKE_ROLE_FROM_USER',
      roleNames: ['analyst'],
      userNames: ['alice', 'bob'],
    })
  })

  it('multiple roles, multiple users', () => {
    expect(parse('REVOKE analyst, admin FROM USER alice, bob, carol')).toEqual({
      type: 'REVOKE_ROLE_FROM_USER',
      roleNames: ['analyst', 'admin'],
      userNames: ['alice', 'bob', 'carol'],
    })
  })

  it('supports email-like usernames', () => {
    expect(parse('REVOKE analyst FROM USER alice@example.com')).toMatchObject({
      type: 'REVOKE_ROLE_FROM_USER',
      userNames: ['alice@example.com'],
    })
  })

  it('is case-insensitive', () => {
    expect(parse('revoke analyst from user alice')).toMatchObject({ type: 'REVOKE_ROLE_FROM_USER' })
  })
})

// ---------------------------------------------------------------------------
// REVOKE <roles> FROM ROLE <parents>
// ---------------------------------------------------------------------------

describe('RevokeRoleFromRole', () => {
  it('single role, single parent', () => {
    expect(parse('REVOKE analyst FROM ROLE senior')).toEqual({
      type: 'REVOKE_ROLE_FROM_ROLE',
      roleNames: ['analyst'],
      parentRoleNames: ['senior'],
    })
  })

  it('multiple roles, single parent', () => {
    expect(parse('REVOKE analyst, auditor FROM ROLE senior')).toEqual({
      type: 'REVOKE_ROLE_FROM_ROLE',
      roleNames: ['analyst', 'auditor'],
      parentRoleNames: ['senior'],
    })
  })

  it('single role, multiple parents', () => {
    expect(parse('REVOKE analyst FROM ROLE senior, manager')).toEqual({
      type: 'REVOKE_ROLE_FROM_ROLE',
      roleNames: ['analyst'],
      parentRoleNames: ['senior', 'manager'],
    })
  })

  it('multiple roles, multiple parents', () => {
    expect(parse('REVOKE analyst, auditor FROM ROLE senior, manager, director')).toEqual({
      type: 'REVOKE_ROLE_FROM_ROLE',
      roleNames: ['analyst', 'auditor'],
      parentRoleNames: ['senior', 'manager', 'director'],
    })
  })

  it('is case-insensitive', () => {
    expect(parse('revoke analyst from role senior')).toMatchObject({ type: 'REVOKE_ROLE_FROM_ROLE' })
  })
})

// ---------------------------------------------------------------------------
// Identifier rules
// ---------------------------------------------------------------------------

describe('Identifier', () => {
  it('accepts alphanumeric characters', () => {
    ok('CREATE ROLE role123')
  })

  it('accepts dots (e.g. table.column)', () => {
    expect(parse('GRANT ACCESS ON table.column TO analyst')).toMatchObject({
      columnNames: ['table.column'],
    })
  })

  it('accepts underscores', () => {
    ok('CREATE ROLE data_analyst')
  })

  it('accepts hyphens', () => {
    ok('CREATE ROLE data-analyst')
  })

  it('accepts @ signs (email usernames)', () => {
    ok('GRANT analyst TO USER user@domain.com')
  })
})

// ---------------------------------------------------------------------------
// Keyword boundary guard
// ---------------------------------------------------------------------------

describe('Keyword boundary', () => {
  it('does not lex grant_admin as GRANT', () => {
    fails('grant_admin analyst TO USER alice')
  })

  it('does not lex revoke_all as REVOKE', () => {
    fails('revoke_all analyst FROM USER alice')
  })

  it('does not lex create_role as CREATE', () => {
    fails('create_role analyst')
  })

  it('does not lex dropzone as DROP', () => {
    fails('dropzone analyst')
  })
})

// ---------------------------------------------------------------------------
// Invalid / malformed input
// ---------------------------------------------------------------------------

describe('Invalid input', () => {
  it('rejects empty string', () => {
    fails('')
  })

  it('rejects garbage input', () => {
    fails('not a valid statement')
  })

  it('rejects incomplete GRANT ACCESS', () => {
    fails('GRANT ACCESS ON')
    fails('GRANT ACCESS ON ssn')
    fails('GRANT ACCESS ON ssn TO')
  })

  it('rejects incomplete CREATE ROLE', () => {
    fails('CREATE')
    fails('CREATE ROLE')
  })

  it('rejects incomplete GRANT role-to-user', () => {
    fails('GRANT analyst TO USER')
    fails('GRANT analyst TO')
    // "TO" is consumed by the roles List as an Ident (PEG is greedy, no backtracking),
    // so the TO keyword fails to match afterwards
    fails('GRANT TO USER alice')
  })
})
