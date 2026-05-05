"""
Minimal DB seeding for E2E tests.

seed_users()       — inserts 4 users (admin, analyst, junior, intern), returns their IDs.
seed_credentials() — inserts S3 credentials once role IDs are known from admin API.
"""

import psycopg2

CONN = "host=localhost port=5433 dbname=osws_e2e_test user=postgres password=postgres"

ANALYST_ACCESS_KEY = "E2EANALYSTKEY00001"
ANALYST_SECRET_KEY = "e2e-analyst-secret-key-for-sigv4-testing-1234"
JUNIOR_ACCESS_KEY = "E2EJUNIORKEY000001"
JUNIOR_SECRET_KEY = "e2e-junior-secret-key-for-sigv4-testing-5678"
INTERN_ACCESS_KEY = "E2EINTERNKEY000001"
INTERN_SECRET_KEY = "e2e-intern-secret-key-for-sigv4-testing-5678"


def seed_users():
    """Seed the four base users. Returns (admin_id, analyst_id, junior_id, intern_id)."""
    with psycopg2.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO "Users" ("Name", "IsRbacAdmin")
                VALUES ('e2e-admin', true), ('e2e-analyst', false), ('e2e-junior', false), ('e2e-intern', false)
                RETURNING "Id"
                """
            )
            ids = [r[0] for r in cur.fetchall()]
        conn.commit()
    return ids[0], ids[1], ids[2], ids[3]


def seed_credentials(
    analyst_id, analyst_role_id, junior_id, junior_role_id, intern_id, intern_role_id
):
    """Seed S3 credentials after role IDs are known (from admin API)."""
    with psycopg2.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO "S3Credentials"
                    ("AccessKeyId", "SecretKey", "UserId", "IsActive",
                     "DefaultRoleId", "CreatedAt")
                VALUES
                    (%s, %s, %s, true, %s, NOW()),
                    (%s, %s, %s, true, %s, NOW()),
                    (%s, %s, %s, true, %s, NOW())
                """,
                (
                    ANALYST_ACCESS_KEY,
                    ANALYST_SECRET_KEY,
                    analyst_id,
                    analyst_role_id,
                    JUNIOR_ACCESS_KEY,
                    JUNIOR_SECRET_KEY,
                    junior_id,
                    junior_role_id,
                    INTERN_ACCESS_KEY,
                    INTERN_SECRET_KEY,
                    intern_id,
                    intern_role_id,
                ),
            )
        conn.commit()
