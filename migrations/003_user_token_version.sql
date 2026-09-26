-- Migration 003: Add token_version to users table
-- Part of BUG-001 fix: JWT role not re-validated after role change.
-- Incrementing token_version invalidates all existing JWTs for that user,
-- forcing re-login and picking up the new role from the DB.

ALTER TABLE users ADD COLUMN IF NOT EXISTS token_version INTEGER NOT NULL DEFAULT 0;

COMMENT ON COLUMN users.token_version IS
    'Incremented on role or department change to invalidate all existing JWTs for that user. '
    'Included in the JWT payload as the "tv" claim and validated on every request.';
