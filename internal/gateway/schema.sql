CREATE TABLE IF NOT EXISTS gateway_meta (
 id integer PRIMARY KEY CHECK (id=1), version integer NOT NULL CHECK(version=1),
 max_active integer NOT NULL, max_records integer NOT NULL,
 delivery_disabled boolean NOT NULL DEFAULT false,
 owner text NOT NULL DEFAULT '', epoch bigint NOT NULL DEFAULT 0,
 expires timestamptz NOT NULL DEFAULT '-infinity');
CREATE TABLE IF NOT EXISTS gateway_records (
 uid text PRIMARY KEY, namespace text NOT NULL, data bytea NOT NULL,
 mutation text NOT NULL, archived boolean NOT NULL DEFAULT false);
CREATE INDEX IF NOT EXISTS gateway_namespace ON gateway_records(namespace);
CREATE TABLE IF NOT EXISTS gateway_history (
 uid text PRIMARY KEY REFERENCES gateway_records(uid) ON DELETE CASCADE, data bytea NOT NULL);
CREATE TABLE IF NOT EXISTS gateway_keys (
 kind text NOT NULL, namespace text NOT NULL, scope text NOT NULL, key text NOT NULL,
 uid text NOT NULL REFERENCES gateway_records(uid) ON DELETE CASCADE, digest text NOT NULL,
 PRIMARY KEY(kind, namespace, scope, key));
CREATE TABLE IF NOT EXISTS gateway_reservations (
 uid text NOT NULL REFERENCES gateway_records(uid) ON DELETE CASCADE, slot text NOT NULL,
 PRIMARY KEY(uid, slot));
CREATE TABLE IF NOT EXISTS gateway_commits (
 mutation text PRIMARY KEY, committed_at timestamptz NOT NULL DEFAULT clock_timestamp());
