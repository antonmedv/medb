# MeDB server

MeDB server exposes a database over HTTP.

## Install

```sh
go install github.com/antonmedv/medb/cmd/medb@latest
```

## Start with authentication

Generate the initial administrator token once and keep the file private:

```sh
medb token generate > admin-token
chmod 600 admin-token

MEDB_INIT_ADMIN_TOKEN_FILE="$PWD/admin-token" \
  medb serve --dir ./data
```

The server listens on `127.0.0.1:6332` by default. In another shell:

```sh
TOKEN="$(cat admin-token)"

curl -sS http://127.0.0.1:6332/v1/set \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"collection":"notes/team","id":"roadmap/2026?#","document":{"status":"draft"}}'

curl -sS http://127.0.0.1:6332/v1/get \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"collection":"notes/team","id":"roadmap/2026?#"}'
```

The second request returns:

```json
{"document":{"status":"draft"}}
```

Containers can use `MEDB_DIR`, `MEDB_LISTEN`, and `MEDB_INIT_ADMIN_TOKEN_FILE`.

## Start without authentication

For a trusted local or externally secured environment:

```sh
medb serve --dir ./data --no-auth
```

`MEDB_NO_AUTH=true` is equivalent. This grants unauthenticated admin access to
all data endpoints.

## API

See the [HTTP API reference](API.md) for endpoints, request schemas, roles, and
errors.

## Recovery and transport

If all administrator credentials are lost, stop the server and run:

```sh
medb auth recover --dir ./data --name admin
```

Bearer tokens require TLS when the server is reachable over an untrusted
network. MeDB binds to loopback by default and does not terminate TLS itself.
