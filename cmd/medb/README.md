# MeDB server

MeDB includes a small HTTP server for exposing one database. It uses fixed HTTP
RPC endpoints with JSON bodies, so collection names and document IDs may
contain `/` and other URL-significant characters.

## Install

From the repository root:

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

The server listens on `127.0.0.1:8080` by default. In another shell:

```sh
TOKEN="$(cat admin-token)"

curl -sS http://127.0.0.1:8080/v1/set \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"collection":"notes/team","id":"roadmap/2026?#","document":{"status":"draft"}}'

curl -sS http://127.0.0.1:8080/v1/get \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"collection":"notes/team","id":"roadmap/2026?#"}'
```

The second request returns:

```json
{"document":{"status":"draft"}}
```

For containers, set `MEDB_DIR`, `MEDB_LISTEN`, and
`MEDB_INIT_ADMIN_TOKEN_FILE`. Initialization variables are used only on the
first authenticated start.

## Start without authentication

For a trusted local or externally secured environment:

```sh
medb serve --dir ./data --no-auth
```

`MEDB_NO_AUTH=true` is equivalent. This opens every data endpoint with admin
permissions, including collection deletion. Authentication-management routes
remain disabled and `_meta` remains inaccessible.

## API

- Public: `GET /healthz`
- Data: `GET /v1/collections` and `POST /v1/get`, `set`, `delete`, `has`,
  `count`, `scan`, and `drop`
- Admin, in authenticated mode: user and token management under `/v1/auth/`

Readers can inspect data, writers can also set and delete, and admins can drop
collections and manage credentials. All `POST` bodies are JSON; scans return
NDJSON.

For complete request and response schemas, authentication rules, and error
codes, see the [HTTP API reference](API.md).

## Recovery and transport

If all administrator credentials are lost, stop the server and run:

```sh
medb auth recover --dir ./data --name admin
```

Bearer tokens require TLS when the server is reachable over an untrusted
network. MeDB binds to loopback by default and does not terminate TLS itself.
