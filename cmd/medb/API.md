# MeDB HTTP API

The default base URL is `http://127.0.0.1:6332`. See the
[server README](README.md) for setup and curl examples.

## Basics

Authenticated requests use `Authorization: Bearer TOKEN`. Every `POST` also
requires `Content-Type: application/json`.

Collection names may contain lowercase letters, digits, `-`, `_`, and `/`.
The `_meta` namespace is reserved. Document IDs may be any valid UTF-8 string,
including an empty string or characters such as `/`, `?`, and `#`. Documents
may be any JSON value, including `null`.

## Roles

| Role     | Access                                     |
|----------|--------------------------------------------|
| `reader` | List, get, check, count, and scan          |
| `writer` | Reader access plus set and delete          |
| `admin`  | Writer access plus drop, users, and tokens |

With `--no-auth` or `MEDB_NO_AUTH=true`, data endpoints need no token and have
admin access. User and token endpoints are unavailable.

## Health

`GET /healthz` is public and returns:

```json
{"status":"ok"}
```

## Data

The common request bodies are:

```json
{"collection":"prod/eu/users","id":"ada/primary"}
```

```json
{"collection":"prod/eu/users"}
```

| Endpoint              | Role     | Body                                   | Success                                   |
|-----------------------|----------|----------------------------------------|-------------------------------------------|
| `GET /v1/collections` | `reader` | None                                   | `200` `{"collections":["audit","users"]}` |
| `POST /v1/get`        | `reader` | Collection and ID                      | `200` `{"document":VALUE}`                |
| `POST /v1/set`        | `writer` | Collection, ID, and `"document":VALUE` | `204`                                     |
| `POST /v1/delete`     | `writer` | Collection and ID                      | `204`                                     |
| `POST /v1/has`        | `reader` | Collection and ID                      | `200` `{"exists":true}`                   |
| `POST /v1/count`      | `reader` | Collection                             | `200` `{"count":42}`                      |
| `POST /v1/scan`       | `reader` | Collection                             | `200` NDJSON stream                       |
| `POST /v1/drop`       | `admin`  | Collection                             | `204`                                     |

Set requests look like:

```json
{
  "collection":"prod/eu/users",
  "id":"ada/primary",
  "document":{"name":"Ada"}
}
```

Scan responses contain one document per line:

```jsonl
{"id":"ada/primary","document":{"name":"Ada"}}
{"id":"grace","document":{"name":"Grace"}}
```

Get returns `404 not_found` when the document is missing. For a missing target,
has returns `false`, count returns zero, and delete and drop remain successful.

## Users

All user endpoints require `admin`. A user response is:

```json
{
  "id":"4d197c71c1ec4e778db714226e28afe3",
  "name":"deployment",
  "role":"writer",
  "disabled":false,
  "created_at":"2026-08-21T12:00:00Z",
  "updated_at":"2026-08-21T12:00:00Z"
}
```

| Endpoint                     | Request body                                            | Success                 |
|------------------------------|---------------------------------------------------------|-------------------------|
| `GET /v1/auth/users`         | None                                                    | `200` `{"users":[...]}` |
| `POST /v1/auth/users/create` | `{"name":"deployment","role":"writer"}`                 | `201` `{"user":{...}}`  |
| `POST /v1/auth/users/update` | `{"user_id":"USER_ID","role":"reader","disabled":true}` | `200` `{"user":{...}}`  |

Roles are `reader`, `writer`, or `admin`. For updates, `role` and `disabled`
are optional, but at least one is required. Disabling a user invalidates all of
their tokens. Creating a user does not create a token.

## Tokens

All token endpoints require `admin`.

| Endpoint                      | Request body                                                   | Success                  |
|-------------------------------|----------------------------------------------------------------|--------------------------|
| `POST /v1/auth/tokens/create` | `{"user_id":"USER_ID","label":"deployment","expires_at":null}` | `201` token response     |
| `POST /v1/auth/tokens/list`   | `{"user_id":"USER_ID"}`                                        | `200` `{"tokens":[...]}` |
| `POST /v1/auth/tokens/revoke` | `{"token_id":"TOKEN_ID"}`                                      | `204`                    |

`expires_at` is optional and accepts `null` or a future UTC RFC 3339 timestamp.
The target user must exist and be enabled. Token creation returns:

```json
{
  "token_id":"e1b85b27d6b8702a9b7f258e52856ccb1d0165d35594052f46132705376c35c9",
  "token":"medb_fD5TjVQk1YgQ2m...",
  "user_id":"4d197c71c1ec4e778db714226e28afe3",
  "label":"deployment",
  "created_at":"2026-08-21T12:00:00Z",
  "expires_at":null
}
```

The plaintext `token` is returned only once and never appears in token-list
responses. List entries contain `token_id`, `user_id`, `label`, `created_at`,
and `expires_at`. Revoking a missing token is successful.

## Errors

Errors use this shape. Use `error.code` in client logic.

```json
{"error":{"code":"not_found","message":"document not found"}}
```

| Status | Code                      | Meaning                                         |
|-------:|---------------------------|-------------------------------------------------|
|  `400` | `invalid_json`            | Body is not valid JSON                          |
|  `400` | `invalid_request`         | Body does not match the endpoint schema         |
|  `400` | `invalid_collection`      | Collection name is invalid                      |
|  `400` | `invalid_id`              | Document ID is invalid or too large             |
|  `401` | `authentication_required` | Authorization header is missing                 |
|  `401` | `invalid_token`           | Token is invalid, expired, revoked, or disabled |
|  `403` | `forbidden`               | Role does not permit the operation              |
|  `403` | `reserved_collection`     | Request targets `_meta`                         |
|  `404` | `not_found`               | Document or user does not exist                 |
|  `404` | `route_not_found`         | Route does not exist                            |
|  `405` | `method_not_allowed`      | HTTP method is not supported                    |
|  `409` | `user_disabled`           | Token target is disabled                        |
|  `413` | `document_too_large`      | Document exceeds its limit                      |
|  `413` | `request_too_large`       | Request exceeds its limit                       |
|  `415` | `unsupported_media_type`  | Request is not uncompressed JSON                |
|  `500` | `storage_error`           | Storage operation failed                        |
|  `500` | `internal_error`          | Server operation failed                         |
|  `503` | `unavailable`             | Server is unavailable or shutting down          |
