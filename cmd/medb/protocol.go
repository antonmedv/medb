package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"
)

type apiFailure struct {
	status  int
	code    string
	message string
}

func failure(status int, code, message string) *apiFailure {
	return &apiFailure{status: status, code: code, message: message}
}

var (
	failInvalidJSON       = failure(http.StatusBadRequest, "invalid_json", "body must be one valid UTF-8 JSON object")
	failInvalidRequest    = failure(http.StatusBadRequest, "invalid_request", "request does not match the endpoint schema")
	failInvalidCollection = failure(http.StatusBadRequest, "invalid_collection", "invalid collection name")
	failInvalidID         = failure(http.StatusBadRequest, "invalid_id", "invalid document ID")
	failAuthRequired      = failure(http.StatusUnauthorized, "authentication_required", "authentication required")
	failInvalidToken      = failure(http.StatusUnauthorized, "invalid_token", "invalid or expired credential")
	failForbidden         = failure(http.StatusForbidden, "forbidden", "insufficient permission")
	failReserved          = failure(http.StatusForbidden, "reserved_collection", "collection belongs to the reserved _meta namespace")
	failNotFound          = failure(http.StatusNotFound, "not_found", "document not found")
	failRouteNotFound     = failure(http.StatusNotFound, "route_not_found", "route not found")
	failMethod            = failure(http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	failUserDisabled      = failure(http.StatusConflict, "user_disabled", "target user is disabled")
	failDocumentTooLarge  = failure(http.StatusRequestEntityTooLarge, "document_too_large", "document exceeds the configured size limit")
	failRequestTooLarge   = failure(http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the configured size limit")
	failMediaType         = failure(http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json without content encoding")
	failStorage           = failure(http.StatusInternalServerError, "storage_error", "database storage operation failed")
	failInternal          = failure(http.StatusInternalServerError, "internal_error", "internal server error")
	failUnavailable       = failure(http.StatusServiceUnavailable, "unavailable", "server is unavailable")
)

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeFailure(w http.ResponseWriter, f *apiFailure) {
	if f.status == http.StatusUnauthorized {
		value := `Bearer realm="medb"`
		if f.code == "invalid_token" {
			value += `, error="invalid_token"`
		}
		w.Header().Set("WWW-Authenticate", value)
	}
	writeJSON(w, f.status, errorEnvelope{Error: errorBody{Code: f.code, Message: f.message}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeNoContent(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func readObject(w http.ResponseWriter, r *http.Request, maxBytes int64, allowed, required []string) (map[string]json.RawMessage, *apiFailure) {
	if r.Header.Get("Content-Encoding") != "" {
		return nil, failMediaType
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, failMediaType
	}
	if r.ContentLength > maxBytes {
		return nil, failRequestTooLarge
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, failRequestTooLarge
		}
		return nil, failInvalidJSON
	}
	if len(body) == 0 || !utf8.Valid(body) || !validJSONUnicode(body) || !json.Valid(body) {
		return nil, failInvalidJSON
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, failInvalidJSON
	}

	fields := make(map[string]json.RawMessage)
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, failInvalidJSON
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, failInvalidJSON
		}
		name, ok := token.(string)
		if !ok {
			return nil, failInvalidJSON
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, failInvalidRequest
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, failInvalidJSON
		}
		fields[name] = raw
	}
	if _, err := decoder.Token(); err != nil {
		return nil, failInvalidJSON
	}

	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allowedSet[name] = struct{}{}
	}
	for name := range fields {
		if _, ok := allowedSet[name]; !ok {
			return nil, failInvalidRequest
		}
	}
	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return nil, failInvalidRequest
		}
	}
	return fields, nil
}

func validJSONUnicode(data []byte) bool {
	inString := false
	for i := 0; i < len(data); {
		c := data[i]
		if !inString {
			if c == '"' {
				inString = true
			}
			i++
			continue
		}
		switch c {
		case '"':
			inString = false
			i++
		case '\\':
			if i+1 >= len(data) {
				return false
			}
			if data[i+1] != 'u' {
				i += 2
				continue
			}
			value, ok := parseHex4(data, i+2)
			if !ok {
				return false
			}
			switch {
			case 0xD800 <= value && value <= 0xDBFF:
				if i+12 > len(data) || data[i+6] != '\\' || data[i+7] != 'u' {
					return false
				}
				low, ok := parseHex4(data, i+8)
				if !ok || low < 0xDC00 || low > 0xDFFF {
					return false
				}
				i += 12
			case 0xDC00 <= value && value <= 0xDFFF:
				return false
			default:
				i += 6
			}
		default:
			i++
		}
	}
	return !inString
}

func parseHex4(data []byte, start int) (uint16, bool) {
	if start+4 > len(data) {
		return 0, false
	}
	var value uint16
	for _, c := range data[start : start+4] {
		value <<= 4
		switch {
		case '0' <= c && c <= '9':
			value |= uint16(c - '0')
		case 'a' <= c && c <= 'f':
			value |= uint16(c-'a') + 10
		case 'A' <= c && c <= 'F':
			value |= uint16(c-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func stringField(fields map[string]json.RawMessage, name string) (string, *apiFailure) {
	var value string
	if err := json.Unmarshal(fields[name], &value); err != nil {
		return "", failInvalidRequest
	}
	return value, nil
}

func optionalStringField(fields map[string]json.RawMessage, name string) (*string, *apiFailure) {
	raw, ok := fields[name]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	value, fail := stringField(fields, name)
	if fail != nil {
		return nil, fail
	}
	return &value, nil
}

func optionalBoolField(fields map[string]json.RawMessage, name string) (*bool, *apiFailure) {
	raw, ok := fields[name]
	if !ok {
		return nil, nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, failInvalidRequest
	}
	return &value, nil
}

func validCollectionName(name string) bool {
	if len(name) == 0 || len(name) > 240 {
		return false
	}
	segment := 0
	for i := range len(name) {
		switch c := name[i]; {
		case c == '/':
			if segment == 0 {
				return false
			}
			segment = 0
		case 'a' <= c && c <= 'z', '0' <= c && c <= '9', c == '-', c == '_':
			segment++
		default:
			return false
		}
	}
	return segment > 0
}

func reservedCollection(name string) bool {
	return name == "_meta" || strings.HasPrefix(name, "_meta/")
}

func documentTarget(fields map[string]json.RawMessage, maxIDBytes int) (collection, id string, fail *apiFailure) {
	collection, fail = stringField(fields, "collection")
	if fail != nil {
		return "", "", fail
	}
	if !validCollectionName(collection) {
		return "", "", failInvalidCollection
	}
	if reservedCollection(collection) {
		return "", "", failReserved
	}
	id, fail = stringField(fields, "id")
	if fail != nil {
		return "", "", fail
	}
	if !utf8.ValidString(id) || len(id) > maxIDBytes {
		return "", "", failInvalidID
	}
	return collection, id, nil
}

func collectionTarget(fields map[string]json.RawMessage) (string, *apiFailure) {
	collection, fail := stringField(fields, "collection")
	if fail != nil {
		return "", fail
	}
	if !validCollectionName(collection) {
		return "", failInvalidCollection
	}
	if reservedCollection(collection) {
		return "", failReserved
	}
	return collection, nil
}
