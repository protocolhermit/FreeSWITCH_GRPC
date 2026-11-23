/* mod_grpc/module/data_api.h
 * Parse, validate and format for the "grpc data" API command. Plain C with no FreeSWITCH dependency so the unit test
 * can include it. */
#pragma once
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dial_guard.h"
#include "../bridge/grpc_bridge.h"
#include "../bridge/utf8.h"

#define DA_MAX_VALUE 65536

typedef struct { int op; char tenant[64]; const char *key; const char *value; int32_t ttl_s; int64_t delta, version; } da_cmd_t;

/* 1-256 bytes of 0x21-0x7e */
static inline int da_key_ok(const char *key)
{
	size_t i, n = key ? strlen(key) : 0;
	if (n < 1 || n > 256) return 0;
	for (i = 0; i < n; i++) if ((unsigned char) key[i] < 0x21 || (unsigned char) key[i] > 0x7e) return 0;
	return 1;
}

/* UTF-8, no control bytes (< 0x20, 0x7f); the length is checked separately */
static inline int da_value_ok(const char *value)
{
	const unsigned char *p;
	if (!gb_utf8_ok(value)) return 0;
	for (p = (const unsigned char *) value; *p; p++) if (*p < 0x20 || *p == 0x7f) return 0;
	return 1;
}

/* next space-delimited token (non-empty); *s moves past the delimiter; *delim says whether a delimiter was there */
static inline char *da_tok(char **s, int *delim)
{
	char *t = *s, *e = strchr(t, ' ');
	if (!*t || *t == ' ') return NULL;
	if (e) { *e = '\0'; *s = e + 1; } else *s = t + strlen(t);
	if (delim) *delim = e != NULL;
	return t;
}

static inline int da_num(const char *t, int64_t *v)
{
	char *end;
	errno = 0;
	*v = strtoll(t, &end, 10);
	return *t && !*end && !errno;
}

static inline int da_ttl(const char *t, int32_t *v)
{
	int64_t n;
	if (!da_num(t, &n) || n < 0 || n > INT32_MAX) return 0;
	*v = (int32_t) n;
	return 1;
}

/* Parses the text after "data " in place (writes NULs into s). Returns 1 or 0 (bad_args). The value of set and cas is
 * the rest of the line after the fixed arguments, spaces included; an empty value is allowed. */
static inline int da_parse(char *s, da_cmd_t *out)
{
	char *op, *tenant, *t;
	int delim = 0;
	memset(out, 0, sizeof *out);
	if (!(op = da_tok(&s, NULL)) || !(tenant = da_tok(&s, &delim))) return 0;
	if (!dg_uuid_shaped(tenant)) return 0;
	memcpy(out->tenant, tenant, 37);
	if (!delim || !(out->key = da_tok(&s, &delim))) return 0;
	if (!strcmp(op, "get") || !strcmp(op, "del")) {
		out->op = op[0] == 'g' ? GB_DATA_GET : GB_DATA_DEL;
		return !delim; /* nothing may follow the key */
	}
	if (!strcmp(op, "set")) {
		out->op = GB_DATA_SET;
		if (!delim || !(t = da_tok(&s, &delim)) || !da_ttl(t, &out->ttl_s) || !delim) return 0;
		out->value = s;
		return 1;
	}
	if (!strcmp(op, "incr")) {
		out->op = GB_DATA_INCR;
		if (!delim || !(t = da_tok(&s, &delim)) || !da_num(t, &out->delta)) return 0;
		if (!delim) return 1;
		return (t = da_tok(&s, &delim)) && !delim && da_ttl(t, &out->ttl_s);
	}
	if (!strcmp(op, "cas")) {
		out->op = GB_DATA_CAS;
		if (!delim || !(t = da_tok(&s, &delim)) || !da_num(t, &out->version) || out->version < 0 || !delim) return 0;
		if (!(t = da_tok(&s, &delim)) || !da_ttl(t, &out->ttl_s) || !delim) return 0;
		out->value = s;
		return 1;
	}
	return 0;
}

/* Formats the one-line reply for rc from gb_data into out. */
static inline void da_reply(int op, int rc, const char *value, int64_t version, const char *error, char *out, size_t outlen)
{
	const char *e = "unavailable";
	switch (rc) {
	case GB_OK:
		if (op == GB_DATA_GET || op == GB_DATA_INCR) {
			if (value && !da_value_ok(value)) e = "bad_value";
			else { snprintf(out, outlen, "+OK %lld %s", (long long) version, value ? value : ""); return; }
		} else if (op == GB_DATA_DEL) { snprintf(out, outlen, "+OK"); return; }
		else { snprintf(out, outlen, "+OK %lld", (long long) version); return; }
		break;
	case GB_ERR_NOT_FOUND: e = "not_found"; break;
	case GB_ERR_CONFLICT: snprintf(out, outlen, "-ERR conflict %lld", (long long) version); return;
	case GB_ERR_ENGINE: if (error && !strcmp(error, "not_integer")) e = "not_integer"; break;
	case GB_ERR_TIMEOUT: e = "timeout"; break;
	case GB_ERR_DISCONNECTED: e = "disconnected"; break;
	default: break; /* GB_ERR_UNAVAILABLE and everything else */
	}
	snprintf(out, outlen, "-ERR %s", e);
}
