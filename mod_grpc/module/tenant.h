/* Tenant resolution for mod_grpc, per registration mode and tenancy. Plain C with no FreeSWITCH dependency so the
 * unit tests can include it; the module supplies the map/engine lookups as callbacks. */
#pragma once
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include "dial_guard.h"

#define TN_MODE_EDGE 0
#define TN_MODE_LOCAL 1
#define TN_MODE_ENGINE 2
#define TN_SRC_NONE 0
#define TN_SRC_DEFAULT 1
#define TN_SRC_HEADER 2
#define TN_SRC_VAR 3
#define TN_SRC_MAP 4
#define TN_SRC_ENGINE 5

/* 1 = found, out filled. The domain passed in is already lowercased. */
typedef int (*tn_lookup_fn)(void *ud, const char *domain, char *out, size_t outlen);
typedef struct {
	int mode;
	int single;
	const char *default_tenant;
	tn_lookup_fn map;
	tn_lookup_fn engine;
	void *ud;
} tn_config_t;
typedef struct {
	const char *header;
	int header_trusted;
	const char *var;
	const char *domain;
} tn_input_t;

static inline int tn_parse_mode(const char *s, int *mode)
{
	if (!s) return 0;
	if (!strcasecmp(s, "edge")) *mode = TN_MODE_EDGE;
	else if (!strcasecmp(s, "local")) *mode = TN_MODE_LOCAL;
	else if (!strcasecmp(s, "engine")) *mode = TN_MODE_ENGINE;
	else return 0;
	return 1;
}

/* Single tenancy stamps every call with default-tenant, so it must be a real tenant id. */
static inline int tn_config_ok(int single, const char *default_tenant)
{
	return (!single || dg_uuid_shaped(default_tenant)) ? 1 : 0;
}

/* Edge trusts the header unless an ACL is configured and the source failed it; local/engine need an ACL pass. */
static inline int tn_header_trusted(int mode, int acl_configured, int acl_passed)
{
	if (mode == TN_MODE_EDGE) return (!acl_configured || acl_passed) ? 1 : 0;
	return (acl_configured && acl_passed) ? 1 : 0;
}

static inline int tn_lower_domain(const char *in, char *out, size_t outlen)
{
	size_t i, n = in ? strlen(in) : 0;
	if (n == 0 || n >= outlen) return 0;
	for (i = 0; i < n; i++) out[i] = (in[i] >= 'A' && in[i] <= 'Z') ? (char)(in[i] + 32) : in[i];
	out[n] = 0;
	return 1;
}

static inline int tn_copy(const char *v, char *out, size_t outlen)
{
	if (!v || !*v || strlen(v) >= outlen) return 0;
	strcpy(out, v);
	return 1;
}

/* A lookup answer is only taken if it fits and (outside edge) is UUID-shaped. */
static inline int tn_lookup(tn_lookup_fn fn, void *ud, const char *dom, int need_uuid, char *out, size_t outlen)
{
	if (!fn || !dom[0] || !fn(ud, dom, out, outlen)) return 0;
	out[outlen - 1] = 0;
	if (strlen(out) >= outlen - 1) return 0; /* the callback may have truncated */
	return (!need_uuid || dg_uuid_shaped(out)) ? 1 : 0;
}

static inline int tn_resolve(const tn_config_t *c, const tn_input_t *in, char *out, size_t outlen)
{
	char dom[256] = "";
	int edge = (c->mode == TN_MODE_EDGE);
	if (outlen) out[0] = 0;
	if (outlen < 2) return TN_SRC_NONE;
	if (c->single) return tn_copy(c->default_tenant, out, outlen) ? TN_SRC_DEFAULT : TN_SRC_NONE;
	/* edge keeps today's behaviour: no UUID check on header or var */
	if (in->header_trusted && (edge || dg_uuid_shaped(in->header)) && tn_copy(in->header, out, outlen))
		return TN_SRC_HEADER;
	if ((edge || dg_uuid_shaped(in->var)) && tn_copy(in->var, out, outlen)) return TN_SRC_VAR;
	tn_lower_domain(in->domain, dom, sizeof dom);
	if (c->mode == TN_MODE_LOCAL && tn_lookup(c->map, c->ud, dom, 1, out, outlen)) return TN_SRC_MAP;
	if (tn_lookup(c->engine, c->ud, dom, !edge, out, outlen)) return TN_SRC_ENGINE;
	out[0] = 0;
	return TN_SRC_NONE;
}

static inline const char *tn_source_name(int src)
{
	switch (src) {
	case TN_SRC_DEFAULT: return "default";
	case TN_SRC_HEADER: return "header";
	case TN_SRC_VAR: return "var";
	case TN_SRC_MAP: return "map";
	case TN_SRC_ENGINE: return "engine";
	}
	return "none";
}
