/*
 * mod_grpc: FreeSWITCH control-plane module (gRPC)
 * Copyright (C) 2026 Protocol Hermit
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

/* mod_grpc/module/xml_fetch.h
 * Rules for the XML fetch binding (RM-28): which sections the engine may serve, the lookup key for each, the tenant
 * a dialplan fetch is made for. Plain C with no FreeSWITCH dependency so the unit tests can include it. Names come
 * from the node's own config or from FreeSWITCH (a context, a config file name); they become part of a lookup key,
 * so only a single safe path component is accepted, and the module never serves mod_grpc.conf to itself. */
#pragma once
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include "step_guard.h"
#include "../bridge/utf8.h"

#define XF_MAX_CONF 16
#define XF_FETCH_CALL 0    /* ask the engine on every dialplan hunt, with the call's details; never cached */
#define XF_FETCH_CONTEXT 1 /* fetch a whole context per tenant; cached until its TTL or an Invalidate */
#define XF_KIND_XML 4      /* GB_KIND_XML */
#define XF_ACL_KEY "configuration/acl.conf"

typedef struct {
	int dialplan;
	int nconf;
	char conf[XF_MAX_CONF][72];
} xf_sections_t;

static inline int xf_conf_name_ok(const char *name)
{
	return sg_safe_name(name) && strcasecmp(name, "mod_grpc.conf");
}

static inline int xf_conf_listed(const xf_sections_t *s, const char *name)
{
	int i;
	for (i = 0; s && name && i < s->nconf; i++)
		if (!strcmp(s->conf[i], name)) return 1;
	return 0;
}

/* Parses "dialplan, configuration/acl.conf". Returns how many entries were ignored; their text is appended to bad,
 * comma separated, truncated to fit (bad may be NULL). */
static inline int xf_parse(const char *value, xf_sections_t *out, char *bad, size_t badlen)
{
	const char *p = value ? value : "";
	int ignored = 0;
	size_t bn = 0;

	memset(out, 0, sizeof(*out));
	if (bad && badlen) bad[0] = 0;
	while (*p) {
		const char *e = strchr(p, ',');
		size_t n = e ? (size_t) (e - p) : strlen(p);
		char item[96];
		int ok = 0;

		while (n && (*p == ' ' || *p == '\t')) { p++; n--; }
		while (n && (p[n - 1] == ' ' || p[n - 1] == '\t')) n--;
		if (n) {
			size_t c = n < sizeof(item) - 1 ? n : sizeof(item) - 1;
			memcpy(item, p, c);
			item[c] = 0;
			if (n < sizeof(item)) {
				if (!strcasecmp(item, "dialplan")) {
					out->dialplan = 1;
					ok = 1;
				} else if (!strncasecmp(item, "configuration/", 14) && xf_conf_name_ok(item + 14)) {
					if (xf_conf_listed(out, item + 14)) {
						ok = 1;
					} else if (out->nconf < XF_MAX_CONF) {
						snprintf(out->conf[out->nconf++], sizeof(out->conf[0]), "%s", item + 14);
						ok = 1;
					}
				}
			}
			if (!ok) {
				ignored++;
				if (bad && bn + 1 < badlen) bn += (size_t) snprintf(bad + bn, badlen - bn, "%s%s", bn ? "," : "", item);
				if (bad && bn >= badlen) bn = badlen - 1; /* snprintf reports the untruncated length */
			}
		}
		p = e ? e + 1 : p + strlen(p);
	}
	return ignored;
}

static inline int xf_any(const xf_sections_t *s)
{
	return s && (s->dialplan || s->nconf > 0);
}

/* "context" -> XF_FETCH_CONTEXT; "call", NULL, "" -> XF_FETCH_CALL; anything else XF_FETCH_CALL with *known = 0. */
static inline int xf_fetch_mode(const char *value, int *known)
{
	if (known) *known = 1;
	if (!value || !*value || !strcasecmp(value, "call")) return XF_FETCH_CALL;
	if (!strcasecmp(value, "context")) return XF_FETCH_CONTEXT;
	if (known) *known = 0;
	return XF_FETCH_CALL;
}

/* "dialplan/<context>". 0 when dialplan is not listed, the name is not one safe word, or it is RM-27's grpc_api. */
static inline int xf_dialplan_key(const xf_sections_t *s, const char *context, char *out, size_t outlen)
{
	if (!s || !s->dialplan || !sg_safe_name(context) || !strcmp(context, "grpc_api")) return 0;
	return (size_t) snprintf(out, outlen, "dialplan/%s", context) < outlen;
}

/* "configuration/<name>". 0 when that document is not listed. */
static inline int xf_conf_key(const xf_sections_t *s, const char *name, char *out, size_t outlen)
{
	if (!name || !xf_conf_name_ok(name) || !xf_conf_listed(s, name)) return 0;
	return (size_t) snprintf(out, outlen, "configuration/%s", name) < outlen;
}

/* The tenant a dialplan fetch is made for: the first non-empty of the channel's tenant_id and the edge's
 * X-Tenant-Id header, if it is UUID-shaped; otherwise "" (a node-level dialplan). */
static inline const char *xf_tenant(const char *var_tenant, const char *hdr_tenant)
{
	const char *t = (var_tenant && *var_tenant) ? var_tenant : hdr_tenant;
	return dg_uuid_shaped(t) ? t : "";
}

/* Per-call params travel in a proto3 string map; see gb_utf8_ok. A header that fails is left out. */
static inline int xf_utf8_ok(const char *v) { return gb_utf8_ok(v); }

/* Does this Invalidate concern the node-level acl.conf document? */
static inline int xf_acl_invalidate(int kind, const char *tenant, const char *key)
{
	if (kind != XF_KIND_XML && kind != 0) return 0;
	if (tenant && *tenant) return 0;
	return key && (!strcmp(key, "*") || !strcmp(key, XF_ACL_KEY));
}
