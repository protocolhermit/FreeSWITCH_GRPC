/* Survival-mode routing decision for mod_grpc. Plain C with no FreeSWITCH dependency so the unit tests can include it.
 * The includer provides cJSON (switch.h in the module, <cjson/cJSON.h> in the tests) and dial_guard.h first.
 *
 * With no engine reachable a call may still reach an enabled, non-DND extension of the caller's own tenant domain,
 * judged from that tenant's cached CONFIG snapshot (the engine's snapshot JSON: tenant_uuid, domains[].fqdn,
 * extensions[].{domain,number,dnd}). Nothing else is routable here: DIDs, groups, flows, queues, outbound numbers,
 * feature codes and other tenants' domains all come out as a refusal. */
#pragma once
#include <stdio.h>
#include <string.h>
#include <strings.h>

#define SV_OK 0
#define SV_BAD_ARG 1         /* tenant not UUID-shaped, domain or number empty or unsafe in a dial string */
#define SV_BAD_JSON 2
#define SV_WRONG_TENANT 3    /* the cached snapshot is not this tenant's */
#define SV_FOREIGN_DOMAIN 4  /* the request domain is not one of the tenant's own */
#define SV_NO_EXTENSION 5    /* not an extension of that domain */
#define SV_NOT_DIALLABLE 6   /* Do Not Disturb, or disabled */
#define SV_NO_DIAL 7         /* bad survival-dial-format, result too long, or the dial guard refuses it */

/* The same character set the engine's legSafe allows in a dial-string part. */
static inline int sv_leg_safe(const char *v)
{
	if (!v || !*v) return 0;
	for (; *v; v++) {
		unsigned char c = (unsigned char) *v;
		if (c <= 32 || c == 127 || strchr(",|[]<>^{}\"'\\", c)) return 0;
	}
	return 1;
}

/* fmt takes (number, domain) as exactly two %s and no other '%'. 1 when out holds the result. */
static inline int sv_format(const char *fmt, const char *number, const char *domain, char *out, size_t len)
{
	const char *args[2];
	int n = 0;
	size_t o = 0;
	args[0] = number;
	args[1] = domain;
	if (!fmt || !number || !domain || !out || len < 1) return 0;
	for (; *fmt; fmt++) {
		if (*fmt == '%') {
			size_t l;
			if (fmt[1] != 's' || n == 2) return 0;
			l = strlen(args[n]);
			if (o + l >= len) return 0;
			memcpy(out + o, args[n++], l);
			o += l;
			fmt++;
		} else {
			if (o + 1 >= len) return 0;
			out[o++] = *fmt;
		}
	}
	out[o] = 0;
	return n == 2;
}

static inline int sv_is(const cJSON *item, int type)
{
	return item && (item->type & 0xFF) == type;
}

static inline int sv_str(const cJSON *item)
{
	return item && cJSON_IsString(item) && item->valuestring;
}

/* "[sip_h_X-Tenant-Id=<tenant>,leg_timeout=30]<leg>" for one extension, SV_OK once dg_safe_dial accepts it. Shared by
 * sv_plan and the module's load-time probe so a format that would refuse every call is refused at load. */
static inline int sv_dial(const char *tenant, const char *fmt, const char *number, const char *domain, char *dial, size_t dial_len)
{
	char leg[400];
	if (!dial || dial_len < 1) return SV_NO_DIAL;
	dial[0] = 0;
	if (!sv_format(fmt, number, domain, leg, sizeof(leg)) || strpbrk(leg, ",|")) return SV_NO_DIAL;
	if (snprintf(dial, dial_len, "[sip_h_X-Tenant-Id=%s,leg_timeout=30]%s", tenant, leg) >= (int) dial_len ||
	    !dg_safe_dial(dial, tenant)) {
		dial[0] = 0;
		return SV_NO_DIAL;
	}
	return SV_OK;
}

/* payload: the tenant's cached CONFIG snapshot. tenant: the edge's stamp. domain, number: the request. On SV_OK, dial
 * is "[sip_h_X-Tenant-Id=<tenant>,leg_timeout=30]<leg>" and has passed dg_safe_dial; on any other result dial is "". */
static inline int sv_plan(const char *payload, const char *tenant, const char *domain, const char *number, const char *fmt,
                          char *dial, size_t dial_len)
{
	cJSON *j, *t, *doms, *exts, *e;
	const char *ext_domain = NULL, *ext_number = NULL;
	int rc = SV_NO_EXTENSION, dom_ok = 0;

	if (dial && dial_len) dial[0] = 0;
	if (!dial || dial_len < 1 || !dg_uuid_shaped(tenant) || !sv_leg_safe(domain) || !sv_leg_safe(number)) return SV_BAD_ARG;
	if (!payload || !(j = cJSON_Parse(payload))) return SV_BAD_JSON;

	t = cJSON_GetObjectItem(j, "tenant_uuid");
	if (!sv_str(t) || strcasecmp(t->valuestring, tenant)) {
		cJSON_Delete(j);
		return SV_WRONG_TENANT;
	}
	doms = cJSON_GetObjectItem(j, "domains");
	if (sv_is(doms, cJSON_Array)) {
		for (e = doms->child; e && !dom_ok; e = e->next) {
			cJSON *f = cJSON_GetObjectItem(e, "fqdn");
			if (sv_str(f) && !strcasecmp(f->valuestring, domain)) dom_ok = 1;
		}
	}
	if (!dom_ok) {
		cJSON_Delete(j);
		return SV_FOREIGN_DOMAIN;
	}
	exts = cJSON_GetObjectItem(j, "extensions");
	if (sv_is(exts, cJSON_Array)) {
		for (e = exts->child; e; e = e->next) {
			cJSON *d = cJSON_GetObjectItem(e, "domain"), *n = cJSON_GetObjectItem(e, "number");
			cJSON *dnd = cJSON_GetObjectItem(e, "dnd"), *en = cJSON_GetObjectItem(e, "enabled");
			if (!sv_str(d) || !sv_str(n) || strcasecmp(d->valuestring, domain) || strcmp(n->valuestring, number)) continue;
			if (sv_is(dnd, cJSON_True) || sv_is(en, cJSON_False)) {
				rc = SV_NOT_DIALLABLE;
			} else {
				rc = SV_OK;
				ext_domain = d->valuestring;
				ext_number = n->valuestring;
			}
			break;
		}
	}
	if (rc == SV_OK) {
		/* the snapshot's own spelling goes into the leg, so it must be safe there too */
		if (!sv_leg_safe(ext_domain) || !sv_leg_safe(ext_number)) rc = SV_BAD_ARG;
		else rc = sv_dial(tenant, fmt, ext_number, ext_domain, dial, dial_len);
		if (rc != SV_OK) dial[0] = 0;
	}
	cJSON_Delete(j);
	return rc;
}
