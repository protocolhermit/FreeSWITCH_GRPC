/* Dial-string guards for mod_grpc. Plain C with no FreeSWITCH dependency so the unit tests can include it. */
#pragma once
#include <string.h>

/* A value is passed to originate inside {k=v}; refuse what could break out. */
static inline int dg_safe_value(const char *v)
{
	return (v && !strpbrk(v, "{}'\r\n")) ? 1 : 0;
}

/* The engine's tenant ids are canonical UUIDs: exactly 36 chars of [0-9a-fA-F-]. Nothing else may enter a stamp
 * (a ',' or '|' would smuggle further per-leg variables, and length games must not truncate the stamp). */
static inline int dg_uuid_shaped(const char *t)
{
	size_t i;
	if (!t || strlen(t) != 36) return 0;
	for (i = 0; i < 36; i++) {
		char c = t[i];
		if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || c == '-')) return 0;
	}
	return 1;
}

static inline int dg_safe_ident(const char *v, size_t vlen)
{
	size_t i;
	if (vlen < 1 || vlen > 64) return 0;
	for (i = 0; i < vlen; i++) {
		char c = v[i];
		if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-'))
			return 0;
	}
	return 1;
}

static inline int dg_safe_call_info(const char *v, size_t vlen)
{
	static const char prefix[] = "<sip:";
	static const char suffix[] = ">;answer-after=0";
	size_t pre_len = sizeof(prefix) - 1;
	size_t suf_len = sizeof(suffix) - 1;
	size_t inner_len, i;
	const char *inner;
	if (vlen < pre_len + suf_len + 1) return 0;
	if (strncmp(v, prefix, pre_len) != 0) return 0;
	if (strncmp(v + vlen - suf_len, suffix, suf_len) != 0) return 0;
	inner = v + pre_len;
	inner_len = vlen - pre_len - suf_len;
	if (inner_len < 1 || inner_len > 64) return 0;
	for (i = 0; i < inner_len; i++) {
		char c = inner[i];
		if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.'))
			return 0;
	}
	return 1;
}

/* A caller-ID pool number: digits with an optional leading '+', 3 to 20 digits (spec 10). */
static inline int dg_safe_cid(const char *v, size_t vlen)
{
	size_t i = (vlen > 0 && v[0] == '+') ? 1 : 0;
	if (vlen - i < 3 || vlen - i > 20) return 0;
	for (; i < vlen; i++)
		if (v[i] < '0' || v[i] > '9') return 0;
	return 1;
}

/* A call the dialer places (Originate with route_on_answer): the only variables its leg may carry, with the shape the
 * engine mints them in. call_id is "dial-" + a UUID (an identifier, 1 to 64 characters), origination_uuid the leg's UUID,
 * dialer_token the one-time token the engine recognises the answered leg by (an identifier of at least 16 characters).
 * Anything else is refused: the answered leg is routed by the engine, so it must not smuggle in channel variables. */
static inline int dg_dialer_var_ok(const char *key, const char *value)
{
	size_t n;
	if (!key || !value) return 0;
	n = strlen(value);
	if (!strcmp(key, "call_id")) return dg_safe_ident(value, n);
	if (!strcmp(key, "dialer_token")) return n >= 16 && dg_safe_ident(value, n);
	if (!strcmp(key, "origination_uuid")) return dg_uuid_shaped(value);
	return 0;
}

/* One "[k=v,k=v]" block starting at *pp (which points at '['); leaves *pp after the ']'. Only these keys pass:
 * sip_h_X-Tenant-Id (value = the command's tenant, which must be UUID-shaped: the claim the edge checks on legs to it),
 * leg_timeout and leg_delay_start (1 to 3 digits),
 * sip_h_X-ES-* context headers and sip_h_Call-Info (allowed only when stamped with sip_h_X-Tenant-Id).
 * origination_caller_id_number and origination_caller_id_name (a pool number, see dg_safe_cid; carrier legs only: no tenant stamp and the leg must be sofia/external/).
 * Nothing else can reach the originate parser from a dial string. */
static inline int dg_leg_block(const char **pp, const char *tenant, int *cid)
{
	static const char stamp[] = "sip_h_X-Tenant-Id";
	static const char lto[] = "leg_timeout";
	static const char lds[] = "leg_delay_start";
	static const char cid_num[] = "origination_caller_id_number";
	static const char cid_name[] = "origination_caller_id_name";
	static const char es_q[] = "sip_h_X-ES-Queue";
	static const char es_cmp[] = "sip_h_X-ES-Campaign";
	static const char es_cnt[] = "sip_h_X-ES-Contact";
	static const char es_cid[] = "sip_h_X-ES-Call-Id";
	static const char es_dir[] = "sip_h_X-ES-Direction";
	static const char es_for[] = "sip_h_X-ES-For";
	static const char ci[] = "sip_h_Call-Info";
	int has_stamp = 0, has_device_var = 0;
	const char *p = *pp + 1, *end = strchr(p, ']');
	*cid = 0;
	if (!end || end == p) return 0;
	while (p < end) {
		const char *comma = (const char *) memchr(p, ',', (size_t) (end - p));
		const char *kv_end = comma ? comma : end;
		const char *eq = (const char *) memchr(p, '=', (size_t) (kv_end - p));
		size_t klen, vlen, i;
		const char *v;
		if (!eq) return 0;
		klen = (size_t) (eq - p);
		v = eq + 1;
		vlen = (size_t) (kv_end - v);
		if (klen == sizeof(stamp) - 1 && !strncmp(p, stamp, klen)) {
			if (!dg_uuid_shaped(tenant) || vlen != 36 || strncmp(v, tenant, 36)) return 0;
			has_stamp = 1;
		} else if ((klen == sizeof(lto) - 1 && !strncmp(p, lto, klen)) || (klen == sizeof(lds) - 1 && !strncmp(p, lds, klen))) {
			if (vlen < 1 || vlen > 3) return 0;
			for (i = 0; i < vlen; i++)
				if (v[i] < '0' || v[i] > '9') return 0;
		} else if ((klen == sizeof(es_q) - 1 && !strncmp(p, es_q, klen)) ||
		           (klen == sizeof(es_cmp) - 1 && !strncmp(p, es_cmp, klen)) ||
		           (klen == sizeof(es_cnt) - 1 && !strncmp(p, es_cnt, klen)) ||
		           (klen == sizeof(es_cid) - 1 && !strncmp(p, es_cid, klen)) ||
		           (klen == sizeof(es_dir) - 1 && !strncmp(p, es_dir, klen)) ||
		           (klen == sizeof(es_for) - 1 && !strncmp(p, es_for, klen))) {
			if (!dg_safe_ident(v, vlen)) return 0;
			has_device_var = 1;
		} else if (klen == sizeof(ci) - 1 && !strncmp(p, ci, klen)) {
			if (!dg_safe_call_info(v, vlen)) return 0;
			has_device_var = 1;
		} else if ((klen == sizeof(cid_num) - 1 && !strncmp(p, cid_num, klen)) ||
		           (klen == sizeof(cid_name) - 1 && !strncmp(p, cid_name, klen))) {
			if (!dg_safe_cid(v, vlen)) return 0;
			*cid = 1;
		} else {
			return 0;
		}
		p = comma ? comma + 1 : end;
		if (comma && p == end) return 0; /* "k=v,]" */
	}
	if (has_device_var && !has_stamp) return 0;
	if (*cid && (has_stamp || has_device_var)) return 0; /* carriers never see the stamp or device headers */
	*pp = end + 1;
	return 1;
}

/* Dial strings: legs joined by ',' (ring together) or '|' (one after another). A leg may start with one variable block
 * (dg_leg_block); the rest of the leg may contain no whitespace and no further variable syntax ([ < ^) that could
 * override the command's tenant. */
static inline int dg_safe_dial(const char *v, const char *tenant)
{
	const char *p = v;
	if (!dg_safe_value(v)) return 0;
	for (;;) {
		const char *body;
		int cid = 0;
		if (*p == '[' && !dg_leg_block(&p, tenant, &cid)) return 0;
		body = p;
		if (cid && strncmp(body, "sofia/external/", 15)) return 0; /* caller-ID keys are for carrier legs */
		while (*p && *p != ',' && *p != '|') {
			if (strchr(" \t[<^", *p)) return 0;
			p++;
		}
		if (p == body) return 0; /* an empty leg */
		if (!*p) return 1;
		p++;
	}
}

/* One leg only: after an optional leading "[...]" variable block (whose own commas are fine), no ',' or '|'. */
static inline int dg_single_leg(const char *dial)
{
	if (!dial) return 0;
	if (*dial == '[') {
		dial = strchr(dial, ']');
		if (!dial) return 0;
		dial++;
	}
	return strpbrk(dial, ",|") ? 0 : 1;
}

/* The peer stamp authorizes one device destination; channel ownership stays with the source tenant. */
static inline int dg_safe_peer_bridge(const char *dial, const char *owner, const char *peer, const char *caller)
{
	static const char prefix[] = "[sip_h_X-Tenant-Id=";
	const char *p, *at;
	if (!dg_uuid_shaped(owner) || !dg_uuid_shaped(peer) || !strcmp(owner, peer)) return 0;
	if (!dial || strpbrk(dial, ",|") || !dg_safe_dial(dial, peer)) return 0;
	if (strncmp(dial, prefix, sizeof(prefix)-1)) return 0;
	p = dial + sizeof(prefix)-1;
	if (strlen(p) < 37 || strncmp(p, peer, 36) || p[36] != ']') return 0;
	if (strncmp(p+37, "sofia/internal/", 15)) return 0;
	if (!caller || !(at = strchr(caller, '@')) || at == caller || !at[1] || strchr(at+1, '@')) return 0;
	for (p = caller; *p; ++p) {
		unsigned char ch = (unsigned char)*p;
		if (ch <= 32 || ch == 127 || strchr(",|[]<>^{}\"'\\", ch)) return 0;
	}
	return 1;
}
