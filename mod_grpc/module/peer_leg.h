/* The includer provides cJSON (survival.h needs it). Peer-hop bridge legs for engine mode: a user registered on another node is reached through that node's SIP address.
 * Plain C with no FreeSWITCH dependency so the unit tests can include it. */
#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dial_guard.h"
#include "survival.h"

/* host:port; host [A-Za-z0-9.-]+, port 1..65535. No IPv6 (the brackets would break the dial string). NULL is refused. */
static inline int pl_addr_ok(const char *addr)
{
	const char *colon, *p;
	long port = 0;
	if (!addr || !(colon = strrchr(addr, ':')) || colon == addr || !colon[1] || strlen(colon + 1) > 5) return 0;
	for (p = addr; p < colon; p++)
		if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') || *p == '.' || *p == '-')) return 0;
	for (p = colon + 1; *p; p++) {
		if (*p < '0' || *p > '9') return 0;
		port = port * 10 + (*p - '0');
	}
	return port >= 1 && port <= 65535;
}

/* local_dial may be "". Returns 1 and the joined dial string in out, 0 on any refusal (out is then empty).
 * The local part is vetted by dg_safe_dial; the hop legs carry '<' '>' and are built only from validated pieces. */
static inline int pl_build(const char *local_dial, const char *const *peers, size_t npeers, const char *profile,
						   const char *target, const char *tenant, int on_hop, char *out, size_t outlen)
{
	size_t i, n = 0;
	if (!out || !outlen) return 0;
	out[0] = 0;
	if (!local_dial || (*local_dial && !dg_safe_dial(local_dial, tenant))) return 0;
	if (npeers && on_hop) return 0; /* loop guard: a call that already hopped never hops again */
	if (npeers && (!peers || !dg_uuid_shaped(tenant) || !profile || !dg_safe_ident(profile, strlen(profile)) ||
				   !sv_leg_safe(target) || strchr(target, ';') || strchr(target, '@') != strrchr(target, '@')))
		return 0;
	if (strlen(local_dial) >= outlen) return 0;
	n = (size_t) snprintf(out, outlen, "%s", local_dial);
	for (i = 0; i < npeers; i++) {
		int w;
		if (!pl_addr_ok(peers[i])) goto fail;
		w = snprintf(out + n, outlen - n, "%s[sip_h_X-Tenant-Id=%s,sip_h_X-Grpc-Hop=1]sofia/%s/%s;fs_path=<sip:%s>", n ? "," : "",
					 tenant, profile, target, peers[i]);
		if (w < 0 || (size_t) w >= outlen - n) goto fail;
		n += (size_t) w;
	}
	return 1;
fail:
	out[0] = 0;
	return 0;
}
