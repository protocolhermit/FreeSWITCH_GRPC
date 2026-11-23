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

/* Peer-hop bridge legs for engine mode: a user registered on another node is reached through that node's SIP address.
 * Plain C with no FreeSWITCH dependency so the unit tests can include it. */
#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dial_guard.h"

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

/* user@domain: exactly one '@', user [A-Za-z0-9._+-], domain [A-Za-z0-9.-], both non-empty. Nothing else may enter the
 * bridge string (':_:' splits it, '?' and '/' reach the URI). NULL is refused. */
static inline int pl_target_ok(const char *t)
{
	const char *at, *p;
	if (!t || !(at = strchr(t, '@')) || at == t || !at[1] || strchr(at + 1, '@')) return 0;
	for (p = t; p < at; p++)
		if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') || strchr("._+-", *p))) return 0;
	for (p = at + 1; *p; p++)
		if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') || *p == '.' || *p == '-')) return 0;
	return 1;
}

/* local_dial may be "". Returns 1 and the joined dial string in out, 0 on any refusal (out is then empty).
 * The local part is vetted by dg_safe_dial; the hop legs are built only from validated pieces. */
static inline int pl_build(const char *local_dial, const char *const *peers, size_t npeers, const char *profile,
						   const char *target, const char *tenant, int on_hop, char *out, size_t outlen)
{
	size_t i, n = 0;
	if (!out || !outlen) return 0;
	out[0] = 0;
	if (!local_dial || (*local_dial && !dg_safe_dial(local_dial, tenant))) return 0;
	if (npeers && on_hop) return 0; /* loop guard: a call that already hopped never hops again */
	if (npeers && (!peers || !dg_uuid_shaped(tenant) || !profile || !dg_safe_ident(profile, strlen(profile)) ||
				   !pl_target_ok(target) || strchr(local_dial, '|'))) /* '|' would put the peers into the last failover group */
		return 0;
	if (strlen(local_dial) >= outlen) return 0;
	n = (size_t) snprintf(out, outlen, "%s", local_dial);
	for (i = 0; i < npeers; i++) {
		int w;
		if (!pl_addr_ok(peers[i])) goto fail;
		/* fs_path=sip:<peer>, no angle brackets: sofia then keeps sip:<target> as the Request-URI and To and adds
		 * Route: <sip:<peer>>. With fs_path=<sip:...> (brackets) it sends the INVITE to sip:<peer> itself, losing the
		 * user and the domain the peer routes on (seen live, FreeSWITCH 1.11). */
		w = snprintf(out + n, outlen - n, "%s[sip_h_X-Tenant-Id=%s,sip_h_X-Grpc-Hop=1]sofia/%s/%s;fs_path=sip:%s", n ? "," : "",
					 tenant, profile, target, peers[i]);
		if (w < 0 || (size_t) w >= outlen - n) goto fail;
		n += (size_t) w;
	}
	return 1;
fail:
	out[0] = 0;
	return 0;
}
