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

/* Builds the directory XML document for one user (plain C, header-only; shared by the module and its unit test). */
#ifndef MOD_GRPC_DIRECTORY_XML_H
#define MOD_GRPC_DIRECTORY_XML_H

#include <stdio.h>
#include <string.h>
#include "api_guard.h"

typedef struct {
	const char *domain, *user, *tenant, *name; /* name NULL -> user */
	const char *a1_hash, *password;			   /* either may be NULL */
	const char *const *var_names, *const *var_values;
	size_t nvars;
	const char *var_allow; /* globals.var_allow */
	int with_auth;		   /* directory-auth */
	int with_dial_string;  /* reg_mode != edge */
} dx_user_t;

#define DX_MAX_VARS 32
#define DX_DIAL_STRING "{^^:sip_invite_domain=${dialed_domain}:presence_id=${dialed_user}@${dialed_domain}}${sofia_contact(*/${dialed_user}@${dialed_domain})}"

typedef struct {
	char *p;
	size_t left;
	int ok;
} dx_buf_t;

static inline void dx_raw(dx_buf_t *b, const char *s)
{
	size_t n = strlen(s);
	if (!b->ok || n >= b->left) { b->ok = 0; return; }
	memcpy(b->p, s, n + 1);
	b->p += n;
	b->left -= n;
}

static inline void dx_esc(dx_buf_t *b, const char *s)
{
	char c[2] = {0, 0};
	for (; s && *s; s++) {
		switch (*s) {
		case '&': dx_raw(b, "&amp;"); break;
		case '<': dx_raw(b, "&lt;"); break;
		case '>': dx_raw(b, "&gt;"); break;
		case '"': dx_raw(b, "&quot;"); break;
		case '\'': dx_raw(b, "&apos;"); break;
		default: c[0] = *s; dx_raw(b, c);
		}
	}
}

static inline void dx_attr(dx_buf_t *b, const char *open, const char *v, const char *close)
{
	dx_raw(b, open);
	dx_esc(b, v);
	dx_raw(b, close);
}

/* 1 = ok, 0 = buffer too small or missing domain/user/tenant */
static inline int dx_user_xml(const dx_user_t *u, char *out, size_t outlen)
{
	dx_buf_t b = {out, outlen, 1};
	size_t i;
	if (!u || !out || !outlen || !u->domain || !u->user || !u->tenant) return 0;
	/* user is the caller's SIP user and lands in effective_caller_id_number: no "${" (see ag_no_api_expansion) */
	if (!ag_no_api_expansion(u->user) || !ag_no_api_expansion(u->domain)) return 0;
	dx_raw(&b, "<document type=\"freeswitch/xml\"><section name=\"directory\"><domain name=\"");
	dx_esc(&b, u->domain);
	dx_raw(&b, "\">");
	if (u->with_dial_string) {
		dx_raw(&b, "<params><param name=\"dial-string\" value=\"");
		dx_esc(&b, DX_DIAL_STRING);
		dx_raw(&b, "\"/></params>");
	}
	dx_attr(&b, "<user id=\"", u->user, "\">");
	if (u->with_auth) {
		dx_raw(&b, "<params>");
		/* a1-hash wins: the engine never has to send the clear password */
		if (u->a1_hash && *u->a1_hash) dx_attr(&b, "<param name=\"a1-hash\" value=\"", u->a1_hash, "\"/>");
		else if (u->password && *u->password) dx_attr(&b, "<param name=\"password\" value=\"", u->password, "\"/>");
		/* sofia's allow-empty-password defaults to true: a user without credentials would accept any password */
		dx_raw(&b, "<param name=\"allow-empty-password\" value=\"false\"/></params>");
	} else {
		dx_raw(&b, "<params/>");
	}
	dx_raw(&b, "<variables>");
	dx_attr(&b, "<variable name=\"tenant_id\" value=\"", u->tenant, "\"/>");
	dx_raw(&b, "<variable name=\"user_context\" value=\"default\"/>");
	dx_attr(&b, "<variable name=\"effective_caller_id_number\" value=\"", u->user, "\"/>");
	/* no "${" in a value: FreeSWITCH expands a variable's value recursively (see ag_no_api_expansion), so a display name
	 * of "${system(...)}" would run whenever something expands ${effective_caller_id_name}. Such a name falls back to the user. */
	dx_attr(&b, "<variable name=\"effective_caller_id_name\" value=\"", u->name && ag_no_api_expansion(u->name) ? u->name : u->user, "\"/>");
	for (i = 0; i < u->nvars; i++) {
		/* ag_var_ok also refuses the reserved names (tenant_id), so the tenant variable cannot be overridden */
		if (!u->var_names[i] || !u->var_values[i] || !ag_var_ok(u->var_names[i], u->var_allow) || !ag_no_api_expansion(u->var_values[i])) continue;
		dx_attr(&b, "<variable name=\"", u->var_names[i], "\" value=\"");
		dx_esc(&b, u->var_values[i]);
		dx_raw(&b, "\"/>");
	}
	dx_raw(&b, "</variables></user></domain></section></document>");
	return b.ok;
}

#endif
