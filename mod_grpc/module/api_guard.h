/* Checks for the actions of an operator API route (RM-27) before FreeSWITCH sees them. Plain C, header-only, so the
 * unit test compiles it without FreeSWITCH. The engine validates too; this is the authority. Return 1 = allowed. */
#ifndef MOD_GRPC_API_GUARD_H
#define MOD_GRPC_API_GUARD_H

#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include "dial_guard.h"

#define AG_MAX_FWD_HEADERS 16

static const char *const ag_deny_api[] = {"system", "bg_system", "fsctl", "load", "unload", "reload", "reloadxml", "eval", "lua", "luarun",
										  "perl", "python", "jsrun", "shutdown", "bgapi", "expand", "uuid_setvar_multi", "sched_api", "sched_transfer", "sched_broadcast", "sched_hangup", "sched_del", "grpc", NULL};
static const char *const ag_deny_app[] = {"system", "bgsystem", "lua", "luarun", "perl", "python", "javascript", "eval", "socket",
										  "execute_extension", "set_global", "set_profile_var", "multiset", "limit_execute", "bind_meta_app", "bind_digit_action",
										  "sched_broadcast", "sched_transfer", "sched_hangup", NULL};
static const char *const ag_var_prefix[] = {"sip_", "grpc_", "api_", "execute_", "exec_", "bridge_pre_execute", "session_in_hangup_hook",
											"origination_", "effective_", "export_",
											/* FreeSWITCH runs these as an api/app later (record end) or transfers to them (after bridge, on fail) */
											"record_post_process_exec", "transfer_after_", "park_after_", "transfer_on_fail", "force_transfer_",
											"group_confirm_", /* group_confirm_key=exec runs group_confirm_file as an app on the B leg */
											NULL};
static const char *const ag_var_reserved[] = {"tenant_id", "call_id", "export_vars", NULL};

static inline int ag_listed(const char *name, const char *const *list)
{
	for (; *list; list++) if (!strcmp(name, *list)) return 1;
	return 0;
}

/* exact member of a comma separated list (no spaces) */
static inline int ag_in_csv(const char *name, const char *csv)
{
	size_t n = name ? strlen(name) : 0;
	const char *p = csv;
	if (!n || !csv) return 0;
	while (*p) {
		const char *e = strchr(p, ',');
		size_t len = e ? (size_t) (e - p) : strlen(p);
		if (len == n && !strncmp(p, name, n)) return 1;
		if (!e) break;
		p = e + 1;
	}
	return 0;
}

static inline int ag_denied_api(const char *name) { return ag_listed(name, ag_deny_api); }
static inline int ag_denied_app(const char *app) { return ag_listed(app, ag_deny_app); }

/* a configured allow-list that names a denied entry: the module refuses to load */
static inline int ag_csv_has_denied(const char *csv, int apps)
{
	const char *const *l = apps ? ag_deny_app : ag_deny_api;
	for (; *l; l++) if (ag_in_csv(*l, csv)) return 1;
	return 0;
}

/* the compiled-in rules, which no var-allow entry overrides */
static inline int ag_var_hard_ok(const char *name)
{
	size_t n = name ? strlen(name) : 0, i;
	const char *const *p;
	if (n == 0 || n > 64) return 0;
	for (i = 0; i < n; i++) if (!(islower((unsigned char) name[i]) || isdigit((unsigned char) name[i]) || name[i] == '_')) return 0;
	if (ag_listed(name, ag_var_reserved)) return 0;
	if (n >= 6 && !strcmp(name + n - 6, "_token")) return 0;
	for (p = ag_var_prefix; *p; p++) if (!strncmp(name, *p, strlen(*p))) return 0;
	return 1;
}

/* var-allow: comma separated, "name" exact or "prefix*" */
static inline int ag_var_listed(const char *name, const char *csv)
{
	size_t n = name ? strlen(name) : 0;
	const char *p = csv;
	if (!n || !csv) return 0;
	while (*p) {
		const char *e = strchr(p, ',');
		size_t len = e ? (size_t) (e - p) : strlen(p);
		if (len && p[len - 1] == '*' ? !strncmp(name, p, len - 1) : (len == n && !strncmp(p, name, n))) return 1;
		if (!e) break;
		p = e + 1;
	}
	return 0;
}

/* route-vars: extra channel variables sent on RouteRequest and StepResult. Listed is enough: no name is refused. */
#define AG_MAX_ROUTE_VARS 64
#define AG_ROUTE_VAR_NAME_MAX 70
#define AG_ROUTE_VAR_VALUE_MAX 1024
static inline int ag_route_var_ok(const char *name, const char *value, const char *csv)
{
	size_t n = name ? strlen(name) : 0, v = value ? strlen(value) : 0;
	return n && n <= AG_ROUTE_VAR_NAME_MAX && v && v <= AG_ROUTE_VAR_VALUE_MAX && ag_var_listed(name, csv);
}

/* settable only when listed in var-allow and the compiled-in rules allow it */
static inline int ag_var_ok(const char *name, const char *var_csv)
{
	return ag_var_hard_ok(name) && ag_var_listed(name, var_csv);
}

/* a var-allow that could admit a hard-denied name: the module refuses to load. An exact entry must pass the compiled-in
 * rules; a "prefix*" entry must be a non-empty valid name start that neither covers nor falls inside a denied prefix and
 * covers no reserved name ("*_token" stays refused at run time whatever the list says). */
static inline int ag_var_csv_bad(const char *csv)
{
	const char *p = csv;
	while (p && *p) {
		const char *e = strchr(p, ',');
		size_t len = e ? (size_t) (e - p) : strlen(p), i;
		char w[72];
		const char *const *d;
		if (!len) goto next; /* an empty entry admits nothing */
		if (len >= sizeof w) return 1;
		memcpy(w, p, len);
		w[len] = '\0';
		if (w[len - 1] != '*') {
			if (!ag_var_hard_ok(w)) return 1;
		} else {
			w[--len] = '\0';
			if (!len) return 1;
			for (i = 0; i < len; i++) if (!(islower((unsigned char) w[i]) || isdigit((unsigned char) w[i]) || w[i] == '_')) return 1;
			for (d = ag_var_prefix; *d; d++) {
				size_t dl = strlen(*d);
				if (!strncmp(w, *d, len < dl ? len : dl)) return 1;
			}
			for (d = ag_var_reserved; *d; d++) if (!strncmp(*d, w, len)) return 1;
		}
	next:
		if (!e) break;
		p = e + 1;
	}
	return 0;
}

/* "request:H" -> sip_h_H, "response:H" -> sip_rh_H, "provisional:H" -> sip_ph_H. Only X- headers unless listed. */
static inline int ag_header_var(const char *name, const char *std_csv, char *out, size_t outlen)
{
	const char *h, *prefix;
	size_t i, n;
	if (!name) return 0;
	if (!strncmp(name, "request:", 8)) { h = name + 8; prefix = "sip_h_"; }
	else if (!strncmp(name, "response:", 9)) { h = name + 9; prefix = "sip_rh_"; }
	else if (!strncmp(name, "provisional:", 12)) { h = name + 12; prefix = "sip_ph_"; }
	else return 0;
	n = strlen(h);
	if (n == 0 || n > 64 || !isalpha((unsigned char) h[0])) return 0;
	for (i = 0; i < n; i++) if (!(isalnum((unsigned char) h[i]) || h[i] == '-')) return 0;
	if (!strcasecmp(h, "X-Tenant-Id")) return 0;
	if (strncasecmp(h, "X-", 2) && !ag_in_csv(h, std_csv)) return 0;
	return snprintf(out, outlen, "%s%s", prefix, h) < (int) outlen;
}

/* No "${" at all. FreeSWITCH runs ${api(args)} while expanding, and expands a variable's value recursively
 * (switch_channel.c:4173), so even a plain ${caller_id_name} or ${DP_MATCH} can expand a caller-supplied
 * ${system(...)}. The engine resolves or rejects its own ${...} templates before sending, so no legitimate value has one. */
static inline int ag_no_api_expansion(const char *s)
{
	return !s || !strstr(s, "${");
}

/* mod_dialplan_xml substitutes regex captures ($1, ${1}) into action data before the app expands it, so a capture of a
 * caller-controlled field could carry ${system(...)}: refuse "$" or "${" followed by a digit. */
static inline int ag_no_capture(const char *s)
{
	for (; s && (s = strchr(s, '$')); s++)
		if (isdigit((unsigned char) s[1]) || (s[1] == '{' && isdigit((unsigned char) s[2]))) return 0;
	return 1;
}

/* a sound file relative to the sounds directory: no absolute path, no "..", no URL or stream scheme, no spaces */
static inline int ag_safe_media(const char *path)
{
	if (!path || !*path || path[0] == '/' || strstr(path, "..") || strstr(path, "://") || strpbrk(path, " \t\r\n,|[]{}<>^$")) return 0;
	return 1;
}

static inline int ag_safe_word(const char *w)
{
	size_t i, n = w ? strlen(w) : 0;
	if (n == 0 || n > 64) return 0;
	for (i = 0; i < n; i++) if (!(isalnum((unsigned char) w[i]) || strchr("_+*#.-", w[i]))) return 0;
	return 1;
}

/* "<dest> [XML [default|grpc_api]]" */
static inline int ag_transfer_target(const char *s)
{
	char a[80] = "", b[16] = "", c[16] = "", extra[2] = "";
	int n = sscanf(s, "%79s %15s %15s %1s", a, b, c, extra);
	if (n < 1 || n > 3 || !ag_safe_word(a)) return 0;
	if (n >= 2 && strcmp(b, "XML")) return 0;
	if (n == 3 && strcmp(c, "default") && strcmp(c, "grpc_api")) return 0;
	return 1;
}

/* Every leg of an operator rule's dial string (already passed dg_safe_dial) reaches a SIP profile the engine also uses:
 * no loopback/ (re-enters the dialplan), user/, group/, error/ or a node gateway. */
static inline int ag_dial_endpoints_ok(const char *dial)
{
	const char *p = dial;
	if (!p) return 0;
	for (;;) {
		if (*p == '[' && !(p = strchr(p, ']'))) return 0;
		if (*p == ']') p++;
		if (strncmp(p, "sofia/internal/", 15) && strncmp(p, "sofia/external/", 15)) return 0;
		p += strcspn(p, ",|");
		if (!*p) return 1;
		p++;
	}
}

static inline int ag_xml_action_ok(const char *app, const char *data, const char *app_csv, const char *var_csv, const char *tenant);

static inline int ag_api_ok(const char *name, const char *args, const char *allow_csv, const char *app_csv, const char *var_csv, const char *uuid,
							const char *tenant)
{
	size_t un;
	const char *rest;
	if (!name || strlen(name) > 32 || ag_denied_api(name) || !ag_in_csv(name, allow_csv)) return 0;
	if (!args) args = "";
	if (strpbrk(args, "\r\n") || !ag_no_api_expansion(args)) return 0;
	if (!strcmp(name, "originate")) {
		char dial[1024];
		const char *sp = strchr(args, ' ');
		size_t n = sp ? (size_t) (sp - args) : strlen(args);
		if (!n || n >= sizeof dial) return 0;
		memcpy(dial, args, n);
		dial[n] = '\0';
		if (strchr(dial, '{') || !dg_safe_dial(dial, tenant) || !dg_single_leg(dial) || !ag_dial_endpoints_ok(dial)) return 0; /* {vars} would set any variable */
		if (!sp) return 0;
		sp++;
		if (*sp == '&') { /* &app(args): the application goes through the same check as an XML action */
			char oa[64], od[512];
			const char *lp = strchr(sp, '('), *end = sp + strlen(sp);
			size_t an, dn;
			if (!lp || end[-1] != ')' || strpbrk(sp, " \t")) return 0; /* originate splits on spaces: no extra positional fields */
			an = (size_t) (lp - sp - 1);
			dn = (size_t) (end - 1 - lp - 1);
			if (!an || an >= sizeof oa || dn >= sizeof od) return 0;
			memcpy(oa, sp + 1, an);
			oa[an] = '\0';
			memcpy(od, lp + 1, dn);
			od[dn] = '\0';
			return ag_xml_action_ok(oa, od, app_csv, var_csv, tenant);
		}
		return 0; /* "<dest> XML ...": the new leg has no tenant stamp nor fragment, so only &app() */
	}
	if (strncmp(name, "uuid_", 5)) return 0; /* no parser for it: refuse even if allow-listed */
	un = uuid ? strlen(uuid) : 0;
	if (!un || strncmp(args, uuid, un) || (args[un] != ' ' && args[un] != '\0')) return 0;
	rest = args[un] ? args + un + 1 : "";
	if (!strcmp(name, "uuid_setvar")) {
		char var[80] = "";
		return sscanf(rest, "%79s", var) == 1 && ag_var_ok(var, var_csv);
	}
	if (!strcmp(name, "uuid_transfer")) {
		if (!strncmp(rest, "-bleg ", 6)) rest += 6;
		else if (!strncmp(rest, "-both ", 6)) rest += 6;
		return ag_transfer_target(rest);
	}
	if (!strcmp(name, "uuid_broadcast")) {
		char what[512] = "", leg[8] = "", extra[2] = "";
		int n = sscanf(rest, "%511s %7s %1s", what, leg, extra);
		const char *sep;
		if (n < 1 || n > 2 || (n == 2 && strcmp(leg, "aleg") && strcmp(leg, "bleg") && strcmp(leg, "both"))) return 0;
		if ((sep = strstr(what, "::"))) {
			char app[64];
			size_t an = (size_t) (sep - what);
			if (!an || an >= sizeof app) return 0;
			memcpy(app, what, an);
			app[an] = '\0';
			if (!strcmp(app, "playback")) return ag_safe_media(sep + 2);
			return !strcmp(app, "hangup") && ag_in_csv(app, app_csv); /* no other application through a broadcast */
		}
		return ag_safe_media(what);
	}
	/* any other allow-listed uuid_* command: only plain words, and no second uuid (uuid_bridge) */
	for (;;) {
		char w[80];
		int used = 0;
		while (*rest == ' ') rest++;
		if (!*rest) return 1;
		if (sscanf(rest, "%79s%n", w, &used) != 1 || strlen(w) > 64 || !ag_safe_word(w) || dg_uuid_shaped(w)) return 0;
		rest += used;
	}
}

static inline int ag_xml_action_ok(const char *app, const char *data, const char *app_csv, const char *var_csv, const char *tenant)
{
	if (!app || !*app || ag_denied_app(app) || !ag_in_csv(app, app_csv)) return 0;
	if (!data) data = "";
	if (strpbrk(data, "\r\n") || !ag_no_api_expansion(data)) return 0;
	if (!strcmp(app, "set") || !strcmp(app, "export") || !strcmp(app, "unset")) {
		char var[80];
		const char *eq = strchr(data, '=');
		size_t n = eq ? (size_t) (eq - data) : strlen(data);
		if (!n || n >= sizeof var) return 0;
		memcpy(var, data, n);
		var[n] = '\0';
		return ag_var_ok(var, var_csv);
	}
	if (!strcmp(app, "playback")) return ag_safe_media(data);
	if (!strcmp(app, "bridge")) return !strchr(data, '{') && dg_safe_dial(data, tenant) && ag_dial_endpoints_ok(data);
	if (!strcmp(app, "transfer")) return ag_transfer_target(data);
	/* any other allowed application: only plain words */
	for (;;) {
		char w[80];
		int used = 0;
		while (isspace((unsigned char) *data)) data++;
		if (!*data) return 1;
		if (sscanf(data, "%79s%n", w, &used) != 1 || strlen(w) > 64 || !ag_safe_word(w)) return 0;
		data += used;
	}
}

#endif
