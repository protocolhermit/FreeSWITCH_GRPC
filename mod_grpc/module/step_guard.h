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

/* mod_grpc/module/step_guard.h
 * Argument guards for the inline primitives (Collect, Record, Conference,
 * Sleep). Plain C with no FreeSWITCH dependency so the unit tests can include
 * it. Paths and room names are built from the channel's own tenant (a UUID the
 * module checked), never from the command, so a command cannot step out of its
 * tenant's directory or conference namespace. */
#pragma once
#include <stdio.h>
#include <string.h>
#include "dial_guard.h"

/* One path component or room name: [A-Za-z0-9_.-], 1..64 chars, no leading '.'. */
static inline int sg_safe_name(const char *v)
{
	size_t i, n = v ? strlen(v) : 0;
	if (n < 1 || n > 64 || v[0] == '.') return 0;
	for (i = 0; i < n; i++) {
		char c = v[i];
		if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '-' || c == '.')) return 0;
	}
	return 1;
}

/* An MWI user: a safe name, or one leading '+' (E.164) and then a safe name. */
static inline int sg_mwi_user(const char *v)
{
	return v && (*v == '+' ? v[1] != '+' && sg_safe_name(v + 1) : sg_safe_name(v));
}

/* Empty is fine (no terminator); otherwise up to 4 of "0123456789#*". */
static inline int sg_safe_terminators(const char *v)
{
	size_t i, n = v ? strlen(v) : 0;
	if (n > 4) return 0;
	for (i = 0; i < n; i++)
		if (!strchr("0123456789#*", v[i])) return 0;
	return 1;
}

/* A prompt is a file or stream name (Collect and Playback); empty means silence. No "..", no "//" or "/./"
 * respellings (a scheme's "://" aside), and wherever <record_dir>/ appears it must be followed by <tenant>/:
 * a prompt may play the channel's own tenant's recordings, never another tenant's. */
static inline int sg_safe_prompt(const char *v, const char *record_dir, const char *tenant)
{
	const char *p;
	size_t dn = record_dir ? strlen(record_dir) : 0;
	if (!v || !*v) return 1;
	if (strlen(v) > 512 || !dg_safe_value(v) || strpbrk(v, " \t\v\f") || strstr(v, "..") || strstr(v, "/./")) return 0;
	for (p = v; (p = strstr(p, "//")); p++)
		if (p == v || p[-1] != ':') return 0;
	/* only the generated-audio schemes the engine sends: shell_stream:// runs commands, http:// reaches internal hosts,
	 * file_string:// chains files. local_stream:// names a configured hold-music stream, so its name must be one safe component */
	if ((p = strstr(v, "://")) && ((strncmp(v, "silence_stream://", 17) && strncmp(v, "tone_stream://", 14) &&
			(strncmp(v, "local_stream://", 15) || !sg_safe_name(v + 15))) || strstr(p + 3, "://")))
		return 0;
	while (dn > 1 && record_dir[dn - 1] == '/') dn--;
	if (dn < 2) return 1; /* no record dir configured */
	for (p = v; *p; p++)
		if (!strncmp(p, record_dir, dn) && p[dn] == '/' &&
			(!dg_uuid_shaped(tenant) || strncmp(p + dn + 1, tenant, 36) || p[dn + 37] != '/')) return 0;
	return 1;
}

/* uuid_broadcast arguments for Announce. Always "playback::": the API runs "app::args" as that application, so a bare
 * prompt such as "system::cmd" would run it. */
static inline int sg_broadcast_args(char *out, size_t n, const char *uuid, const char *file, int leg_only)
{
	int w = snprintf(out, n, "%s playback::%s %s", uuid, file, leg_only ? "aleg" : "both");
	return w > 0 && (size_t) w < n;
}

/* timeout_ms 0 is "no timeout" to switch_ivr_read (holds the call forever), so it is refused. */
static inline int sg_collect_ok(int min_digits, int max_digits, int timeout_ms)
{
	return min_digits >= 0 && max_digits >= 1 && max_digits <= 32 && min_digits <= max_digits && timeout_ms >= 1 && timeout_ms <= 60000;
}

static inline int sg_record_ok(int max_len_s, int silence_s)
{
	return max_len_s >= 1 && max_len_s <= 3600 && silence_s >= 0 && silence_s <= 60;
}

static inline int sg_sleep_ok(int ms) { return ms >= 0 && ms <= 60000; }

/* <dir>/<tenant>/<name>.wav; dir must be absolute, without "..", spaces or dial-string characters. */
static inline int sg_record_path(char *out, size_t n, const char *dir, const char *tenant, const char *name)
{
	int w;
	if (!dir || dir[0] != '/' || strstr(dir, "..") || !dg_safe_value(dir) || strpbrk(dir, " \t")) return 0;
	if (!dg_uuid_shaped(tenant) || !sg_safe_name(name)) return 0;
	w = snprintf(out, n, "%s/%s/%s.wav", dir, tenant, name);
	return w > 0 && (size_t) w < n;
}

/* <dir>/<tenant>/conf-<rec_id>.wav. rec_id is a UUID; max_s is the cap (1..14400). */
static inline int sg_conf_record_path(char *out, size_t n, const char *dir,
	const char *tenant, const char *rec_id, int max_s)
{
	char name[64];
	if (!dg_uuid_shaped(rec_id) || max_s < 1 || max_s > 14400) return 0;
	if (snprintf(name, sizeof(name), "conf-%s", rec_id) >= (int) sizeof(name)) return 0;
	return sg_record_path(out, n, dir, tenant, name);
}

/* <tenant>_<room>: the conference namespace of one tenant. */
static inline int sg_room(char *out, size_t n, const char *tenant, const char *room)
{
	int w;
	if (!dg_uuid_shaped(tenant) || !sg_safe_name(room)) return 0;
	w = snprintf(out, n, "%s_%s", tenant, room);
	return w > 0 && (size_t) w < n;
}

/* The MWI-Voice-Message value "<new>/<saved> (<new_urgent>/<saved_urgent>)". 0 on a negative count or a too-small buffer. */
static inline int sg_mwi_voice(int nw, int sv, int nu, int su, char *out, size_t n)
{
	int w;
	if (nw < 0 || sv < 0 || nu < 0 || su < 0) return 0;
	w = snprintf(out, n, "%d/%d (%d/%d)", nw, sv, nu, su);
	return w > 0 && (size_t) w < n;
}

/* A presigned object-store URL: http(s), bounded, no whitespace, control or quoting characters. */
static inline int sg_safe_url(const char *v)
{
	size_t i, n = v ? strlen(v) : 0;
	if (n < 8 || n > 2048) return 0;
	if (strncmp(v, "http://", 7) && strncmp(v, "https://", 8)) return 0;
	for (i = 0; i < n; i++) {
		unsigned char c = (unsigned char) v[i];
		if (c <= 0x20 || c >= 0x7f || strchr("'\"<>\\{}^`|", c)) return 0;
	}
	return 1;
}

static inline char sg_supervise_mode_dtmf(int mode)
{
	switch (mode) {
	case 1: return '0'; /* Listen: both heard, supervisor muted */
	case 2: return '2'; /* Whisper: target leg only hears supervisor */
	case 3: return '3'; /* Barge: both legs hear supervisor */
	default: return 0;
	}
}

/* The member flags for `conference <room>@<profile>+flags{...}`; "" when none are set. wait-mod holds members (their audio is gated, no mute flag) until a moderator joins. */
static inline int sg_conf_flags(char *out, size_t n, int mute, int moderator, int wait_mod)
{
	int w;
	if (!n) return 0;
	if (!mute && !moderator && !wait_mod) { out[0] = 0; return 1; }
	w = snprintf(out, n, "+flags{%s%s%s%s%s}", mute ? "mute" : "", mute && (moderator || wait_mod) ? "|" : "",
				 moderator ? "moderator" : "", moderator && wait_mod ? "|" : "", wait_mod ? "wait-mod" : "");
	return w > 0 && (size_t) w < n;
}

enum { SG_CONF_MUTE = 1, SG_CONF_UNMUTE, SG_CONF_MUTE_ALL, SG_CONF_UNMUTE_ALL, SG_CONF_KICK, SG_CONF_LOCK, SG_CONF_UNLOCK, SG_CONF_END, SG_CONF_RECORD_START, SG_CONF_RECORD_STOP };

/* A FreeSWITCH conference member id: decimal digits only, so "all", "last" and "non_moderator" can never come from a caller. */
static inline int sg_conf_member_id(const char *v)
{
	size_t i, n = v ? strlen(v) : 0;
	if (n < 1 || n > 9) return 0;
	for (i = 0; i < n; i++) if (v[i] < '0' || v[i] > '9') return 0;
	return 1;
}

/* The argument of the `conference` API for one ConferenceControl action. room is <tenant>_<room> (from sg_room). */
static inline int sg_conf_control(char *out, size_t n, int action, const char *room, const char *member_id)
{
	int w;
	if (!room || !*room || strlen(room) > 120 || strpbrk(room, " \t\r\n") || !n) return 0;
	switch (action) {
	case SG_CONF_MUTE: case SG_CONF_UNMUTE: case SG_CONF_KICK:
		if (!sg_conf_member_id(member_id)) return 0;
		w = snprintf(out, n, "%s %s %s", room, action == SG_CONF_MUTE ? "mute" : action == SG_CONF_UNMUTE ? "unmute" : "kick", member_id);
		break;
	case SG_CONF_MUTE_ALL: w = snprintf(out, n, "%s mute non_moderator", room); break;
	case SG_CONF_UNMUTE_ALL: w = snprintf(out, n, "%s unmute non_moderator", room); break;
	case SG_CONF_LOCK: w = snprintf(out, n, "%s lock", room); break;
	case SG_CONF_UNLOCK: w = snprintf(out, n, "%s unlock", room); break;
	case SG_CONF_END: w = snprintf(out, n, "%s kick all", room); break;
	default: return 0;
	}
	return w > 0 && (size_t) w < n;
}

