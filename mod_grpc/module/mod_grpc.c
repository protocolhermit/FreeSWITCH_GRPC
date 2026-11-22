/*
 * mod_grpc -- FreeSWITCH side of the control-plane stream.
 *
 * Thin shell: all transport (stream, cache, reconnect, TLS) is in
 * libgrpc_bridge behind a flat C API. Nothing here blocks the bridge's
 * threads: engine commands are queued and run on this module's own thread.
 */
#include <switch.h>

#include <ctype.h>
#include <curl/curl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "grpc_bridge.h"
#include "dial_guard.h"
#include "survival.h"
#include "step_guard.h"
#include "conference_recording.h"
#include "api_guard.h"
#include "xml_fetch.h"

SWITCH_MODULE_LOAD_FUNCTION(mod_grpc_load);
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_grpc_shutdown);
SWITCH_MODULE_DEFINITION(mod_grpc, mod_grpc_load, mod_grpc_shutdown, NULL);

#define CMDQ_SIZE 1024
#define ROUTE_TIMEOUT_MS 3000
#define STEP_TIMEOUT_MS 3000
#define STEP_ATTEMPTS 6
#define STEP_RETRY_US 1000000
#define XML_FETCH_TIMEOUT_MS 1500

static struct {
	switch_memory_pool_t *pool;
	gb_bridge_t *bridge;
	switch_queue_t *cmdq;
	switch_thread_t *cmd_thread;
	switch_event_node_t *nodes[4];
	switch_event_node_t *conf_node; /* conference::maintenance, separate from the channel events */
	switch_xml_binding_t *dir_binding;
	switch_xml_binding_t *dp_binding;
	switch_xml_binding_t *xml_binding; /* RM-28: engine-served dialplan and configuration; NULL unless xml-sections lists something */
	xf_sections_t xf;
	int xf_mode;                  /* XF_FETCH_CALL or XF_FETCH_CONTEXT */
	const char *xml_sections_raw; /* as configured, for `grpc xml` */
	unsigned xml_served, xml_rejected; /* state_mutex */
	char xml_last_reject[200];         /* "<key>: <reason>", state_mutex */
	/* acl.conf from the engine: see acl_reload_thread. All but the thread handle and acl_stop are under state_mutex. */
	switch_thread_t *acl_thread;
	volatile int acl_stop;
	int acl_pending;              /* a connect or an Invalidate asked for a reload */
	int acl_served;               /* the binding served acl.conf during the reload in progress */
	int64_t acl_served_version;
	int acl_rejected;             /* this document version locked the node out: not served again */
	int64_t acl_rejected_version;
	char engine_ip[64];           /* the engine address apply-outbound-acl last allowed */
	unsigned acl_reloads, acl_fetch_failed;
	const char *acl_source;       /* "engine" or "disk" after a reload */
	int acl_doc_active;           /* a reload started by acl_reload_thread is running: the binding serves acl_doc */
	int acl_doc_found;
	int64_t acl_doc_version;
	char *acl_doc;                /* the answer that thread fetched (malloc'd), NULL when not found */
	const char *api_allow;
	const char *api_headers;
	const char *xml_app_allow;
	const char *var_allow;
	const char *route_vars; /* extra channel variables sent on RouteRequest and StepResult; "" = none */
	int nnodes;
	switch_mutex_t *rec_mutex; /* job list only; never held across a conference API or HTTP call */
	switch_queue_t *uploadq;
	switch_thread_t *maint_thread, *upload_thread;
	int rec_refuse, rec_maint_stop, rec_stop;
	cr_job_t jobs[CR_MAX_JOBS];
	char *token_file;
	char *acl;
	char *endpoints;
	char *node_id;
	char *record_dir;         /* Record writes <record-dir>/<tenant>/<name>.wav */
	char *unavailable_prompt; /* played before hanging up when a step cannot be reported */
	int survival;                /* survival mode on: see survival_route */
	int survival_refresh_s, survival_max_age_s;
	char *survival_dial_format;  /* (number, domain) -> one device leg through the edge, like the engine's -dial-format */
	switch_bool_t draining;
	switch_mutex_t *state_mutex;
	char last_reason[256]; /* why the stream last changed state, for `grpc status` */
} globals;

/* ---------------------------------------------------------------- commands */

typedef struct {
	char *command_id, *tenant_id, *uuid, *dial_string, *file, *cause, *peer_tenant_id, *peer_caller_id;
	char *target_uuid, *token, *room, *rec_id, *upload_url;
	int type, conf_action, timeout_s, deadline_s, mode, leg_only, route_on_answer, max_len_s;
	char **keys, **values;
	size_t nvars;
} cmd_item_t;

static char *dupz(const char *s) { return strdup(s ? s : ""); }

static cmd_item_t *cmd_dup(const gb_command_t *c)
{
	size_t i;
	cmd_item_t *o = calloc(1, sizeof(*o));
	if (!o) return NULL;
	o->command_id = dupz(c->command_id);
	o->tenant_id = dupz(c->tenant_id);
	o->uuid = dupz(c->uuid);
	o->dial_string = dupz(c->dial_string);
	o->peer_tenant_id = dupz(c->peer_tenant_id);
	o->peer_caller_id = dupz(c->peer_caller_id);
	o->target_uuid = dupz(c->target_uuid);
	o->token = dupz(c->token);
	o->room = dupz(c->room);
	o->rec_id = dupz(c->rec_id);
	o->upload_url = dupz(c->upload_url);
	o->conf_action = c->conf_action;
	o->max_len_s = c->max_len_s;
	o->file = dupz(c->file);
	o->cause = dupz(c->cause);
	o->type = c->type;
	o->timeout_s = c->timeout_s;
	o->deadline_s = c->deadline_s;
	o->mode = c->mode;
	o->leg_only = c->leg_only;
	o->route_on_answer = c->route_on_answer;
	if (c->nvars) {
		o->keys = calloc(c->nvars, sizeof(char *));
		o->values = calloc(c->nvars, sizeof(char *));
		if (o->keys && o->values) {
			o->nvars = c->nvars;
			for (i = 0; i < c->nvars; i++) {
				o->keys[i] = dupz(c->vars[i].key);
				o->values[i] = dupz(c->vars[i].value);
			}
		}
	}
	return o;
}

static void cmd_free(cmd_item_t *o)
{
	size_t i;
	if (!o) return;
	free(o->command_id); free(o->tenant_id); free(o->uuid);
	free(o->dial_string); free(o->file); free(o->cause);
	free(o->peer_tenant_id); free(o->peer_caller_id);
	free(o->target_uuid); free(o->token); free(o->room); free(o->rec_id); free(o->upload_url);
	for (i = 0; i < o->nvars; i++) { free(o->keys[i]); free(o->values[i]); }
	free(o->keys); free(o->values);
	free(o);
}

/* Runs an API command and reports whether it answered +OK. */
static switch_bool_t run_api(const char *name, const char *args, char *err, size_t errlen)
{
	switch_stream_handle_t stream = { 0 };
	switch_bool_t ok;
	SWITCH_STANDARD_STREAM(stream);
	switch_api_execute(name, args, NULL, &stream);
	ok = (stream.data && !strncmp((char *) stream.data, "+OK", 3)) ? SWITCH_TRUE : SWITCH_FALSE;
	if (!ok) switch_snprintf(err, errlen, "%s: %s", name, stream.data ? (char *) stream.data : "no answer");
	switch_safe_free(stream.data);
	return ok;
}

/* The conference API answers "OK ..." (not "+OK") and says why in plain text otherwise. */
static switch_bool_t run_conference_api(const char *args, char *err, size_t errlen)
{
	switch_stream_handle_t stream = { 0 };
	switch_bool_t ok;
	SWITCH_STANDARD_STREAM(stream);
	switch_api_execute("conference", args, NULL, &stream);
	ok = (stream.data && (!strncmp((char *) stream.data, "OK", 2) || !strncmp((char *) stream.data, "+OK", 3))) ? SWITCH_TRUE : SWITCH_FALSE;
	if (!ok) switch_snprintf(err, errlen, "conference: %s", stream.data ? (char *) stream.data : "no answer");
	switch_safe_free(stream.data);
	return ok;
}

static switch_bool_t safe_value(const char *v) { return dg_safe_value(v) ? SWITCH_TRUE : SWITCH_FALSE; }
static switch_bool_t safe_dial(const char *v, const char *tenant) { return dg_safe_dial(v, tenant) ? SWITCH_TRUE : SWITCH_FALSE; }

/* A command that names a call must name the tenant that owns the channel. */
static switch_bool_t tenant_owns(const char *uuid, const char *tenant)
{
	switch_core_session_t *s = switch_core_session_locate(uuid);
	const char *owner;
	switch_bool_t ok = SWITCH_FALSE;
	if (!s) return SWITCH_FALSE;
	owner = switch_channel_get_variable(switch_core_session_get_channel(s), "tenant_id");
	ok = (owner && tenant && *tenant && !strcmp(owner, tenant)) ? SWITCH_TRUE : SWITCH_FALSE;
	switch_core_session_rwunlock(s);
	return ok;
}

/* Appends to buf at *n; refuses (writing nothing) when the result plus NUL does not fit. */
static switch_bool_t sappend(char *buf, size_t size, size_t *n, const char *fmt, ...)
{
	va_list ap;
	int w;
	va_start(ap, fmt);
	w = vsnprintf(buf + *n, size - *n, fmt, ap); /* glibc returns the would-be length */
	va_end(ap);
	if (w < 0 || (size_t) w >= size - *n) {
		buf[*n] = 0;
		return SWITCH_FALSE;
	}
	*n += (size_t) w;
	return SWITCH_TRUE;
}

/* The originate leg carries the command's tenant as X-Tenant-Id (the claim the edge requires on legs to it) when the
 * tenant is UUID-shaped; the dial string itself may not carry a stamp (safe_dial with no tenant).
 *
 * route_on_answer (the dialer's calls): the leg goes to a carrier, so it carries no stamp header; the request domain is
 * set here to dialer.invalid, never taken from the engine; the leg may carry only the variables dg_dialer_var_ok allows;
 * it is one leg (no failover list: ringing a second gateway would call the contact twice); and when the far end answers
 * the new leg runs grpc_route, which hands it to the engine like a call that arrived, instead of parking. */
static switch_bool_t exec_originate(const cmd_item_t *c, char *err, size_t errlen)
{
	char args[4096];
	size_t i, n = 0;
	const int routed = c->route_on_answer;
	if (!zstr(c->peer_tenant_id) || !zstr(c->peer_caller_id) || !c->tenant_id[0] || !safe_value(c->tenant_id) || !safe_dial(c->dial_string, NULL) ||
		(routed && (!dg_uuid_shaped(c->tenant_id) || !dg_single_leg(c->dial_string)))) {
		switch_snprintf(err, errlen, "originate: bad tenant or dial string");
		return SWITCH_FALSE;
	}
	if (!sappend(args, sizeof(args), &n, "{tenant_id=%s", c->tenant_id)) goto too_long;
	if (!routed && dg_uuid_shaped(c->tenant_id) && !sappend(args, sizeof(args), &n, ",sip_h_X-Tenant-Id=%s", c->tenant_id)) goto too_long;
	if (routed && !sappend(args, sizeof(args), &n, ",domain_name=dialer.invalid")) goto too_long;
	if (c->timeout_s > 0 && !sappend(args, sizeof(args), &n, ",originate_timeout=%d", c->timeout_s)) goto too_long;
	for (i = 0; i < c->nvars; i++) {
		if (!safe_value(c->keys[i]) || !safe_value(c->values[i]) || strpbrk(c->keys[i], ",= \t")) {
			switch_snprintf(err, errlen, "originate: bad variable %s", c->keys[i]);
			return SWITCH_FALSE;
		}
		if (routed && !dg_dialer_var_ok(c->keys[i], c->values[i])) {
			switch_snprintf(err, errlen, "originate: variable %s not allowed on a routed leg", c->keys[i]);
			return SWITCH_FALSE;
		}
		if (!strcmp(c->keys[i], "tenant_id") || !strcasecmp(c->keys[i], "sip_h_X-Tenant-Id")) continue; /* the command's tenant wins */
		if (!sappend(args, sizeof(args), &n, ",%s='%s'", c->keys[i], c->values[i])) goto too_long;
	}
	if (!sappend(args, sizeof(args), &n, "}%s %s", c->dial_string, routed ? "&grpc_route()" : "&park()")) goto too_long;
	return run_api("originate", args, err, errlen);
too_long:
	switch_snprintf(err, errlen, "originate: too long");
	return SWITCH_FALSE;
}

/* Peer identity is engine-authorized; ordinary bridges remove metadata from an earlier peer bridge. */
static void bridge_identity(switch_channel_t *channel, const char *peer, const char *caller)
{
	/* Origination reads exported variables before Sofia builds the INVITE; bridge exports run after answer. */
	switch_channel_export_variable_var_check(channel, "origination_caller_id_number", zstr(peer) ? NULL : caller, SWITCH_EXPORT_VARS_VARIABLE, SWITCH_FALSE);
	switch_channel_export_variable_var_check(channel, "origination_caller_id_name", zstr(peer) ? NULL : caller, SWITCH_EXPORT_VARS_VARIABLE, SWITCH_FALSE);
	switch_channel_export_variable_var_check(channel, "sip_invite_from_uri", zstr(peer) ? NULL : caller, SWITCH_EXPORT_VARS_VARIABLE, SWITCH_FALSE);
	switch_channel_export_variable_var_check(channel, "sip_from_display", zstr(peer) ? NULL : caller, SWITCH_EXPORT_VARS_VARIABLE, SWITCH_FALSE);
	switch_channel_set_variable(channel, "eswitch_peer_tenant", zstr(peer) ? NULL : peer);
	switch_channel_set_variable(channel, "bridge_export_vars", "tenant_id,call_id,eswitch_peer_tenant");
}

static switch_bool_t exec_conf_record(const cmd_item_t *c, const char *conf, char *err, size_t errlen);
static void cr_end_stop(const char *conf);
static const char *channel_tenant(switch_channel_t *channel);

/* ConferenceControl has no channel: the room is built from the command's tenant, and a member action names a channel
 * that must belong to that tenant and sit in that very room (member ids are only unique within one conference). */
static switch_bool_t exec_conference_control(const cmd_item_t *c, char *err, size_t errlen)
{
	char room[128], args[256], member_id[16] = "";
	if (!sg_room(room, sizeof(room), c->tenant_id, c->room)) {
		switch_snprintf(err, errlen, "conference_control: bad room");
		return SWITCH_FALSE;
	}
	/* 9 = RECORD_START, 10 = RECORD_STOP. Before sg_conf_control, which has no recording arm. */
	if (c->conf_action == 9 || c->conf_action == 10) return exec_conf_record(c, room, err, errlen);
	/* End kicks everyone, but the known recording stops by its saved path first. The job stays until upload. */
	if (c->conf_action == SG_CONF_END) cr_end_stop(room);
	if (c->conf_action == SG_CONF_MUTE || c->conf_action == SG_CONF_UNMUTE || c->conf_action == SG_CONF_KICK) {
		switch_core_session_t *s;
		const char *id, *name;
		if (zstr(c->target_uuid) || !safe_value(c->target_uuid) || !tenant_owns(c->target_uuid, c->tenant_id) ||
			!(s = switch_core_session_locate(c->target_uuid))) {
			switch_snprintf(err, errlen, "no such member for this tenant");
			return SWITCH_FALSE;
		}
		id = switch_channel_get_variable(switch_core_session_get_channel(s), "conference_member_id");
		name = switch_channel_get_variable(switch_core_session_get_channel(s), "conference_name");
		if (zstr(id) || zstr(name) || strcmp(name, room) || !sg_conf_member_id(id)) {
			switch_core_session_rwunlock(s);
			switch_snprintf(err, errlen, "member is not in this room");
			return SWITCH_FALSE;
		}
		switch_snprintf(member_id, sizeof(member_id), "%s", id);
		switch_core_session_rwunlock(s);
	}
	if (!sg_conf_control(args, sizeof(args), c->conf_action, room, member_id)) {
		switch_snprintf(err, errlen, "conference_control: bad action %d", c->conf_action);
		return SWITCH_FALSE;
	}
	return run_conference_api(args, err, errlen);
}

/* Commands that arrive on the stream (or name another call): by uuid, via the API layer. */
static switch_bool_t exec_command(const cmd_item_t *c, char *err, size_t errlen)
{
	char args[2048];
	int ctl = 1;

	if (c->type < GB_CMD_ORIGINATE || c->type > GB_CMD_CONFERENCE_CONTROL) {
		switch_snprintf(err, errlen, "unknown command type %d", c->type);
		return SWITCH_FALSE;
	}

	switch (c->type) {
	case GB_CMD_ORIGINATE:
		return exec_originate(c, err, errlen);
	case GB_CMD_CONFERENCE_CONTROL:
		return exec_conference_control(c, err, errlen);
	case GB_CMD_DRAIN:
		globals.draining = SWITCH_TRUE;
		switch_core_session_ctl(SCSC_PAUSE_INBOUND, &ctl); /* non-zero sets SCF_NO_NEW_INBOUND_SESSIONS */
		/* the deadline is the operator's: the engine keeps established calls up, so the node never cuts them; "grpc undrain"
		 * or a module reload takes new calls again */
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "draining: no new inbound calls (operator deadline %ds; established calls are kept)\n", c->deadline_s);
		return SWITCH_TRUE;
	default:
		break;
	}
	if (!c->uuid[0] || !safe_value(c->uuid)) {
		switch_snprintf(err, errlen, "command needs a uuid");
		return SWITCH_FALSE;
	}
	if (!tenant_owns(c->uuid, c->tenant_id)) {
		switch_snprintf(err, errlen, "no such call for this tenant");
		return SWITCH_FALSE;
	}
	switch (c->type) {
	case GB_CMD_BRIDGE:
		if (zstr(c->peer_tenant_id) ? !safe_dial(c->dial_string, c->tenant_id) :
			!dg_safe_peer_bridge(c->dial_string, c->tenant_id, c->peer_tenant_id, c->peer_caller_id)) {
			switch_snprintf(err, errlen, "bridge: bad dial string");
			return SWITCH_FALSE;
		}
		{
			switch_core_session_t *target = switch_core_session_locate(c->uuid);
			switch_channel_t *channel;
			if (!target) { switch_snprintf(err, errlen, "no such call for this tenant"); return SWITCH_FALSE; }
			channel = switch_core_session_get_channel(target);
			if (zstr(switch_channel_get_variable(channel, "tenant_id")) || strcmp(c->tenant_id, switch_channel_get_variable(channel, "tenant_id"))) {
				switch_core_session_rwunlock(target);
				switch_snprintf(err, errlen, "no such call for this tenant"); return SWITCH_FALSE;
			}
			bridge_identity(channel, c->peer_tenant_id, c->peer_caller_id);
			switch_core_session_rwunlock(target);
		}
		if (c->timeout_s > 0) {
			switch_snprintf(args, sizeof(args), "%s call_timeout=%d", c->uuid, c->timeout_s);
			run_api("uuid_setvar", args, err, errlen);
		}
		switch_snprintf(args, sizeof(args), "%s bridge::%s aleg", c->uuid, c->dial_string);
		return run_api("uuid_broadcast", args, err, errlen);
	case GB_CMD_PARK:
		return run_api("uuid_park", c->uuid, err, errlen);
	case GB_CMD_PLAYBACK:
		if (!sg_safe_prompt(c->file, globals.record_dir, c->tenant_id)) { /* c->tenant_id owns the call (tenant_owns above) */
			switch_snprintf(err, errlen, "playback: bad file");
			return SWITCH_FALSE;
		}
		switch_snprintf(args, sizeof(args), "%s playback::%s aleg", c->uuid, c->file);
		return run_api("uuid_broadcast", args, err, errlen);
	case GB_CMD_HANGUP:
		switch_snprintf(args, sizeof(args), "%s %s", c->uuid, c->cause[0] ? c->cause : "NORMAL_CLEARING");
		return run_api("uuid_kill", args, err, errlen);
	case GB_CMD_SUPERVISE_MODE: {
		char digit = sg_supervise_mode_dtmf(c->mode);
		switch_core_session_t *s;
		switch_channel_t *channel;
		if (!digit) {
			switch_snprintf(err, errlen, "supervise_mode: invalid mode %d", c->mode);
			return SWITCH_FALSE;
		}
		s = switch_core_session_locate(c->uuid);
		if (!s) {
			switch_snprintf(err, errlen, "no such call for this tenant");
			return SWITCH_FALSE;
		}
		channel = switch_core_session_get_channel(s);
		if (zstr(switch_channel_get_variable(channel, "supervise"))) {
			switch_core_session_rwunlock(s);
			switch_snprintf(err, errlen, "not a supervision channel");
			return SWITCH_FALSE;
		}
		switch_core_session_rwunlock(s);
		switch_snprintf(args, sizeof(args), "%s %c", c->uuid, digit);
		return run_api("uuid_recv_dtmf", args, err, errlen);
	}
	case GB_CMD_ANNOUNCE: {
		if (!sg_safe_prompt(c->file, globals.record_dir, c->tenant_id) || !sg_broadcast_args(args, sizeof(args), c->uuid, c->file, c->leg_only)) {
			switch_snprintf(err, errlen, "invalid announce prompt: %s", c->file);
			return SWITCH_FALSE;
		}
		return run_api("uuid_broadcast", args, err, errlen);
	}
	case GB_CMD_TRANSFER: {
		if (!dg_uuid_shaped(c->token)) {
			switch_snprintf(err, errlen, "transfer: bad token");
			return SWITCH_FALSE;
		}
		switch_snprintf(args, sizeof(args), "%s xfer_token %s", c->uuid, c->token);
		if (!run_api("uuid_setvar", args, err, errlen)) {
			return SWITCH_FALSE;
		}
		switch_snprintf(args, sizeof(args), "%s xfer XML default", c->uuid);
		return run_api("uuid_transfer", args, err, errlen);
	}
	case GB_CMD_BRIDGE_LEGS: {
		if (!c->target_uuid[0] || !safe_value(c->target_uuid)) {
			switch_snprintf(err, errlen, "bridge_legs: target uuid required");
			return SWITCH_FALSE;
		}
		if (!tenant_owns(c->target_uuid, c->tenant_id)) {
			switch_snprintf(err, errlen, "no such call for this tenant");
			return SWITCH_FALSE;
		}
		switch_snprintf(args, sizeof(args), "%s %s", c->uuid, c->target_uuid);
		return run_api("uuid_bridge", args, err, errlen);
	}
	default:
		switch_snprintf(err, errlen, "unknown command type %d", c->type);
		return SWITCH_FALSE;
	}
}

static void run_and_report(const cmd_item_t *c)
{
	char err[256] = "";
	switch_bool_t ok = exec_command(c, err, sizeof(err));
	gb_send_command_result(globals.bridge, c->command_id, ok == SWITCH_TRUE, err);
	if (!ok) switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "command %s failed: %s\n", c->command_id, err);
}

static void *SWITCH_THREAD_FUNC command_thread(switch_thread_t *thread, void *obj)
{
	void *item = NULL;
	while (switch_queue_pop(globals.cmdq, &item) == SWITCH_STATUS_SUCCESS) {
		if (!item) break; /* shutdown sentinel */
		run_and_report((cmd_item_t *) item);
		cmd_free((cmd_item_t *) item);
	}
	return NULL;
}

/* ------------------------------------------------------------ bridge hooks */

static void cb_state(void *ud, int connected, const char *reason)
{
	switch_mutex_lock(globals.state_mutex);
	switch_snprintf(globals.last_reason, sizeof(globals.last_reason), "%s", reason);
	if (connected) globals.acl_pending = 1; /* FreeSWITCH loaded acl.conf before this module existed */
	switch_mutex_unlock(globals.state_mutex);
	switch_log_printf(SWITCH_CHANNEL_LOG, connected ? SWITCH_LOG_NOTICE : SWITCH_LOG_WARNING, "engine stream %s: %s\n",
					  connected ? "up" : "down", reason);
}

static void cb_command(void *ud, const gb_command_t *c)
{
	cmd_item_t *item = cmd_dup(c);
	if (!item || switch_queue_trypush(globals.cmdq, item) != SWITCH_STATUS_SUCCESS) {
		gb_send_command_result(globals.bridge, c->command_id, 0, "node command queue full");
		cmd_free(item);
	}
}

#define TOKEN_LINE_MAX 512 /* token-file line, newline included; cb_token and read_token_file share it */

static void cb_token(void *ud, const char *token, int overlap_s)
{
	char tmp[1024];
	int fd, ok = 0;
	if (!globals.token_file) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "engine rotated the token but no token-file is configured; the new token is lost on restart\n");
		return;
	}
	/* unlink first so a stale file (old mode) or a symlink at the tmp path is never reused; O_EXCL|O_NOFOLLOW enforce it */
	if (switch_snprintf(tmp, sizeof(tmp), "%s.tmp", globals.token_file) < (int) sizeof(tmp) - 1 &&
		(unlink(tmp), 1) &&
		(fd = open(tmp, O_WRONLY | O_CREAT | O_EXCL | O_TRUNC | O_NOFOLLOW, 0600)) >= 0) {
		char line[TOKEN_LINE_MAX];
		int len = switch_snprintf(line, sizeof(line), "%s\n", token);
		ok = len < (int) sizeof(line) - 1 && write(fd, line, len) == len && fsync(fd) == 0;
		ok = (close(fd) == 0) && ok && rename(tmp, globals.token_file) == 0;
		if (!ok) unlink(tmp);
	}
	if (!ok) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "cannot write token-file %s; keeping the old file\n", globals.token_file);
		return;
	}
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "token rotated (old token valid %ds)\n", overlap_s);
}

static int cb_allow_addr(void *ud, const char *ip)
{
	int ok = zstr(globals.acl) || switch_check_network_list_ip(ip, globals.acl) == SWITCH_TRUE; /* unknown ACL name refuses */
	if (ok) { /* ponytail: the last allowed address stands for the engine; track the connected peer if hosts resolve to many */
		switch_mutex_lock(globals.state_mutex);
		switch_snprintf(globals.engine_ip, sizeof(globals.engine_ip), "%s", ip);
		switch_mutex_unlock(globals.state_mutex);
	}
	return ok;
}

/* The bridge has already dropped the cached entries. Only a flag is set here: the reload itself re-enters the XML
 * binding and gb_lookup, which a bridge callback must not do. */
static void cb_invalidate(void *ud, const char *tenant, int kind, const char *key, int64_t version)
{
	if (!xf_acl_invalidate(kind, tenant, key)) return;
	switch_mutex_lock(globals.state_mutex);
	globals.acl_rejected = 0; /* the engine may have corrected the document */
	globals.acl_pending = 1;
	switch_mutex_unlock(globals.state_mutex);
}

static void cb_stats(void *ud, int32_t *calls, int32_t *cpu)
{
	double idle = switch_core_idle_cpu();
	*calls = (int32_t) switch_core_session_count();
	*cpu = (int32_t) (idle >= 0 && idle <= 100 ? 100.0 - idle : 0);
}

static void cb_log(void *ud, int level, const char *msg)
{
	static const switch_log_level_t map[] = { SWITCH_LOG_ERROR, SWITCH_LOG_WARNING, SWITCH_LOG_INFO, SWITCH_LOG_DEBUG };
	switch_log_printf(SWITCH_CHANNEL_LOG, map[level < 0 ? 0 : (level > 3 ? 3 : level)], "%s\n", msg);
}

/* ------------------------------------------------------------ call events */

static void event_handler(switch_event_t *event)
{
	const char *tenant = switch_event_get_header(event, "variable_tenant_id");
	const char *uuid = switch_event_get_header(event, "Unique-ID");
	const char *call_id = switch_event_get_header(event, "variable_call_id");
	const char *digit = NULL, *cause = NULL, *peer = NULL;
	char tbuf[128] = "", cbuf[256] = "";
	int type;

	/* FreeSWITCH puts channel variables on DTMF events only with verbose events on, so read them from the session. */
	if (zstr(tenant) && !zstr(uuid) && event->event_id == SWITCH_EVENT_DTMF) {
		switch_core_session_t *ls = switch_core_session_locate(uuid);
		if (ls) {
			switch_channel_t *ch = switch_core_session_get_channel(ls);
			const char *t = switch_channel_get_variable(ch, "tenant_id"), *c = switch_channel_get_variable(ch, "call_id");
			if (!zstr(t)) switch_snprintf(tbuf, sizeof(tbuf), "%s", t);
			if (!zstr(c)) switch_snprintf(cbuf, sizeof(cbuf), "%s", c);
			switch_core_session_rwunlock(ls);
			tenant = tbuf;
			call_id = cbuf;
		}
	}
	if (zstr(tenant) || zstr(uuid)) return;
	switch (event->event_id) {
	case SWITCH_EVENT_CHANNEL_CREATE: type = GB_EVT_CREATED; break;
	case SWITCH_EVENT_CHANNEL_ANSWER: type = GB_EVT_ANSWERED; peer = switch_event_get_header(event, "Caller-Destination-Number"); break;
	case SWITCH_EVENT_DTMF: type = GB_EVT_DTMF; digit = switch_event_get_header(event, "DTMF-Digit"); break;
	case SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE: type = GB_EVT_HANGUP; cause = switch_event_get_header(event, "Hangup-Cause"); break;
	default: return;
	}
	gb_send_call_event(globals.bridge, tenant, zstr(call_id) ? uuid : call_id, uuid, type, digit, cause, peer, switch_event_get_header(event, "variable_eswitch_peer_tenant"));
}

/* ------------------------------------------------------- tenant resolution */

/* Kamailio normally names the tenant (sip_h_X-Tenant-Id). When a call or a
 * directory lookup arrives without one, ask the engine which tenant owns the
 * domain: one tenant per domain, so the answer is unique. The engine
 * re-validates the domain against the tenant on every later request. */
static switch_bool_t resolve_tenant(const char *domain, char *out, size_t outlen)
{
	char key[300], *p;
	gb_lookup_result_t r;
	switch_bool_t ok = SWITCH_FALSE;

	if (zstr(domain) || strlen(domain) > 250) return SWITCH_FALSE;
	switch_snprintf(key, sizeof(key), "domain:%s", domain);
	for (p = key; *p; p++) *p = (char) tolower((unsigned char) *p);
	if (gb_lookup(globals.bridge, "", GB_KIND_CONFIG, key, 1500, &r) != GB_OK) return SWITCH_FALSE;
	if (r.found && r.payload) {
		cJSON *j = cJSON_Parse(r.payload);
		cJSON *t = j ? cJSON_GetObjectItem(j, "tenant_id") : NULL;
		if (t && cJSON_IsString(t) && !zstr(t->valuestring) && strlen(t->valuestring) < outlen) {
			switch_snprintf(out, outlen, "%s", t->valuestring);
			ok = SWITCH_TRUE;
		}
		if (j) cJSON_Delete(j);
	}
	gb_lookup_result_free(&r);
	return ok;
}

/* ------------------------------------------------------ directory binding */

static void xml_escape(switch_stream_handle_t *s, const char *in)
{
	for (; in && *in; in++) {
		switch (*in) {
		case '&': s->write_function(s, "&amp;"); break;
		case '<': s->write_function(s, "&lt;"); break;
		case '>': s->write_function(s, "&gt;"); break;
		case '"': s->write_function(s, "&quot;"); break;
		default: s->write_function(s, "%c", *in);
		}
	}
}

/* Answers FreeSWITCH's directory questions from the engine: the user's entry
 * for a domain, with the tenant as a channel variable. */
static switch_xml_t directory_search(const char *section, const char *tag_name, const char *key_name, const char *key_value,
									 switch_event_t *params, void *user_data)
{
	const char *domain, *user, *given;
	char tenant[64] = "", lookup_key[300];
	gb_lookup_result_t r;
	switch_stream_handle_t xml = { 0 };
	switch_xml_t result = NULL;
	cJSON *j;

	if (!section || strcmp(section, "directory") || !params) return NULL;
	domain = switch_event_get_header(params, "domain");
	user = switch_event_get_header(params, "user");
	if (zstr(domain) || zstr(user) || strlen(domain) + strlen(user) > 250) return NULL;
	given = switch_event_get_header(params, "tenant_id");
	if (!zstr(given) && strlen(given) < sizeof(tenant)) switch_snprintf(tenant, sizeof(tenant), "%s", given);
	else if (!resolve_tenant(domain, tenant, sizeof(tenant))) return NULL;

	switch_snprintf(lookup_key, sizeof(lookup_key), "%s@%s", user, domain);
	if (gb_lookup(globals.bridge, tenant, GB_KIND_DIRECTORY, lookup_key, 1500, &r) != GB_OK) return NULL;
	if (!r.found || !r.payload || !(j = cJSON_Parse(r.payload))) {
		gb_lookup_result_free(&r);
		return NULL;
	}
	{
		cJSON *name = cJSON_GetObjectItem(j, "name");
		SWITCH_STANDARD_STREAM(xml);
		xml.write_function(&xml, "<document type=\"freeswitch/xml\"><section name=\"directory\"><domain name=\"");
		xml_escape(&xml, domain);
		xml.write_function(&xml, "\"><user id=\"");
		xml_escape(&xml, user);
		xml.write_function(&xml, "\"><params/><variables>");
		xml.write_function(&xml, "<variable name=\"tenant_id\" value=\"");
		xml_escape(&xml, tenant);
		xml.write_function(&xml, "\"/><variable name=\"user_context\" value=\"default\"/><variable name=\"effective_caller_id_number\" value=\"");
		xml_escape(&xml, user);
		xml.write_function(&xml, "\"/><variable name=\"effective_caller_id_name\" value=\"");
		xml_escape(&xml, name && cJSON_IsString(name) ? name->valuestring : user);
		xml.write_function(&xml, "\"/></variables></user></domain></section></document>");
		result = switch_xml_parse_str_dup((char *) xml.data);
		switch_safe_free(xml.data);
	}
	cJSON_Delete(j);
	gb_lookup_result_free(&r);
	return result;
}

/* ------------------------------------------------------- dialplan app stub */

static const char *first_var(switch_channel_t *channel, const char *a, const char *b)
{
	const char *v = switch_channel_get_variable(channel, a);
	return zstr(v) && b ? switch_channel_get_variable(channel, b) : v;
}

/* What the last inline command produced, for the StepResult. */
typedef struct {
	int answered, elapsed_s, timed_out, length_ms, uploaded;
	char cause[64], digits[64], path[512], error[256];
	char amd_result[32], amd_cause[32];
} outcome_t;

static void bridge_outcome(switch_channel_t *channel, switch_time_t secs, outcome_t *oc)
{
	const char *disp = switch_channel_get_variable(channel, "originate_disposition");
	const char *cause = switch_channel_get_variable(channel, "last_bridge_hangup_cause");
	oc->answered = (disp && !strcmp(disp, "SUCCESS")) ? 1 : 0;
	oc->elapsed_s = (int) secs;
	switch_snprintf(oc->cause, sizeof(oc->cause), "%s",
					oc->answered ? (zstr(cause) ? "NORMAL_CLEARING" : cause) : (zstr(disp) ? "NO_ROUTE_DESTINATION" : disp));
}

/* The channel's own tenant (set by this module), never the command's. */
static const char *channel_tenant(switch_channel_t *channel)
{
	const char *t = switch_channel_get_variable(channel, "tenant_id");
	return dg_uuid_shaped(t) ? t : NULL;
}

static switch_bool_t inline_collect(switch_core_session_t *session, const gb_command_t *c, outcome_t *oc, char *err, size_t errlen)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	switch_status_t st;
	if (!sg_safe_prompt(c->file, globals.record_dir, channel_tenant(channel)) || !sg_safe_terminators(c->terminators) || !sg_collect_ok(c->min_digits, c->max_digits, c->timeout_ms)) {
		switch_snprintf(err, errlen, "collect: bad arguments");
		return SWITCH_FALSE;
	}
	switch_channel_set_variable(channel, SWITCH_READ_TERMINATOR_USED_VARIABLE, NULL); /* switch_ivr_read only sets it, never clears it */
	st = switch_ivr_read(session, c->min_digits, c->max_digits, zstr(c->file) ? NULL : c->file, NULL, oc->digits, sizeof(oc->digits),
						 c->timeout_ms, zstr(c->terminators) ? NULL : c->terminators, 0);
	/* with min_digits >= 1, switch_ivr_read turns a timeout into TOO_SMALL; TOO_SMALL without a terminator is a timeout too */
	oc->timed_out = (st == SWITCH_STATUS_TIMEOUT ||
					 (st == SWITCH_STATUS_TOO_SMALL && zstr(switch_channel_get_variable(channel, SWITCH_READ_TERMINATOR_USED_VARIABLE)))) ? 1 : 0;
	return SWITCH_TRUE;
}

/* PUTs the recording to a presigned URL: three tries, one and two seconds apart. The node never holds object-store
 * credentials. No cap on total time (a 4 h recording is hundreds of MB): an attempt fails only when the transfer
 * stalls below 1 KB/s for 30 s. ponytail: a stalled store costs 3 x 30 s plus 3 s backoff on the calling thread; a storage outage longer than the retries loses the message (the local file stays); a spool and an
 * uploader thread are the upgrade. */
static switch_bool_t upload_file(const char *path, const char *url)
{
	int attempt;
	for (attempt = 0; attempt < 3; attempt++) {
		FILE *fp = fopen(path, "rb");
		CURL *c = curl_easy_init();
		struct curl_slist *hdrs = NULL;
		struct stat st;
		long code = 0;
		CURLcode rc = CURLE_FAILED_INIT;
		if (fp && c && fstat(fileno(fp), &st) == 0) {
			hdrs = curl_slist_append(hdrs, "Content-Type: audio/wav");
			curl_easy_setopt(c, CURLOPT_URL, url);
			curl_easy_setopt(c, CURLOPT_UPLOAD, 1L);
			curl_easy_setopt(c, CURLOPT_READDATA, fp);
			curl_easy_setopt(c, CURLOPT_INFILESIZE_LARGE, (curl_off_t) st.st_size);
			curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
			curl_easy_setopt(c, CURLOPT_LOW_SPEED_LIMIT, 1024L);
			curl_easy_setopt(c, CURLOPT_LOW_SPEED_TIME, 30L);
			curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, 5L);
			curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 0L);
			curl_easy_setopt(c, CURLOPT_PROTOCOLS_STR, "http,https");
			curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
			curl_easy_setopt(c, CURLOPT_PROXY, ""); /* ignore env proxies */
			rc = curl_easy_perform(c);
			curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &code);
		}
		if (hdrs) curl_slist_free_all(hdrs);
		if (c) curl_easy_cleanup(c);
		if (fp) fclose(fp);
		if (rc == CURLE_OK && code >= 200 && code < 300) return SWITCH_TRUE;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "recording upload attempt %d failed (curl %d, http %ld)\n", attempt + 1, (int) rc, code);
		if (attempt < 2) switch_yield((attempt + 1) * 1000000);
	}
	return SWITCH_FALSE;
}

/* ------------------------------------------------- conference recording */

static int64_t cr_now_ms(void) { return (int64_t)(switch_mono_micro_time_now() / 1000); }

static int cr_sn(char *buf, size_t n, const char *fmt, ...)
{
	va_list ap;
	int w;
	va_start(ap, fmt);
	w = switch_vsnprintf(buf, n, fmt, ap);
	va_end(ap);
	return w >= 0 && (size_t) w < n;
}

static int cr_copy(char *dst, size_t n, const char *src) { return cr_sn(dst, n, "%s", src ? src : ""); }

static void cr_release(cr_job_t *j) { memset(j, 0, sizeof(*j)); }

static cr_job_t *cr_find(const char *conf, const char *path)
{
	int i;
	for (i = 0; i < CR_MAX_JOBS; i++) {
		cr_job_t *j = &globals.jobs[i];
		if (!j->used || j->abandoned || strcmp(j->conf, conf)) continue;
		if (path && strcmp(j->path, path)) continue;
		return j;
	}
	return NULL;
}

static int cr_admitted(void)
{
	int i, n = 0;
	for (i = 0; i < CR_MAX_JOBS; i++) if (globals.jobs[i].used) n++;
	return n;
}

static cr_job_t *cr_slot(void)
{
	int i;
	for (i = 0; i < CR_MAX_JOBS; i++) if (!globals.jobs[i].used) return &globals.jobs[i];
	return NULL;
}

static void cr_send_done(const char *tenant, const char *room, const char *rec_id, int64_t length_ms, int uploaded)
{
	int rc;
	if (length_ms < 0) length_ms = 0;
	rc = gb_send_conf_event(globals.bridge, 1 /* RECORDING_DONE */, tenant, room, rec_id, length_ms, uploaded, "", "");
	if (rc != GB_OK)
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "conference recording %s completion not sent; recovery left to the pending sweep\n", rec_id ? rec_id : "");
}

static void cr_enqueue(cr_job_t *j)
{
	j->uploading = 1;
	if (j->queued || !globals.uploadq) return;
	if (switch_queue_trypush(globals.uploadq, j) == SWITCH_STATUS_SUCCESS) j->queued = 1;
}

static void cr_note_closed(cr_job_t *j, int64_t ms)
{
	if (!j || j->abandoned) return;
	if (ms < 0) ms = 0;
	if (j->state == CR_NEW) { /* queued until START_OK, so a short room cannot lose the close */
		j->pending_close = 1;
		j->pending_ms = ms;
		return;
	}
	if (cr_transition(&j->state, CR_FILE_CLOSED) & CR_UPLOAD) {
		j->closed = 1;
		j->length_ms = ms;
		cr_enqueue(j);
	}
}

static int cr_arm_stop(cr_job_t *j, enum cr_event ev, char *conf, size_t confn, char *path, size_t pathn)
{
	if (!j || j->abandoned || j->state != CR_RECORDING) return 0;
	if (!(cr_transition(&j->state, ev) & CR_STOP)) return 0;
	j->stop_needed = 1;
	j->stopping_since_ms = cr_now_ms();
	j->stop_after_ms = j->stopping_since_ms + 1000;
	return cr_copy(conf, confn, j->conf) && cr_copy(path, pathn, j->path);
}

static switch_bool_t cr_api_stop(const char *conf, const char *path, char *err, size_t errlen)
{
	char args[2048 + CR_PATH_MAX];
	if (!cr_sn(args, sizeof(args), "%s recording stop %s", conf, path)) {
		switch_snprintf(err, errlen, "conference record: stop too long");
		return SWITCH_FALSE;
	}
	return run_conference_api(args, err, errlen);
}

static void cr_finish_stop(const char *conf, const char *path)
{
	char serr[256];
	cr_job_t *j;
	if (cr_api_stop(conf, path, serr, sizeof(serr)) != SWITCH_TRUE) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "conference recording stop failed: %s\n", serr);
		return;
	}
	switch_mutex_lock(globals.rec_mutex);
	j = cr_find(conf, path);
	if (j) j->stop_needed = 0;
	switch_mutex_unlock(globals.rec_mutex);
}

static void cr_upload_one(cr_job_t *j)
{
	char path[CR_PATH_MAX], url[CR_URL_MAX], tenant[40], room[68], rec[40];
	int64_t length = 0;
	int ok, effects = 0;
	switch_mutex_lock(globals.rec_mutex);
	if (!j->used || j->abandoned || j->state != CR_UPLOADING) {
		if (j->used) j->queued = 0;
		switch_mutex_unlock(globals.rec_mutex);
		return;
	}
	cr_copy(path, sizeof(path), j->path);
	cr_copy(url, sizeof(url), j->url);
	cr_copy(tenant, sizeof(tenant), j->tenant);
	cr_copy(room, sizeof(room), j->room);
	cr_copy(rec, sizeof(rec), j->rec_id);
	length = j->length_ms;
	switch_mutex_unlock(globals.rec_mutex);
	ok = url[0] && upload_file(path, url) == SWITCH_TRUE;
	if (ok) unlink(path); /* only after the object store has it */
	else switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "conference recording %s upload failed path=%s\n", rec, path);
	switch_mutex_lock(globals.rec_mutex);
	if (j->used && !j->abandoned && j->state == CR_UPLOADING)
		effects = cr_transition(&j->state, ok ? CR_UPLOAD_OK : CR_UPLOAD_FAILED);
	if (j->used && !j->abandoned) cr_release(j);
	switch_mutex_unlock(globals.rec_mutex);
	if (effects & CR_REPORT_OK) cr_send_done(tenant, room, rec, length, 1);
	else if (effects & CR_REPORT_FAILED) cr_send_done(tenant, room, rec, length, 0);
}

static void *SWITCH_THREAD_FUNC cr_upload_thread(switch_thread_t *thread, void *obj)
{
	(void) thread; (void) obj;
	for (;;) {
		void *item = NULL;
		if (globals.uploadq && switch_queue_trypop(globals.uploadq, &item) == SWITCH_STATUS_SUCCESS) {
			cr_upload_one((cr_job_t *) item);
			continue;
		}
		if (globals.rec_stop) break;
		switch_yield(50000);
	}
	return NULL;
}

static int cr_wav_writer_open(const char *path);
static int cr_seal_wav(const char *path);
static int64_t cr_wav_ms(const char *path);

/* A stop whose stop-recording event never came: once nobody writes the file, seal it and upload what is there (a
 * missing file fails the upload and is reported as not uploaded), so the slot and the room are freed. A writer that
 * still has it open means the conference lives: wait another round. */
static void cr_give_up_stop(const char *conf, const char *path, int64_t now)
{
	int64_t ms = 0;
	int open = cr_wav_writer_open(path), sealed = 0;
	cr_job_t *j;
	if (!open && (sealed = cr_seal_wav(path))) ms = cr_wav_ms(path);
	switch_mutex_lock(globals.rec_mutex);
	j = cr_find(conf, path);
	if (j && j->state == CR_STOPPING) {
		if (open) {
			j->stopping_since_ms = now;
		} else {
			if (!sealed || ms <= 0) memset(j->url, 0, sizeof(j->url)); /* no audio: report not uploaded, keep the file */
			j->stop_needed = 0;
			cr_note_closed(j, ms);
		}
	}
	switch_mutex_unlock(globals.rec_mutex);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "conference recording stop at %s got no stop-recording in %ds; %s\n", path,
					  CR_STOP_GIVEUP_MS / 1000, open ? "the file is still open, waiting again" : sealed && ms > 0 ? "uploading what is on disk" : "no audio, reporting it not uploaded");
}

static void *SWITCH_THREAD_FUNC cr_maint_thread(switch_thread_t *thread, void *obj)
{
	(void) thread; (void) obj;
	while (!globals.rec_maint_stop) {
		int i;
		int64_t now = cr_now_ms();
		for (i = 0; i < CR_MAX_JOBS && !globals.rec_maint_stop; i++) {
			char conf[128], path[CR_PATH_MAX];
			int go = 0, giveup = 0;
			switch_mutex_lock(globals.rec_mutex);
			{
				cr_job_t *j = &globals.jobs[i];
				if (j->used && !j->abandoned && j->state == CR_RECORDING && now >= j->deadline_ms)
					go = cr_arm_stop(j, CR_CAP, conf, sizeof(conf), path, sizeof(path));
				else if (j->used && !j->abandoned && j->state == CR_STOPPING && now - j->stopping_since_ms >= CR_STOP_GIVEUP_MS)
					giveup = cr_copy(conf, sizeof(conf), j->conf) && cr_copy(path, sizeof(path), j->path);
				else if (j->used && !j->abandoned && j->state == CR_STOPPING && j->stop_needed && now >= j->stop_after_ms) {
					j->stop_after_ms = now + 1000; /* one exact-path retry a second, until CR_STOP_GIVEUP_MS */
					go = cr_copy(conf, sizeof(conf), j->conf) && cr_copy(path, sizeof(path), j->path);
				}
				if (j->used && !j->abandoned && j->state == CR_UPLOADING && !j->queued) cr_enqueue(j);
			}
			switch_mutex_unlock(globals.rec_mutex);
			if (go) cr_finish_stop(conf, path);
			if (giveup) cr_give_up_stop(conf, path, now);
		}
		if (!globals.rec_maint_stop) switch_yield(200000);
	}
	return NULL;
}

static void cr_fail_admitted(cr_job_t *j, char *err, size_t errlen, const char *why)
{
	char msg[256], tenant[40], room[68], rec[40];
	int report = 0;
	switch_snprintf(msg, sizeof(msg), "%s", why && *why ? why : "conference record: start failed");
	tenant[0] = room[0] = rec[0] = 0;
	switch_mutex_lock(globals.rec_mutex);
	if (j && j->used && !j->abandoned && j->state == CR_NEW) {
		if (cr_transition(&j->state, CR_START_FAILED) & CR_REPORT_FAILED)
			report = cr_copy(tenant, sizeof(tenant), j->tenant) && cr_copy(room, sizeof(room), j->room) && cr_copy(rec, sizeof(rec), j->rec_id);
		cr_release(j);
	}
	switch_mutex_unlock(globals.rec_mutex);
	if (report) cr_send_done(tenant, room, rec, 0, 0);
	if (err && errlen) switch_snprintf(err, errlen, "%s", msg);
}

static void cr_reject(const cmd_item_t *c, char *err, size_t errlen, const char *why)
{
	cr_send_done(c->tenant_id, c->room, c->rec_id, 0, 0);
	switch_snprintf(err, errlen, "%s", why);
}

static switch_bool_t exec_conf_record_stop(const cmd_item_t *c, const char *conf, char *err, size_t errlen)
{
	char saved_conf[128], path[CR_PATH_MAX];
	int go = 0;
	cr_job_t *j;
	switch_mutex_lock(globals.rec_mutex);
	j = cr_find(conf, NULL);
	if (!j) {
		switch_mutex_unlock(globals.rec_mutex);
		switch_snprintf(err, errlen, "conference record: no recording");
		return SWITCH_FALSE; /* no admitted job: do not invent a completion */
	}
	if (!zstr(c->rec_id) && strcmp(c->rec_id, j->rec_id)) {
		switch_mutex_unlock(globals.rec_mutex);
		switch_snprintf(err, errlen, "conference record: rec_id does not match");
		return SWITCH_FALSE;
	}
	if (j->state == CR_NEW) { j->stop_early = 1; switch_mutex_unlock(globals.rec_mutex); return SWITCH_TRUE; }
	if (j->state != CR_RECORDING) { switch_mutex_unlock(globals.rec_mutex); return SWITCH_TRUE; }
	go = cr_arm_stop(j, CR_STOP_REQUEST, saved_conf, sizeof(saved_conf), path, sizeof(path));
	switch_mutex_unlock(globals.rec_mutex);
	if (!go) { switch_snprintf(err, errlen, "conference record: stop failed"); return SWITCH_FALSE; }
	if (cr_api_stop(saved_conf, path, err, errlen) != SWITCH_TRUE) return SWITCH_FALSE;
	switch_mutex_lock(globals.rec_mutex);
	j = cr_find(saved_conf, path);
	if (j) j->stop_needed = 0;
	switch_mutex_unlock(globals.rec_mutex);
	return SWITCH_TRUE;
}

static switch_bool_t exec_conf_record_start(const cmd_item_t *c, const char *conf, char *err, size_t errlen)
{
	char path[CR_PATH_MAX], dir[1024], args[2048 + CR_PATH_MAX];
	char saved_conf[128], saved_path[CR_PATH_MAX];
	cr_job_t *j;
	int armed = 0;
	if (!dg_uuid_shaped(c->rec_id) || c->max_len_s < 1 || c->max_len_s > 14400) {
		switch_snprintf(err, errlen, "conference record: bad id or duration");
		return SWITCH_FALSE; /* malformed: command result only */
	}
	if (zstr(c->upload_url) || !sg_safe_url(c->upload_url)) {
		switch_snprintf(err, errlen, "conference record: bad upload url");
		return SWITCH_FALSE;
	}
	if (zstr(c->file) || !sg_safe_prompt(c->file, globals.record_dir, c->tenant_id)) {
		switch_snprintf(err, errlen, "conference record: bad prompt");
		return SWITCH_FALSE;
	}
	if (!globals.record_dir || !sg_conf_record_path(path, sizeof(path), globals.record_dir, c->tenant_id, c->rec_id, c->max_len_s)) {
		cr_reject(c, err, errlen, globals.record_dir ? "conference record: path refused" : "conference record: no record-dir");
		return SWITCH_FALSE;
	}
	if (!cr_sn(dir, sizeof(dir), "%s/%s", globals.record_dir, c->tenant_id)) {
		cr_reject(c, err, errlen, "conference record: directory too long");
		return SWITCH_FALSE;
	}
	switch_mutex_lock(globals.rec_mutex);
	if (globals.rec_refuse || cr_admitted() >= CR_MAX_JOBS || cr_find(conf, NULL) || !(j = cr_slot())) {
		switch_mutex_unlock(globals.rec_mutex);
		cr_reject(c, err, errlen, "conference record: refused");
		return SWITCH_FALSE;
	}
	memset(j, 0, sizeof(*j));
	j->used = 1;
	j->state = CR_NEW;
	if (!cr_copy(j->tenant, sizeof(j->tenant), c->tenant_id) || !cr_copy(j->room, sizeof(j->room), c->room) ||
		!cr_copy(j->conf, sizeof(j->conf), conf) || !cr_copy(j->rec_id, sizeof(j->rec_id), c->rec_id) ||
		!cr_copy(j->path, sizeof(j->path), path) || !cr_copy(j->url, sizeof(j->url), c->upload_url)) {
		cr_release(j);
		switch_mutex_unlock(globals.rec_mutex);
		cr_reject(c, err, errlen, "conference record: refused");
		return SWITCH_FALSE;
	}
	j->deadline_ms = cr_now_ms() + (int64_t) c->max_len_s * 1000;
	switch_mutex_unlock(globals.rec_mutex);
	if (switch_dir_make_recursive(dir, SWITCH_DEFAULT_DIR_PERMS, globals.pool) != SWITCH_STATUS_SUCCESS) {
		cr_fail_admitted(j, err, errlen, "conference record: cannot create directory");
		return SWITCH_FALSE;
	}
	if (!cr_sn(args, sizeof(args), "%s play %s", conf, c->file)) {
		cr_fail_admitted(j, err, errlen, "conference record: play too long");
		return SWITCH_FALSE;
	}
	if (!run_conference_api(args, err, errlen)) {
		char msg[256];
		switch_snprintf(msg, sizeof(msg), "%s", err[0] ? err : "conference record: announcement failed");
		cr_fail_admitted(j, err, errlen, msg); /* announcement not queued: do not record */
		return SWITCH_FALSE;
	}
	if (!cr_sn(args, sizeof(args), "%s recording start %s", conf, path) || !run_conference_api(args, err, errlen)) {
		char msg[256];
		switch_snprintf(msg, sizeof(msg), "%s", err[0] ? err : "conference record: start failed");
		cr_fail_admitted(j, err, errlen, msg);
		return SWITCH_FALSE;
	}
	switch_mutex_lock(globals.rec_mutex);
	if (!j->used || j->abandoned) {
		switch_mutex_unlock(globals.rec_mutex);
		cr_api_stop(conf, path, err, errlen); /* file may already be open; completion was sent by shutdown */
		switch_snprintf(err, errlen, "conference record: shutting down");
		return SWITCH_FALSE;
	}
	cr_transition(&j->state, CR_START_OK);
	j->started = 1;
	if (j->pending_close) {
		j->length_ms = j->pending_ms;
		j->closed = 1;
		if (cr_transition(&j->state, CR_FILE_CLOSED) & CR_UPLOAD) cr_enqueue(j);
	} else if (j->stop_early || globals.rec_refuse) {
		armed = cr_arm_stop(j, j->stop_early ? CR_STOP_REQUEST : CR_SHUTDOWN, saved_conf, sizeof(saved_conf), saved_path, sizeof(saved_path));
	}
	switch_mutex_unlock(globals.rec_mutex);
	if (armed) cr_finish_stop(saved_conf, saved_path);
	return SWITCH_TRUE;
}

static switch_bool_t exec_conf_record(const cmd_item_t *c, const char *conf, char *err, size_t errlen)
{
	if (c->conf_action == 10) return exec_conf_record_stop(c, conf, err, errlen);
	return exec_conf_record_start(c, conf, err, errlen);
}

static void cr_end_stop(const char *conf)
{
	char saved_conf[128], path[CR_PATH_MAX];
	cr_job_t *j;
	int go = 0;
	switch_mutex_lock(globals.rec_mutex);
	j = cr_find(conf, NULL);
	if (j && j->state == CR_NEW) j->stop_early = 1;
	else go = cr_arm_stop(j, CR_STOP_REQUEST, saved_conf, sizeof(saved_conf), path, sizeof(path));
	switch_mutex_unlock(globals.rec_mutex);
	if (go) cr_finish_stop(saved_conf, path);
}

static int64_t cr_header_ms(const char *s)
{
	int64_t n = 0;
	if (!s) return 0;
	for (; *s >= '0' && *s <= '9'; s++) {
		if (n > (INT64_MAX - 9) / 10) return INT64_MAX;
		n = n * 10 + (*s - '0');
	}
	return n;
}

static void cr_on_add_member(switch_event_t *event)
{
	const char *uuid = switch_event_get_header(event, "Unique-ID");
	const char *ghost = switch_event_get_header(event, "Member-Ghost");
	const char *cname = switch_event_get_header(event, "Conference-Name");
	const char *inst = switch_event_get_header(event, "Conference-Unique-ID");
	switch_core_session_t *s;
	switch_channel_t *ch;
	const char *tenant, *short_room, *joined;
	char full[128], tcopy[40], rcopy[68], ucopy[64], icopy[80];
	if (zstr(uuid) || (ghost && !strcasecmp(ghost, "true"))) return;
	s = switch_core_session_locate(uuid);
	if (!s) return;
	ch = switch_core_session_get_channel(s);
	tenant = channel_tenant(ch);
	short_room = switch_channel_get_variable(ch, "eswitch_conf_room");
	joined = switch_channel_get_variable(ch, "conference_name");
	if (!tenant || zstr(short_room) || !sg_room(full, sizeof(full), tenant, short_room) ||
		zstr(joined) || strcmp(joined, full) || zstr(cname) || strcmp(cname, full)) {
		switch_core_session_rwunlock(s);
		return; /* not a conference this tenant's channel was placed in */
	}
	cr_copy(tcopy, sizeof(tcopy), tenant);
	cr_copy(rcopy, sizeof(rcopy), short_room);
	cr_copy(ucopy, sizeof(ucopy), uuid);
	cr_copy(icopy, sizeof(icopy), inst ? inst : "");
	switch_core_session_rwunlock(s);
	gb_send_conf_event(globals.bridge, 2 /* ROOM_READY */, tcopy, rcopy, "", 0, 0, ucopy, icopy);
}

static void cr_maint_event(switch_event_t *event)
{
	const char *action = switch_event_get_header(event, "Action");
	if (zstr(action)) return;
	if (!strcmp(action, "add-member")) { cr_on_add_member(event); return; }
	if (!strcmp(action, "stop-recording")) {
		const char *conf = switch_event_get_header(event, "Conference-Name");
		const char *path = switch_event_get_header(event, "Path");
		cr_job_t *j;
		if (zstr(conf) || zstr(path)) return;
		switch_mutex_lock(globals.rec_mutex);
		j = cr_find(conf, path);
		if (j) cr_note_closed(j, cr_header_ms(switch_event_get_header(event, "Milliseconds-Elapsed")));
		switch_mutex_unlock(globals.rec_mutex);
	}
}

static int cr_busy(void)
{
	int i, n = 0;
	for (i = 0; i < CR_MAX_JOBS; i++) {
		cr_job_t *j = &globals.jobs[i];
		if (j->used && !j->abandoned && (j->state == CR_NEW || j->state == CR_RECORDING || j->state == CR_STOPPING)) n++;
	}
	return n;
}

static void cr_shutdown_sweep(void)
{
	int i;
	for (i = 0; i < CR_MAX_JOBS; i++) {
		char conf[128], path[CR_PATH_MAX];
		int go = 0;
		switch_mutex_lock(globals.rec_mutex);
		{
			cr_job_t *j = &globals.jobs[i];
			if (j->used && !j->abandoned && j->state == CR_RECORDING)
				go = cr_arm_stop(j, CR_SHUTDOWN, conf, sizeof(conf), path, sizeof(path));
			else if (j->used && !j->abandoned && j->state == CR_STOPPING && j->stop_needed)
				go = cr_copy(conf, sizeof(conf), j->conf) && cr_copy(path, sizeof(path), j->path);
		}
		switch_mutex_unlock(globals.rec_mutex);
		if (go) cr_finish_stop(conf, path);
	}
}

/* Duration of a PCM wav whose data-chunk size is already written. 0 when that size is 0 or the fourcc is not in the scanned preamble. */
static int64_t cr_wav_ms(const char *path)
{
	FILE *fp;
	unsigned char h[CR_WAV_SCAN];
	size_t n;
	if (!path || !(fp = fopen(path, "rb"))) return 0;
	memset(h, 0, sizeof(h));
	n = fread(h, 1, sizeof(h), fp);
	fclose(fp);
	return cr_wav_header_ms(h, n);
}

/* ponytail: scans /proc/self/fd; a writer that opens after the scan can still race. The upgrade is to wait on the conference file handle. */
static int cr_wav_writer_open(const char *path)
{
	DIR *d;
	struct dirent *de;
	d = opendir("/proc/self/fd");
	if (!d) return 1; /* cannot prove the writer has finished */
	while ((de = readdir(d))) {
		char link[64], target[CR_PATH_MAX];
		ssize_t n;
		if (de->d_name[0] == '.') continue;
		if (!cr_sn(link, sizeof(link), "/proc/self/fd/%s", de->d_name)) continue;
		n = readlink(link, target, sizeof(target) - 1);
		if (n <= 0 || (size_t) n >= sizeof(target) - 1) continue;
		target[n] = 0;
		if (!strcmp(target, path)) { closedir(d); return 1; }
	}
	closedir(d);
	return 0;
}

/* Fill a zero data-chunk size from the bytes on disk, only when nobody still has the file open. */
static int cr_seal_wav(const char *path)
{
	FILE *fp;
	unsigned char h[CR_WAV_SCAN];
	size_t n;
	struct stat st;
	if (!path || cr_wav_writer_open(path)) return 0;
	if (stat(path, &st) != 0 || st.st_size < 44 || st.st_size > 0xffffffffu) return 0;
	if (!(fp = fopen(path, "r+b"))) return 0;
	memset(h, 0, sizeof(h));
	n = fread(h, 1, sizeof(h), fp);
	if (!cr_wav_seal(h, n, (uint32_t) st.st_size, 0) ||
		fseek(fp, 0, SEEK_SET) != 0 || fwrite(h, 1, n, fp) != n || fflush(fp) != 0) {
		fclose(fp);
		return 0;
	}
	fclose(fp);
	return 1;
}

static void cr_abandon_open(void)
{
	int i;
	for (i = 0; i < CR_MAX_JOBS; i++) {
		char tenant[40], room[68], rec[40], path[CR_PATH_MAX], url[CR_URL_MAX];
		int64_t length = 0;
		int report = 0, uploaded = 0;
		switch_mutex_lock(globals.rec_mutex);
		{
			cr_job_t *j = &globals.jobs[i];
			if (j->used && !j->abandoned && (j->state == CR_NEW || j->state == CR_RECORDING || j->state == CR_STOPPING)) {
				length = j->length_ms;
				url[0] = 0;
				report = cr_copy(tenant, sizeof(tenant), j->tenant) && cr_copy(room, sizeof(room), j->room) &&
					cr_copy(rec, sizeof(rec), j->rec_id) && cr_copy(path, sizeof(path), j->path);
				cr_copy(url, sizeof(url), j->url);
				memset(j->url, 0, sizeof(j->url));
				j->abandoned = 1;
				j->done = 1;
				j->state = CR_DONE;
				j->stop_needed = 0;
			}
		}
		switch_mutex_unlock(globals.rec_mutex);
		if (!report) continue;
		/* Conference stop from module shutdown often never delivers stop-recording. PUT only after the
		 * data-chunk size is real: a file still being written, or whose size is still zero, is not audio. */
		if (url[0] && cr_seal_wav(path) && upload_file(path, url) == SWITCH_TRUE) {
			int64_t ms = cr_wav_ms(path);
			if (ms > 0) {
				length = ms;
				unlink(path);
				uploaded = 1;
			}
		}
		if (!uploaded)
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "conference recording %s left unfinished at shutdown path=%s\n", rec, path);
		else
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "conference recording %s uploaded at shutdown length_ms=%lld\n", rec, (long long) length);
		cr_send_done(tenant, room, rec, length, uploaded);
	}
}

static void cr_drain_uploads(void)
{
	void *item = NULL;
	if (!globals.uploadq) return;
	while (switch_queue_trypop(globals.uploadq, &item) == SWITCH_STATUS_SUCCESS) cr_upload_one((cr_job_t *) item);
}

static void cr_stop_threads(void)
{
	switch_status_t st;
	globals.rec_maint_stop = 1;
	globals.rec_stop = 1;
	if (globals.maint_thread) switch_thread_join(&st, globals.maint_thread);
	if (globals.upload_thread) switch_thread_join(&st, globals.upload_thread);
	globals.maint_thread = globals.upload_thread = NULL;
}

static void cr_shutdown_recordings(void)
{
	int spins;
	switch_status_t st;
	if (!globals.rec_mutex) return;
	switch_mutex_lock(globals.rec_mutex);
	globals.rec_refuse = 1;
	switch_mutex_unlock(globals.rec_mutex);
	globals.rec_maint_stop = 1;
	if (globals.maint_thread) { switch_thread_join(&st, globals.maint_thread); globals.maint_thread = NULL; }
	/* ponytail: 15s for stop-recording; a stuck conference thread is the ceiling */
	for (spins = 0; spins < 75; spins++) {
		int busy;
		cr_shutdown_sweep();
		switch_mutex_lock(globals.rec_mutex);
		busy = cr_busy();
		switch_mutex_unlock(globals.rec_mutex);
		if (!busy) break;
		switch_yield(200000);
	}
	cr_abandon_open();
	if (globals.conf_node) { switch_event_unbind(&globals.conf_node); globals.conf_node = NULL; }
	switch_yield(100000);
	globals.rec_stop = 1;
	if (globals.upload_thread) { switch_thread_join(&st, globals.upload_thread); globals.upload_thread = NULL; }
	cr_drain_uploads();
	/* gb_stop cancels the stream and the writer drops whatever is still queued. */
	switch_yield(1000000);
}

static switch_status_t cr_threads_start(switch_memory_pool_t *pool)
{
	switch_threadattr_t *attr = NULL;
	if (switch_mutex_init(&globals.rec_mutex, SWITCH_MUTEX_NESTED, pool) != SWITCH_STATUS_SUCCESS) return SWITCH_STATUS_FALSE;
	if (switch_queue_create(&globals.uploadq, CR_MAX_JOBS, pool) != SWITCH_STATUS_SUCCESS) return SWITCH_STATUS_FALSE;
	switch_threadattr_create(&attr, pool);
	if (switch_thread_create(&globals.maint_thread, attr, cr_maint_thread, NULL, pool) != SWITCH_STATUS_SUCCESS) return SWITCH_STATUS_FALSE;
	if (switch_thread_create(&globals.upload_thread, attr, cr_upload_thread, NULL, pool) != SWITCH_STATUS_SUCCESS) {
		cr_stop_threads();
		return SWITCH_STATUS_FALSE;
	}
	return SWITCH_STATUS_SUCCESS;
}

static switch_bool_t inline_record(switch_core_session_t *session, const gb_command_t *c, outcome_t *oc, char *err, size_t errlen)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	const char *tenant = channel_tenant(channel), *ms;
	char dir[512], args[800];
	if (!tenant || !sg_record_ok(c->max_len_s, c->silence_s) || !sg_record_path(oc->path, sizeof(oc->path), globals.record_dir, tenant, c->name)) {
		switch_snprintf(err, errlen, "record: bad arguments or no record-dir");
		oc->path[0] = 0;
		return SWITCH_FALSE;
	}
	if (!zstr(c->upload_url) && !sg_safe_url(c->upload_url)) {
		switch_snprintf(err, errlen, "record: bad upload url");
		oc->path[0] = 0;
		return SWITCH_FALSE;
	}
	switch_snprintf(dir, sizeof(dir), "%s/%s", globals.record_dir, tenant);
	if (switch_dir_make_recursive(dir, SWITCH_DEFAULT_DIR_PERMS, switch_core_session_get_pool(session)) != SWITCH_STATUS_SUCCESS) {
		switch_snprintf(err, errlen, "record: cannot create %s", dir);
		oc->path[0] = 0;
		return SWITCH_FALSE;
	}
	if (c->beep) switch_ivr_play_file(session, NULL, "tone_stream://%(500,0,800)", NULL);
	if (c->silence_s > 0) switch_snprintf(args, sizeof(args), "%s %d 200 %d", oc->path, c->max_len_s, c->silence_s);
	else switch_snprintf(args, sizeof(args), "%s %d", oc->path, c->max_len_s);
	switch_channel_set_variable(channel, "record_ms", NULL);
	switch_core_session_execute_application(session, "record", args);
	ms = switch_channel_get_variable(channel, "record_ms");
	oc->length_ms = ms ? atoi(ms) : 0;
	if (!zstr(c->upload_url) && switch_file_exists(oc->path, switch_core_session_get_pool(session)) == SWITCH_STATUS_SUCCESS) {
		oc->uploaded = upload_file(oc->path, c->upload_url) == SWITCH_TRUE ? 1 : 0;
		if (oc->uploaded) unlink(oc->path); /* the object store has it now */
	}
	return SWITCH_TRUE;
}

static switch_bool_t inline_conference(switch_core_session_t *session, const gb_command_t *c, char *err, size_t errlen)
{
	const char *tenant = channel_tenant(switch_core_session_get_channel(session));
	const char *profile = zstr(c->profile) ? "default" : c->profile;
	char room[128], args[256], flags[48];
	if (!tenant || !sg_room(room, sizeof(room), tenant, c->room) || !sg_safe_name(profile)) {
		switch_snprintf(err, errlen, "conference: bad room or profile");
		return SWITCH_FALSE;
	}
	if (!sg_conf_flags(flags, sizeof(flags), c->mute, c->moderator, c->wait_for_moderator)) {
		switch_snprintf(err, errlen, "conference: bad flags");
		return SWITCH_FALSE;
	}
	/* Short room from the engine, so add-member can rebuild the full name instead of splitting Conference-Name. */
	switch_channel_set_variable(switch_core_session_get_channel(session), "eswitch_conf_room", c->room);
	switch_snprintf(args, sizeof(args), "%s@%s%s", room, profile, flags);
	switch_core_session_execute_application(session, "conference", args);
	return SWITCH_TRUE;
}

static switch_bool_t inline_eavesdrop(switch_core_session_t *session, const gb_command_t *c, char *err, size_t errlen)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	const char *tenant = channel_tenant(channel);
	char dtmf_digit;

	if (zstr(c->target_uuid) || !safe_value(c->target_uuid) || !tenant_owns(c->target_uuid, tenant)) {
		switch_snprintf(err, errlen, "eavesdrop: target not found for this tenant");
		return SWITCH_FALSE;
	}

	/* Mark the supervisor channel so SuperviseMode commands are accepted */
	switch_channel_set_variable(channel, "supervise", "true");

	/* Queue initial mode DTMF if whisper or barge */
	dtmf_digit = sg_supervise_mode_dtmf(c->mode);
	if (dtmf_digit == '2' || dtmf_digit == '3') {
		switch_dtmf_t dt = { .digit = dtmf_digit, .duration = switch_core_default_dtmf_duration(0) };
		switch_channel_queue_dtmf(channel, &dt);
	}

	switch_core_session_execute_application(session, "eavesdrop", c->target_uuid);
	return SWITCH_TRUE;
}

/* Runs one command of the engine's answer on this session's thread. oc receives what it produced. */
static void run_inline(switch_core_session_t *session, const gb_command_t *c, outcome_t *oc)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	const char *mine = switch_core_session_get_uuid(session);
	char err[256] = "";
	switch_bool_t ok = SWITCH_TRUE;

	if (c->type < GB_CMD_ORIGINATE || c->type > GB_CMD_AMD) { /* the bridge maps an unknown action to type 0 */
		switch_snprintf(err, sizeof(err), "unknown command type %d", c->type);
		ok = SWITCH_FALSE;
	} else if (c->type == GB_CMD_CONFERENCE_CONTROL) { /* no channel to check the tenant against: stream commands only */
		switch_snprintf(err, sizeof(err), "conference_control: stream only");
		ok = SWITCH_FALSE;
	} else if (c->uuid && *c->uuid && strcmp(c->uuid, mine)) { /* names another call: same path as stream commands */
		cmd_item_t *item = cmd_dup(c);
		ok = item ? exec_command(item, err, sizeof(err)) : SWITCH_FALSE;
		cmd_free(item);
	} else switch (c->type) {
	case GB_CMD_BRIDGE: {
		switch_time_t t0;
		/* Command ownership must match the channel; only an explicit peer grant permits a different SIP stamp. */
		if (zstr(c->tenant_id) || zstr(switch_channel_get_variable(channel, "tenant_id")) ||
			strcmp(c->tenant_id, switch_channel_get_variable(channel, "tenant_id")) ||
			(zstr(c->peer_tenant_id) ? !safe_dial(c->dial_string, c->tenant_id) :
			 !dg_safe_peer_bridge(c->dial_string, c->tenant_id, c->peer_tenant_id, c->peer_caller_id))) {
			switch_snprintf(err, sizeof(err), "bridge: bad dial string");
			ok = SWITCH_FALSE;
			break;
		}
		if (c->timeout_s > 0) switch_channel_set_variable_printf(channel, "call_timeout", "%d", c->timeout_s);
		if (!zstr(c->step_id)) { /* a flow drives this call: the bridge ending or failing must not end it */
			switch_channel_set_variable(channel, "hangup_after_bridge", "false");
			switch_channel_set_variable(channel, "continue_on_fail", "true");
			switch_channel_set_variable(channel, "grpc_flow_bridge", "true");
		} else if (switch_channel_get_variable(channel, "grpc_flow_bridge")) { /* undo what an earlier step bridge set */
			switch_channel_set_variable(channel, "hangup_after_bridge", NULL);
			switch_channel_set_variable(channel, "continue_on_fail", NULL);
			switch_channel_set_variable(channel, "grpc_flow_bridge", NULL);
		}
		switch_channel_set_variable(channel, "originate_disposition", NULL);
		switch_channel_set_variable(channel, "last_bridge_hangup_cause", NULL);
		t0 = switch_micro_time_now();
		/* the far leg inherits the call's identity: its answer and hangup then reach the engine as call events */
		bridge_identity(channel, c->peer_tenant_id, c->peer_caller_id);
		switch_core_session_execute_application(session, "bridge", c->dial_string);
		bridge_outcome(channel, (switch_micro_time_now() - t0) / 1000000, oc);
		break;
	}
	case GB_CMD_PARK:
		switch_core_session_execute_application(session, "park", "");
		break;
	case GB_CMD_PLAYBACK:
		if (!sg_safe_prompt(c->file, globals.record_dir, channel_tenant(channel))) {
			switch_snprintf(err, sizeof(err), "playback: bad file");
			ok = SWITCH_FALSE;
			break;
		}
		switch_core_session_execute_application(session, "playback", c->file);
		break;
	case GB_CMD_HANGUP: {
		switch_call_cause_t cause = switch_channel_str2cause(c->cause && *c->cause ? c->cause : "NORMAL_CLEARING");
		switch_channel_hangup(channel, cause ? cause : SWITCH_CAUSE_NORMAL_CLEARING);
		break;
	}
	case GB_CMD_COLLECT:
		ok = inline_collect(session, c, oc, err, sizeof(err));
		break;
	case GB_CMD_RECORD:
		ok = inline_record(session, c, oc, err, sizeof(err));
		break;
	case GB_CMD_CONFERENCE:
		ok = inline_conference(session, c, err, sizeof(err));
		break;
	case GB_CMD_SLEEP:
		if (!sg_sleep_ok(c->ms)) {
			switch_snprintf(err, sizeof(err), "sleep: bad duration");
			ok = SWITCH_FALSE;
			break;
		}
		switch_ivr_sleep(session, c->ms, SWITCH_TRUE, NULL);
		break;
	case GB_CMD_EAVESDROP:
		ok = inline_eavesdrop(session, c, err, sizeof(err));
		break;
	case GB_CMD_AMD: {
		const char *result, *cause;
		switch_channel_set_variable(channel, "amd_result", NULL);
		switch_channel_set_variable(channel, "amd_cause", NULL);
		switch_core_session_execute_application(session, "amd", NULL);
		result = switch_channel_get_variable(channel, "amd_result");
		cause = switch_channel_get_variable(channel, "amd_cause");
		if (zstr(result)) {
			switch_snprintf(err, sizeof(err), "amd: no result");
			ok = SWITCH_FALSE;
			break;
		}
		switch_snprintf(oc->amd_result, sizeof(oc->amd_result), "%s", result);
		if (!zstr(cause)) switch_snprintf(oc->amd_cause, sizeof(oc->amd_cause), "%s", cause);
		break;
	}
	default: {
		cmd_item_t *item = cmd_dup(c);
		ok = item ? exec_command(item, err, sizeof(err)) : SWITCH_FALSE;
		cmd_free(item);
	}
	}
	if (ok != SWITCH_TRUE) switch_snprintf(oc->error, sizeof(oc->error), "%s", err);
	gb_send_command_result(globals.bridge, c->command_id, ok == SWITCH_TRUE, err);
}

/* Runs one batch. When its last command carries a step id, the node must report on it: *step is a copy of
 * the id (session pool), *type its command type and oc holds what that command produced. */
static void run_batch(switch_core_session_t *session, const gb_route_result_t *res, outcome_t *oc, const char **step, int *type)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	size_t i;
	*step = NULL;
	*type = 0;
	memset(oc, 0, sizeof(*oc));
	for (i = 0; i < res->ncmds && switch_channel_ready(channel); i++) {
		memset(oc, 0, sizeof(*oc));
		run_inline(session, &res->cmds[i], oc);
		if (res->cmds[i].type == GB_CMD_HANGUP) return;
		if (i + 1 == res->ncmds && !zstr(res->cmds[i].step_id)) {
			*step = switch_core_session_strdup(session, res->cmds[i].step_id);
			*type = res->cmds[i].type;
		}
	}
	/* the caller left before the step-stamped last command ran: the engine still hears about the step (hung_up) */
	if (i < res->ncmds && !zstr(res->cmds[res->ncmds - 1].step_id)) {
		memset(oc, 0, sizeof(*oc));
		switch_snprintf(oc->error, sizeof(oc->error), "not run: caller hung up");
		*step = switch_core_session_strdup(session, res->cmds[res->ncmds - 1].step_id);
		*type = res->cmds[res->ncmds - 1].type;
	}
}

static void end_call_unavailable(switch_core_session_t *session)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	if (switch_channel_ready(channel) && !zstr(globals.unavailable_prompt)) switch_ivr_play_file(session, NULL, globals.unavailable_prompt, NULL);
	switch_channel_hangup(channel, SWITCH_CAUSE_NORMAL_TEMPORARY_FAILURE);
}

/* Appends the route-vars channel variables to kv[0..nv) and returns the new count: at most AG_MAX_ROUTE_VARS added, never
 * past cap, a name already in kv skipped. Past the cap the newest win: FreeSWITCH moves a variable to the end of the list
 * each time it is set, so a value set or changed mid-call is never the one cut. Copies are strdup (heap: the caller frees
 * them) or session memory. The variable lock is released before this returns, so the caller sends without holding it. */
static int route_var_new(const switch_event_header_t *hi, const gb_kv_t *kv, size_t nfixed)
{
	size_t k;
	if (!ag_route_var_ok(hi->name, hi->value, globals.route_vars)) return 0;
	for (k = 0; k < nfixed; k++) if (!strcmp(kv[k].key, hi->name)) return 0;
	return 1; /* channel variable names are unique, so only the entries already in kv can repeat one */
}

static size_t add_route_vars(switch_core_session_t *session, switch_channel_t *channel, gb_kv_t *kv, size_t nv, size_t cap, int heap)
{
	switch_event_header_t *first, *hi;
	size_t nfixed = nv, room, eligible = 0, skip;

	if (zstr(globals.route_vars) || nv >= cap) return nv;
	room = cap - nv < AG_MAX_ROUTE_VARS ? cap - nv : AG_MAX_ROUTE_VARS;
	first = switch_channel_variable_first(channel);
	for (hi = first; hi; hi = hi->next) eligible += route_var_new(hi, kv, nfixed);
	skip = eligible > room ? eligible - room : 0;
	for (hi = first; hi && nv - nfixed < room; hi = hi->next) {
		if (!route_var_new(hi, kv, nfixed)) continue;
		if (skip) { skip--; continue; }
		kv[nv].key = heap ? strdup(hi->name) : switch_core_session_strdup(session, hi->name);
		kv[nv].value = heap ? strdup(hi->value) : switch_core_session_strdup(session, hi->value);
		if (!kv[nv].key || !kv[nv].value) {
			if (heap) { free((char *) kv[nv].key); free((char *) kv[nv].value); }
			break;
		}
		nv++;
	}
	if (first) switch_channel_variable_last(channel);
	return nv;
}

/* Reports a finished step and fetches the next batch into res. The same step id is retried while the engine is
 * unreachable (a replica takeover looks like that), and the engine answers a repeated id with its first reply.
 * True: res holds a non-empty next batch to run. False: the call is over or ending; res is freed. */
static switch_bool_t report_step(switch_core_session_t *session, const char *tenant, const char *call_id, const char *step_id,
								 const outcome_t *oc, int type, gb_route_result_t *res)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	gb_step_result_t sr;
	gb_kv_t kv[AG_MAX_ROUTE_VARS];
	size_t nkv, i;
	int hung_up = !switch_channel_ready(channel), rc = GB_ERR_DISCONNECTED, attempt;

	memset(&sr, 0, sizeof(sr));
	sr.tenant_id = tenant ? tenant : "";
	sr.call_id = call_id;
	sr.uuid = switch_core_session_get_uuid(session);
	sr.step_id = step_id;
	sr.type = type;
	sr.hung_up = hung_up;
	sr.error = oc->error;
	sr.answered = oc->answered;
	sr.elapsed_s = oc->elapsed_s;
	sr.cause = oc->cause;
	sr.digits = oc->digits;
	sr.timed_out = oc->timed_out;
	sr.path = oc->path;
	sr.length_ms = oc->length_ms;
	sr.uploaded = oc->uploaded;
	sr.amd_result = oc->amd_result;
	sr.amd_cause = oc->amd_cause;
	nkv = add_route_vars(session, channel, kv, 0, AG_MAX_ROUTE_VARS, 1); /* heap: a long call must not grow the session pool per step */
	sr.vars = kv;
	sr.nvars = nkv;
	for (attempt = 0; attempt < STEP_ATTEMPTS; attempt++) {
		memset(res, 0, sizeof(*res));
		rc = gb_step(globals.bridge, &sr, STEP_TIMEOUT_MS, res);
		if (rc == GB_OK) break;
		gb_route_result_free(res);
		if (rc != GB_ERR_DISCONNECTED && rc != GB_ERR_TIMEOUT && rc != GB_ERR_UNAVAILABLE && rc != GB_ERR_QUEUE_FULL) break;
		if (attempt + 1 < STEP_ATTEMPTS) switch_yield(STEP_RETRY_US);
	}
	for (i = 0; i < nkv; i++) { free((char *) kv[i].key); free((char *) kv[i].value); }
	if (rc != GB_OK) {
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "step %s not reported (%d): ending the call\n", step_id, rc);
		end_call_unavailable(session);
		return SWITCH_FALSE;
	}
	if (hung_up || !switch_channel_ready(channel) || res->ncmds == 0) {
		gb_route_result_free(res);
		return SWITCH_FALSE;
	}
	return SWITCH_TRUE;
}

/* Survival mode: with no engine reachable, bridge an extension-to-extension call of one tenant from the bridge's cached
 * CONFIG snapshot (see survival.h for what is allowed). The tenant is the edge's stamp and nothing else: no domain
 * lookup, no tenant_id variable. True: the call was bridged (and is over or ending). False: not routable, nothing done. */
static switch_bool_t survival_route(switch_core_session_t *session)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	switch_caller_profile_t *cp = switch_channel_get_caller_profile(channel);
	const char *tenant = switch_channel_get_variable(channel, "sip_h_X-Tenant-Id");
	const char *domain = first_var(channel, "domain_name", "sip_req_host");
	const char *number = cp && cp->destination_number ? cp->destination_number : "";
	gb_lookup_result_t r;
	char dial[512];
	int rc;

	tenant = tenant ? switch_core_session_strdup(session, tenant) : NULL;
	domain = domain ? switch_core_session_strdup(session, domain) : NULL;
	if (!dg_uuid_shaped(tenant) || zstr(domain)) return SWITCH_FALSE;
	if (gb_survival_get(globals.bridge, tenant, &r) != GB_OK) {
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "survival: no usable snapshot for tenant %s\n", tenant);
		return SWITCH_FALSE;
	}
	rc = sv_plan(r.payload, tenant, domain, number, globals.survival_dial_format, dial, sizeof(dial));
	gb_lookup_result_free(&r);
	if (rc != SV_OK) {
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "survival: %s -> %s refused (%d)\n", number, domain, rc);
		return SWITCH_FALSE;
	}
	switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "survival: routing %s in %s without an engine\n", number, domain);
	switch_channel_set_variable(channel, "survival", "1");
	switch_channel_set_variable(channel, "bridge_export_vars", "tenant_id,call_id,survival");
	switch_core_session_execute_application(session, "bridge", dial);
	if (switch_channel_ready(channel)) { /* the leg did not answer or ended without taking the caller with it */
		const char *c = switch_channel_get_variable(channel, "last_bridge_hangup_cause");
		switch_call_cause_t cause = zstr(c) ? SWITCH_CAUSE_NONE : switch_channel_str2cause(c);
		switch_channel_hangup(channel, cause != SWITCH_CAUSE_NONE ? cause : SWITCH_CAUSE_NORMAL_TEMPORARY_FAILURE);
	}
	return SWITCH_TRUE;
}

/* An API answer: fail on no answer or -ERR (commands differ on what success looks like). */
static void run_api_checked(const char *name, const char *args, char *err, size_t errlen)
{
	switch_stream_handle_t stream = { 0 };
	SWITCH_STANDARD_STREAM(stream);
	if (switch_api_execute(name, args, NULL, &stream) != SWITCH_STATUS_SUCCESS || zstr((char *) stream.data) || !strncmp((char *) stream.data, "-ERR", 4))
		switch_snprintf(err, errlen, "%s: %s", name, zstr((char *) stream.data) ? "no answer" : (char *) stream.data);
	switch_safe_free(stream.data);
}

#define API_XML_MAX_DEPTH 16

/* Refuses the fragment when any element, at any depth up to API_XML_MAX_DEPTH, carries a ${cmd(...)} expansion in a decoded attribute value or
 * in its text (mod_dialplan_xml expands condition fields, expressions and <expression> text, not only action data), or
 * an action/anti-action the guard refuses. Decoded: switch_xml has already turned &#36; and entities into "$". */
static int api_xml_refused(switch_xml_t x, const char *tenant, int depth, char *err, size_t errlen)
{
	switch_xml_t c;
	int i;
	if (depth > API_XML_MAX_DEPTH) { switch_snprintf(err, errlen, "xml too deep"); return 1; }
	for (i = 0; x->attr && x->attr[i]; i += 2)
		if (!ag_no_api_expansion(x->attr[i + 1])) { switch_snprintf(err, errlen, "xml expansion refused"); return 1; }
	if (!ag_no_api_expansion(x->txt)) { switch_snprintf(err, errlen, "xml expansion refused"); return 1; }
	if (!strcmp(x->name, "action") || !strcmp(x->name, "anti-action")) {
		/* what mod_dialplan_xml runs: the element text when it has any, else the data attribute */
		const char *data = !zstr(x->txt) ? x->txt : switch_xml_attr_soft(x, "data");
		if (switch_xml_attr(x, "loop")) { switch_snprintf(err, errlen, "xml loop refused"); return 1; } /* runs the action N times */
		if (!ag_no_capture(data)) { switch_snprintf(err, errlen, "xml capture refused"); return 1; }
		if (!ag_xml_action_ok(switch_xml_attr_soft(x, "application"), data, globals.xml_app_allow, globals.var_allow, tenant)) {
			switch_snprintf(err, errlen, "xml application %s refused", switch_xml_attr_soft(x, "application"));
			return 1;
		}
	}
	for (c = x->child; c; c = c->ordered)
		if (api_xml_refused(c, tenant, depth + 1, err, errlen)) return 1;
	return 0;
}

/* Checks the fragment, keeps its text for api_dialplan_search, and moves the call into it. */
static void run_api_xml(switch_core_session_t *session, const char *tenant, const char *dest, const char *text, char *err, size_t errlen)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	switch_xml_t x;
	if (switch_stristr("<!DOCTYPE", text) || switch_stristr("<!ENTITY", text)) { /* entities can spell "${" */
		switch_snprintf(err, errlen, "xml doctype refused");
		return;
	}
	/* a parse error still returns a root, with no name ("x", "</a>"): check before strcmp */
	if (!(x = switch_xml_parse_str_dup((char *) text)) || zstr(x->name) || !zstr(switch_xml_error(x))) {
		if (x) switch_xml_free(x);
		switch_snprintf(err, errlen, "xml does not parse");
		return;
	}
	if (strcmp(x->name, "context") || strcmp(switch_xml_attr_soft(x, "name"), "grpc_api") || !ag_safe_word(dest)) {
		switch_snprintf(err, errlen, "xml root or destination refused");
		switch_xml_free(x);
		return;
	}
	if (api_xml_refused(x, tenant, 1, err, errlen)) {
		switch_xml_free(x);
		return;
	}
	switch_xml_free(x);
	switch_channel_set_private(channel, "grpc_api_xml", switch_core_session_strdup(session, text)); /* session pool: freed with the session */
	switch_ivr_session_transfer(session, dest, "XML", "grpc_api");
}

/* Runs an operator API route's actions in order (RM-27). The first refusal or failure hangs the call up. */
static void run_api_batch(switch_core_session_t *session, const char *tenant, const gb_route_result_t *res)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	const char *uuid = switch_core_session_get_uuid(session);
	const char *rule = res->api_rule_id ? res->api_rule_id : "";
	char err[512] = "", var[96], cmd[2048];
	size_t i;

	if (zstr(rule)) { /* grpc_api_rule is the loop stop: without it the call could match a rule again */
		switch_channel_set_variable(channel, "grpc_api_error", ":0:no rule id");
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "api route without a rule id on %s\n", uuid);
		switch_channel_hangup(channel, SWITCH_CAUSE_NORMAL_TEMPORARY_FAILURE);
		return;
	}
	switch_channel_set_variable(channel, "grpc_api_rule", rule);
	for (i = 0; i < res->napi; i++) {
		const gb_api_action_t *a = &res->api[i];
		const char *name = a->name ? a->name : "", *value = a->value ? a->value : "";
		if (!ag_no_api_expansion(value)) {
			switch_snprintf(err, sizeof err, "expansion refused");
		} else switch (a->kind) {
		case GB_API_SET_HEADER:
		case GB_API_UNSET_HEADER:
			if (!ag_header_var(name, globals.api_headers, var, sizeof var) || (a->kind == GB_API_SET_HEADER && !dg_safe_value(value)))
				switch_snprintf(err, sizeof err, "header refused");
			else
				switch_channel_set_variable(channel, var, a->kind == GB_API_SET_HEADER ? value : NULL);
			break;
		case GB_API_SET_VAR:
			if (!ag_var_ok(name, globals.var_allow)) switch_snprintf(err, sizeof err, "variable refused");
			else switch_channel_set_variable(channel, name, value);
			break;
		case GB_API_API:
		case GB_API_BGAPI:
			if (!ag_api_ok(name, value, globals.api_allow, globals.xml_app_allow, globals.var_allow, uuid, tenant)) {
				switch_snprintf(err, sizeof err, "command refused");
			} else if (a->kind == GB_API_API) {
				run_api_checked(name, value, err, sizeof err);
			} else if (switch_snprintf(cmd, sizeof cmd, "%s %s", name, value) >= (int) sizeof cmd - 1) {
				switch_snprintf(err, sizeof err, "command too long");
			} else {
				run_api_checked("bgapi", cmd, err, sizeof err);
			}
			break;
		case GB_API_XML:
			if (i != res->napi - 1) switch_snprintf(err, sizeof err, "xml must be last");
			else run_api_xml(session, tenant, name, value, err, sizeof err);
			break;
		default:
			switch_snprintf(err, sizeof err, "unknown action kind %d", a->kind);
		}
		if (*err) {
			switch_channel_set_variable_printf(channel, "grpc_api_error", "%s:%u:%s", rule, (unsigned) i, err);
			switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "api route %s failed on %s: %s\n", rule, uuid, err);
			switch_channel_hangup(channel, SWITCH_CAUSE_NORMAL_TEMPORARY_FAILURE);
			return;
		}
	}
	switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "api route %s ran on %s\n", rule, uuid);
}

SWITCH_STANDARD_APP(grpc_route_function)
{
	switch_channel_t *channel = switch_core_session_get_channel(session);
	switch_caller_profile_t *cp = switch_channel_get_caller_profile(channel);
	const char *uuid = switch_core_session_get_uuid(session);
	/* switch_channel_set_variable frees the old value, so every variable read that outlives a set must be copied first */
	const char *tenant = first_var(channel, "sip_h_X-Tenant-Id", "tenant_id"); /* header set by Kamailio */
	const char *call_id = first_var(channel, "call_id", NULL);
	const char *domain = first_var(channel, "domain_name", "sip_req_host");
	const char *vnames[] = { "sip_call_id", "network_addr", "sip_from_user", "sip_from_host", "xfer_token", "dialer_token", "grpc_api_rule", NULL };
	gb_kv_t vars[8 + AG_MAX_FWD_HEADERS + AG_MAX_ROUTE_VARS];
	gb_route_request_t req;
	gb_route_result_t res;
	size_t i, nv = 0;
	int rc;

	char resolved[64] = "";

	tenant = tenant ? switch_core_session_strdup(session, tenant) : NULL;
	call_id = call_id ? switch_core_session_strdup(session, call_id) : NULL;
	domain = domain ? switch_core_session_strdup(session, domain) : NULL;
	if (zstr(tenant) && resolve_tenant(domain, resolved, sizeof(resolved))) tenant = switch_core_session_strdup(session, resolved);
	if (zstr(call_id)) call_id = uuid;
	switch_channel_set_variable(channel, "call_id", call_id);
	if (!zstr(tenant)) switch_channel_set_variable(channel, "tenant_id", tenant);
	if (globals.survival && dg_uuid_shaped(tenant)) gb_survival_touch(globals.bridge, tenant); /* keep its snapshot warm */
	for (i = 0; vnames[i]; i++) {
		const char *v = switch_channel_get_variable(channel, vnames[i]);
		if (!zstr(v)) { vars[nv].key = vnames[i]; vars[nv].value = switch_core_session_strdup(session, v); nv++; }
	}
	{ /* the caller's X-* headers, for API route matching and templates (RM-27); the tenant claim is not forwarded */
		switch_event_header_t *first = switch_channel_variable_first(channel), *hi;
		size_t nh = 0;
		for (hi = first; hi && nh < AG_MAX_FWD_HEADERS; hi = hi->next) {
			if (strncmp(hi->name, "sip_h_X-", 8) || !strcasecmp(hi->name, "sip_h_X-Tenant-Id")) continue;
			if (strlen(hi->name) > 70 || zstr(hi->value) || strlen(hi->value) > 256) continue;
			vars[nv].key = switch_core_session_strdup(session, hi->name);
			vars[nv].value = switch_core_session_strdup(session, hi->value);
			nv++;
			nh++;
		}
		if (first) switch_channel_variable_last(channel);
	}
	nv = add_route_vars(session, channel, vars, nv, sizeof(vars) / sizeof(*vars), 0);
	memset(&req, 0, sizeof(req));
	req.tenant_id = tenant ? tenant : "";
	req.call_id = call_id;
	req.uuid = uuid;
	req.domain = domain ? domain : "";
	req.caller_id_number = cp && cp->caller_id_number ? cp->caller_id_number : "";
	req.destination_number = cp && cp->destination_number ? cp->destination_number : "";
	req.vars = vars;
	req.nvars = nv;

	if (!zstr(tenant)) gb_send_call_event(globals.bridge, tenant, call_id, uuid, GB_EVT_CREATED, NULL, NULL, NULL, NULL);
	memset(&res, 0, sizeof(res));
	rc = gb_route(globals.bridge, &req, ROUTE_TIMEOUT_MS, &res);
	if (rc != GB_OK && globals.survival && (rc == GB_ERR_DISCONNECTED || rc == GB_ERR_UNAVAILABLE)) {
		gb_route_result_free(&res);
		if (!survival_route(session)) end_call_unavailable(session); /* the prompt, then the hangup */
		return;
	}
	if (rc != GB_OK) {
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "route request failed (%d): rejecting the call\n", rc);
		switch_channel_hangup(channel, SWITCH_CAUSE_NORMAL_TEMPORARY_FAILURE);
		gb_route_result_free(&res); /* zeroed by gb_route: safe */
		return;
	}
	if (res.napi) {
		run_api_batch(session, tenant, &res);
		gb_route_result_free(&res);
		return;
	}
	if (!res.ncmds) { /* the engine declined to say, for example because the node lacks the originate role */
		switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "engine returned no commands: rejecting the call\n");
		switch_channel_hangup(channel, SWITCH_CAUSE_NORMAL_TEMPORARY_FAILURE);
		gb_route_result_free(&res);
		return;
	}
	for (;;) { /* one batch per engine answer; a step id on the last command asks for the next one */
		outcome_t oc;
		const char *step = NULL;
		int type = 0;
		run_batch(session, &res, &oc, &step, &type);
		gb_route_result_free(&res);
		if (!step || !report_step(session, tenant, call_id, step, &oc, type, &res)) break;
	}
}

/* ------------------------------------------------------------- api: grpc */

/* Takes new inbound calls again after a Drain. */
static void undrain(void)
{
	int ctl = 0;
	if (!globals.draining) return;
	globals.draining = SWITCH_FALSE;
	switch_core_session_ctl(SCSC_PAUSE_INBOUND, &ctl);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "drain lifted: new inbound calls accepted\n");
}

#define GRPC_SYNTAX "status | undrain | lookup <tenant> <directory|dialplan|config> <key> | survival <tenant> | cache flush | xml"

SWITCH_STANDARD_API(grpc_api)
{
	char *mycmd = NULL, *argv[5] = { 0 };
	int argc;

	if (zstr(cmd)) goto usage;
	mycmd = strdup(cmd);
	argc = switch_separate_string(mycmd, ' ', argv, 5);
	if (argc < 1) goto usage;

	if (!strcasecmp(argv[0], "status")) {
		gb_cache_stats_t st;
		char reason[256];
		gb_cache_stats(globals.bridge, &st);
		switch_mutex_lock(globals.state_mutex);
		switch_snprintf(reason, sizeof(reason), "%s", globals.last_reason);
		switch_mutex_unlock(globals.state_mutex);
		stream->write_function(stream, "node=%s endpoints=%s connected=%s draining=%s cache_entries=%u hits=%u stale_served=%u invalidated=%u survival=%s last=\"%s\"\n",
							   globals.node_id, globals.endpoints, gb_connected(globals.bridge) ? "yes" : "no",
							   globals.draining ? "yes" : "no", (unsigned) st.entries, (unsigned) st.hits,
							   (unsigned) st.stale_served, (unsigned) st.invalidated, globals.survival ? "on" : "off", reason);
	} else if (!strcasecmp(argv[0], "undrain") && argc == 1) {
		undrain();
		stream->write_function(stream, "+OK\n");
	} else if (!strcasecmp(argv[0], "cache") && argc == 2 && !strcasecmp(argv[1], "flush")) {
		gb_cache_flush(globals.bridge);
		stream->write_function(stream, "+OK\n");
	} else if (!strcasecmp(argv[0], "lookup") && argc == 4) {
		gb_lookup_result_t r;
		int kind = !strcasecmp(argv[2], "directory") ? GB_KIND_DIRECTORY : !strcasecmp(argv[2], "dialplan") ? GB_KIND_DIALPLAN : !strcasecmp(argv[2], "config") ? GB_KIND_CONFIG : 0;
		int rc = kind ? gb_lookup(globals.bridge, argv[1], kind, argv[3], 2000, &r) : GB_ERR_ARG;
		if (rc != GB_OK) {
			stream->write_function(stream, "-ERR lookup failed (%d)\n", rc);
		} else {
			if (!r.found) stream->write_function(stream, "-ERR not found\n");
			else stream->write_function(stream, "%s%s\n", r.payload, r.stale ? " (stale)" : "");
			gb_lookup_result_free(&r);
		}
	} else if (!strcasecmp(argv[0], "xml") && argc == 1) {
		char last[sizeof(globals.xml_last_reject)];
		unsigned served, rejected, reloads, failed;
		const char *source;
		switch_mutex_lock(globals.state_mutex);
		served = globals.xml_served;
		rejected = globals.xml_rejected;
		reloads = globals.acl_reloads;
		failed = globals.acl_fetch_failed;
		source = globals.acl_source ? globals.acl_source : "none";
		switch_snprintf(last, sizeof(last), "%s", globals.xml_last_reject);
		switch_mutex_unlock(globals.state_mutex);
		stream->write_function(stream, "sections=%s mode=%s served=%u rejected=%u last_reject=\"%s\" acl_reloads=%u acl_source=%s acl_fetch_failed=%u\n",
							   zstr(globals.xml_sections_raw) ? "-" : globals.xml_sections_raw,
							   globals.xf_mode == XF_FETCH_CONTEXT ? "context" : "call", served, rejected, last, reloads, source, failed);
	} else if (!strcasecmp(argv[0], "survival") && argc == 2) {
		gb_lookup_result_t r;
		if (globals.survival && gb_survival_get(globals.bridge, argv[1], &r) == GB_OK) {
			stream->write_function(stream, "ready version=%ld bytes=%u\n", (long) r.version, (unsigned) r.payload_len);
			gb_lookup_result_free(&r);
		} else {
			stream->write_function(stream, "not ready\n");
		}
	} else {
		goto usage;
	}
	switch_safe_free(mycmd);
	return SWITCH_STATUS_SUCCESS;
usage:
	switch_safe_free(mycmd);
	stream->write_function(stream, "-USAGE: %s\n", GRPC_SYNTAX);
	return SWITCH_STATUS_SUCCESS;
}

/* ---------------------------------------------------------- load / unload */

static char *read_token_file(const char *path)
{
	char buf[TOKEN_LINE_MAX];
	FILE *f = fopen(path, "r");
	size_t n;
	if (!f) return NULL;
	n = fread(buf, 1, sizeof(buf) - 1, f);
	fclose(f);
	buf[n] = 0;
	if (n == sizeof(buf) - 1) { /* cb_token never writes this much: refuse rather than send a cut token */
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "token-file %s is too long; ignoring it\n", path);
		return NULL;
	}
	while (n && (buf[n - 1] == '\n' || buf[n - 1] == '\r' || buf[n - 1] == ' ')) buf[--n] = 0;
	return n ? strdup(buf) : NULL;
}

/* Serves an API route's XML fragment (RM-27): only context grpc_api, only to the session that owns it. Every other
 * dialplan lookup gets NULL and falls through to the static dialplan. */
static switch_xml_t api_dialplan_search(const char *section, const char *tag_name, const char *key_name, const char *key_value,
										switch_event_t *params, void *user_data)
{
	const char *ctx, *uuid, *text;
	switch_core_session_t *s;
	switch_xml_t x = NULL;
	char *doc;

	if (!section || strcmp(section, "dialplan") || !params) return NULL;
	ctx = switch_event_get_header(params, "Hunt-Context");
	uuid = switch_event_get_header(params, "Unique-ID");
	if (zstr(ctx) || strcmp(ctx, "grpc_api") || zstr(uuid) || !(s = switch_core_session_locate(uuid))) return NULL;
	text = switch_channel_get_private(switch_core_session_get_channel(s), "grpc_api_xml");
	if (text && (doc = switch_mprintf("<document type=\"freeswitch/xml\"><section name=\"dialplan\">%s</section></document>", text))) {
		x = switch_xml_parse_str_dup(doc);
		free(doc);
		if (x) switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "api route xml served for %s\n", uuid);
	}
	switch_core_session_rwunlock(s);
	return x;
}

/* ------------------------------------------------- XML fetch binding (RM-28) */

/* The engine's fragment as the document FreeSWITCH expects from a binding; NULL when it does not parse. */
static switch_xml_t xf_wrap(const char *section, const char *payload)
{
	char *doc = switch_mprintf("<document type=\"freeswitch/xml\"><section name=\"%s\">%s</section></document>", section, payload);
	switch_xml_t x = doc ? switch_xml_parse_str_dup(doc) : NULL;
	switch_safe_free(doc);
	if (x && !zstr(switch_xml_error(x))) { /* the parser hands back a root even for broken input */
		switch_xml_free(x);
		x = NULL;
	}
	return x;
}

static void xf_reject(const char *key, const char *reason, const char *arg, switch_log_level_t level)
{
	switch_mutex_lock(globals.state_mutex);
	globals.xml_rejected++;
	switch_snprintf(globals.xml_last_reject, sizeof(globals.xml_last_reject), "%s: %s%s%s", key, reason, arg ? " " : "", arg ? arg : "");
	switch_mutex_unlock(globals.state_mutex);
	switch_log_printf(SWITCH_CHANNEL_LOG, level, "xml fetch: %s rejected: %s%s%s\n", key, reason, arg ? " " : "", arg ? arg : "");
}

/* During a reload acl_reload_thread started, the binding serves the answer that thread already fetched. Asking the
 * engine again here would wait under FreeSWITCH's ACL lock (every ACL check on the box blocks), and the cache may not
 * hold the answer (TTL 0, or a version below an Invalidate floor), so a failure here would load the disk lists. */
static int acl_doc_get(gb_lookup_result_t *r)
{
	int have;
	memset(r, 0, sizeof(*r));
	switch_mutex_lock(globals.state_mutex);
	have = globals.acl_doc_active;
	if (have) {
		r->found = globals.acl_doc_found;
		r->version = globals.acl_doc_version;
		r->payload = globals.acl_doc ? strdup(globals.acl_doc) : NULL; /* freed by gb_lookup_result_free */
		r->payload_len = r->payload ? strlen(r->payload) : 0;
	}
	switch_mutex_unlock(globals.state_mutex);
	return have;
}

/* Serves the sections listed in xml-sections from the engine, as mod_xml_curl would from a web server. The engine's
 * XML is trusted as it is (no allow-lists). Anything that goes wrong returns NULL, and FreeSWITCH then reads its
 * files: a dialplan answer must hold the asked context and a configuration answer the asked document, or the
 * caller would fail on a half answer instead of falling back. */
static switch_xml_t xml_fetch_search(const char *section, const char *tag_name, const char *key_name, const char *key_value,
									 switch_event_t *params, void *user_data)
{
	char key[160];
	const char *tenant = "", *context = NULL;
	gb_lookup_result_t r;
	switch_xml_t x, sec;
	int rc, dialplan, acl = 0;
	int64_t version;

	if (!globals.bridge || !section) return NULL;
	dialplan = !strcmp(section, "dialplan");
	if (dialplan) {
		if (!params) return NULL;
		context = switch_event_get_header(params, "Hunt-Context");
		if (!xf_dialplan_key(&globals.xf, context, key, sizeof(key))) return NULL;
		tenant = xf_tenant(switch_event_get_header(params, "variable_tenant_id"), switch_event_get_header(params, "variable_sip_h_X-Tenant-Id"));
	} else if (!strcmp(section, "configuration")) {
		if (!xf_conf_key(&globals.xf, key_value, key, sizeof(key))) return NULL;
		acl = !strcmp(key, XF_ACL_KEY);
	} else {
		return NULL;
	}

	if (acl && acl_doc_get(&r)) {
		rc = GB_OK;
	} else if (dialplan && globals.xf_mode == XF_FETCH_CALL) { /* the engine decides per call: send what mod_xml_curl would post */
		switch_event_header_t *hp;
		gb_kv_t *kv;
		size_t n = 0, i = 0;
		for (hp = params->headers; hp; hp = hp->next) n++;
		if (!(kv = calloc(n ? n : 1, sizeof(*kv)))) return NULL;
		for (hp = params->headers; hp; hp = hp->next) {
			if (zstr(hp->name) || !hp->value || !xf_utf8_ok(hp->name) || !xf_utf8_ok(hp->value)) continue; /* see xf_utf8_ok */
			kv[i].key = hp->name;
			kv[i++].value = hp->value;
		}
		rc = gb_fetch(globals.bridge, tenant, GB_KIND_XML, key, kv, i, XML_FETCH_TIMEOUT_MS, &r);
		free(kv);
	} else {
		rc = gb_lookup(globals.bridge, tenant, GB_KIND_XML, key, XML_FETCH_TIMEOUT_MS, &r);
	}
	if (rc != GB_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "xml fetch: %s not served (%d)\n", key, rc);
		return NULL;
	}
	if (!r.found || zstr(r.payload)) {
		gb_lookup_result_free(&r);
		return NULL;
	}
	version = r.version;
	if (acl) { /* a document that would lock the node out of its engine stays rejected until the engine sends another */
		int rejected;
		switch_mutex_lock(globals.state_mutex);
		rejected = globals.acl_rejected && globals.acl_rejected_version == version;
		switch_mutex_unlock(globals.state_mutex);
		if (rejected) {
			gb_lookup_result_free(&r);
			return NULL;
		}
	}
	x = xf_wrap(section, r.payload);
	gb_lookup_result_free(&r);
	if (!x) {
		xf_reject(key, "does not parse", NULL, SWITCH_LOG_WARNING);
		return NULL;
	}
	sec = switch_xml_child(x, "section");
	if (dialplan ? !switch_xml_find_child(sec, "context", "name", context) : !switch_xml_find_child(sec, "configuration", "name", key_value)) {
		xf_reject(key, dialplan ? "no context" : "no configuration", dialplan ? context : key_value, SWITCH_LOG_WARNING);
		switch_xml_free(x);
		return NULL;
	}
	if (acl && !zstr(globals.acl)) { /* the engine's document replaces the disk one whole: it must keep the list this node dials by */
		switch_xml_t lists = switch_xml_child(switch_xml_find_child(sec, "configuration", "name", key_value), "network-lists");
		if (!lists || !switch_xml_find_child(lists, "list", "name", globals.acl)) {
			xf_reject(key, "no list", globals.acl, SWITCH_LOG_WARNING);
			switch_xml_free(x);
			return NULL;
		}
	}
	switch_mutex_lock(globals.state_mutex);
	globals.xml_served++;
	if (acl) {
		globals.acl_served = 1;
		globals.acl_served_version = version;
	}
	switch_mutex_unlock(globals.state_mutex);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "xml fetch: served %s\n", key);
	return x;
}

/* FreeSWITCH loads acl.conf before any module, so an engine-served one only takes effect on a reload: after the
 * stream connects and when the engine invalidates it. The reload runs here, not on a bridge thread. It asks the
 * engine first and reloads only on a definite answer: an Invalidate has already emptied the cache, so reloading
 * blind during an engine hiccup would put the disk lists back without anyone asking for that. */
#define ACL_TICK_US 250000
#define ACL_RETRY_TICKS 20 /* 5 s between attempts while the engine cannot answer */
static void *SWITCH_THREAD_FUNC acl_reload_thread(switch_thread_t *thread, void *obj)
{
	int wait = 0, warned = 0;

	while (!globals.acl_stop) {
		gb_lookup_result_t r;
		char ip[sizeof(globals.engine_ip)];
		int pending, served;
		int64_t version;

		switch_yield(ACL_TICK_US);
		if (wait > 0) {
			wait--;
			continue;
		}
		switch_mutex_lock(globals.state_mutex);
		pending = globals.acl_pending;
		switch_mutex_unlock(globals.state_mutex);
		if (!pending || !gb_connected(globals.bridge)) continue;

		if (gb_lookup(globals.bridge, "", GB_KIND_XML, XF_ACL_KEY, XML_FETCH_TIMEOUT_MS, &r) != GB_OK) {
			switch_mutex_lock(globals.state_mutex);
			globals.acl_fetch_failed++;
			switch_mutex_unlock(globals.state_mutex);
			if (!warned) switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "acl fetch failed, keeping the loaded lists\n");
			warned = 1;
			wait = ACL_RETRY_TICKS;
			continue;
		}
		warned = 0;

		switch_mutex_lock(globals.state_mutex);
		globals.acl_pending = 0; /* cleared before the reload: a trigger that arrives during it asks for another */
		globals.acl_served = 0;
		globals.acl_doc_active = 1; /* the binding serves this answer during the reloads below (acl_doc_get) */
		globals.acl_doc_found = r.found;
		globals.acl_doc_version = r.version;
		globals.acl_doc = r.found ? r.payload : NULL;
		if (r.found) r.payload = NULL; /* owned by globals.acl_doc now */
		switch_mutex_unlock(globals.state_mutex);
		gb_lookup_result_free(&r);
		switch_load_network_lists(SWITCH_TRUE);
		switch_mutex_lock(globals.state_mutex);
		served = globals.acl_served;
		version = globals.acl_served_version;
		switch_snprintf(ip, sizeof(ip), "%s", globals.engine_ip);
		switch_mutex_unlock(globals.state_mutex);

		if (served && !zstr(globals.acl) && !zstr(ip) && switch_check_network_list_ip(ip, globals.acl) != SWITCH_TRUE) {
			switch_mutex_lock(globals.state_mutex);
			globals.acl_rejected = 1;
			globals.acl_rejected_version = version;
			switch_mutex_unlock(globals.state_mutex);
			xf_reject(XF_ACL_KEY, "refuses engine", ip, SWITCH_LOG_ERROR);
			switch_load_network_lists(SWITCH_TRUE); /* the binding now declines that version: the disk lists are back */
			served = 0;
		}
		switch_mutex_lock(globals.state_mutex);
		globals.acl_doc_active = 0;
		switch_safe_free(globals.acl_doc);
		globals.acl_reloads++;
		globals.acl_source = served ? "engine" : "disk";
		switch_mutex_unlock(globals.state_mutex);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "acl reloaded from %s\n", served ? "engine" : "disk");
	}
	return NULL;
}

SWITCH_MODULE_LOAD_FUNCTION(mod_grpc_load)
{
	switch_xml_t cfg, xml, settings, param;
	switch_api_interface_t *api_interface;
	switch_application_interface_t *app_interface;
	gb_config_t c;
	gb_callbacks_t cb;
	char *file_token = NULL;
	char version[64];
	static const switch_event_types_t types[] = { SWITCH_EVENT_CHANNEL_CREATE, SWITCH_EVENT_CHANNEL_ANSWER, SWITCH_EVENT_DTMF,
												  SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE };
	int i, require_tls = 0;

	memset(&globals, 0, sizeof(globals));
	curl_global_init(CURL_GLOBAL_DEFAULT); /* reference counted; other modules may have called it */
	memset(&c, 0, sizeof(c));
	memset(&cb, 0, sizeof(cb));
	globals.pool = pool;
	switch_mutex_init(&globals.state_mutex, SWITCH_MUTEX_NESTED, pool);
	*module_interface = switch_loadable_module_create_module_interface(pool, modname);

	if (!(xml = switch_xml_open_cfg("mod_grpc.conf", &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "mod_grpc.conf.xml not found\n");
		return SWITCH_STATUS_TERM;
	}
	c.capacity = 500;
	if ((settings = switch_xml_child(cfg, "settings"))) {
		for (param = switch_xml_child(settings, "param"); param; param = param->next) {
			const char *var = switch_xml_attr_soft(param, "name"), *val = switch_xml_attr_soft(param, "value");
			if (!strcasecmp(var, "engine-endpoints")) c.endpoints = globals.endpoints = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "node-id")) c.node_id = globals.node_id = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "password")) c.token = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "token-file")) globals.token_file = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "record-dir")) globals.record_dir = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "unavailable-prompt")) globals.unavailable_prompt = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "survival")) globals.survival = switch_true(val);
			else if (!strcasecmp(var, "survival-max-age")) globals.survival_max_age_s = atoi(val);
			else if (!strcasecmp(var, "survival-refresh")) globals.survival_refresh_s = atoi(val);
			else if (!strcasecmp(var, "survival-dial-format")) globals.survival_dial_format = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "apply-outbound-acl")) globals.acl = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "require-tls")) require_tls = switch_true(val);
			else if (!strcasecmp(var, "tls-ca")) c.tls_ca = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "tls-cert")) c.tls_cert = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "tls-key")) c.tls_key = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "tls-server-name")) c.tls_server_name = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "tags")) c.tags = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "api-allow")) globals.api_allow = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "api-headers")) globals.api_headers = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "xml-app-allow")) globals.xml_app_allow = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "var-allow")) globals.var_allow = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "route-vars")) globals.route_vars = switch_core_strdup(pool, val);
			else if (!strcasecmp(var, "xml-sections")) {
				char bad[200];
				globals.xml_sections_raw = switch_core_strdup(pool, val);
				if (xf_parse(val, &globals.xf, bad, sizeof(bad))) switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "xml-sections: ignored %s\n", bad);
			} else if (!strcasecmp(var, "xml-dialplan-fetch")) {
				int known;
				globals.xf_mode = xf_fetch_mode(val, &known);
				if (!known) switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "xml-dialplan-fetch %s unknown, using call\n", val);
			}
			else if (!strcasecmp(var, "capacity")) c.capacity = atoi(val);
			else if (!strcasecmp(var, "cache-max-entries")) c.cache_max_entries = atoi(val);
			else if (!strcasecmp(var, "keepalive-ms")) c.keepalive_ms = atoi(val);
			else if (!strcasecmp(var, "hello-timeout-ms")) c.hello_timeout_ms = atoi(val);
			else switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "unknown mod_grpc setting %s\n", var);
		}
	}
	if (globals.token_file && (file_token = read_token_file(globals.token_file))) c.token = switch_core_strdup(pool, file_token);
	free(file_token);
	switch_xml_free(xml);
	if (c.token && !strcmp(c.token, "change-me")) { /* the shipped sample's placeholder */
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "mod_grpc: password is still the sample's change-me; refusing to load\n");
		return SWITCH_STATUS_TERM;
	}
	if (zstr(c.endpoints) || zstr(c.node_id) || zstr(c.token)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "mod_grpc needs engine-endpoints, node-id and password (or token-file)\n");
		return SWITCH_STATUS_TERM;
	}
	if (!globals.api_allow) globals.api_allow = "uuid_setvar,uuid_transfer,uuid_broadcast,originate";
	if (!globals.api_headers) globals.api_headers = "";
	if (!globals.xml_app_allow) globals.xml_app_allow = "answer,set,export,playback,bridge,transfer,hangup,sleep";
	if (!globals.var_allow) globals.var_allow = ""; /* no variable is settable until listed */
	if (!globals.route_vars) globals.route_vars = ""; /* nothing extra is sent until listed */
	if (ag_var_csv_bad(globals.var_allow)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "mod_grpc: var-allow admits a variable that is never settable; refusing to load\n");
		return SWITCH_STATUS_TERM;
	}
	if (ag_csv_has_denied(globals.api_allow, 0) || ag_csv_has_denied(globals.xml_app_allow, 1)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "mod_grpc: api-allow or xml-app-allow names a command that is never allowed; refusing to load\n");
		return SWITCH_STATUS_TERM;
	}
	c.tags = zstr(c.tags) ? "api-batch" : switch_core_sprintf(pool, "%s,api-batch", c.tags);
	if ((c.tls_cert || c.tls_key) && (zstr(c.tls_ca) || zstr(c.tls_cert) || zstr(c.tls_key))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "mod_grpc: tls-cert and tls-key must both be set, and need tls-ca; refusing to load\n");
		return SWITCH_STATUS_TERM;
	}
	if (zstr(c.tls_ca) && require_tls) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "mod_grpc: require-tls is set but no tls-ca is configured; refusing to load\n");
		return SWITCH_STATUS_TERM;
	}
	if (zstr(c.tls_ca)) switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "mod_grpc: no tls-ca configured, the engine stream is NOT encrypted or authenticated\n");
	if (globals.survival) {
		char probe[512];
		if (sv_dial("00000000-0000-4000-8000-000000000000", globals.survival_dial_format, "1000", "example.test", probe, sizeof(probe)) != SV_OK) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "mod_grpc: survival needs a survival-dial-format with exactly two %%s (number, domain) that yields a safe dial string; refusing to load\n");
			return SWITCH_STATUS_TERM;
		}
		if (globals.survival_max_age_s <= 0) globals.survival_max_age_s = 86400;
		if (globals.survival_refresh_s <= 0) globals.survival_refresh_s = 300;
		c.survival_refresh_s = globals.survival_refresh_s;
		c.survival_max_age_s = globals.survival_max_age_s;
	}
	switch_snprintf(version, sizeof(version), "%s", switch_version_full());
	c.version = version;

	cb.on_state = cb_state;
	cb.on_command = cb_command;
	cb.on_token = cb_token;
	cb.allow_addr = cb_allow_addr;
	cb.on_invalidate = cb_invalidate;
	cb.stats = cb_stats;
	cb.log = cb_log;

	if (switch_queue_create(&globals.cmdq, CMDQ_SIZE, pool) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "cannot create the command queue\n");
		return SWITCH_STATUS_TERM;
	}
	if (!(globals.bridge = gb_create(&c, &cb, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "cannot create the gRPC bridge\n");
		return SWITCH_STATUS_TERM;
	}
	{
		switch_threadattr_t *attr = NULL;
		switch_threadattr_create(&attr, pool);
		if (switch_thread_create(&globals.cmd_thread, attr, command_thread, NULL, pool) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "cannot start the command thread\n");
			gb_destroy(globals.bridge); /* never started: nothing else can reference it yet */
			globals.bridge = NULL;
			return SWITCH_STATUS_TERM;
		}
	}
	if (cr_threads_start(pool) != SWITCH_STATUS_SUCCESS) {
		switch_status_t st;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "cannot start conference recording threads\n");
		cr_stop_threads();
		if (globals.cmdq) switch_queue_push(globals.cmdq, NULL);
		if (globals.cmd_thread) switch_thread_join(&st, globals.cmd_thread);
		gb_destroy(globals.bridge);
		globals.bridge = NULL;
		return SWITCH_STATUS_TERM;
	}
	if (switch_event_bind_removable(modname, SWITCH_EVENT_CUSTOM, "conference::maintenance", cr_maint_event, NULL, &globals.conf_node) != SWITCH_STATUS_SUCCESS) {
		switch_status_t st;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "cannot bind conference::maintenance\n");
		cr_stop_threads();
		if (globals.cmdq) switch_queue_push(globals.cmdq, NULL);
		if (globals.cmd_thread) switch_thread_join(&st, globals.cmd_thread);
		gb_destroy(globals.bridge);
		globals.bridge = NULL;
		return SWITCH_STATUS_TERM;
	}
	for (i = 0; i < 4; i++) {
		if (switch_event_bind_removable(modname, types[i], SWITCH_EVENT_SUBCLASS_ANY, event_handler, NULL, &globals.nodes[globals.nnodes]) == SWITCH_STATUS_SUCCESS) globals.nnodes++;
	}
	SWITCH_ADD_API(api_interface, "grpc", "mod_grpc control", grpc_api, GRPC_SYNTAX);
	/* NOMEDIA: FreeSWITCH must not pre-answer (183 with SDP) before the engine has said what to do; a bridge then ringsback the caller and answers it when the callee answers (pre-answered, the caller was answered only 5 s after the callee) */
	SWITCH_ADD_APP(app_interface, "grpc_route", "Ask the engine how to route this call", "Route through the control engine",
				   grpc_route_function, "", SAF_SUPPORT_NOMEDIA);
	switch_xml_bind_search_function_ret(directory_search, SWITCH_XML_SECTION_DIRECTORY, NULL, &globals.dir_binding);
	switch_xml_bind_search_function_ret(api_dialplan_search, SWITCH_XML_SECTION_DIALPLAN, NULL, &globals.dp_binding);
	if (xf_any(&globals.xf)) /* off unless xml-sections lists something: no binding, nothing changes */
		switch_xml_bind_search_function_ret(xml_fetch_search,
											(globals.xf.dialplan ? SWITCH_XML_SECTION_DIALPLAN : 0) | (globals.xf.nconf ? SWITCH_XML_SECTION_CONFIG : 0),
											NULL, &globals.xml_binding);
	if (xf_conf_listed(&globals.xf, "acl.conf")) {
		switch_threadattr_t *attr = NULL;
		switch_threadattr_create(&attr, pool);
		if (switch_thread_create(&globals.acl_thread, attr, acl_reload_thread, NULL, pool) != SWITCH_STATUS_SUCCESS) {
			globals.acl_thread = NULL;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "cannot start the ACL reload thread: the engine's acl.conf will only load on a manual reloadacl\n");
		}
	}
	gb_start(globals.bridge);
	return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_grpc_shutdown)
{
	switch_status_t st;
	void *item = NULL;
	int i;
	cr_shutdown_recordings(); /* refuse, stop, wait for the file close, drain uploads; then the custom event is unbound */
	if (globals.dir_binding) switch_xml_unbind_search_function(&globals.dir_binding);
	if (globals.dp_binding) switch_xml_unbind_search_function(&globals.dp_binding);
	if (globals.xml_binding) switch_xml_unbind_search_function(&globals.xml_binding);
	for (i = 0; i < globals.nnodes; i++) switch_event_unbind(&globals.nodes[i]);
	globals.acl_stop = 1;
	if (globals.bridge) gb_stop(globals.bridge); /* no callback fires after this */
	if (globals.acl_thread) { /* after gb_stop: a fetch in progress returns at once on a stopped bridge */
		switch_thread_join(&st, globals.acl_thread);
		globals.acl_thread = NULL;
	}
	if (globals.cmdq) {
		switch_queue_push(globals.cmdq, NULL); /* wake the command thread */
		if (globals.cmd_thread) switch_thread_join(&st, globals.cmd_thread);
		globals.cmd_thread = NULL;
		while (switch_queue_trypop(globals.cmdq, &item) == SWITCH_STATUS_SUCCESS) cmd_free((cmd_item_t *) item);
	}
	undrain(); /* after the command thread: no Drain can land behind it; an unloaded module must not leave the node paused */
	/* Deliberately leave the stopped bridge allocated and globals.bridge set (a small leak per unload): in-flight
	 * event/route/API users are not waited for, and on a stopped bridge they just get GB_ERR_DISCONNECTED instead of
	 * touching freed memory or NULL. */
	return SWITCH_STATUS_SUCCESS;
}
