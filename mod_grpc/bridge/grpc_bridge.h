/* libgrpc_bridge: transport for mod_grpc. Flat C API; no FreeSWITCH headers,
 * no exceptions across it. Callbacks run on bridge threads: they must return
 * quickly, and must not call back into gb_* except gb_send_*. */
#ifndef GRPC_BRIDGE_H
#define GRPC_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GB_OK 0
#define GB_ERR_ARG (-1)
#define GB_ERR_DISCONNECTED (-2) /* no stream, or it ended while waiting */
#define GB_ERR_TIMEOUT (-3)
#define GB_ERR_QUEUE_FULL (-4)
#define GB_ERR_UNAVAILABLE (-5) /* the engine answered "cannot serve right now" */
#define GB_ERR_CONFLICT (-6)
#define GB_ERR_NOT_FOUND (-7)
#define GB_ERR_ENGINE (-8) /* the engine answered ok=false with error set (e.g. not_integer) */
#define GB_ERR_INTERNAL (-9)

/* values match controlplane.v1.LookupKind */
#define GB_KIND_DIRECTORY 1
#define GB_KIND_DIALPLAN 2
#define GB_KIND_CONFIG 3
#define GB_KIND_XML 4 /* a FreeSWITCH XML fragment: key dialplan/<context> or configuration/<name> */

/* the CONFIG lookup key under which a tenant's snapshot is kept warm for survival mode (the engine ignores the key) */
#define GB_SURVIVAL_KEY "survival"

/* values match controlplane.v1.CallEventType */
#define GB_CONF_RECORDING_DONE 1 /* ConfEvent.Type */
#define GB_CONF_ROOM_READY 2
#define GB_EVT_CREATED 1
#define GB_EVT_ANSWERED 2
#define GB_EVT_DTMF 3
#define GB_EVT_HANGUP 4

/* values match the Command.action oneof order */
#define GB_CMD_ORIGINATE 1
#define GB_CMD_BRIDGE 2
#define GB_CMD_PARK 3
#define GB_CMD_PLAYBACK 4
#define GB_CMD_HANGUP 5
#define GB_CMD_DRAIN 6
#define GB_CMD_COLLECT 7
#define GB_CMD_RECORD 8
#define GB_CMD_CONFERENCE 9
#define GB_CMD_SLEEP 10
#define GB_CMD_EAVESDROP 11
#define GB_CMD_SUPERVISE_MODE 12
#define GB_CMD_ANNOUNCE 13
#define GB_CMD_TRANSFER 14
#define GB_CMD_BRIDGE_LEGS 15
#define GB_CMD_CONFERENCE_CONTROL 16
#define GB_CMD_AMD 17
#define GB_CMD_MESSAGE_WAITING 18
#define GB_CMD_ANSWER 19

#define GB_LOG_ERR 0
#define GB_LOG_WARN 1
#define GB_LOG_INFO 2
#define GB_LOG_DEBUG 3

typedef struct gb_bridge gb_bridge_t;

#define GB_DATA_GET 1
#define GB_DATA_SET 2
#define GB_DATA_DEL 3
#define GB_DATA_INCR 4
#define GB_DATA_CAS 5
typedef struct {
  const char *tenant_id, *key, *value;
  int op;
  int32_t ttl_s;
  int64_t delta, expect_version;
} gb_data_req_t;
typedef struct {
  char *value; /* malloc'd or NULL */
  int64_t version;
  char error[64];
} gb_data_res_t;
/* One uncached round trip to the engine's data API. GB_OK (out->version, out->value for GET/INCR); GB_ERR_NOT_FOUND (get);
 * GB_ERR_CONFLICT (out->version = current); GB_ERR_UNAVAILABLE; GB_ERR_ENGINE (out->error); GB_ERR_TIMEOUT;
 * GB_ERR_DISCONNECTED; GB_ERR_ARG (NULL, bad op, or tenant/key/value not UTF-8). Never touches the cache. */
int gb_data(gb_bridge_t *b, const gb_data_req_t *r, int timeout_ms, gb_data_res_t *out);
void gb_data_res_free(gb_data_res_t *r);
/* 1 when the live session's HelloAck listed the feature */
int gb_has_feature(gb_bridge_t *b, const char *name);
/* comma-joined features of the live session ("" with none); returns the length written */
size_t gb_features(gb_bridge_t *b, char *out, size_t outlen);

typedef struct {
	const char *key;
	const char *value;
} gb_kv_t;

typedef struct {
	const char *endpoints; /* "host:port[,host:port...]", tried round-robin */
	const char *node_id;
	const char *version;
	const char *token;
	const char *tags; /* comma separated, may be NULL */
	int32_t capacity;
	const char *tls_ca; /* PEM file paths; tls_ca NULL means plaintext (development only) */
	const char *tls_cert;
	const char *tls_key;
	const char *tls_server_name; /* name to verify; NULL means the endpoint host */
	int hello_timeout_ms;        /* 0 = 10000 */
	int backoff_min_ms;          /* 0 = 500 */
	int backoff_max_ms;          /* 0 = 15000 */
	int cache_max_entries;       /* 0 = 10000 */
	int keepalive_ms;            /* HTTP/2 keepalive ping interval, 0 = 30000 */
	int survival_refresh_s;      /* 0 = survival cache off; else the periodic refetch interval */
	int survival_max_age_s;      /* 0 = 86400: how long a fetched snapshot may be served with no engine */
	const char *sip_address;       /* host:port of this node's SIP profile, may be NULL */
	const char *registration_mode; /* "engine" or "local"; NULL in edge mode */
	int registration_streams;      /* engine mode: Registrations shards per session, 0 = 1, capped at GB_MAX_REG_STREAMS */
} gb_config_t;
#define GB_MAX_REG_STREAMS 16

typedef struct {
	const char *command_id;
	const char *tenant_id;
	int type; /* GB_CMD_* */
	const char *uuid;
	const char *dial_string;
	const char *peer_tenant_id;
	const char *peer_caller_id;
	const char *file;
	const char *cause;
	int32_t timeout_s;
	int32_t deadline_s;
	const gb_kv_t *vars; /* Originate only */
	size_t nvars;
	int route_on_answer; /* Originate: run grpc_route when the far end answers instead of parking the leg */
	const char *step_id;     /* non-empty: report on this command with gb_step */
	const char *terminators; /* Collect (the prompt arrives in file) */
	const char *room;        /* Conference */
	const char *profile;
	const char *name;        /* Record */
	int32_t min_digits, max_digits, timeout_ms; /* Collect */
	int32_t max_len_s, silence_s;               /* Record */
	int32_t ms;                                 /* Sleep */
	int beep;                                   /* Record */
	const char *rec_id;     /* ConferenceControl: recording identifier */
	const char *url; /* Playback url / Collect prompt_url */
	const char *upload_url; /* Record/ConferenceControl: presigned PUT URL ("" = keep the file local) */
	int mute;               /* Conference: join muted */
	const char *target_uuid; /* Eavesdrop */
	int32_t mode;            /* Eavesdrop / SuperviseMode: 1=listen, 2=whisper, 3=barge */
	int leg_only;            /* Announce: 1=aleg only, 0=both */
	const char *token;       /* Transfer */
	int moderator;           /* Conference: join with the moderator flag */
	int wait_for_moderator;  /* Conference: members wait until a moderator is in */
	int32_t conf_action;     /* ConferenceControl: 1=mute 2=unmute 3=mute_all 4=unmute_all 5=kick 6=lock 7=unlock 8=end 9=record_start 10=record_stop (room in room, member in target_uuid) */
	const char *const *peer_addresses; /* Bridge: hop addresses, npeer_addresses entries */
	size_t npeer_addresses;
	const char *peer_target;           /* Bridge: user@domain for the hop legs */
	const char *user, *domain;                  /* MessageWaiting */
	int32_t new_msgs, saved_msgs, new_urgent, saved_urgent;
} gb_command_t;

typedef struct {
	/* connected=1 after an accepted HelloAck, 0 whenever the stream is down */
	void (*on_state)(void *ud, int connected, const char *reason);
	/* the command is only valid during the call */
	void (*on_command)(void *ud, const gb_command_t *cmd);
	/* optional: after the bridge cache has dropped the matching entries */
	void (*on_invalidate)(void *ud, const char *tenant, int kind, const char *key, int64_t version);
	/* optional: the engine rotated this node's token; persist it */
	void (*on_token)(void *ud, const char *new_token, int overlap_s);
	/* optional ACL hook: called on the connection thread for every resolved engine address;
	   must return quickly (it can delay gb_stop), and NULL means every address is allowed
	   (the module must install it when an ACL is configured) */
	int (*allow_addr)(void *ud, const char *ip);
	/* optional: heartbeat figures */
	void (*stats)(void *ud, int32_t *active_calls, int32_t *cpu_pct);
	/* optional */
	void (*log)(void *ud, int level, const char *msg);
} gb_callbacks_t;

const char *gb_version(void);

/* Copies cfg and cb. Does not connect. NULL on bad arguments. */
gb_bridge_t *gb_create(const gb_config_t *cfg, const gb_callbacks_t *cb, void *ud);
int gb_start(gb_bridge_t *b);   /* starts the connection thread */
/* cancels the stream, joins threads; idempotent. A blocking DNS lookup in progress can delay it. */
void gb_stop(gb_bridge_t *b);
void gb_destroy(gb_bridge_t *b); /* gb_stop + free */
int gb_connected(gb_bridge_t *b);

typedef struct {
	int found;
	int64_t version;
	int32_t ttl_s;
	char *payload; /* NUL-terminated copy; free with gb_lookup_result_free */
	size_t payload_len;
	int from_cache;
	int stale; /* served after expiry because the engine is unreachable */
} gb_lookup_result_t;

/* Cache first; on a miss asks the engine. When the engine cannot be reached
 * a stale entry is served rather than an error (fail-static). A tenant or key that is not UTF-8 is never sent
 * (GB_ERR_ARG, unless a cached entry answers); gb_fetch refuses it the same way. */
int gb_lookup(gb_bridge_t *b, const char *tenant, int kind, const char *key, int timeout_ms, gb_lookup_result_t *out);
void gb_lookup_result_free(gb_lookup_result_t *r);

/* One round trip carrying params (may be NULL when nparams is 0), for answers that depend on the call: the per-call
 * XML dialplan. Never reads or writes the cache and never serves stale, so with no stream it fails at once
 * (GB_ERR_DISCONNECTED). GB_OK fills out (found, version, payload; from_cache and stale are 0; free with
 * gb_lookup_result_free); GB_ERR_UNAVAILABLE when the engine answers "cannot serve right now". */
int gb_fetch(gb_bridge_t *b, const char *tenant, int kind, const char *key, const gb_kv_t *params, size_t nparams,
			 int timeout_ms, gb_lookup_result_t *out);

typedef struct {
	size_t entries;
	uint64_t hits, misses, stale_served, invalidated;
} gb_cache_stats_t;
void gb_cache_stats(gb_bridge_t *b, gb_cache_stats_t *out);
void gb_cache_flush(gb_bridge_t *b);

/* Survival cache (see survival.cc). Touch registers a tenant that routed a call here: its CONFIG snapshot is fetched
 * at once and then kept fresh while an engine is reachable. Get returns GB_OK with the snapshot (free with
 * gb_lookup_result_free) when a found entry no older than survival_max_age_s exists; otherwise GB_ERR_UNAVAILABLE.
 * A tenant that was never touched is never served. Touch never blocks on the engine. */
void gb_survival_touch(gb_bridge_t *b, const char *tenant);
int gb_survival_get(gb_bridge_t *b, const char *tenant, gb_lookup_result_t *out);

typedef struct {
	const char *tenant_id;
	const char *call_id;
	const char *uuid;
	const char *domain;
	const char *caller_id_number;
	const char *destination_number;
	const gb_kv_t *vars;
	size_t nvars;
} gb_route_request_t;

/* An operator API route's action (RM-27); kinds match controlplane.v1.ApiAction.Kind. */
#define GB_API_SET_HEADER 1
#define GB_API_UNSET_HEADER 2
#define GB_API_SET_VAR 3
#define GB_API_API 4
#define GB_API_BGAPI 5
#define GB_API_XML 6

typedef struct {
	int kind;
	const char *name;
	const char *value;
} gb_api_action_t;

typedef struct {
	gb_command_t *cmds; /* deep copy; free with gb_route_result_free */
	size_t ncmds;
	gb_api_action_t *api; /* non-empty: run these instead of cmds (only from gb_route) */
	size_t napi;
	const char *api_rule_id;
} gb_route_result_t;

int gb_route(gb_bridge_t *b, const gb_route_request_t *req, int timeout_ms, gb_route_result_t *out);
void gb_route_result_free(gb_route_result_t *r);

typedef struct {
	const char *tenant_id;
	const char *call_id;
	const char *uuid;
	const char *step_id;
	int type;            /* GB_CMD_* of the command that was reported on */
	int hung_up;         /* the caller was gone when it ended */
	const char *error;   /* non-empty: the command could not run */
	int answered;        /* BRIDGE */
	int32_t elapsed_s;
	const char *cause;
	const char *digits;  /* COLLECT */
	int timed_out;
	const char *path;    /* RECORD */
	int32_t length_ms;
	int uploaded; /* RECORD: the file reached the upload_url */
	const char *amd_result; /* AMD: HUMAN, MACHINE, NOTSURE */
	const char *amd_cause;
	const gb_kv_t *vars; /* route-vars, borrowed: copied during gb_step */
	size_t nvars;
} gb_step_result_t;

/* Reports how the last command of a batch ended and returns the next batch in
 * out (free with gb_route_result_free). Safe to retry with the same step_id. */
int gb_step(gb_bridge_t *b, const gb_step_result_t *s, int timeout_ms, gb_route_result_t *out);

/* fire and forget; GB_ERR_DISCONNECTED when there is no stream, GB_ERR_QUEUE_FULL when the send queue is full */
int gb_send_call_event(gb_bridge_t *b, const char *tenant, const char *call_id, const char *uuid, int type, const char *digit, const char *cause, const char *peer, const char *peer_tenant);
#define GB_REG_REGISTER 1
#define GB_REG_UNREGISTER 2
#define GB_REG_EXPIRE 3
typedef struct {
	int type; /* GB_REG_* */
	const char *tenant_id, *user, *domain, *contact;
	int32_t expires_s;
	const char *network_ip;
	int32_t network_port;
	const char *user_agent, *call_id;
	int resync;
} gb_reg_event_t;
/* GB_ERR_ARG on NULL or a bad type, otherwise as gb_send_call_event. Goes on Registrations shard
 * hash(user@domain) % n when the session has shards (each with its own queue), else on the Connect stream, where a
 * resync event is refused (GB_ERR_QUEUE_FULL) once that queue is half full, leaving room for routes and steps. */
int gb_send_reg_event(gb_bridge_t *b, const gb_reg_event_t *e);
/* Registrations shards of the live session; 0 when RegEvents go on the Connect stream (or no session). */
int gb_reg_streams(gb_bridge_t *b);
/* Conference lifecycle events: type 1=recording done, 2=room ready. */
int gb_send_conf_event(gb_bridge_t *b, int type, const char *tenant,
    const char *room, const char *rec_id, int64_t length_ms, int uploaded,
    const char *member_uuid, const char *room_instance);

int gb_send_command_result(gb_bridge_t *b, const char *command_id, int ok, const char *error);

#ifdef __cplusplus
}
#endif
#endif
