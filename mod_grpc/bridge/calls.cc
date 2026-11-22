// Call control: route requests, step reports, call events, command delivery and results.
#include <cstdlib>
#include <cstring>

#include "bridge_internal.h"

namespace {

const char* dup_cstr(const std::string& s) {
  char* p = static_cast<char*>(std::malloc(s.size() + 1));
  if (!p) return nullptr;
  std::memcpy(p, s.data(), s.size());
  p[s.size()] = 0;
  return p;
}

const char* borrow_cstr(const std::string& s) { return s.c_str(); }

int64_t now_ms() {
  return std::chrono::duration_cast<Ms>(std::chrono::system_clock::now().time_since_epoch()).count();
}

// Fills o from c. str maps a proto string to what o stores: the proto's own
// buffer (borrow_cstr) or a malloc'd copy (dup_cstr). Originate variables go
// to *vars as pointers into c; the caller decides how to keep them.
// False when the command has no action.
bool fill(const cp::Command& c, gb_command_t* o, const char* (*str)(const std::string&), std::vector<gb_kv_t>* vars) {
  o->command_id = str(c.command_id());
  o->tenant_id = str(c.tenant_id());
  o->step_id = str(c.step_id());
  switch (c.action_case()) {
    case cp::Command::kOriginate:
      o->type = GB_CMD_ORIGINATE;
      o->dial_string = str(c.originate().dial_string());
      o->timeout_s = c.originate().timeout_s();
      o->route_on_answer = c.originate().route_on_answer() ? 1 : 0;
      for (auto& kv : c.originate().vars()) vars->push_back({kv.first.c_str(), kv.second.c_str()});
      return true;
    case cp::Command::kBridge:
      o->type = GB_CMD_BRIDGE;
      o->uuid = str(c.bridge().uuid());
      o->dial_string = str(c.bridge().dial_string());
      o->peer_tenant_id = str(c.bridge().peer_tenant_id());
      o->peer_caller_id = str(c.bridge().peer_caller_id());
      o->timeout_s = c.bridge().timeout_s();
      return true;
    case cp::Command::kPark:
      o->type = GB_CMD_PARK;
      o->uuid = str(c.park().uuid());
      return true;
    case cp::Command::kPlayback:
      o->type = GB_CMD_PLAYBACK;
      o->uuid = str(c.playback().uuid());
      o->file = str(c.playback().file());
      return true;
    case cp::Command::kHangup:
      o->type = GB_CMD_HANGUP;
      o->uuid = str(c.hangup().uuid());
      o->cause = str(c.hangup().cause());
      return true;
    case cp::Command::kDrain:
      o->type = GB_CMD_DRAIN;
      o->deadline_s = c.drain().deadline_s();
      return true;
    case cp::Command::kCollect:
      o->type = GB_CMD_COLLECT;
      o->uuid = str(c.collect().uuid());
      o->file = str(c.collect().prompt());
      o->min_digits = c.collect().min_digits();
      o->max_digits = c.collect().max_digits();
      o->timeout_ms = c.collect().timeout_ms();
      o->terminators = str(c.collect().terminators());
      return true;
    case cp::Command::kRecord:
      o->type = GB_CMD_RECORD;
      o->uuid = str(c.record().uuid());
      o->name = str(c.record().name());
      o->max_len_s = c.record().max_len_s();
      o->silence_s = c.record().silence_s();
      o->beep = c.record().beep() ? 1 : 0;
      o->upload_url = str(c.record().upload_url());
      return true;
    case cp::Command::kConference:
      o->type = GB_CMD_CONFERENCE;
      o->uuid = str(c.conference().uuid());
      o->room = str(c.conference().room());
      o->mute = c.conference().mute() ? 1 : 0;
      o->moderator = c.conference().moderator() ? 1 : 0;
      o->wait_for_moderator = c.conference().wait_for_moderator() ? 1 : 0;
      o->profile = str(c.conference().profile());
      return true;
    case cp::Command::kSleep:
      o->type = GB_CMD_SLEEP;
      o->uuid = str(c.sleep().uuid());
      o->ms = c.sleep().ms();
      return true;
    case cp::Command::kEavesdrop:
      o->type = GB_CMD_EAVESDROP;
      o->uuid = str(c.eavesdrop().uuid());
      o->target_uuid = str(c.eavesdrop().target_uuid());
      o->mode = static_cast<int32_t>(c.eavesdrop().mode());
      return true;
    case cp::Command::kSuperviseMode:
      o->type = GB_CMD_SUPERVISE_MODE;
      o->uuid = str(c.supervise_mode().uuid());
      o->mode = static_cast<int32_t>(c.supervise_mode().mode());
      return true;
    case cp::Command::kAnnounce:
      o->type = GB_CMD_ANNOUNCE;
      o->uuid = str(c.announce().uuid());
      o->file = str(c.announce().prompt());
      o->leg_only = c.announce().leg_only() ? 1 : 0;
      return true;
    case cp::Command::kTransfer:
      o->type = GB_CMD_TRANSFER;
      o->uuid = str(c.transfer().uuid());
      o->token = str(c.transfer().token());
      return true;
    case cp::Command::kBridgeLegs:
      o->type = GB_CMD_BRIDGE_LEGS;
      o->uuid = str(c.bridge_legs().uuid_a());
      o->target_uuid = str(c.bridge_legs().uuid_b());
      return true;
    case cp::Command::kConferenceControl:
      o->type = GB_CMD_CONFERENCE_CONTROL;
      o->room = str(c.conference_control().room());
      o->target_uuid = str(c.conference_control().member_uuid());
      o->conf_action = static_cast<int32_t>(c.conference_control().action());
      o->upload_url = str(c.conference_control().upload_url());
      o->rec_id = str(c.conference_control().rec_id());
      o->max_len_s = c.conference_control().max_s();
      o->file = str(c.conference_control().recording_prompt());
      return true;
    case cp::Command::kAmd:
      o->type = GB_CMD_AMD;
      o->uuid = str(c.amd().uuid());
      return true;
    default:
      return false;
  }
}

// Deep-copies a RouteResponse into out (the caller owns it). GB_OK with no
// commands leaves out zeroed.
int export_commands(const cp::RouteResponse& r, gb_route_result_t* out) {
  if (r.unavailable()) return GB_ERR_UNAVAILABLE;
  if (r.has_api_batch()) {
    const cp::ApiBatch& b = r.api_batch();
    out->api_rule_id = dup_cstr(b.rule_id());
    if (b.actions_size() > 0) {
      out->api = static_cast<gb_api_action_t*>(std::calloc(b.actions_size(), sizeof(gb_api_action_t)));
      if (!out->api) return GB_ERR_INTERNAL;
      out->napi = b.actions_size();
      size_t k = 0;
      for (const cp::ApiAction& a : b.actions()) {
        out->api[k].kind = static_cast<int>(a.kind());
        out->api[k].name = dup_cstr(a.name());
        out->api[k].value = dup_cstr(a.value());
        k++;
      }
    }
  }
  const auto& cmds = r.commands();
  if (cmds.empty()) return GB_OK;
  out->cmds = static_cast<gb_command_t*>(std::calloc(cmds.size(), sizeof(gb_command_t)));
  if (!out->cmds) return GB_ERR_INTERNAL;
  out->ncmds = cmds.size();
  size_t i = 0;
  for (const cp::Command& c : cmds) {
    gb_command_t& o = out->cmds[i++];
    std::vector<gb_kv_t> vars;
    if (!fill(c, &o, dup_cstr, &vars) || vars.empty()) continue;
    gb_kv_t* kv = static_cast<gb_kv_t*>(std::calloc(vars.size(), sizeof(gb_kv_t)));
    if (!kv) {
      gb_route_result_free(out);
      return GB_ERR_INTERNAL;
    }
    for (size_t k = 0; k < vars.size(); k++) {
      kv[k].key = dup_cstr(vars[k].key);
      kv[k].value = dup_cstr(vars[k].value);
    }
    o.vars = kv;
    o.nvars = vars.size();
  }
  return GB_OK;
}

}  // namespace

// Engine -> node command: hand a borrowed view to the callback, which queues it.
void gb_bridge::on_command(const cp::Command& c) {
  gb_command_t out{};
  std::vector<gb_kv_t> vars;
  if (!fill(c, &out, borrow_cstr, &vars)) {
    log(GB_LOG_WARN, "command %s has no action", c.command_id().c_str());
    return;
  }
  out.vars = vars.data();
  out.nvars = vars.size();
  cb.on_command(ud, &out);
}

extern "C" {

int gb_route(gb_bridge_t* b, const gb_route_request_t* req, int timeout_ms, gb_route_result_t* out) {
  try {
    if (!b || !req || !out) return GB_ERR_ARG;
    std::memset(out, 0, sizeof *out);
    cp::NodeMessage m;
    cp::RouteRequest* r = m.mutable_route_request();
    r->set_tenant_id(gb::text(req->tenant_id));
    r->set_call_id(gb::text(req->call_id));
    r->set_uuid(gb::text(req->uuid));
    r->set_domain(gb::text(req->domain));
    r->set_caller_id_number(gb::text(req->caller_id_number));
    r->set_destination_number(gb::text(req->destination_number));
    for (size_t i = 0; i < req->nvars; i++)
      if (gb::text_ok(req->vars[i].key) && gb::text_ok(req->vars[i].value))  // a bad pair is left out, not blanked
        (*r->mutable_vars())[req->vars[i].key] = req->vars[i].value;
    cp::EngineMessage reply;
    int rc = b->call(&m, timeout_ms > 0 ? timeout_ms : 2000, &reply);
    if (rc != GB_OK) return rc;
    rc = export_commands(reply.route_response(), out);
    if (rc != GB_OK) gb_route_result_free(out);
    return rc;
  } catch (...) {
    gb_route_result_free(out);  // zeroed at entry, or a partial result: either way safe
    return GB_ERR_INTERNAL;
  }
}

int gb_step(gb_bridge_t* b, const gb_step_result_t* s, int timeout_ms, gb_route_result_t* out) {
  try {
    if (!b || !s || !out) return GB_ERR_ARG;
    std::memset(out, 0, sizeof *out);
    cp::NodeMessage m;
    cp::StepResult* r = m.mutable_step_result();
    r->set_tenant_id(gb::text(s->tenant_id));
    r->set_call_id(gb::text(s->call_id));
    r->set_uuid(gb::text(s->uuid));
    r->set_step_id(gb::text(s->step_id));
    r->set_hung_up(s->hung_up != 0);
    r->set_error(gb::text(s->error));
    for (size_t i = 0; i < s->nvars; i++)
      if (gb::text_ok(s->vars[i].key) && gb::text_ok(s->vars[i].value))  // a bad pair is left out, not blanked
        (*r->mutable_vars())[s->vars[i].key] = s->vars[i].value;
    switch (s->type) {
      case GB_CMD_BRIDGE: {
        cp::BridgeOutcome* o = r->mutable_bridge();
        o->set_answered(s->answered != 0);
        o->set_cause(gb::text(s->cause));
        o->set_elapsed_s(s->elapsed_s);
        break;
      }
      case GB_CMD_COLLECT: {
        cp::CollectOutcome* o = r->mutable_collect();
        o->set_digits(gb::text(s->digits));
        o->set_timed_out(s->timed_out != 0);
        break;
      }
      case GB_CMD_PLAYBACK:
        r->mutable_playback();
        break;
      case GB_CMD_RECORD: {
        cp::RecordOutcome* o = r->mutable_record();
        o->set_path(gb::text(s->path));
        o->set_length_ms(s->length_ms);
        o->set_uploaded(s->uploaded != 0);
        break;
      }
      case GB_CMD_CONFERENCE:
        r->mutable_conference();
        break;
      case GB_CMD_SLEEP:
        r->mutable_sleep();
        break;
      case GB_CMD_EAVESDROP:
        r->mutable_eavesdrop();
        break;
      case GB_CMD_AMD: {
        cp::AmdOutcome* o = r->mutable_amd();
        o->set_result(gb::text(s->amd_result));
        o->set_cause(gb::text(s->amd_cause));
        break;
      }
      default:
        return GB_ERR_ARG;
    }
    cp::EngineMessage reply;
    int rc = b->call(&m, timeout_ms > 0 ? timeout_ms : 3000, &reply);
    if (rc != GB_OK) return rc;
    rc = export_commands(reply.route_response(), out);
    if (rc != GB_OK) gb_route_result_free(out);
    return rc;
  } catch (...) {
    gb_route_result_free(out);
    return GB_ERR_INTERNAL;
  }
}

void gb_route_result_free(gb_route_result_t* r) {
  if (!r) return;
  for (size_t i = 0; i < r->napi; i++) {
    std::free(const_cast<char*>(r->api[i].name));
    std::free(const_cast<char*>(r->api[i].value));
  }
  std::free(r->api);
  std::free(const_cast<char*>(r->api_rule_id));
  r->api = nullptr;
  r->napi = 0;
  r->api_rule_id = nullptr;
  if (!r->cmds) return;
  for (size_t i = 0; i < r->ncmds; i++) {
    gb_command_t& c = r->cmds[i];
    std::free(const_cast<char*>(c.command_id));
    std::free(const_cast<char*>(c.tenant_id));
    std::free(const_cast<char*>(c.step_id));
    std::free(const_cast<char*>(c.uuid));
    std::free(const_cast<char*>(c.dial_string));
    std::free(const_cast<char*>(c.peer_tenant_id));
    std::free(const_cast<char*>(c.peer_caller_id));
    std::free(const_cast<char*>(c.file));
    std::free(const_cast<char*>(c.cause));
    std::free(const_cast<char*>(c.terminators));
    std::free(const_cast<char*>(c.room));
    std::free(const_cast<char*>(c.profile));
    std::free(const_cast<char*>(c.name));
    std::free(const_cast<char*>(c.upload_url));
    std::free(const_cast<char*>(c.rec_id));
    std::free(const_cast<char*>(c.target_uuid));
    std::free(const_cast<char*>(c.token));
    for (size_t k = 0; k < c.nvars; k++) {
      std::free(const_cast<char*>(c.vars[k].key));
      std::free(const_cast<char*>(c.vars[k].value));
    }
    std::free(const_cast<gb_kv_t*>(c.vars));
  }
  std::free(r->cmds);
  r->cmds = nullptr;
  r->ncmds = 0;
}

int gb_send_call_event(gb_bridge_t* b, const char* tenant, const char* call_id, const char* uuid, int type,
                       const char* digit, const char* cause, const char* peer, const char* peer_tenant) {
  try {
    if (!b) return GB_ERR_ARG;
    auto s = b->current();
    if (!s || !b->connected) return GB_ERR_DISCONNECTED;
    cp::NodeMessage m;
    cp::CallEvent* e = m.mutable_call_event();
    e->set_tenant_id(gb::text(tenant));
    e->set_call_id(gb::text(call_id));
    e->set_uuid(gb::text(uuid));
    e->set_type(static_cast<cp::CallEventType>(type));
    e->set_digit(gb::text(digit));
    e->set_cause(gb::text(cause));
    e->set_peer(gb::text(peer));
    e->set_peer_tenant_id(gb::text(peer_tenant));
    e->set_ts_unix_ms(now_ms());
    return s->send(std::move(m));
  } catch (...) {
    return GB_ERR_INTERNAL;
  }
}

int gb_send_conf_event(gb_bridge_t* b, int type, const char* tenant,
                       const char* room, const char* rec_id, int64_t length_ms, int uploaded,
                       const char* member_uuid, const char* room_instance) {
  try {
    if (!b || !tenant || !room || length_ms < 0 ||
        (type != cp::ConfEvent::RECORDING_DONE && type != cp::ConfEvent::ROOM_READY)) return GB_ERR_ARG;
    auto s = b->current();
    if (!s || !b->connected) return GB_ERR_DISCONNECTED;
    cp::NodeMessage m;
    cp::ConfEvent* e = m.mutable_conf_event();
    e->set_type(static_cast<cp::ConfEvent::Type>(type));
    e->set_tenant(gb::text(tenant));
    e->set_room(gb::text(room));
    e->set_rec_id(gb::text(rec_id));
    e->set_length_ms(length_ms);
    e->set_uploaded(uploaded != 0);
    e->set_member_uuid(gb::text(member_uuid));
    e->set_room_instance(gb::text(room_instance));
    return s->send(std::move(m));
  } catch (...) {
    return GB_ERR_INTERNAL;
  }
}

int gb_send_command_result(gb_bridge_t* b, const char* command_id, int ok, const char* error) {
  try {
    if (!b) return GB_ERR_ARG;
    auto s = b->current();
    if (!s || !b->connected) return GB_ERR_DISCONNECTED;
    cp::NodeMessage m;
    cp::CommandResult* r = m.mutable_command_result();
    r->set_command_id(gb::text(command_id));
    r->set_ok(ok != 0);
    r->set_error(gb::text(error));
    return s->send(std::move(m));
  } catch (...) {
    return GB_ERR_INTERNAL;
  }
}

}  // extern "C"
