// Connection lifecycle. One worker thread owns it: resolve, ACL, dial, Hello,
// read loop, reconnect. Each live stream also has a writer thread (single
// writer, bounded queue) and a heartbeat thread. Blocking callers (lookup,
// route) wait on per-request slots that the read loop completes.
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>

#include "bridge_internal.h"

using gb::Pending;
using gb::Session;

namespace {

std::vector<std::string> split(const std::string& s, char sep) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, sep)) {
    size_t a = item.find_first_not_of(" \t"), b = item.find_last_not_of(" \t");
    if (a != std::string::npos) out.push_back(item.substr(a, b - a + 1));
  }
  return out;
}

// 32 lowercase hex chars from /dev/urandom. Called once per bridge, not per Hello:
// a reconnect must present the same generation the engine already stored.
std::string new_boot_id() {
  std::ifstream in("/dev/urandom", std::ios::in | std::ios::binary);
  unsigned char b[16] = {};
  in.read(reinterpret_cast<char*>(b), 16);
  if (!in) return "";
  static const char* hexd = "0123456789abcdef";
  std::string out(32, '0');
  for (int i = 0; i < 16; i++) {
    out[i * 2] = hexd[b[i] >> 4];
    out[i * 2 + 1] = hexd[b[i] & 0xf];
  }
  return out;
}

bool read_file(const std::string& path, std::string* out) {
  std::ifstream f(path, std::ios::binary);
  if (!f) return false;
  std::stringstream ss;
  ss << f.rdbuf();
  *out = ss.str();
  return true;
}

// "host:port" or "[v6]:port"
bool split_endpoint(const std::string& ep, std::string* host, std::string* port) {
  if (ep.empty()) return false;
  if (ep[0] == '[') {
    size_t e = ep.find(']');
    if (e == std::string::npos || e + 2 > ep.size() || ep[e + 1] != ':') return false;
    *host = ep.substr(1, e - 1);
    *port = ep.substr(e + 2);
  } else {
    size_t c = ep.rfind(':');
    if (c == std::string::npos || c == 0) return false;
    *host = ep.substr(0, c);
    *port = ep.substr(c + 1);
  }
  return !port->empty();
}

std::shared_ptr<grpc::ChannelCredentials> make_creds(gb_bridge* b, bool* ok) {
  *ok = true;
  if (b->tls_ca.empty()) return grpc::InsecureChannelCredentials();
  grpc::SslCredentialsOptions o;
  if (!read_file(b->tls_ca, &o.pem_root_certs)) {
    b->log(GB_LOG_ERR, "cannot read tls-ca %s", b->tls_ca.c_str());
    *ok = false;
    return nullptr;
  }
  if (!b->tls_cert.empty() || !b->tls_key.empty()) {
    if (!read_file(b->tls_cert, &o.pem_cert_chain) || !read_file(b->tls_key, &o.pem_private_key)) {
      b->log(GB_LOG_ERR, "cannot read tls-cert/tls-key");
      *ok = false;
      return nullptr;
    }
  }
  return grpc::SslCredentials(o);
}

}  // namespace

void gb_bridge::log(int level, const char* fmt, ...) {
  if (!cb.log) return;
  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof buf, fmt, ap);
  va_end(ap);
  cb.log(ud, level, buf);
}

void gb_bridge::state(int connected_now, const std::string& reason) {
  connected = connected_now != 0;
  cb.on_state(ud, connected_now, reason.c_str());
}

bool gb_bridge::stopping() {
  std::lock_guard<std::mutex> l(mu);
  return stop;
}

std::shared_ptr<Session> gb_bridge::current() {
  std::lock_guard<std::mutex> l(mu);
  return sess;
}

void gb_bridge::run() {
  std::vector<std::string> eps = split(endpoints, ',');
  size_t idx = 0;
  int backoff = backoff_min_ms;
  std::minstd_rand rng(std::random_device{}());
  while (!stopping()) {
    auto t0 = std::chrono::steady_clock::now();
    bool established = !eps.empty() && connect_once(eps[idx++ % eps.size()]);
    if (stopping()) break;
    // A stream that ended within a minute is a flap, not a recovery: keep backing off.
    bool lasted = established && std::chrono::steady_clock::now() - t0 >= std::chrono::seconds(60);
    backoff = lasted ? backoff_min_ms : std::min(backoff * 2, backoff_max_ms);
    // Full jitter on the upper half, so nodes that lost one engine together do not reconnect in step.
    int wait = backoff / 2 + static_cast<int>(rng() % static_cast<unsigned>(backoff / 2 + 1));
    std::unique_lock<std::mutex> l(mu);
    stop_cv.wait_for(l, Ms(wait), [&] { return stop; });
  }
}

// Resolves the endpoint, applies the ACL hook to every address and tries the
// allowed ones in order. Returns true when a stream was established.
bool gb_bridge::connect_once(const std::string& endpoint) {
  std::string host, port;
  if (!split_endpoint(endpoint, &host, &port)) {
    state(0, "bad endpoint: " + endpoint);
    return false;
  }
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  int rc = getaddrinfo(host.c_str(), port.c_str(), &hints, &res);
  if (rc != 0) {
    state(0, std::string("resolve ") + host + ": " + gai_strerror(rc));
    return false;
  }
  std::vector<std::string> targets;
  for (addrinfo* a = res; a; a = a->ai_next) {
    char ip[INET6_ADDRSTRLEN] = {0};
    if (a->ai_family == AF_INET) {
      inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(a->ai_addr)->sin_addr, ip, sizeof ip);
    } else if (a->ai_family == AF_INET6) {
      inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6*>(a->ai_addr)->sin6_addr, ip, sizeof ip);
    } else {
      continue;
    }
    if (cb.allow_addr && !cb.allow_addr(ud, ip)) {
      log(GB_LOG_WARN, "engine address %s refused by ACL", ip);
      continue;
    }
    targets.push_back(a->ai_family == AF_INET6 ? std::string("[") + ip + "]:" + port : std::string(ip) + ":" + port);
  }
  freeaddrinfo(res);
  if (targets.empty()) {
    state(0, "acl: no allowed address for " + host);
    return false;
  }
  for (auto& t : targets) {
    if (stopping()) return false;
    if (run_stream(t, host)) return true;
  }
  return false;
}

bool gb_bridge::run_stream(const std::string& target, const std::string& host) {
  bool creds_ok = false;
  auto creds = make_creds(this, &creds_ok);
  if (!creds_ok) {
    state(0, "tls files unreadable");
    return false;
  }
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, keepalive_ms);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
  if (!tls_ca.empty()) args.SetSslTargetNameOverride(tls_server_name.empty() ? host : tls_server_name);
  auto channel = grpc::CreateCustomChannel(target, creds, args);
  auto stub = cp::ControlPlane::NewStub(channel);

  auto s = std::make_shared<Session>();
  s->stream = stub->Connect(&s->ctx);
  {
    std::lock_guard<std::mutex> l(mu);
    if (stop) {
      s->ctx.TryCancel();
      s->stream->Finish();
      return false;
    }
    sess = s;  // gb_stop cancels through this pointer
  }

  std::string tok;
  {
    std::lock_guard<std::mutex> l(mu);
    tok = token;
  }
  cp::NodeMessage hello;
  hello.set_request_id("hello");
  cp::Hello* h = hello.mutable_hello();
  h->set_node_id(node_id);
  h->set_version(version);
  h->set_token(tok);
  h->set_capacity(capacity);
  h->set_boot_id(boot_id);
  h->set_sip_address(sip_address);
  h->set_registration_mode(registration_mode);
  for (auto& t : split(tags, ',')) h->add_tags(t);

  // The watchdog cancels the stream when no HelloAck arrives in time.
  // It covers the Registrations opens too: those shards are in s->regs (appended under wm) by then.
  std::mutex wm;
  std::condition_variable wcv;
  bool handshake_done = false, timed_out = false;
  std::thread watchdog([&] {
    std::unique_lock<std::mutex> l(wm);
    if (wcv.wait_for(l, Ms(hello_timeout_ms), [&] { return handshake_done; })) return;
    timed_out = true;
    s->ctx.TryCancel();
    for (auto& r : s->regs) r->ctx.TryCancel();
  });
  auto finish_handshake = [&] {
    {
      std::lock_guard<std::mutex> l(wm);
      handshake_done = true;
    }
    wcv.notify_all();
    watchdog.join();
  };

  cp::EngineMessage ack;
  bool got = s->stream->Write(hello) && s->stream->Read(&ack);
  bool accepted = got && ack.has_hello_ack() && ack.hello_ack().accepted();
  if (accepted && reg_streams > 0) open_regs(stub.get(), s, ack.hello_ack().session_id(), tok, wm);
  finish_handshake();
  if (!accepted || timed_out) {
    std::string reason = got && ack.has_hello_ack() ? ack.hello_ack().reason() : "";
    if (accepted) reason = "no RegStreamAck in time";
    close_regs(s);
    s->ctx.TryCancel();
    grpc::Status st = s->stream->Finish();
    if (reason.empty()) reason = st.error_message().empty() ? "no HelloAck" : st.error_message();
    {
      std::lock_guard<std::mutex> l(mu);
      sess.reset();
    }
    state(0, "hello refused: " + reason);
    return false;
  }

  {
    std::lock_guard<std::mutex> l(s->fm_);
    s->features.assign(ack.hello_ack().features().begin(), ack.hello_ack().features().end());
  }
  cache.expire_all();  // Invalidates sent while we were away never arrived: refetch rather than trust old entries
  int hb_s = ack.hello_ack().heartbeat_interval_s() > 0 ? ack.hello_ack().heartbeat_interval_s() : 5;
  state(1, "session " + ack.hello_ack().session_id());

  std::thread writer([s] {
    for (;;) {
      cp::NodeMessage m;
      {
        std::unique_lock<std::mutex> l(s->qm);
        s->qcv.wait(l, [&] { return s->closed || !s->q.empty(); });
        if (s->closed) return;
        m = std::move(s->q.front());
        s->q.pop_front();
      }
      if (!s->stream->Write(m)) {
        s->close();
        s->ctx.TryCancel();
        return;
      }
    }
  });
  std::thread heartbeat([this, s, hb_s] {
    for (;;) {
      {
        std::unique_lock<std::mutex> l(s->qm);
        if (s->hbcv.wait_for(l, std::chrono::seconds(hb_s), [&] { return s->closed; })) return;
      }
      cp::NodeMessage m;
      cp::Heartbeat* hb = m.mutable_heartbeat();
      int32_t calls = 0, cpu = 0;
      if (cb.stats) cb.stats(ud, &calls, &cpu);
      hb->set_active_calls(calls);
      hb->set_cpu_pct(cpu);
      hb->set_ts_unix_ms(std::chrono::duration_cast<Ms>(std::chrono::system_clock::now().time_since_epoch()).count());
      s->push(std::move(m));
    }
  });

  cp::EngineMessage in;
  while (s->stream->Read(&in)) dispatch(s, in);

  s->close();
  s->ctx.TryCancel();
  close_regs(s);
  writer.join();
  heartbeat.join();
  grpc::Status st = s->stream->Finish();
  {
    std::lock_guard<std::mutex> l(mu);
    sess.reset();
  }
  state(0, "stream ended: " + (st.error_message().empty() ? std::string("closed") : st.error_message()));
  return true;
}

bool gb_bridge::open_regs(cp::ControlPlane::Stub* stub, const std::shared_ptr<Session>& s, const std::string& session_id,
                          const std::string& tok, std::mutex& wm) {
  std::string why;
  bool unimplemented = false;
  for (int i = 0; i < reg_streams && why.empty(); i++) {
    gb::RegStream* r;
    {
      auto owned = std::make_unique<gb::RegStream>();
      r = owned.get();
      std::lock_guard<std::mutex> l(wm);  // the watchdog walks s->regs; TryCancel before the call starts still cancels it
      s->regs.push_back(std::move(owned));
    }
    r->stream = stub->Registrations(&r->ctx);
    cp::RegStreamMessage open;
    cp::RegStreamOpen* o = open.mutable_open();
    o->set_node_id(node_id);
    o->set_token(tok);
    o->set_session_id(session_id);
    o->set_shard(i);
    o->set_shards(reg_streams);
    cp::RegStreamAck ack;
    if (r->stream->Write(open) && r->stream->Read(&ack)) {
      if (!ack.accepted()) why = "refused: " + (ack.reason().empty() ? std::string("no reason") : ack.reason());
      continue;
    }
    grpc::Status st = r->stream->Finish();
    r->stream.reset();  // finished: close_regs must not Finish it again
    unimplemented = st.error_code() == grpc::StatusCode::UNIMPLEMENTED;
    why = unimplemented ? "the engine has no Registrations RPC" : st.error_message().empty() ? "stream closed" : st.error_message();
  }
  if (!why.empty()) {
    close_regs(s);
    {
      std::lock_guard<std::mutex> l(wm);
      s->regs.clear();
    }
    log(unimplemented ? GB_LOG_INFO : GB_LOG_WARN, "registration streams: %s; RegEvents go on the Connect stream", why.c_str());
    return false;
  }
  for (size_t i = 0; i < s->regs.size(); i++) {
    gb::RegStream* r = s->regs[i].get();
    r->writer = std::thread([this, s, r, i] {
      for (;;) {
        cp::RegStreamMessage m;
        {
          std::unique_lock<std::mutex> l(r->qm);
          r->qcv.wait(l, [&] { return r->closed || !r->q.empty(); });
          if (r->closed) return;
          m = std::move(r->q.front());
          r->q.pop_front();
        }
        if (!r->stream->Write(m)) {
          if (!s->closed_now()) log(GB_LOG_WARN, "registration stream %zu write failed: closing the session", i);
          r->close();
          s->ctx.TryCancel();  // the shards live and die with the session: reconnect and resync everything
          return;
        }
      }
    });
    r->reader = std::thread([this, s, r, i] {
      cp::RegStreamAck a;
      while (r->stream->Read(&a)) {
      }
      if (!s->closed_now()) {
        log(GB_LOG_WARN, "registration stream %zu ended: closing the session", i);
        s->ctx.TryCancel();
      }
    });
  }
  reg_active = static_cast<int>(s->regs.size());
  return true;
}

// Stops and finishes every shard of s. The vector is left as is: senders may still hold s.
void gb_bridge::close_regs(const std::shared_ptr<Session>& s) {
  for (auto& r : s->regs) {
    r->close();
    r->ctx.TryCancel();
  }
  for (auto& r : s->regs) {
    if (r->writer.joinable()) r->writer.join();
    if (r->reader.joinable()) r->reader.join();
    if (r->stream) {
      r->stream->Finish();
      r->stream.reset();
    }
  }
  reg_active = 0;
}

void gb_bridge::dispatch(const std::shared_ptr<Session>& s, const cp::EngineMessage& m) {
  switch (m.body_case()) {
    case cp::EngineMessage::kLookupResponse:
    case cp::EngineMessage::kDataResponse:
    case cp::EngineMessage::kRouteResponse: {
      std::shared_ptr<Pending> p;
      {
        std::lock_guard<std::mutex> l(s->pm);
        auto it = s->pending.find(m.request_id());
        if (it == s->pending.end()) return;  // the caller already timed out
        p = it->second;
        s->pending.erase(it);
      }
      std::lock_guard<std::mutex> l(p->m);
      p->reply = m;
      p->done = true;
      p->cv.notify_all();
      return;
    }
    case cp::EngineMessage::kInvalidate:
      on_invalidate(m.invalidate());
      return;
    case cp::EngineMessage::kCommand:
      on_command(m.command());
      return;
    case cp::EngineMessage::kTokenRotate:
      on_token_rotate(m.token_rotate());
      return;
    default:
      return;
  }
}

// The engine rotated this node's token: keep it for the next Hello and let the
// module persist it. The old token stays valid on the engine for the overlap.
void gb_bridge::on_token_rotate(const cp::TokenRotate& t) {
  if (t.token().empty()) return;
  {
    std::lock_guard<std::mutex> l(mu);
    token = t.token();
  }
  if (cb.on_token) cb.on_token(ud, t.token().c_str(), t.overlap_s());
}

// Sends m and waits for the reply with the same request id.
int gb_bridge::call(cp::NodeMessage* m, int timeout_ms, cp::EngineMessage* reply) {
  auto s = current();
  if (!s || !connected) return GB_ERR_DISCONNECTED;
  std::string id = "r" + std::to_string(++seq);
  m->set_request_id(id);
  auto p = std::make_shared<Pending>();
  {
    std::lock_guard<std::mutex> l(s->pm);
    if (s->dead) return GB_ERR_DISCONNECTED;
    s->pending[id] = p;
  }
  auto forget = [&] {
    std::lock_guard<std::mutex> l(s->pm);
    s->pending.erase(id);
  };
  if (!s->push(std::move(*m))) {
    forget();
    return s->closed_now() ? GB_ERR_DISCONNECTED : GB_ERR_QUEUE_FULL;
  }
  std::unique_lock<std::mutex> l(p->m);
  if (!p->cv.wait_for(l, Ms(timeout_ms), [&] { return p->done; })) {
    l.unlock();
    forget();
    return GB_ERR_TIMEOUT;
  }
  if (p->err != GB_OK) return p->err;
  *reply = std::move(p->reply);
  return GB_OK;
}

// ---------------------------------------------------------------- C API

extern "C" {

const char* gb_version(void) { return "grpc_bridge 1"; }

gb_bridge_t* gb_create(const gb_config_t* cfg, const gb_callbacks_t* cb, void* ud) {
  try {
    if (!cfg || !cb || !cfg->endpoints || !cfg->node_id || !cb->on_state || !cb->on_command) return nullptr;
    static std::once_flag once;
    std::call_once(once, [] { setenv("GRPC_ENABLE_FORK_SUPPORT", "0", 1); });
    auto* b = new gb_bridge(cfg->cache_max_entries > 0 ? static_cast<size_t>(cfg->cache_max_entries) : 10000);
    b->endpoints = cfg->endpoints;
    b->node_id = cfg->node_id;
    b->version = gb::str(cfg->version);
    b->token = gb::str(cfg->token);
    b->tags = gb::str(cfg->tags);
    b->capacity = cfg->capacity;
    b->boot_id = new_boot_id();
    b->sip_address = gb::str(cfg->sip_address);
    b->registration_mode = gb::str(cfg->registration_mode);
    if (b->registration_mode == "engine")
      b->reg_streams = cfg->registration_streams <= 0 ? 1 : std::min(cfg->registration_streams, GB_MAX_REG_STREAMS);
    b->tls_ca = gb::str(cfg->tls_ca);
    b->tls_cert = gb::str(cfg->tls_cert);
    b->tls_key = gb::str(cfg->tls_key);
    b->tls_server_name = gb::str(cfg->tls_server_name);
    if (cfg->hello_timeout_ms > 0) b->hello_timeout_ms = cfg->hello_timeout_ms;
    if (cfg->backoff_min_ms > 0) b->backoff_min_ms = cfg->backoff_min_ms;
    if (cfg->backoff_max_ms > 0) b->backoff_max_ms = cfg->backoff_max_ms;
    if (b->backoff_max_ms < b->backoff_min_ms) b->backoff_max_ms = b->backoff_min_ms;
    if (cfg->keepalive_ms > 0) b->keepalive_ms = cfg->keepalive_ms;
    if (cfg->survival_refresh_s > 0) {
      b->survival_refresh_s = cfg->survival_refresh_s;
      b->survival_max_age_s = cfg->survival_max_age_s > 0 ? cfg->survival_max_age_s : 86400;
    }
    b->cb = *cb;
    b->ud = ud;
    return b;
  } catch (...) {
    return nullptr;
  }
}

int gb_start(gb_bridge_t* b) {
  try {
    if (!b) return GB_ERR_ARG;
    std::lock_guard<std::mutex> l(b->mu);
    if (b->started) return GB_OK;
    b->started = true;
    b->worker = std::thread([b] { b->run(); });
    if (b->survival_refresh_s > 0) b->sthread = std::thread([b] { b->survival_loop(); });
    return GB_OK;
  } catch (...) {
    return GB_ERR_INTERNAL;
  }
}

void gb_stop(gb_bridge_t* b) {
  try {
    if (!b) return;
    std::shared_ptr<Session> s;
    std::thread t, st;
    {
      std::lock_guard<std::mutex> l(b->mu);
      b->stop = true;
      s = b->sess;
      b->stop_cv.notify_all();
      t = std::move(b->worker);
    }
    {
      std::lock_guard<std::mutex> l(b->sm);
      b->sstop = true;
      b->scv.notify_all();
      st = std::move(b->sthread);
    }
    if (s) s->ctx.TryCancel();
    if (t.joinable()) t.join();
    if (st.joinable()) st.join();
  } catch (...) {
  }
}

void gb_destroy(gb_bridge_t* b) {
  if (!b) return;
  gb_stop(b);
  delete b;
}

int gb_connected(gb_bridge_t* b) { return b && b->connected ? 1 : 0; }

int gb_reg_streams(gb_bridge_t* b) { return b ? b->reg_active.load() : 0; }

}  // extern "C"
