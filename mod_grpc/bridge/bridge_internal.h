// Internal declarations shared by the bridge's source files. Not part of the C API.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "cache.h"
#include "controlplane/v1/control.grpc.pb.h"
#include "grpc_bridge.h"
#include "utf8.h"

namespace cp = controlplane::v1;
using Ms = std::chrono::milliseconds;

namespace gb {

const size_t kMaxQueue = 4096;

inline std::string str(const char* s) { return s ? std::string(s) : std::string(); }
// For proto string fields: a value that is not UTF-8 would make the engine drop the stream, so it is sent empty.
inline bool text_ok(const char* s) { return s && gb_utf8_ok(s); }
inline std::string text(const char* s) { return text_ok(s) ? std::string(s) : std::string(); }

// A caller blocked in lookup/route, waiting for the reply with its request id.
struct Pending {
  std::mutex m;
  std::condition_variable cv;
  bool done = false;
  int err = GB_OK;
  cp::EngineMessage reply;
};

// One engine lookup that concurrent cache misses for the same key wait on, instead of each asking.
struct Flight {
  std::mutex m;
  std::condition_variable cv;
  bool done = false;
  int rc = GB_ERR_INTERNAL;
  bool unavailable = false;
  Cache::Entry en;
};

// Everything that lives and dies with one stream.
struct Session {
  grpc::ClientContext ctx;
  std::unique_ptr<grpc::ClientReaderWriter<cp::NodeMessage, cp::EngineMessage>> stream;

  std::mutex qm;
  std::condition_variable qcv;   // wakes the writer
  std::condition_variable hbcv;  // wakes the heartbeat thread: a separate variable, or notify_one can wake the wrong thread
  std::deque<cp::NodeMessage> q;
  bool closed = false;

  std::mutex pm;
  std::unordered_map<std::string, std::shared_ptr<Pending>> pending;
  bool dead = false;

  bool push(cp::NodeMessage&& m) {
    std::lock_guard<std::mutex> l(qm);
    if (closed || q.size() >= kMaxQueue) return false;
    q.push_back(std::move(m));
    qcv.notify_one();
    return true;
  }

  // push for the fire-and-forget senders: GB_OK, GB_ERR_DISCONNECTED (stream closed) or GB_ERR_QUEUE_FULL
  int send(cp::NodeMessage&& m) { return push(std::move(m)) ? GB_OK : closed_now() ? GB_ERR_DISCONNECTED : GB_ERR_QUEUE_FULL; }

  bool closed_now() {
    std::lock_guard<std::mutex> l(qm);
    return closed;
  }

  // Stops the writer and heartbeat threads and fails everything waiting.
  void close() {
    {
      std::lock_guard<std::mutex> l(qm);
      closed = true;
      qcv.notify_all();
      hbcv.notify_all();
    }
    std::lock_guard<std::mutex> l(pm);
    dead = true;
    for (auto& kv : pending) {
      std::lock_guard<std::mutex> pl(kv.second->m);
      kv.second->done = true;
      kv.second->err = GB_ERR_DISCONNECTED;
      kv.second->cv.notify_all();
    }
    pending.clear();
  }
};

}  // namespace gb

struct gb_bridge {
  // configuration (owned copies)
  std::string endpoints, node_id, version, tags, tls_ca, tls_cert, tls_key, tls_server_name, sip_address, registration_mode;
  int32_t capacity = 0;
  int hello_timeout_ms = 10000, backoff_min_ms = 500, backoff_max_ms = 15000, keepalive_ms = 30000;
  gb_callbacks_t cb{};
  void* ud = nullptr;

  gb::Cache cache;
  std::mutex fm;  // guards flights
  std::unordered_map<std::string, std::shared_ptr<gb::Flight>> flights;

  std::mutex mu;  // guards token, stop, sess
  std::condition_variable stop_cv;
  std::string token;
  std::string boot_id;  // set in gb_create; every Hello of this process sends it
  bool stop = false;
  bool started = false;
  std::shared_ptr<gb::Session> sess;
  std::thread worker;
  std::atomic<bool> connected{false};
  std::atomic<uint64_t> seq{0};
  // survival cache (survival.cc)
  int survival_refresh_s = 0, survival_max_age_s = 0;
  std::mutex sm;  // guards stenants, sdirty
  std::condition_variable scv;
  std::atomic<bool> sstop{false};
  std::set<std::string> stenants;  // every tenant that routed a call here
  std::set<std::string> sdirty;    // touched or invalidated since the last pass: fetched at once
  std::thread sthread;

  explicit gb_bridge(size_t cache_max) : cache(cache_max) {}

  void log(int level, const char* fmt, ...);
  void state(int connected_now, const std::string& reason);
  bool stopping();
  std::shared_ptr<gb::Session> current();

  // connection.cc
  void run();
  bool connect_once(const std::string& endpoint);
  bool run_stream(const std::string& target, const std::string& host);
  void dispatch(const std::shared_ptr<gb::Session>& s, const cp::EngineMessage& m);
  int call(cp::NodeMessage* m, int timeout_ms, cp::EngineMessage* reply);

  // lookup.cc
  // Asks the engine and caches the answer (found: its TTL; not found: briefly). GB_OK fills *en; *unavailable is
  // true when the engine could not read its backend (nothing is cached then).
  // params non-null: a per-call fetch, sent with those params and never cached
  int fetch(const std::string& tenant, int kind, const std::string& key, int timeout_ms, gb::Cache::Entry* en, bool* unavailable,
            const gb_kv_t* params = nullptr, size_t nparams = 0);
  // fetch, but concurrent misses for one key share a single engine round trip (gb_lookup)
  int fetch_shared(const std::string& tenant, int kind, const std::string& key, int timeout_ms, gb::Cache::Entry* en,
                   bool* unavailable);
  // survival.cc
  void survival_loop();
  void survival_wake(const std::string& tenant);
  void on_invalidate(const cp::Invalidate& v);
  // calls.cc
  void on_command(const cp::Command& c);
  // connection.cc
  void on_token_rotate(const cp::TokenRotate& t);
};
