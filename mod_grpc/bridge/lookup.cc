// Cached lookups and Invalidate handling.
#include <algorithm>
#include <cstdlib>
#include <cstring>

#include "bridge_internal.h"

namespace {

const int kNegativeTtlCapS = 30;

char* dup_bytes(const std::string& s) {
  char* p = static_cast<char*>(std::malloc(s.size() + 1));
  if (!p) return nullptr;
  std::memcpy(p, s.data(), s.size());
  p[s.size()] = 0;
  return p;
}

void fill(gb_lookup_result_t* out, const gb::Cache::Entry& e, int from_cache) {
  out->found = e.found;
  out->version = e.version;
  out->ttl_s = e.ttl_s;
  out->payload = dup_bytes(e.payload);
  out->payload_len = out->payload ? e.payload.size() : 0;
  out->from_cache = from_cache;
  out->stale = e.stale;
}

}  // namespace

void gb_bridge::on_invalidate(const cp::Invalidate& v) {
  cache.invalidate(v.tenant_id(), v.kind(), v.key(), v.version());
  survival_wake(v.tenant_id());
  if (cb.on_invalidate) cb.on_invalidate(ud, v.tenant_id().c_str(), v.kind(), v.key().c_str(), v.version());
}

int gb_bridge::fetch(const std::string& tenant, int kind, const std::string& key, int timeout_ms, gb::Cache::Entry* en,
                     bool* unavailable, const gb_kv_t* params, size_t nparams) {
  cp::NodeMessage m;
  cp::LookupRequest* r = m.mutable_lookup();
  r->set_tenant_id(tenant);
  r->set_kind(static_cast<cp::LookupKind>(kind));
  r->set_key(key);
  for (size_t i = 0; params && i < nparams; i++)
    if (gb::text_ok(params[i].key) && gb::text_ok(params[i].value)) (*r->mutable_params())[params[i].key] = params[i].value;
  cp::EngineMessage reply;
  int rc = call(&m, timeout_ms, &reply);
  if (rc != GB_OK) return rc;
  const cp::LookupResponse& lr = reply.lookup_response();
  *unavailable = lr.unavailable();  // a backend failure is not "not found": never cache it
  if (*unavailable) return GB_OK;
  en->found = lr.found();
  en->version = lr.version();
  en->ttl_s = lr.ttl_s();
  en->payload = lr.payload();
  if (params) return GB_OK;  // the answer belongs to one call
  int ttl = lr.found() ? lr.ttl_s() : std::min<int>(lr.ttl_s(), kNegativeTtlCapS);
  // An engine that does not serve XML answers not found with no TTL; without a floor every dialplan hunt would ask.
  if (!lr.found() && kind == GB_KIND_XML && ttl <= 0) ttl = kNegativeTtlCapS;
  cache.put(tenant, kind, key, en->found, en->version, ttl, en->payload, survival_pin(tenant, kind, key));
  return GB_OK;
}

int gb_bridge::fetch_shared(const std::string& tenant, int kind, const std::string& key, int timeout_ms, gb::Cache::Entry* en,
                            bool* unavailable) {
  const std::string fk = tenant + '\x1f' + std::to_string(kind) + '\x1f' + key;
  std::shared_ptr<gb::Flight> f;
  bool leader = false;
  {
    std::lock_guard<std::mutex> l(fm);
    auto& slot = flights[fk];
    if (!slot) {
      slot = std::make_shared<gb::Flight>();
      leader = true;
    }
    f = slot;
  }
  if (!leader) {
    std::unique_lock<std::mutex> l(f->m);
    if (!f->cv.wait_for(l, Ms(timeout_ms), [&] { return f->done; })) return GB_ERR_TIMEOUT;
    *en = f->en;
    *unavailable = f->unavailable;
    return f->rc;
  }
  // Publishes and retires the flight even if fetch throws, so no later miss waits on a dead one.
  struct Finish {
    gb_bridge* b;
    const std::string& fk;
    gb::Flight& f;
    ~Finish() {
      {
        std::lock_guard<std::mutex> l(b->fm);
        b->flights.erase(fk);
      }
      std::lock_guard<std::mutex> l(f.m);
      f.done = true;
      f.cv.notify_all();
    }
  } finish{this, fk, *f};
  int rc = fetch(tenant, kind, key, timeout_ms, en, unavailable);
  std::lock_guard<std::mutex> l(f->m);
  f->rc = rc;
  f->en = *en;
  f->unavailable = *unavailable;
  return rc;
}

extern "C" {

int gb_lookup(gb_bridge_t* b, const char* tenant, int kind, const char* key, int timeout_ms, gb_lookup_result_t* out) {
  try {
    if (!b || !tenant || !key || !out) return GB_ERR_ARG;
    std::memset(out, 0, sizeof *out);
    gb::Cache::Entry e;
    if (b->cache.get(tenant, kind, key, !b->connected, &e)) {  // offline: an expired entry beats an error
      fill(out, e, 1);
      return GB_OK;
    }
    gb::Cache::Entry en;
    bool unavailable = false;
    int rc = b->fetch_shared(tenant, kind, key, timeout_ms > 0 ? timeout_ms : 2000, &en, &unavailable);
    if (rc != GB_OK || unavailable) {
      if (b->cache.get(tenant, kind, key, true, &e)) {  // fail-static
        fill(out, e, 1);
        return GB_OK;
      }
      return rc != GB_OK ? rc : GB_ERR_UNAVAILABLE;
    }
    fill(out, en, 0);
    return GB_OK;
  } catch (...) {
    return GB_ERR_INTERNAL;
  }
}

int gb_fetch(gb_bridge_t* b, const char* tenant, int kind, const char* key, const gb_kv_t* params, size_t nparams,
             int timeout_ms, gb_lookup_result_t* out) {
  try {
    if (!b || !tenant || !key || !out || (nparams && !params)) return GB_ERR_ARG;
    std::memset(out, 0, sizeof *out);
    static const gb_kv_t none[1] = {{nullptr, nullptr}};
    gb::Cache::Entry en;
    bool unavailable = false;
    int rc = b->fetch(tenant, kind, key, timeout_ms > 0 ? timeout_ms : 2000, &en, &unavailable, params ? params : none, nparams);
    if (rc != GB_OK) return rc;
    if (unavailable) return GB_ERR_UNAVAILABLE;
    fill(out, en, 0);
    return GB_OK;
  } catch (...) {
    return GB_ERR_INTERNAL;
  }
}

void gb_lookup_result_free(gb_lookup_result_t* r) {
  if (!r) return;
  std::free(r->payload);
  r->payload = nullptr;
}

int gb_survival_get(gb_bridge_t* b, const char* tenant, gb_lookup_result_t* out) {
  try {
    if (!b || !tenant || !out) return GB_ERR_ARG;
    std::memset(out, 0, sizeof *out);
    gb::Cache::Entry e;
    if (b->survival_max_age_s <= 0 ||
        !b->cache.get_within(tenant, GB_KIND_CONFIG, GB_SURVIVAL_KEY, std::chrono::seconds(b->survival_max_age_s), &e) ||
        !e.found)
      return GB_ERR_UNAVAILABLE;
    fill(out, e, 1);
    return GB_OK;
  } catch (...) {
    return GB_ERR_INTERNAL;
  }
}

void gb_cache_stats(gb_bridge_t* b, gb_cache_stats_t* out) {
  if (!b || !out) return;
  gb::Cache::Stats s = b->cache.stats();
  out->entries = s.entries;
  out->hits = s.hits;
  out->misses = s.misses;
  out->stale_served = s.stale_served;
  out->invalidated = s.invalidated;
}

void gb_cache_flush(gb_bridge_t* b) {
  if (b) b->cache.clear();
}

}  // extern "C"
