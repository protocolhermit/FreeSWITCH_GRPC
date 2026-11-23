// In-process lookup cache: TTL, per-entry version, LRU cap. Header-only so the
// unit test needs no gRPC. Thread-safe.
#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

namespace gb {

class Cache {
 public:
  using Clock = std::chrono::steady_clock;
  using Now = std::function<Clock::time_point()>;

  struct Entry {
    bool found = false;
    int64_t version = 0;
    std::string payload;
    int32_t ttl_s = 0;
    bool stale = false;  // filled by get() when served past expiry
  };
  struct Stats {
    size_t entries = 0;
    uint64_t hits = 0, misses = 0, stale_served = 0, invalidated = 0;
  };

  explicit Cache(size_t max_entries, Now now = Clock::now) : max_(max_entries ? max_entries : 1), now_(std::move(now)) {}

  // ttl_s <= 0 means "do not cache". pin: kept out of the LRU (never evicted, not counted against max_entries), for
  // the survival snapshots, which must outlive any amount of other traffic. The caller bounds how many it pins.
  void put(const std::string& tenant, int kind, const std::string& key, bool found, int64_t version, int32_t ttl_s,
           std::string payload, bool pin = false) {
    if (ttl_s <= 0) return;
    std::lock_guard<std::mutex> l(mu_);
    std::string id = make_id(tenant, kind, key);
    // version 0 = unversioned, exempt; otherwise refuse answers older than a seen Invalidate (wildcard or this key)
    if (version != 0 && (version < floor_of(tenant, kind) || version < key_floor_of(id))) return;
    auto it = index_.find(id);
    if (it != index_.end()) {
      list_of(*it->second).erase(it->second);
      index_.erase(it);
    }
    auto t = now_();
    auto& to = pin ? pinned_ : lru_;
    to.push_front(Node{tenant, kind, key, id, Entry{found, version, std::move(payload), ttl_s, false},
                       t + std::chrono::seconds(ttl_s), t, pin});
    index_[id] = to.begin();
    while (lru_.size() > max_) {
      index_.erase(lru_.back().id);
      lru_.pop_back();
    }
  }

  // A fresh entry is a hit. An expired entry is a hit only when allow_stale;
  // otherwise the call is a miss and the entry stays until it is replaced,
  // invalidated or evicted by the LRU cap.
  bool get(const std::string& tenant, int kind, const std::string& key, bool allow_stale, Entry* out) {
    std::lock_guard<std::mutex> l(mu_);
    auto it = index_.find(make_id(tenant, kind, key));
    if (it == index_.end()) {
      ++st_.misses;
      return false;
    }
    Node& n = *it->second;
    bool expired = now_() >= n.expires;
    if (expired && !allow_stale) {  // kept: a later failed refresh may still serve it stale
      ++st_.misses;
      return false;
    }
    list_of(n).splice(list_of(n).begin(), list_of(n), it->second);
    *out = n.e;
    out->stale = expired;
    expired ? ++st_.stale_served : ++st_.hits;
    return true;
  }

  // Survival read: the TTL is ignored, the age is not. An entry put no more than max_age ago is served (stale is
  // set when it is past its TTL). Counters and LRU order are left alone.
  bool get_within(const std::string& tenant, int kind, const std::string& key, std::chrono::seconds max_age, Entry* out) {
    std::lock_guard<std::mutex> l(mu_);
    auto it = index_.find(make_id(tenant, kind, key));
    if (it == index_.end()) return false;
    const Node& n = *it->second;
    auto t = now_();
    if (t - n.stored > max_age) return false;
    *out = n.e;
    out->stale = t >= n.expires;
    return true;
  }

  // Drops entries of `tenant` older than `version`. kind 0 matches every kind;
  // key "*" matches every key. Returns how many were dropped.
  size_t invalidate(const std::string& tenant, int kind, const std::string& key, int64_t version) {
    std::lock_guard<std::mutex> l(mu_);
    if (key == "*") {
      int64_t& f = floors_[make_id(tenant, kind, "")];
      if (version > f) f = version;
    } else if (kind != 0) {
      set_key_floor(make_id(tenant, kind, key), version);
    }
    size_t dropped = 0;
    for (auto* l : {&lru_, &pinned_})
      for (auto it = l->begin(); it != l->end();) {
        bool match = it->tenant == tenant && (kind == 0 || it->kind == kind) && (key == "*" || it->key == key) &&
                     it->e.version < version;
        if (match) {
          index_.erase(it->id);
          it = l->erase(it);
          ++dropped;
        } else {
          ++it;
        }
      }
    st_.invalidated += dropped;
    return dropped;
  }

  // Drops one entry, setting no floor: a later answer for the key, of any version, is cached normally.
  void erase(const std::string& tenant, int kind, const std::string& key) {
    std::lock_guard<std::mutex> l(mu_);
    auto it = index_.find(make_id(tenant, kind, key));
    if (it == index_.end()) return;
    list_of(*it->second).erase(it->second);
    index_.erase(it);
  }

  // Marks every entry expired without deleting it: fail-static can still serve
  // it stale, the next normal lookup refetches. Version floors are untouched.
  void expire_all() {
    std::lock_guard<std::mutex> l(mu_);
    auto t = now_();
    for (auto* l : {&lru_, &pinned_})
      for (auto& n : *l) n.expires = t;
  }

  void clear() {
    std::lock_guard<std::mutex> l(mu_);
    lru_.clear();
    pinned_.clear();
    index_.clear();
  }

  Stats stats() {
    std::lock_guard<std::mutex> l(mu_);
    Stats s = st_;
    s.entries = lru_.size() + pinned_.size();
    return s;
  }

 private:
  struct Node {
    std::string tenant;
    int kind;
    std::string key;
    std::string id;
    Entry e;
    Clock::time_point expires;
    Clock::time_point stored;
    bool pinned;
  };
  std::list<Node>& list_of(const Node& n) { return n.pinned ? pinned_ : lru_; }
  // An exact-key Invalidate: an answer already in flight when it arrived carries the older version and must not
  // re-cache the stale value. Kept kKeyFloorS, far longer than any lookup can be in flight, then forgotten, so a
  // stream of Invalidates for distinct keys does not grow this without bound.
  static constexpr std::chrono::seconds kKeyFloorS{60};
  void set_key_floor(const std::string& id, int64_t version) {
    auto t = now_();
    while (!key_floor_order_.empty() && t - key_floor_order_.front().first >= kKeyFloorS) {
      auto it = key_floors_.find(key_floor_order_.front().second);
      if (it != key_floors_.end() && it->second.second == key_floor_order_.front().first) key_floors_.erase(it);
      key_floor_order_.pop_front();
    }
    auto& f = key_floors_[id];
    if (version > f.first) f.first = version;
    f.second = t;
    key_floor_order_.emplace_back(t, id);
  }
  int64_t key_floor_of(const std::string& id) const {
    auto it = key_floors_.find(id);
    return it == key_floors_.end() || now_() - it->second.second >= kKeyFloorS ? 0 : it->second.first;
  }
  int64_t floor_of(const std::string& tenant, int kind) const {
    int64_t f = 0;
    for (int k : {kind, 0}) {
      auto it = floors_.find(make_id(tenant, k, ""));
      if (it != floors_.end() && it->second > f) f = it->second;
    }
    return f;
  }
  static std::string make_id(const std::string& tenant, int kind, const std::string& key) {
    return tenant + '\x1f' + std::to_string(kind) + '\x1f' + key;
  }

  std::mutex mu_;
  size_t max_;
  Now now_;
  std::list<Node> lru_;
  std::list<Node> pinned_;  // survival snapshots: out of the LRU
  std::unordered_map<std::string, std::list<Node>::iterator> index_;
  std::unordered_map<std::string, int64_t> floors_;  // (tenant, kind) -> highest Invalidate version; kept by clear()
  std::unordered_map<std::string, std::pair<int64_t, Clock::time_point>> key_floors_;  // id -> (version, set at)
  std::deque<std::pair<Clock::time_point, std::string>> key_floor_order_;              // oldest first, for expiry
  Stats st_;
};

}  // namespace gb
