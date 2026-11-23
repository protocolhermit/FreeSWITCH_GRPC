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

// Survival cache: the CONFIG snapshot of every tenant that routed a call on this node, kept warm while an engine is
// reachable so mod_grpc can route extension-to-extension calls from it when none is. Reads: gb_survival_get (lookup.cc).
#include <algorithm>

#include "bridge_internal.h"

namespace {
const int kFetchTimeoutMs = 3000;
const int kRetryS = 5;
const size_t kMaxTenants = 10000;  // ponytail: a tenant id the edge never stamped cannot grow this without bound
}  // namespace

void gb_bridge::survival_wake(const std::string& tenant) {
  std::lock_guard<std::mutex> l(sm);
  if (survival_refresh_s <= 0 || !stenants.count(tenant)) return;
  sdirty.insert(tenant);
  scv.notify_all();
}

bool gb_bridge::survival_pin(const std::string& tenant, int kind, const std::string& key) {
  if (survival_refresh_s <= 0 || kind != GB_KIND_CONFIG || key != GB_SURVIVAL_KEY) return false;
  std::lock_guard<std::mutex> l(sm);
  return stenants.count(tenant) > 0;  // bounded by kMaxTenants
}

// Fetches the dirty tenants as soon as there are any (first touch, Invalidate) and every tenant each survival_refresh_s.
// A failed fetch keeps the old entry; a "not found" answer drops it, which ends survival for a gone tenant.
void gb_bridge::survival_loop() {
  using Clock = std::chrono::steady_clock;
  const auto refresh = std::chrono::seconds(survival_refresh_s);
  const auto backoff = std::chrono::seconds(std::min(kRetryS, survival_refresh_s));
  std::set<std::string> retry;  // fetch failed or no engine: tried again after a short backoff, not a whole refresh
  auto next_full = Clock::now() + refresh;  // independent of the retry timer: a stuck tenant must not starve the others
  std::unique_lock<std::mutex> l(sm);
  while (!sstop) {
    auto deadline = retry.empty() ? next_full : std::min(next_full, Clock::now() + backoff);
    scv.wait_until(l, deadline, [&] { return sstop || !sdirty.empty(); });
    if (sstop) break;
    std::set<std::string> todo = retry;
    todo.insert(sdirty.begin(), sdirty.end());
    if (Clock::now() >= next_full) {
      todo.insert(stenants.begin(), stenants.end());
      next_full = Clock::now() + refresh;
    }
    sdirty.clear();
    retry.clear();
    l.unlock();
    for (const auto& t : todo) {
      gb::Cache::Entry en;
      bool unavailable = false;
      bool ok = !sstop && connected && fetch(t, GB_KIND_CONFIG, GB_SURVIVAL_KEY, kFetchTimeoutMs, &en, &unavailable) == GB_OK &&
                !unavailable;
      if (!ok) retry.insert(t);
      // a tenant that is gone: drop its snapshot (erase sets no floor, so a tenant that comes back is cached again)
      else if (!en.found) cache.erase(t, GB_KIND_CONFIG, GB_SURVIVAL_KEY);
    }
    l.lock();
  }
}

extern "C" void gb_survival_touch(gb_bridge_t* b, const char* tenant) {
  try {
    if (!b || !tenant || !*tenant || b->survival_refresh_s <= 0) return;
    std::lock_guard<std::mutex> l(b->sm);
    if (b->stenants.size() >= kMaxTenants && !b->stenants.count(tenant)) return;
    if (b->stenants.insert(tenant).second) {
      b->sdirty.insert(tenant);
      b->scv.notify_all();
    }
  } catch (...) {
  }
}
