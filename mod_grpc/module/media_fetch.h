/* mod_grpc/module/media_fetch.h
 * URL rules for remote media (Playback.url, Collect.prompt_url). Plain C with no FreeSWITCH dependency so the unit
 * tests can include it. */
#pragma once
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include "step_guard.h"

/* Splits url into host (lowercased, port stripped) and the path up to '?' or '#'. 0 on a shape we refuse. */
static inline int mf_split(const char *url, char *host, size_t hostlen, const char **path, size_t *pathlen)
{
	const char *p, *e;
	size_t n, i;
	if (!sg_safe_url(url)) return 0;
	p = url + (url[4] == 's' ? 8 : 7);
	e = p + strcspn(p, "/?#");
	if (*e != '/') return 0; /* an object path is required */
	n = (size_t) (e - p);
	if (memchr(p, '@', n) || memchr(p, '[', n)) return 0; /* userinfo; IPv6 literals are not supported */
	{
		const char *c = (const char *) memchr(p, ':', n);
		if (c) n = (size_t) (c - p);
	}
	if (n < 1 || n >= hostlen) return 0;
	for (i = 0; i < n; i++) host[i] = (p[i] >= 'A' && p[i] <= 'Z') ? (char) (p[i] + 32) : p[i];
	host[n] = 0;
	*path = e;
	*pathlen = strcspn(e, "?#");
	return 1;
}

static inline const char *mf_ext(const char *path, size_t n)
{
	static const char *const exts[] = { ".wav", ".mp3", ".ogg" };
	int i;
	for (i = 0; i < 3; i++)
		if (n >= 4 && !memcmp(path + n - 4, exts[i], 4)) return exts[i];
	return NULL;
}

/* 1 when url passes sg_safe_url, is http(s) with no userinfo ('@' before the path), its host (exact, case-insensitive,
 * port stripped) is in the comma-separated hosts list, and the path (before '?' or '#') ends in .wav/.mp3/.ogg. */
static inline int mf_url_ok(const char *url, const char *hosts)
{
	char host[256];
	const char *path, *h;
	size_t pn, hn;
	if (!hosts || !*hosts || !mf_split(url, host, sizeof host, &path, &pn) || !mf_ext(path, pn)) return 0;
	hn = strlen(host);
	for (h = hosts; *h;) {
		const char *e = h + strcspn(h, ",");
		const char *s = h, *t = e;
		while (s < t && *s == ' ') s++;
		while (t > s && t[-1] == ' ') t--;
		if ((size_t) (t - s) == hn && !strncasecmp(s, host, hn)) return 1;
		h = *e ? e + 1 : e;
	}
	return 0;
}

/* "<16 hex of FNV-1a 64 of host+path>.<ext>" for an mf_url_ok url; 0 when it does not fit. */
static inline int mf_cache_name(const char *url, char *out, size_t outlen)
{
	char host[256];
	const char *path, *ext;
	size_t pn, i;
	uint64_t h = 0xcbf29ce484222325ULL;
	if (!mf_split(url, host, sizeof host, &path, &pn) || !(ext = mf_ext(path, pn))) return 0;
	for (i = 0; host[i]; i++) h = (h ^ (unsigned char) host[i]) * 0x100000001b3ULL;
	for (i = 0; i < pn; i++) h = (h ^ (unsigned char) path[i]) * 0x100000001b3ULL;
	return snprintf(out, outlen, "%016llx%s", (unsigned long long) h, ext) < (int) outlen;
}
