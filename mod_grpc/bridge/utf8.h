/* mod_grpc/bridge/utf8.h
 * Strict UTF-8 (no overlongs, no surrogates, nothing past U+10FFFF). Every string the node sends travels in a proto3
 * string field, and an engine that meets other bytes rejects the whole message and drops the stream; callers pick
 * many of those bytes (a Latin-1 display name or SIP header is enough). Plain C so the module can share it. */
#pragma once

static inline int gb_utf8_ok(const char *v)
{
	const unsigned char *p = (const unsigned char *) v;
	if (!p) return 0;
	while (*p) {
		unsigned c = *p;
		int n, i;
		unsigned cp;
		if (c < 0x80) { p++; continue; }
		if (c >= 0xc2 && c <= 0xdf) { n = 1; cp = c & 0x1f; }
		else if (c >= 0xe0 && c <= 0xef) { n = 2; cp = c & 0x0f; }
		else if (c >= 0xf0 && c <= 0xf4) { n = 3; cp = c & 0x07; }
		else return 0;
		for (i = 1; i <= n; i++) {
			if ((p[i] & 0xc0) != 0x80) return 0; /* also stops at the terminator */
			cp = (cp << 6) | (p[i] & 0x3f);
		}
		if ((n == 2 && cp < 0x800) || (n == 3 && cp < 0x10000) || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) return 0;
		p += n + 1;
	}
	return 1;
}
