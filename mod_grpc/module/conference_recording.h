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

/* mod_grpc/module/conference_recording.h
 * Pure conference-recording lifecycle. No FreeSWITCH types: the module applies
 * the returned effects outside its list mutex. One job stays reserved until
 * it is done, which is after upload. */
#pragma once
#include <stdint.h>
#include <string.h>

enum cr_state { CR_NEW, CR_RECORDING, CR_STOPPING, CR_UPLOADING, CR_DONE };
enum cr_event { CR_START_OK, CR_START_FAILED, CR_STOP_REQUEST, CR_CAP,
	CR_SHUTDOWN, CR_FILE_CLOSED, CR_UPLOAD_OK, CR_UPLOAD_FAILED };
enum { CR_STOP = 1, CR_UPLOAD = 2, CR_REPORT_OK = 4, CR_REPORT_FAILED = 8 };

static inline int cr_transition(enum cr_state *s, enum cr_event e)
{
	switch (*s) {
	case CR_NEW:
		if (e == CR_START_OK) { *s = CR_RECORDING; return 0; }
		if (e == CR_START_FAILED) { *s = CR_DONE; return CR_REPORT_FAILED; }
		return 0;
	case CR_RECORDING:
		if (e == CR_FILE_CLOSED) { *s = CR_UPLOADING; return CR_UPLOAD; }
		if (e == CR_STOP_REQUEST || e == CR_CAP || e == CR_SHUTDOWN) {
			*s = CR_STOPPING; return CR_STOP;
		}
		return 0;
	case CR_STOPPING:
		if (e == CR_FILE_CLOSED) { *s = CR_UPLOADING; return CR_UPLOAD; }
		return 0;
	case CR_UPLOADING:
		if (e == CR_UPLOAD_OK) { *s = CR_DONE; return CR_REPORT_OK; }
		if (e == CR_UPLOAD_FAILED) { *s = CR_DONE; return CR_REPORT_FAILED; }
		return 0;
	case CR_DONE: return 0;
	}
	return 0;
}

/* Admitted jobs and the completion queue share this bound so a completion is never dropped. */
enum { CR_MAX_JOBS = 100, CR_PATH_MAX = 1024, CR_URL_MAX = 2049 };
#define CR_STOP_GIVEUP_MS 60000 /* stop-recording never came (the conference died): stop waiting, upload what is on disk */

typedef struct {
	int used;
	int abandoned; /* shutdown already reported; the command thread must not report again */
	enum cr_state state;
	char tenant[40];
	char room[68];  /* short room sent to the engine */
	char conf[128]; /* <tenant>_<room>, the FreeSWITCH conference name */
	char rec_id[40];
	char path[CR_PATH_MAX];
	char url[CR_URL_MAX];
	int64_t deadline_ms; /* monotonic milliseconds */
	int64_t length_ms;
	int started, closed, uploading, done;
	int pending_close; /* FILE_CLOSED arrived while the job was still CR_NEW */
	int64_t pending_ms;
	int stop_early; /* RECORD_STOP arrived while the job was still CR_NEW */
	int stop_needed;
	int64_t stop_after_ms;
	int64_t stopping_since_ms; /* when the job entered CR_STOPPING; past CR_STOP_GIVEUP_MS it is closed without the event */
	int queued;
} cr_job_t;

/* Bytes of preamble scanned for the data fourcc. This build puts data at
 * offset 204 after a LIST chunk. ponytail: a longer preamble is not sealed;
 * raise the bound if a larger chunk lands in front of data. */
enum { CR_WAV_SCAN = 512 };

/* Offset of the data fourcc. Chunks before it, including LIST, are skipped.
 * A canonical header whose fmt length was left 0 still has data at offset 36. */
static inline int cr_wav_data_off(const unsigned char *h, size_t n, size_t *off)
{
	size_t at = 12;
	if (!h || !off || n < 44 || memcmp(h, "RIFF", 4) || memcmp(h + 8, "WAVE", 4)) return 0;
	while (at + 8 <= n) {
		uint32_t sz = 0;
		if (!memcmp(h + at, "data", 4)) { *off = at; return 1; }
		memcpy(&sz, h + at + 4, 4);
		if ((size_t) sz > n - at - 8) break;
		at += 8 + (size_t) sz;
		if (sz & 1) {
			if (at >= n) break;
			at++;
		}
	}
	if (!memcmp(h + 36, "data", 4)) { *off = 36; return 1; }
	return 0;
}

/* Duration of a PCM WAV. A zero data-chunk size is the still-open shape and
 * is not audio. Host endianness matches the WAV file (little-endian); this
 * module runs on that host. */
static inline int64_t cr_wav_header_ms(const unsigned char *h, size_t n)
{
	size_t data_off = 0;
	uint32_t rate = 0, bytes = 0;
	uint16_t ch = 0, bits = 0;
	if (!cr_wav_data_off(h, n, &data_off) || data_off < 36 || data_off > n - 8) return 0;
	memcpy(&ch, h + 22, 2);
	memcpy(&rate, h + 24, 4);
	memcpy(&bits, h + 34, 2);
	memcpy(&bytes, h + data_off + 4, 4);
	if (!bytes || !rate || !ch || bits < 8 || bits % 8) return 0;
	return (int64_t) bytes * 1000 / ((int64_t) rate * ch * (bits / 8));
}

/* Write a truthful data-chunk size when the writer has finished and left the
 * size at 0. PCM starts after that fourcc, not at byte 44 when LIST precedes
 * it. writer_open refuses: the file is still being written. Returns 1 only
 * when the header describes a non-empty data chunk. */
static inline int cr_wav_seal(unsigned char *h, size_t n, uint32_t file_size, int writer_open)
{
	size_t data_off = 0;
	uint32_t data = 0, riff, pcm_off;
	if (writer_open || !cr_wav_data_off(h, n, &data_off) || data_off < 36 || data_off > n - 8) return 0;
	if (data_off > file_size || file_size - (uint32_t) data_off <= 8) return 0;
	pcm_off = (uint32_t) data_off + 8;
	memcpy(&data, h + data_off + 4, 4);
	if (!data) {
		data = file_size - pcm_off;
		riff = file_size - 8;
		memcpy(h + 4, &riff, 4);
		memcpy(h + data_off + 4, &data, 4);
	}
	return cr_wav_header_ms(h, n) > 0;
}
