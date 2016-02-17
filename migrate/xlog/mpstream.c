/*
 * Copyright 2010-2015, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "mpstream.h"

#include <stdio.h>

/**
 * A streaming API so that it's possible to encode to any output
 * stream.
 */

void
mmpstream_init(struct mpstream *stream, void *ctx, mmpstream_reserve_f reserve,
	      mmpstream_alloc_f alloc, mmpstream_error_f error, void *error_ctx)
{
	memset(stream, 0, sizeof(struct mpstream));
	stream->from = false;
	stream->ctx = ctx;
	stream->reserve = reserve;
	stream->alloc = alloc;
	stream->error = error;
	stream->error_ctx = error_ctx;
	mmpstream_reset(stream);
}

void
mmpstream_init_from(struct mpstream *stream, char *pos, size_t len,
		   mmpstream_error_f error, void *error_ctx)
{
	memset(stream, 0, sizeof(struct mpstream));
	stream->from = true;
	stream->buf = pos;
	stream->pos = pos + len;
	stream->end = pos + len;
	stream->error = error;
	stream->error_ctx = error_ctx;
}

void
mmpstream_reserve_slow(struct mpstream *stream, size_t size)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	stream->alloc(stream->ctx, stream->pos - stream->buf);
	stream->buf = (char *) stream->reserve(stream->ctx, &size);
	if (stream->buf == NULL) {
		size_t errlen = 0;
		char err[256];
		errlen = snprintf(err, 256, "Failed to allocate %zd bytes in "
				            "'reserve' for 'mpstream'", size);
		stream->error(stream->error_ctx, err, errlen);
	}
	stream->pos = stream->buf;
	stream->end = stream->pos + size;
}

void
mmpstream_reset(struct mpstream *stream)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	size_t size = 0;
	stream->buf = (char *) stream->reserve(stream->ctx, &size);
	if (stream->buf == NULL) {
		size_t errlen = 0;
		char err[256];
		errlen = snprintf(err, 256, "Failed to allocate %zd bytes in "
				            "'reset' for 'mpstream'", size);
		stream->error(stream->error_ctx, err, errlen);
	}
	stream->pos = stream->buf;
	stream->end = stream->pos + size;
}

char *
mmpstream_encode_array(struct mpstream *stream, uint32_t size)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_array(size) <= 5);
	char *data = mmpstream_reserve(stream, 5);
	if (data) {
		char *pos = mp_encode_array(data, size);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_map(struct mpstream *stream, uint32_t size)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_map(size) <= 5);
	char *data = mmpstream_reserve(stream, 5);
	if (data) {
		char *pos = mp_encode_map(data, size);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_uint(struct mpstream *stream, uint64_t num)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_uint(num) <= 9);
	char *data = mmpstream_reserve(stream, 9);
	if (data) {
		char *pos = mp_encode_uint(data, num);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_int(struct mpstream *stream, int64_t num)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_int(num) <= 9);
	char *data = mmpstream_reserve(stream, 9);
	if (data) {
		char *pos = mp_encode_int(data, num);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_float(struct mpstream *stream, float num)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_float(num) <= 5);
	char *data = mmpstream_reserve(stream, 5);
	if (data) {
		char *pos = mp_encode_float(data, num);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_double(struct mpstream *stream, double num)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_double(num) <= 9);
	char *data = mmpstream_reserve(stream, 9);
	if (data) {
		char *pos = mp_encode_double(data, num);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_str(struct mpstream *stream, const char *str, uint32_t len)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_str(len) <= 5 + len);
	char *data = mmpstream_reserve(stream, 5 + len);
	if (data) {
		char *pos = mp_encode_str(data, str, len);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_bin(struct mpstream *stream, const char *bin, uint32_t len)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_bin(len) <= 5 + len);
	char *data = mmpstream_reserve(stream, 5 + len);
	if (data) {
		char *pos = mp_encode_bin(data, bin, len);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_nil(struct mpstream *stream)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_nil() <= 1);
	char *data = mmpstream_reserve(stream, 1);
	if (data) {
		char *pos = mp_encode_nil(data);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_bool(struct mpstream *stream, bool val)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_bool(val) <= 1);
	char *data = mmpstream_reserve(stream, 1);
	if (data) {
		char *pos = mp_encode_bool(data, val);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

char *
mmpstream_encode_ext(struct mpstream *stream, const char *ext, uint32_t len,
		    uint8_t type)
{
	if (stream->from && stream->error) {
		const char *err = "Immutable 'mpstream' buffer";
		stream->error(stream->error_ctx, err, strlen(err));
	}
	assert(mp_sizeof_ext(len) < 5 + 1 + len);
	char *data = mmpstream_reserve(stream, 1);
	if (data) {
		char *pos = mp_encode_ext(data, ext, len, type);
		mmpstream_advance(stream, pos - data);
	}
	return data;
}

void
mmpstream_iter_init(struct mpstream *stream, struct mmpstream_iter *iter)
{
	memset(iter, 0, sizeof(struct mmpstream_iter));
	iter->stream = stream;
	iter->pos = stream->buf;
}

int
mmpstream_skip(struct mmpstream_iter *iter)
{
	const char *pos = iter->pos;
	if (mp_check(&pos, iter->stream->pos) == 0) {
		iter->pos = pos;
		return 0;
	}
	return 1;
}

struct mmpstream_decode_t *
mmpstream_next(struct mmpstream_iter *iter)
{
	const char *pos = iter->pos;
	if (pos == iter->stream->pos)
		return NULL;
	struct mmpstream_decode_t *out = &(iter->obj);
	out->type = mp_typeof(*pos);
	switch (out->type) {
	case MP_UINT:
		if (mp_check_uint(pos, iter->stream->pos) > 0)
			return NULL;
		out->uval = mp_decode_uint(&pos);
		break;
	case MP_INT:
		if (mp_check_int(pos, iter->stream->pos) > 0)
			return NULL;
		out->ival = mp_decode_int(&pos);
		break;
	case MP_FLOAT:
		if (mp_check_float(pos, iter->stream->pos) > 0)
			return NULL;
		out->fval = mp_decode_float(&pos);
		break;
	case MP_DOUBLE:
		if (mp_check_double(pos, iter->stream->pos) > 0)
			return NULL;
		out->fval = mp_decode_double(&pos);
		break;
	case MP_STR:
		if (mp_check_str(pos, iter->stream->pos) > 0)
			return NULL;
		out->sval.val = mp_decode_str(&pos, &out->sval.size);
		break;
	case MP_BIN:
		if (mp_check_bin(pos, iter->stream->pos) > 0)
			return NULL;
		out->sval.val = mp_decode_bin(&pos, &out->sval.size);
		break;
	case MP_BOOL:
		if (mp_check_bool(pos, iter->stream->pos) > 0)
			return NULL;
		out->bval = mp_decode_bool(&pos);
		break;
	case MP_NIL:
		if (mp_check_nil(pos, iter->stream->pos) > 0)
			return NULL;
		break;
	case MP_ARRAY:
		out->size = mp_decode_array(&pos);
		if (mp_check_array(pos, iter->stream->pos) > 0)
			return NULL;
		break;
	case MP_MAP:
		if (mp_check_map(pos, iter->stream->pos) > 0)
			return NULL;
		out->size = mp_decode_map(&pos);
		break;
	case MP_EXT:
		if (mp_check_ext(pos, iter->stream->pos) > 0)
			return NULL;
		out->eval.val = mp_decode_ext(&pos, &out->eval.size,
					      &out->eval.type);
		break;
	}
	iter->pos = pos;
	return out;
}
