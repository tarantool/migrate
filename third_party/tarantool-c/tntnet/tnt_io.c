
/*
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

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <math.h>

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>

#include <tarantool/tnt.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_io.h>

#if !defined(IOV_MAX)
#	define IOV_MAX 16
#endif

#if !defined(MIN)
#	define MIN(a, b) (a) < (b) ? (a) : (b)
#endif /* !defined(MIN) */

const int64_t micro = 1000000;

static double
tv_to_double(struct timeval *tv) {
	double converted = (double )tv->tv_sec + (double )tv->tv_usec / micro;
	return converted;
}

static void
update_timeout(struct timeval *start, double *timeout) {
	(void)start;
	struct timeval now = {0, 0};
	assert(gettimeofday(&now, NULL) == 0);
	*timeout -= tv_to_double(&now);
}

/**
 * Blocking by default
 * Slow, because 'select' is used
 **/
enum tnt_error
iowait_cb_default(int fd, int *event, double tm)
{
	int64_t passd_usec, curr_timeout;
	int64_t timeout_usec = floor(tm) * micro;
	struct timeval start, timeout;
	if (gettimeofday(&start, NULL) == -1)
		return TNT_ESYSTEM;
	timeout.tv_sec  = floor(tm);
	timeout.tv_usec = (tm - floor(tm)) * micro;
	while (1) {
		fd_set fd_read, fd_write;
		if (*event & IO_READ) {
			FD_ZERO(&fd_read);
			FD_SET(fd, &fd_read);
		}
		if (*event & IO_WRITE) {
			FD_ZERO(&fd_write);
			FD_SET(fd, &fd_write);
		}
		int ret = select(fd + 1, &fd_read, &fd_write, NULL, &timeout);
		if (ret == -1) {
			/* check for errno */
			if (errno == EINTR || errno == EAGAIN) {
				struct timeval curr;
				if (gettimeofday(&curr, NULL) == -1)
					return TNT_ESYSTEM;
				passd_usec = (curr.tv_sec - start.tv_sec) * micro +
					     (curr.tv_usec - start.tv_usec);
				curr_timeout = passd_usec - timeout_usec;
				if (curr_timeout <= 0)
					return TNT_ETMOUT;
				timeout.tv_sec  = curr_timeout / micro;
				timeout.tv_usec = curr_timeout % micro;
				continue;
			} else {
				*event = 0;
				return TNT_ESYSTEM;
			}
		} else if (ret == 0) {
			/* timeout */
			*event = 0;
			return TNT_ETMOUT;
		}
		*event = 0;
		*event |= FD_ISSET(fd, &fd_read) ? IO_READ : 0;
		*event |= FD_ISSET(fd, &fd_write) ? IO_WRITE : 0;
		break;
	}

	return TNT_EOK;
}

enum tnt_error
gaiwait_cb_default(const char *host, const char *port,
		   const struct addrinfo *hints,
		   struct addrinfo **res, double tm)
{
	/* Given timeout is ignored */
	(void )tm;
	if (getaddrinfo(host, port, hints, res) == -1) {
		freeaddrinfo(*res);
		return TNT_ERESOLVE;
	}
	return TNT_EOK;
}

static enum tnt_error
tnt_io_resolve(struct tnt_stream_net *s, struct sockaddr_in *addr,
	       const char *hostname, unsigned short port)
{
	memset(addr, 0, sizeof(struct sockaddr_in));
	addr->sin_family = AF_INET;
	addr->sin_port = htons(port);
	struct addrinfo *addr_info = NULL;
	assert(s->gaiwait != NULL);
	/* Construct helpers */
	struct addrinfo hints;
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC; /* Allow IPv4 or IPv6 */
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_ADDRCONFIG|AI_NUMERICSERV|AI_PASSIVE;
	hints.ai_protocol = 0;
	/* Call getaddrinfo */
	enum tnt_error rv = s->gaiwait(hostname, NULL, &hints, &addr_info,
				       tv_to_double(&s->opt.tmout_connect));
	if (rv != TNT_EOK) {
		return rv;
	}
	assert(addr_info != NULL);
	memcpy(&addr->sin_addr,
	       (void*)&((struct sockaddr_in *)addr_info->ai_addr)->sin_addr,
	       sizeof(addr->sin_addr));
	freeaddrinfo(addr_info);
	return TNT_EOK;
}

enum tnt_error tnt_io_nonblock(struct tnt_stream_net *s, int set) {
	int flags = fcntl(s->fd, F_GETFL);
	if (flags == -1) {
		s->errno_ = errno;
		return TNT_ESYSTEM;
	}
	if (set)
		flags |= O_NONBLOCK;
	else
		flags &= ~O_NONBLOCK;
	if (fcntl(s->fd, F_SETFL, flags) == -1) {
		s->errno_ = errno;
		return TNT_ESYSTEM;
	}
	return TNT_EOK;
}

static enum tnt_error
tnt_io_connect_do(struct tnt_stream_net *s, const char *host, int port)
{
	/* resolving address */
	struct sockaddr_in addr;
	enum tnt_error result = tnt_io_resolve(s, &addr, host, port);
	if (result != TNT_EOK) {
		s->errno_ = errno;
		return result;
	}

	/* setting nonblock */
	result = tnt_io_nonblock(s, 1);
	if (result != TNT_EOK)
		return result;

	int rv = connect(s->fd, (struct sockaddr*)&addr, sizeof(addr));
	if (rv == -1) {
		switch (errno) {
		case EAGAIN:
		case EINTR:
		case EINPROGRESS:
			break;
		default:
			result = TNT_ESYSTEM;
			goto error;
		}
	}
	int events = IO_WRITE;
	assert(s->iowait != NULL);
	result = s->iowait(s->fd, &events, tv_to_double(&s->opt.tmout_connect));

	if (result != TNT_EOK)
		goto error;

	/* checking error status */
	result = TNT_ESYSTEM;
	int opt = 0;
	socklen_t len = sizeof(opt);
	if ((getsockopt(s->fd, SOL_SOCKET, SO_ERROR, &opt, &len) == -1) || opt) {
		errno = (opt) ? opt : errno;
		goto error;
	}
	/* we're in blocking mode */
	if (s->iowait == iowait_cb_default) {
		/* setting block */
		result = tnt_io_nonblock(s, 0);
		if (result != TNT_EOK)
			return result;
		return TNT_EOK;
	}
	return TNT_EOK;
error:
	s->errno_ = errno;
	return result;
}

static enum tnt_error tnt_io_xbufmax(struct tnt_stream_net *s, int opt, int min) {
	int max = 128 * 1024 * 1024;
	if (min == 0)
		min = 16384;
	unsigned int avg = 0;
	while (min <= max) {
		avg = ((unsigned int)(min + max)) / 2;
		if (setsockopt(s->fd, SOL_SOCKET, opt, &avg, sizeof(avg)) == 0)
			min = avg + 1;
		else
			max = avg - 1;
	}
	return TNT_EOK;
}

static enum tnt_error tnt_io_setopts(struct tnt_stream_net *s) {
	int opt = 1;
	if (setsockopt(s->fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) == -1)
		goto error;

	tnt_io_xbufmax(s, SO_SNDBUF, s->opt.send_buf);
	tnt_io_xbufmax(s, SO_RCVBUF, s->opt.recv_buf);

	if (setsockopt(s->fd, SOL_SOCKET, SO_SNDTIMEO,
		       &s->opt.tmout_send, sizeof(s->opt.tmout_send)) == -1)
		goto error;
	if (setsockopt(s->fd, SOL_SOCKET, SO_RCVTIMEO,
		       &s->opt.tmout_recv, sizeof(s->opt.tmout_recv)) == -1)
		goto error;
	return TNT_EOK;
error:
	s->errno_ = errno;
	return TNT_ESYSTEM;
}

enum tnt_error
tnt_io_connect(struct tnt_stream_net *s, const char *host, int port)
{
	s->fd = socket(AF_INET, SOCK_STREAM, 0);
	if (s->fd < 0) {
		s->errno_ = errno;
		return TNT_ESYSTEM;
	}
	enum tnt_error result = tnt_io_setopts(s);
	if (result != TNT_EOK)
		goto out;
	result = tnt_io_connect_do(s, host, port);
	if (result != TNT_EOK)
		goto out;
	s->connected = 1;
	return TNT_EOK;
out:
	tnt_io_close(s);
	return result;
}

void tnt_io_close(struct tnt_stream_net *s)
{
	if (s->fd > 0) {
		close(s->fd);
		s->fd = -1;
	}
	s->connected = 0;
}

ssize_t tnt_io_flush(struct tnt_stream_net *s) {
	if (s->sbuf.off == 0)
		return 0;
	ssize_t rc = tnt_io_send_raw(s, s->sbuf.buf, s->sbuf.off, 1);
	if (rc == -1)
		return -1;
	s->sbuf.off = 0;
	return rc;
}

static ssize_t
tnt_io_writecb(struct tnt_stream_net *s, const char *buf, size_t size, int all)
{
	ssize_t written = 0;
	enum tnt_error err = TNT_EOK;
	struct timeval start = {0, 0};
	double tm = tv_to_double(&s->opt.tmout_send);
	if (tm == 0) tm = LONG_MAX;

	while (size > 0) {
		assert(gettimeofday(&start, NULL) != -1);
		ssize_t rv = write(s->fd, buf, size);
		err = TNT_ESYSTEM;
		if (rv == 0) {
			s->connected = 0;
			s->error = TNT_ECLOSED;
			return -1;
		} else if (rv > 0) {
			written += rv;
			goto next;
		}
		if (errno == EWOULDBLOCK)
			errno = EAGAIN;
		switch (errno) {
		case EINTR:
		case EAGAIN: {
			int events = IO_WRITE;
			assert(s->iowait != NULL);
			err = s->iowait(s->fd, &events, tm);
			if (err == TNT_EOK)
				break;
			/* FALLTHROUGH */
		}
		default:
			s->errno_ = errno;
			s->error  = err;
			return -1;
		};
next:
		if (rv > 0 && size > 0) {
			if (!all) return rv;
			buf  += rv;
			size -= rv;
		}
		/* affects only nonblocking connections */
		update_timeout(&start, &tm);
	}
	return written;
}

ssize_t
tnt_io_send_raw(struct tnt_stream_net *s, const char *buf, size_t size, int all)
{
	size_t off = 0;
	do {
		ssize_t r;
		if (s->sbuf.tx) {
			r = s->sbuf.tx(s->sbuf.buf, buf + off, size - off);
		} else {
			r = tnt_io_writecb(s, buf+off, size-off, 0);
		}
		if (r <= 0) {
			s->error = TNT_ESYSTEM;
			s->errno_ = errno;
			return -1;
		}
		off += r;
	} while (off != size && all);
	return off;
}

static ssize_t
tnt_io_writevcb(struct tnt_stream_net *s, struct iovec *iov, int count, int all)
{
	ssize_t written = 0;
	enum tnt_error err = TNT_EOK;
	struct timeval start = {0, 0};
	double tm = tv_to_double(&s->opt.tmout_send);
	if (tm == 0) tm = LONG_MAX;

	while (count > 0) {
		assert(gettimeofday(&start, NULL) != -1);
		ssize_t rv = writev(s->fd, iov, count);
		err = TNT_ESYSTEM;
		if (rv == 0) {
			s->connected = 0;
			s->error = TNT_ECLOSED;
			return -1;
		} else if (rv > 0) {
			written += rv;
			goto next;
		}
		if (errno == EWOULDBLOCK)
			errno = EAGAIN;
		switch (errno) {
		case EINTR:
		case EAGAIN: {
			int events = IO_WRITE;
			assert(s->iowait != NULL);
			err = s->iowait(s->fd, &events, tm);
			if (err == TNT_EOK)
				break;
			/* FALLTHROUGH */
		}
		default:
			s->errno_ = errno;
			s->error  = err;
			return -1;
		};
next:

		while (count > 0 && rv > 0) {
			if (!all) return rv;
			if (iov->iov_len > (size_t )rv) {
				iov->iov_base += rv;
				iov->iov_len -= rv;
				break;
			} else {
				rv -= iov->iov_len;
				iov++;
				count--;
			}
		}
		/* affects only nonblocking connections */
		update_timeout(&start, &tm);
	}
	return written;
}

ssize_t
tnt_io_sendv_raw(struct tnt_stream_net *s, struct iovec *iov, int count, int all)
{
	size_t total = 0;
	while (count > 0) {
		ssize_t r;
		if (s->sbuf.txv) {
			r = s->sbuf.txv(s->sbuf.buf, iov, MIN(count, IOV_MAX));
		} else {
			r = tnt_io_writevcb(s, iov, count, all);
		}
		if (r <= 0) {
			s->error = TNT_ESYSTEM;
			s->errno_ = errno;
			return -1;
		}
		total += r;
		if (!all)
			break;
		while (count > 0) {
			if (iov->iov_len > (size_t)r) {
				iov->iov_base += r;
				iov->iov_len -= r;
				break;
			} else {
				r -= iov->iov_len;
				iov++;
				count--;
			}
		}
	}
	return total;
}

ssize_t
tnt_io_send(struct tnt_stream_net *s, const char *buf, size_t size)
{
	if (s->sbuf.buf == NULL)
		return tnt_io_send_raw(s, buf, size, 1);
	if (size > s->sbuf.size) {
		s->error = TNT_EBIG;
		return -1;
	}
	if ((s->sbuf.off + size) <= s->sbuf.size) {
		memcpy(s->sbuf.buf + s->sbuf.off, buf, size);
		s->sbuf.off += size;
		return size;
	}
	ssize_t r = tnt_io_send_raw(s, s->sbuf.buf, s->sbuf.off, 1);
	if (r == -1)
		return -1;
	s->sbuf.off = size;
	memcpy(s->sbuf.buf, buf, size);
	return size;
}

inline static void
tnt_io_sendv_put(struct tnt_stream_net *s, struct iovec *iov, int count) {
	int i;
	for (i = 0 ; i < count ; i++) {
		memcpy(s->sbuf.buf + s->sbuf.off,
		       iov[i].iov_base,
		       iov[i].iov_len);
		s->sbuf.off += iov[i].iov_len;
	}
}

ssize_t
tnt_io_sendv(struct tnt_stream_net *s, struct iovec *iov, int count)
{
	if (s->sbuf.buf == NULL)
		return tnt_io_sendv_raw(s, iov, count, 1);
	size_t size = 0;
	int i;
	for (i = 0 ; i < count ; i++)
		size += iov[i].iov_len;
	if (size > s->sbuf.size) {
		s->error = TNT_EBIG;
		return -1;
	}
	if ((s->sbuf.off + size) <= s->sbuf.size) {
		tnt_io_sendv_put(s, iov, count);
		return size;
	}
	ssize_t r = tnt_io_send_raw(s, s->sbuf.buf, s->sbuf.off, 1);
	if (r == -1)
		return -1;
	s->sbuf.off = 0;
	tnt_io_sendv_put(s, iov, count);
	return size;
}

static ssize_t
tnt_io_readcb(struct tnt_stream_net *s, char *buf, size_t size, int all)
{
	ssize_t bytes_read = 0;
	enum tnt_error err = TNT_EOK;
	struct timeval start = {0, 0};
	double tm = tv_to_double(&s->opt.tmout_recv);
	if (tm == 0) tm = LONG_MAX;

	while (size > 0) {
		assert(gettimeofday(&start, NULL) != -1);
		ssize_t rv = read(s->fd, buf, size);
		err = TNT_ESYSTEM;
		if (rv == 0) {
			s->connected = 0;
			s->error = TNT_ECLOSED;
			return -1;
		} else if (rv > 0) {
			bytes_read += rv;
			goto next;
		}
		if (errno == EWOULDBLOCK)
			errno = EAGAIN;
		switch (errno) {
		case EINTR:
		case EAGAIN: {
			int events = IO_READ;
			assert(s->iowait != NULL);
			err = s->iowait(s->fd, &events, tm);
			if (err == TNT_EOK)
				break;
			/* FALLTHROUGH */
		}
		default:
			s->errno_ = errno;
			s->error  = err;
			return -1;
		};
next:
		if (rv > 0 && size > 0) {
			if (!all) return rv;
			buf  += rv;
			size -= rv;
		}
		/* affects only nonblocking connections */
		update_timeout(&start, &tm);
	}
	return bytes_read;
}

ssize_t
tnt_io_recv_raw(struct tnt_stream_net *s, char *buf, size_t size, int all)
{
	size_t off = 0;
	do {
		ssize_t r;
		if (s->rbuf.tx) {
			r = s->rbuf.tx(s->rbuf.buf, buf + off, size - off);
		} else {
			r = tnt_io_readcb(s, buf + off, size - off, 0);
		}
		if (r <= 0) {
			s->error = TNT_ESYSTEM;
			s->errno_ = errno;
			return -1;
		}
		off += r;
	} while (off != size && all);
	return off;
}

ssize_t
tnt_io_recv(struct tnt_stream_net *s, char *buf, size_t size)
{
	if (s->rbuf.buf == NULL)
		return tnt_io_recv_raw(s, buf, size, 1);
	size_t lv, rv, off = 0, left = size;
	while (1) {
		if ((s->rbuf.off + left) <= s->rbuf.top) {
			memcpy(buf + off, s->rbuf.buf + s->rbuf.off, left);
			s->rbuf.off += left;
			return size;
		}

		lv = s->rbuf.top - s->rbuf.off;
		rv = left - lv;
		if (lv) {
			memcpy(buf + off, s->rbuf.buf + s->rbuf.off, lv);
			off += lv;
		}

		s->rbuf.off = 0;
		ssize_t top = tnt_io_recv_raw(s, s->rbuf.buf, s->rbuf.size, 0);
		if (top <= 0) {
			s->errno_ = errno;
			s->error = TNT_ESYSTEM;
			return -1;
		}

		s->rbuf.top = top;
		if (rv <= s->rbuf.top) {
			memcpy(buf + off, s->rbuf.buf, rv);
			s->rbuf.off = rv;
			return size;
		}
		left -= lv;
	}
	return -1;
}
