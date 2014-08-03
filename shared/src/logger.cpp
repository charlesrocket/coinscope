#include <cassert>

#include <unistd.h>
#include <arpa/inet.h>

#include "logger.hpp"



using namespace std;

log_buffer::log_buffer(int fd) : write_queue(), fd(fd), io() { 
	io.set<log_buffer, &log_buffer::io_cb>(this);
	io.set(fd, ev::WRITE);
	io.start();
}
void log_buffer::append(wrapped_buffer<uint8_t> &ptr, size_t len) {
	/* TODO: make no copy */
	size_t to_write = write_queue.to_write();
	uint32_t netlen = hton((uint32_t)len);
	write_queue.append((uint8_t*)&netlen, sizeof(netlen));
	write_queue.append(ptr, len);
	if (to_write == 0) {
		io.set(fd, ev::WRITE);
	}
}

void log_buffer::io_cb(ev::io &watcher, int /*revents*/) {
	ssize_t r(1);
	while(write_queue.to_write() && r > 0) {
		auto res = write_queue.do_write(watcher.fd);
		r = res.first;
		if (r < 0 && errno != EWOULDBLOCK && errno != EAGAIN && errno != EINTR) { 
			/* where to log when the log is dead... */
			cerr << "Cannot write out log: " << strerror(errno) << endl;
			io.stop();
			close(io.fd);
			g_log_buffer = NULL;
			delete this;
			/* TODO: re-establish connection? */
			return;
		}
	}
	if (write_queue.to_write() == 0) {
		io.set(watcher.fd, ev::NONE);
	}
}
log_buffer::~log_buffer() {
	io.stop();
	close(io.fd);
}

log_buffer *g_log_buffer;



template <> void g_log<BITCOIN_MSG>(uint32_t id, bool is_sender, const struct bitcoin::packed_message *m) {

	uint64_t net_time = hton((uint64_t)time(NULL));
	uint32_t net_id = hton(id);
	size_t len = 1 + sizeof(net_time) + sizeof(net_id) + 1 + sizeof(*m) + m->length;
	wrapped_buffer<uint8_t> wbuf(len);
	uint8_t *ptr = wbuf.ptr();

	uint8_t typ = BITCOIN_MSG;
	copy((uint8_t*) &typ, ((uint8_t*) &typ) + 1, ptr);
	ptr += 1;

	copy((uint8_t*) &net_time, ((uint8_t*)&net_time) + sizeof(net_time),
	     ptr);
	ptr += sizeof(net_time);

	copy((uint8_t*) &net_id, ((uint8_t*)&net_id) + sizeof(net_id), 
	     ptr);
	ptr += sizeof(net_id);

	copy((uint8_t*) &is_sender, ((uint8_t*)&is_sender) + sizeof(is_sender), 
	     ptr);
	ptr += sizeof(is_sender);

	copy((uint8_t*) m, ((uint8_t*)m) + sizeof(*m) + m->length, 
	     ptr);
	ptr += sizeof(*m) + m->length;
	g_log_buffer->append(wbuf, len);
}


ostream & operator<<(ostream &o, const struct ctrl::message *m) {
	o << "MSG { length => " << ntoh(m->length);
	o << ", type => " << m->message_type;
	o << ", payload => ommitted"; //o.write((char*)m, m->length + sizeof(*m));
	o << "}";
	return o;
}

ostream & operator<<(ostream &o, const struct ctrl::message &m) {
	return o << &m;
}


ostream & operator<<(ostream &o, const struct bitcoin::packed_message *m) {
	o << "MSG { length => " << m->length;
	o << ", magic => " << hex << m->magic;
	o << ", command => " << m->command;
	o << ", checksum => " << hex << m->checksum;		
	o << ", payload => ommitted"; //o.write((char*)m, m->length + sizeof(*m));
	o << "}";
	return o;
}

ostream & operator<<(ostream &o, const struct bitcoin::packed_message &m) {
	return o << &m;
}

ostream & operator<<(ostream &o, const struct sockaddr &addr) {
	char str[24];
	if (addr.sa_family == AF_INET) {
		const struct sockaddr_in *saddr = (struct sockaddr_in*)&addr;
		inet_ntop(addr.sa_family, &saddr->sin_addr, str, sizeof(str));
		o << str << ':' << ntoh(saddr->sin_port);
	} else if (addr.sa_family == AF_INET6) {
		const struct sockaddr_in6 *saddr = (struct sockaddr_in6*)&addr;
		inet_ntop(addr.sa_family, &saddr->sin6_addr, str, sizeof(str));
		o << str << ':' << ntoh(saddr->sin6_port);
	} else {
		cerr << "add support converting other addr types";
	}
	return o;
}

string type_to_str(enum log_type type) {
	switch(type) {
	case DEBUG:
		return "DEBUG";
		break;
	case CTRL:
		return "CTRL";
		break;
	case ERROR:
		return "ERROR";
		break;
	case BITCOIN:
		return "BITCOIN";
		break;
	case BITCOIN_MSG:
		return "BITCOIN_MSG";
		break;
	default:
		return "huh?";
		break;
	};
}
