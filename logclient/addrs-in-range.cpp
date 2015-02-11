/* standard C libraries */
#include <cstdlib>
#include <cstring>
#include <cassert>

/* standard C++ libraries */
#include <iostream>
#include <utility>
#include <iomanip>
#include <fstream>

/* standard unix libraries */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>

/* external libraries */
#include <boost/program_options.hpp>

#include "netwrap.hpp"
#include "logger.hpp"

const size_t PAGE_SIZE = 4096;
const size_t MULT = 10000;

using namespace std;

long int starttime, stoptime;

string time_to_str(const time_t *t)  {
	// return put_time(localtime(t), "%FT%T%z") !!!NOT IN G++ YET
	
	/* uncomment abouve when it is available...*/
	struct tm *tm = localtime(t);
	long offset = tm->tm_gmtoff;
	ostringstream oss;
	oss << (1900 + tm->tm_year) << '-' << setfill('0') << setw(2) << (tm->tm_mon + 1)
	    << '-' << setfill('0') << setw(2) << tm->tm_mday
	    << 'T' << setfill('0') << setw(2) << tm->tm_hour << ':' 
	    << setfill('0') << setw(2) << tm->tm_min << ':'  
	    << setfill('0') << setw(2) << tm->tm_sec;
	if (offset < 0) {
		oss << '-';
		offset = -offset;
	} else if (offset > 0) {
		oss << '+';
	} else {
		oss << 'Z';
		return oss.str();
	}

	int hours = offset / (60*60);
	offset -= hours * 60*60;

	int minutes = offset / 60;
	offset -= minutes * 60;

	int seconds = offset;

	oss << setfill('0') << setw(2) << hours << ':'
	    << setfill('0') << setw(2) << minutes << ':' << setfill('0') << setw(2) << seconds;
	return oss.str();
}

int print_message(const uint8_t *buf, size_t len) {
	(void) len;
	const struct log_format *log = (const struct log_format*) buf;
	enum log_type lt(static_cast<log_type>(log->type));
	time_t time = ntoh(log->timestamp);

	static int count = 0;
	//if (!(count % 100000)) cerr << time_to_str(&time) << endl;
	count++;

	if (starttime && !(starttime <= time)) return 0;
	if (stoptime && !(stoptime >= time)) return 1;
	
	const uint8_t *msg = log->rest;

	if (lt == BITCOIN_MSG) {
		msg += 5;
		const struct bitcoin::packed_message* b = (const struct bitcoin::packed_message*)(msg);
		if (!strncmp(b->command, "version", sizeof(b->command)) ||
		    !strncmp(b->command, "addr", sizeof(b->command))
		    ) {
			/*
			cerr << time_to_str(&time);
			cerr << " ID:" << ntoh(*((uint32_t*) msg)) << " IS_SENDER:" << *((bool*) (msg+4));
			cerr << " (" << ntoh(log->source_id) << ") ";
			cerr << type_to_str(lt);
			cerr << " " << b << endl;
			*/
			uint32_t netlen = (uint32_t) len;
			netlen = hton(netlen);
			cout.write((const char *) &netlen, 4);
			cout.write((const char*) buf, len);
			cout.flush();
		}
	} else if (lt == BITCOIN) {
		/* TODO: write a function to unwrap this as a struct */
		uint32_t update_type = ntoh(*((uint32_t*)(msg + 4)));
		switch (update_type) {
		case ACCEPT_SUCCESS:
		case CONNECT_SUCCESS:
			uint32_t netlen = (uint32_t) len;
			netlen = hton(netlen);
			cout.write((const char *) &netlen, 4);
			cout.write((const char*) buf, len);
			cout.flush();
			break;
		}
	}
	return 0;
}

void grow_buf(uint8_t **buf, uint32_t sz) {
	*buf = (uint8_t *) realloc((void *) *buf, sz);
}

int robust_read(int fd, uint8_t *buf, size_t len) {
	size_t remaining = len;
	do { /* this loop never be necessary in practice */
		ssize_t rd = read(fd, buf + len - remaining, remaining);
		if (rd > 0) {
			remaining -= rd;
		} else if (rd == 0) {
			break;
		} else if (rd < 0) {
			cerr << "Error reading file: " << strerror(errno) << endl;
			return -1;
		}
	} while(remaining > 0);
	return len - remaining;
}

namespace po = boost::program_options;

int main(int argc, char *argv[]) {
	po::options_description desc("Options");
	desc.add_options()
		("help", "Produce help message")
		("logfile", po::value<string>(), "specify the log file")
		("starttime", po::value<long int>(), "start time")
		("stoptime", po::value<long int>(), "stop time");

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
		cout << desc << endl;
		return 1;
	}

	int read_fd;
	if (vm.count("logfile") != 1) {
		cerr << "No logfile specified: assuming stdin" << endl;
		read_fd = STDIN_FILENO;
	} else {
		string filename = vm["logfile"].as<string>();

		read_fd = open(filename.c_str(), O_RDONLY);
		if (read_fd < 0) {
			cerr << "Could not open file: " << strerror(errno) << endl;
			return EXIT_FAILURE;
		}
	}

	if (vm.count("starttime")) {
		starttime = vm["starttime"].as<long int>();
	}

	if (vm.count("stoptime")) {
		stoptime = vm["stoptime"].as<long int>();
	}

	bool reading_len(true);

	uint32_t to_read = 0;
	uint32_t bytes_read = 0;
	
	uint32_t buf_size = PAGE_SIZE * MULT;
	uint8_t *buf = (uint8_t *) malloc(buf_size);

	uint32_t netlen;

	cerr << "Start: " << time_to_str(&starttime) << endl;
	cerr << "Stop: " << time_to_str(&stoptime) << endl;

	while(true) {
		if (reading_len) {
			if(robust_read(read_fd,buf,4) != (ssize_t) 4) {
				cerr << "Hit end at partial length" << endl;
				break;
			}
			netlen = *((const uint32_t*) buf);
			to_read = ntoh(netlen);
			reading_len = false;
		} else {
			if(to_read > buf_size) {
				buf_size = ((to_read * 2) / PAGE_SIZE + 1) * PAGE_SIZE;
				grow_buf(&buf, buf_size);
			}
			bytes_read = robust_read(read_fd,buf,to_read);
			if (bytes_read != to_read) {
				cerr << "Hit end at partial message" << bytes_read << ", " << to_read << endl;
				break;
			}
			if (print_message(buf, (size_t) bytes_read)) break;
			reading_len = true;
		}
	}
}
