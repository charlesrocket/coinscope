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
#include <signal.h>

/* external libraries */
#include <boost/program_options.hpp>


#include "netwrap.hpp"
#include "read_buffer.hpp"
#include "logger.hpp"
#include "config.hpp"


using namespace std;

void print_message(read_buffer &input_buf) {
	const uint8_t *buf = input_buf.extract_buffer().const_ptr();

	const struct log_format *log = (const struct log_format*)(buf+4);
	enum log_type lt(static_cast<log_type>(log->type));
	time_t time = ntoh(log->timestamp);
	
	const uint8_t *msg = log->rest;

	if (lt == BITCOIN_MSG) {
		msg += 5;
		// Now check that it is a version message
		const struct bitcoin::packed_message* b = (const struct bitcoin::packed_message*)(msg);
		const char VER[12] = "version\0";
		if (!strncmp(b->command, VER, sizeof(b->command))) {
			//cerr << b << endl;
			cout.write((const char*)buf, input_buf.cursor());
			cout.flush();
		}
	}
}


int main(int argc, char *argv[]) {

	if (startup_setup(argc, argv) != 0) {
		return EXIT_FAILURE;
	}
	const libconfig::Config *cfg(get_config());

	string root((const char*)cfg->lookup("logger.root"));

	mkdir(root.c_str(), 0777);
	string client_dir(root + "clients/");

	int client = unix_sock_client(client_dir + "all", false);

	bool reading_len(true);

	read_buffer input_buf(sizeof(uint32_t));

	while(true) {
		auto ret = input_buf.do_read(client);
		int r = ret.first;
		if (r == 0) {
			cerr << "Disconnected\n";
			return EXIT_SUCCESS;
		} else if (r < 0) {
			cerr << "Got error, " << strerror(errno) << endl;
			return EXIT_FAILURE;
		}

		if (!input_buf.hungry()) {
			if (reading_len) {
				uint32_t netlen = *((const uint32_t*) input_buf.extract_buffer().const_ptr());
				//input_buf.cursor(0);
				input_buf.to_read(ntoh(netlen));
				reading_len = false;
			} else {
				print_message(input_buf);
				input_buf.cursor(0);
				input_buf.to_read(4);
				reading_len = true;
			}
		}
	}

}
