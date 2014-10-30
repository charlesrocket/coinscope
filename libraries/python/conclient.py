import sys
from struct import *
import socket
import argparse

sys.path.append('lib')

import logger
from connector import *

class Config:
    bitcoin_control = '/tmp/bitcoin_control'
    bitcoin = '/tmp/logger/clients/bitcoin'
    bitcoin_msg = '/tmp/logger/clients/bitcoin_msg'

class ConfigTestnet:
    bitcoin_control = '/tmp/netmine-testnet/bitcoin_control'
    bitcoin = '/tmp/netmine-testnet/logger/clients/bitcoin'
    bitcoin_msg = '/tmp/netmine-testnet/logger/clients/bitcoin_msg'

class ConfigLitecoin:
    bitcoin_control = '/tmp/netmine-litecoin/bitcoin_control'
    bitcoin = '/tmp/netmine-litecoin/logger/clients/bitcoin'
    bitcoin_msg = '/tmp/netmine-litecoin/logger/clients/bitcoin_msg'

# 96.126.102.140:9327
def do_send(sock, msg):
    written = 0
    while (written < len(msg)):
        rv = sock.send(msg[written:], 0)
        if rv > 0:
            written = written + rv
        if rv < 0:
            raise Exception("Error on write (this happens automatically in python?)");
        

# Just demonstrates some connector fun

# There are bindings to libconfig, which will parse our config
# file. I'm just hard-coding for this example code.

parser = argparse.ArgumentParser()
parser.add_argument("test", type=str, 
                    help="test you want to run",
                    choices=["connect", "getaddr", "get_cxn", "disconnect"])
parser.add_argument("--testnet", 
                    help="run on testnet instead of mainnet",
                    action='store_true')
parser.add_argument("--litecoin", 
                    help="run on litecoin instead of bitcoin",
                    action='store_true')
args = parser.parse_args()

if args.testnet: config = ConfigTestnet
elif args.litecoin: config = ConfigLitecoin
else: config = Config

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
#socket.create_connection
sock.connect(config.bitcoin_control)


if args.test == "connect":
    logsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    logsock.connect(config.bitcoin)

    address = ('54.187.48.148',8333)
    #address = ('54.187.173.83',8333)
    #address = ('54.69.82.165',8333)
    msg = connect_msg(address[0], address[1], '0.0.0.0', 0)
    #msg = connect_msg('54.69.232.231', 8339, '0.0.0.0', 0)
    #msg = connect_msg('50.175.116.111 ', 9333, '0.0.0.0', 0)
    ser = msg.serialize()
    do_send(sock, ser)

    while(True):
        length = logsock.recv(4, socket.MSG_WAITALL);
        length, = unpack('>I', length)
        record = logsock.recv(length, socket.MSG_WAITALL)
        sid, log_type, timestamp, rest = logger.log.deserialize_parts(record)
        log = logger.type_to_obj[log_type].deserialize(sid, timestamp, rest)
        print log
        break
elif args.test == "getaddr":
    logsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    logsock.connect(config.bitcoin_msg)

    getaddr = pack('<I12sII', 0xD9B4BEF9, "getaddr", 0, 0xE2E0F65D)
    msg = bitcoin_msg(getaddr);

    ser = msg.serialize()
    do_send(sock, ser)

    rid = sock.recv(4)
    rid, = unpack('>I', rid)  # message is now saved and can be sent to users with this id
    print "rid is " + str(rid)

    if (rid == 0):
        print "Invalid message"
    else:
        cmsg = command_msg(commands.COMMAND_SEND_MSG, rid, [targets.BROADCAST])
        ser = cmsg.serialize()
        do_send(sock, ser)

        while(True):
            length = logsock.recv(4, socket.MSG_WAITALL);
            length, = unpack('>I', length)
            record = logsock.recv(length, socket.MSG_WAITALL)
            log_type, timestamp, rest = logger.log.deserialize_parts(record)
            log = logger.type_to_obj[log_type].deserialize(timestamp, rest)
            print log
            break

elif args.test == "get_cxn":
    cmsg = command_msg(commands.COMMAND_GET_CXN, 0)
    ser = cmsg.serialize()
    do_send(sock, ser)

    length = sock.recv(4, socket.MSG_WAITALL)
    length, = unpack('>I', length)
    infos = sock.recv(length, socket.MSG_WAITALL)
    sid, log_type, timestamp, rest = logger.log.deserialize_parts(infos)
    # Each info chunk should be 36 bytes

    cur = 0
    while(len(infos[cur:cur+36]) > 0):
        cinfo = connection_info.deserialize(infos[cur:cur+36])
        print "{5} {0} {1}:{2} - {3}:{4}".format(cinfo.handle_id, cinfo.remote_addr, cinfo.remote_port, cinfo.local_addr, cinfo.local_port, sid)
        cur = cur + 36
elif args.test == "disconnect":
    cmsg = command_msg(commands.COMMAND_DISCONNECT, 0, [targets.BROADCAST])
    ser = cmsg.serialize()
    do_send(sock, ser)


sock.close()
