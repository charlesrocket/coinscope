from bitcoin.core import *
from bitcoin.net import *
from bitcoin.core.key import *
from bitcoin.core.script import *
from bitcoin.core.scripteval import *
from bitcoin import base58
from bitcoin.messages import *
import socket
from connector import *
import time
import struct
import logger

def read_logs_until(logsock, deadline):
    while(True):
        logsock.settimeout(deadline - time.time())
        try:
            length = logsock.recv(4);
            length, = struct.unpack('>I', length)
            logsock.settimeout(deadline - time.time())
            record = ''
            while len(record) < length:
                record += logsock.recv(length-len(record))
            assert(len(record) == length)
        except socket.timeout: break
        except struct.error: continue
        sid, log_type, timestamp, rest = logger.log.deserialize_parts(record)
        log = logger.type_to_obj[log_type].deserialize(sid, timestamp, rest)
        yield log

    logsock.settimeout(None)

class ConnectorSocket(object):
    def __init__(self, sockpath="/container_wide/connector-testnet/bitcoin_control"):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        self.sock.connect(sockpath)

    def send(self, msg):
        written = 0
        while (written < len(msg)):
            rv = self.sock.send(msg[written:], 0)
            if rv > 0:
                written = written + rv
            if rv < 0:
                raise Exception("Error on write (this happens automatically in python?)");

    def recv(self, length):
        msg = ''
        count = 0
        while (len(msg) < length):
            msg += self.sock.recv(length-len(msg), socket.MSG_WAITALL)
        assert len(msg) == length
        return msg

    def register_tx(self, tx):
        assert type(tx) is CTransaction
        m = msg_tx()
        m.tx = tx
        cmsg = bitcoin_msg(m.serialize())
        ser = cmsg.serialize()
        self.send(ser)
        rid = self.recv(4)
        rid, = unpack('>I', rid)  # message is now saved and can be sent to users with this id
        return rid
    
    def register_getdata(self, hashes):
        m = msg_getdata()
        for h in hashes:
            assert len(h) == 32
            inv = CInv()
            inv.type = 1 # TX
            inv.hash = h
            m.inv.append(inv)
        cmsg = bitcoin_msg(m.serialize())
        ser = cmsg.serialize()
        self.send(ser)
        rid = self.recv(4)
        rid, = unpack('>I', rid)  # message is now saved and can be sent to users with this id
        return rid

    def register_inv(self, hashes):
        m = msg_inv()
        for h in hashes:
            assert len(h) == 32
            inv = CInv()
            inv.type = 1 # TX
            inv.hash = h
            m.inv.append(inv)
        cmsg = bitcoin_msg(m.serialize())
        ser = cmsg.serialize()
        self.send(ser)
        rid = self.recv(4)
        rid, = unpack('>I', rid)  # message is now saved and can be sent to users with this id
        return rid
    
    def broadcast(self, rid):
        cmsg = command_msg(commands.COMMAND_SEND_MSG, rid, (targets.BROADCAST,))
        ser = cmsg.serialize()
        self.send(ser)

    def send_to_nodes(self, rid, nodes):
        cmsg = command_msg(commands.COMMAND_SEND_MSG, rid, nodes)
        ser = cmsg.serialize()
        self.send(ser)

    def get_cxns(self):
        cmsg = command_msg(commands.COMMAND_GET_CXN, 0)
        ser = cmsg.serialize()
        self.send(ser)
        
        length = self.recv(4)
        length, = unpack('>I', length)
        infos = self.recv(length)
        # Each info chunk should be 36 bytes
        
        cur = 0
        while(len(infos[cur:cur+36]) > 0):
            cinfo = connection_info.deserialize(infos[cur:cur+36])
            # print "{0} {1}:{2} - {3}:{4}".format(cinfo.handle_id, cinfo.remote_addr, cinfo.remote_port, cinfo.local_addr, cinfo.local_port)
            yield (cinfo.remote_addr, cinfo.handle_id[0])
            cur = cur + 36

    def connect_to_ip(self, ip, port=18333, localip='128.8.124.7', localport=18333):
        cmsg = connect_msg(ip, port, localip, localport)
        ser = cmsg.serialize()
        self.send(ser)
