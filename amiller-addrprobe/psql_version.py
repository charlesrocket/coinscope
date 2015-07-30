from bitcoin.messages import *
from cStringIO import StringIO
import logger
from struct import *
import struct
import os
import psycopg2
import datetime
import select

def read_logs_until(logsock, deadline):
    while(True):
        logsock.settimeout(deadline - time.time())
        try:
            length = logsock.recv(4);
            length, = unpack('>I', length)
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

def do_recv(sock, length):
    msg = ''
    count = 0
    while (len(msg) < length):
        msg += sock.recv(length-len(msg), socket.MSG_WAITALL)
    assert len(msg) == length
    return msg

def reader_thread():
    logsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    logsock.connect("/container_wide/connector-dreyfus/logger/clients/bitcoin_msg")

    while True:
        length = do_recv(logsock, 4)
        length, = unpack('>I', length)
        record = ''
        record = do_recv(logsock, length)
        sid, log_type, timestamp, rest = logger.log.deserialize_parts(record)
        if 'version' not in rest[:100]: continue # shortcut, hopefully
        log = logger.type_to_obj[log_type].deserialize(sid, timestamp, rest)
        #if log.is_sender: continue
        if not hasattr(log, 'bitcoin_msg'): continue
        try:
            msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))
        except Exception, e:
            print 'Error deserializing bitcoin msg', e, log.handle_id, log.sid
            continue
        if msg.command not in ('version',): continue
        print 'version:', log.handle_id, msg

        yield sid, log.handle_id, timestamp, str(msg)

def versions_for_nodes(nodes):
    conn = psycopg2.connect("dbname=connector")    
    cur = conn.cursor()
    cur.execute("SELECT (Handle, Timestamp, Version) FROM versions WHERE Handle = ANY(%s);", (nodes,))
    return cur.fetchall()

def addr_for_nodes(nodes):
    conn = psycopg2.connect("dbname=connector")
    cur = conn.cursor()
    cur.execute("SELECT (Handle, Ip) FROM versions_plus WHERE Handle = ANY(%s) ORDER BY Ip;", (nodes,))
    return cur.fetchall()

import signal
def handler(signum, frame): 
    print 'timeout'
    raise Exception('timeout')
def main():
    #signal.signal(signal.SIGALRM, handler)
    #signal.alarm(5)

    try:
        conn = psycopg2.connect("dbname=connector")
        for sid, handle, timestamp, ver in reader_thread():
            cur = conn.cursor()
            date = datetime.datetime.fromtimestamp(timestamp)
            data = (sid, handle, date, ver)
            query = 'INSERT INTO versions (Sid, Handle, Timestamp, Version) VALUES (%s, %s, %s, %s)'
            cur.execute(query, data)
            conn.commit()
    except Exception:
        return

#if __name__ == '__main__':
#    main()
