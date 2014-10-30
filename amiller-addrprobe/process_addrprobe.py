import logger
import sys
from struct import *
import gzip
from bitcoin.messages import *
from cStringIO import StringIO
from collections import defaultdict
import cPickle as pickle
import networkx as nx
import subprocess
import glob
from datetime import datetime
import os

START_ADDR = 1413850620

addrprobe_timestamps = [START_ADDR + i*60*60*4 for i in range(70)]

def pass_one_and_two(ts):
    trimfn = prepare_snippets(ts)
    ipmap, addrmap = parse_addrprobe_pass1(trimfn) # Generates a list of good IPs, stores in ipmap,addrmap
    relevantaddrs = parse_addrprobe_pass2(trimfn, ipmap, addrmap) # Generates a pkl
    relfn = 'addr-relevant-%s.pkl' % datetime.strftime(datetime.fromtimestamp(ts), '%F-%s')
    pickle.dump(relevantaddrs, open(relfn,'wb'), 2)
    os.remove(trimfn)

def prepare_snippets(ts):
    outfn = 'addr-trimmed-%s.log' % datetime.strftime(datetime.fromtimestamp(ts), '%F-%s')

    starttime = ts-60*2 # Two minutes before schedule
    stoptime = ts+60*60 # An hour after schedule
    print 'looking for a range of', starttime, 'to', stoptime
    fns = sorted(glob.glob('/var/log/connector/verbatim.log-*.gz'))
    relevant_files = []
    for fn in fns:
        timestamp = float(fn.split('.')[-2].split('-')[-1])
        if timestamp < starttime:
            print 'reject by filename', starttime, timestamp
            continue
        firstlog = logger.logs_from_stream(gzip.open(fn)).next()
        if firstlog.timestamp > stoptime:
            print 'reject by first log', stoptime, firstlog
            break
        relevant_files.append(fn)
    print 'relevant files:', relevant_files
    cmd = 'cat %s | gzip -d | /home/amiller/projects/netmine/logclient/addrs-in-range --starttime=%d --stoptime=%d > %s' % (' '.join(relevant_files), starttime, stoptime, outfn)
    print cmd
    subprocess.call(cmd, shell=True)
    return outfn

def parse_addrprobe_pass1(fn):
    ipmap = {} # map (sid,handleid) -> (ip,port)
    addrmap = {} # map (ip,port) -> (sid,handleid) with first timestamp

    count = 0
    f = gzip.open(fn) if fn.endswith('.gz') else open(fn)
    for log in logger.logs_from_stream(f):
        if not count % 10000: print count
        count += 1
        if log.is_sender: continue
        nid = (log.source_id, log.handle_id)
        if nid in ipmap:
            addr = ipmap[nid]
            if addr in addrmap: continue # already good, skip
        msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))
        if msg.command == 'version':
            ipmap[(log.source_id, log.handle_id)] = (msg.addrFrom.ip, msg.addrFrom.port)
        if msg.command == 'addr':
            if not nid in ipmap: 
                #print nid, 'not found in ipmap'
                continue
            addr = ipmap[nid]
            if not addr in addrmap:
                addrmap[addr] = nid
    return ipmap, addrmap

def test(fn):
    for log in logger.logs_from_stream(gzip.open(fn)):
        msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))
        break

def parse_addrprobe_pass2(fn, ipmap, addrmap):
    relevantaddrs = defaultdict(lambda:defaultdict(list))

    count = 0
    f = gzip.open(fn) if fn.endswith('.gz') else open(fn)
    for log in logger.logs_from_stream(f):
        if not count % 10000: print count
        count += 1
        if log.is_sender: continue
        nid = (log.source_id, log.handle_id)
        if nid not in ipmap: continue
        if ipmap[nid] not in addrmap: continue

        msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))
        if msg.command != 'addr': continue

        for addr in msg.addrs:
            a = (addr.ip, addr.port)
            if a not in addrmap: continue
            ts = addr.nTime
            relevantaddrs[a][ts].append((ipmap[nid], log.timestamp))

    for k in relevantaddrs: relevantaddrs[k] = dict(relevantaddrs[k])
    relevantaddrs = dict(relevantaddrs)
    return relevantaddrs

def count_reports(ra):
    count = 0
    for src in ra:
        for times in ra[src]:
            count += len(ra[src][times])
    print count

def infer_edges(ra):
    g = nx.Graph()

    for src in ra:
        for times in ra[src]:
            if len(ra[src][times]) == 1:
                tgt = list(ra[src][times])[0]
                g.add_edge(src,tgt)
    return g

def main():
    import sys
    if not len(sys.argv) == 2:
        print 'usage: process_addrprobe.py <timestamp>'
        sys.exit(1)
    ts = float(sys.argv[1])
    if not ts in addrprobe_timestamps:
        print ts, 'doesn\'t seem like an addrprobe start time'
        sys.exit(1)
    pass_one_and_two(ts)

if __name__ == '__main__':
    try:
        __IPYTHON__
    except NameError:
        main()
