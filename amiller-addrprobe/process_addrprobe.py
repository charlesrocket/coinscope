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
import math

class NoRelevantLogsException(Exception):
    pass

# START_ADDR = 1413850620

START_ADDR = (((1420642201/60)/240) * 240+17)*60 # Start time for connector-dreyfus
addrprobe_timestamps = [START_ADDR]
while True:
    next_timestamp = addrprobe_timestamps[-1] + 240*60
    if time.time() - next_timestamp < 12*60*60:
        # Ignore addrprobes more recent than 12 hours ago
        break
    addrprobe_timestamps.append(next_timestamp)
    
#START_ADDR = (98741*240+17)*60 # 

def pass_one_and_two(ts):
    relfn = 'miller-addrprobe-data/addr-relevant-%s.pkl' % datetime.strftime(datetime.fromtimestamp(ts), '%F-%s')
    if os.path.exists(relfn):
        print "Relevant-addrs already exists: skipping", relfn
        return
    try:
        trimfn = prepare_snippets(ts)
    except NoRelevantLogsException:
        return
    ipmap, addrmap = parse_addrprobe_pass1(trimfn) # Generates a list of good IPs, stores in ipmap,addrmap
    relevantaddrs = parse_addrprobe_pass2(trimfn, ipmap, addrmap) # Generates a pkl
    pickle.dump(relevantaddrs, open(relfn,'wb'), 2)
    os.remove(trimfn)
    do_edges(relfn)

def prepare_snippets(ts):
    outfn = 'miller-addrprobe-data/addr-trimmed-%s.log' % datetime.strftime(datetime.fromtimestamp(ts), '%F-%s')

    starttime = ts-60*2 # Two minutes before schedule
    stoptime = ts+60*60 # An hour after schedule
    print 'looking for a range of', starttime, 'to', stoptime
    #fns = sorted(glob.glob('/mnt/xvdb/verbatim.log-*.gz'))
    fns = sorted(glob.glob('/container_wide/connector-dreyfus/verbatim/verbatim.log-*.gz'))
    relevant_files = []
    for fn in fns:
        timestamp = float(fn.split('.')[-2].split('-')[-1])
        if timestamp < starttime:
            continue
        firstlog = logger.logs_from_stream(gzip.open(fn)).next()
        if firstlog.timestamp > stoptime:
            break
        relevant_files.append(fn)
    print 'relevant files:', relevant_files
    if not relevant_files: 
        raise NoRelevantLogsException(outfn)
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
        #if not count % 10000: print count
        count += 1

        # Store in "ipmap" oncec we see the connection accepted message
        if log.log_type == logger.log_types.BITCOIN:
            nid = (log.source_id, log.handle_id)
            if nid in ipmap: continue # already registered, skip
            addr = (log.remote_addr,log.remote_port)
            ipmap[nid] = addr
            continue

        # Upgrade to "addrmap" once we receive an addr message
        if log.log_type == logger.log_types.BITCOIN_MSG:

            if log.is_sender: continue
            nid = (log.source_id, log.handle_id)
            if nid not in ipmap: continue # received addr, but not the connection?
            addr = ipmap[nid]
            if addr in addrmap: continue # already good, possibly with a different nid in case of dupes
            try:
                msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))
            except ValueError, e:
                print 'FAILED TO DECODE MESSAGE'
                print e
                continue
            #if msg.command == 'version':
                #ipmap[(log.source_id, log.handle_id)] = (msg.addrFrom.ip, msg.addrFrom.port)
            #    pass # No longer care about version messages
            if msg.command == 'addr':
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
        #if not count % 10000: print count
        count += 1

        if log.log_type != logger.log_types.BITCOIN_MSG: continue
        if log.is_sender: continue
        nid = (log.source_id, log.handle_id)
        if nid not in ipmap: continue
        if ipmap[nid] not in addrmap: continue

        try:
            msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))
        except ValueError, e:
            print 'FAILED TO DECODE MESSAGE'
            print e
            continue
            
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
        for ts in ra[src]:
            d = list(dict(sorted(ra[src][ts])).iteritems())
            # Filter by unique
            if len(d) == 1:
                # Filter by <2h
                for tgt,logtime in d:
                    if logtime - 60*(60*2-20) > ts: continue
                    g.add_edge(src,tgt)
    return g

def do_edges(fn):
    dat = '-'.join(fn.split('.')[-2].split('-')[-4:])
    gexfn = 'miller-addrprobe-data/addrprobe-%s.gexf' % dat
    if os.path.exists(gexfn):
        print gexfn, "already exists, skipping"
        return
    print fn
    ra = pickle.load(open(fn))
    g = infer_edges(ra)
    for n in g.nodes(): 
        g.node[n]['viz'] = {}
        g.node[n]['viz']['size'] = (0.1 + math.log(g.degree(n)))/2
    ts = int(dat.split('-')[-1])
    nx.write_gexf(g, gexfn)
    import subprocess
    cmd = "CLASSPATH=./gephi-toolkit.jar jython gephi.py %s ./%s.gexf-test.gexf" % (gexfn, gexfn)
    subprocess.call(cmd,shell=True)

def all_edges():
    fns = sorted(glob.glob('miller-addrprobe-data/addr-relevant-*.pkl'))
    for fn in fns:
        do_edges(fn)


def main():
    import sys
    if not len(sys.argv) == 2:
        print 'usage: process_addrprobe.py <timestamp>'
        sys.exit(1)
    ts = float(sys.argv[1].strip())
    if not ts in addrprobe_timestamps:
        print ts, 'doesn\'t seem like an addrprobe start time'
        sys.exit(1)
    pass_one_and_two(ts)


def main2():
    for ts in addrprobe_timestamps:
        pass_one_and_two(ts)
    all_edges()

if __name__ == '__main__':
    try:
        __IPYTHON__
    except NameError:
#        main2()
        main()
