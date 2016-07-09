import logger
import re
import sys
from struct import *
import gzip
from bitcoin.core.serialize import SerializationTruncationError
from bitcoin.messages import *
from cStringIO import StringIO
from collections import defaultdict
import cPickle as pickle
import networkx as nx
import subprocess
import glob
from datetime import datetime
import time
import os
import math
import psycopg2
from signal import signal, SIGPIPE, SIG_DFL
from pybloomfilter import BloomFilter
class NoRelevantLogsException(Exception):
    pass

#DATA_DIR = 'miller-addprobe-data'
#DATA_DIR = 'addrprobe-2014-redo'
#DATA_DIR = 'addrprobe-2014-redoall'
#DATA_DIR = 'addrprobe-BOTH'




def pass_one_and_two(ts):
    relfn = '%s/addr-relevant-%s.pkl' % (DATA_DIR, datetime.strftime(datetime.fromtimestamp(ts), '%F-%s'))
    if os.path.exists(relfn):
        #print "Relevant-addrs already exists: skipping", relfn
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
    outfn = '%s/addr-trimmed-%s.log' % (DATA_DIR, datetime.strftime(datetime.fromtimestamp(ts), '%F-%s'))

    starttime = ts-60*5 # Five minutes before schedule
    stoptime = ts+60*40 # Forty minutes after schedule
    print 'looking for a range of', starttime, 'to', stoptime

    #fns = sorted(glob.glob('/container_wide/oldverbatim/verbatim.log-*.gz'))
    #fns += sorted(glob.glob('/container_wide/connector-dreyfus/verbatim/verbatim.log-*.gz'))
    fns = sorted(glob.glob('/scratch/verbatim-%s/verbatim/verbatim.log-*.gz' % (ALTCOIN,)))

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
    subprocess.call(cmd, shell=True, close_fds=True, preexec_fn = lambda: signal(SIGPIPE, SIG_DFL))
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
            except SerializationTruncationError, e:
                print 'TRUNCATED MESSAGE'
                print e
                continue

            #if msg.command == 'version':
                #ipmap[(log.source_id, log.handle_id)] = (msg.addrFrom.ip, msg.addrFrom.port)
            #    pass # No longer care about version messages
            if msg.command == 'addr':
                addrmap[addr] = nid
    return ipmap, addrmap

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
    g = nx.DiGraph()
    for src in ra:
        g.add_node(src)
        for ts in ra[src]:
            d = list(dict(sorted(ra[src][ts])).iteritems())
            # Filter by unique
            if len(d) == 1:
                # Filter by <2h
                for tgt,logtime in d:
                    if logtime - 60*(60*2-20) > ts: continue
                    g.add_edge(src,tgt)
    return g

def infer_edges_detail(ra, bf_fn):
    # This method is similer to infer_edges,
    # except it records 'all the squares' for edges involving GT nodes

    global g_infer
    g_infer = nx.DiGraph()

    if not ra: return
    bf = BloomFilter(len(ra)**2, 0.0000001, bf_fn, perm=0644)
    
    for src in ra:
        src_ip = src[0]
        g_infer.add_node(src_ip)
        for ts in ra[src]:
            d = list(dict(sorted(ra[src][ts])).iteritems())
            # Filter by <2h
            for tgt,logtime in d:
                tgt_ip = tgt[0]
                # Filter by unique
                if logtime - 60*(60*2-20) > ts:
                    bf.add((src_ip, tgt_ip))
                    #g_late.add_edge(src_ip, tgt_ip)
                else:
                    if len(d) == 1:
                        g_infer.add_edge(src_ip,tgt_ip)
    return g_infer, bf

def load_gexf_bloom(gexfn):
    g = nx.read_gexf(gexfn)
    d,b = os.path.split(gexfn)
    ts = re.match('addrprobe-pos-(.+)\.gexf', b)
    bf_fn = os.path.join(d, 'addrprobe-neg-%s.gexf' % ts)
    bf = BloomFilter.open(bf_fn)
    return g, bf


def do_edges(fn):
    dat = '-'.join(fn.split('.')[-2].split('-')[-4:])
    gexfn = '%s/addrprobe-%s.gexf' % (DATA_DIR, dat)
    print gexfn
    if os.path.exists(gexfn):
        #print gexfn, "already exists, skipping"
        #return
        pass

    ra = pickle.load(open(fn))
    bf_fn  = '%s/addrprobe-late-%s.bloom' % (DATA_DIR, dat)
    g_infer, bf = infer_edges_detail(ra, bf_fn)

    
    gexfn_infer = '%s/addrprobe-infer-%s.gexf' % (DATA_DIR, dat)
    nx.write_gexf(g_infer, gexfn_infer)

    #g = g_infer.to_undirected()
    #for n in g.nodes(): 
    #    g.node[n]['viz'] = {}
    #    g.node[n]['viz']['size'] = (0.1 + math.log(max(1,g.degree(n))))/2
    #ts = int(dat.split('-')[-1])
    #nx.write_gexf(g, gexfn)
    #import subprocess
    #cmd = "CLASSPATH=./gephi-toolkit.jar jython gephi.py %s ./%s.gexf-test.gexf" % (gexfn, gexfn)
    #subprocess.call(cmd,shell=True)

def all_edges():
    fns = sorted(glob.glob('%s/addr-relevant-*.pkl' % (DATA_DIR,)))
    for fn in fns:
        do_edges(fn)


def main():
    import sys
    if not len(sys.argv) == 2:
        print 'usage: process_addrprobe.py <timestamp>'
        sys.exit(1)
    ts = float(sys.argv[1].strip())
    if not ts in ADDRPROBE_TIMESTAMPS:
        print ts, 'doesn\'t seem like an addrprobe start time'
        sys.exit(1)
    pass_one_and_two(ts)


def main2():
    if not len(sys.argv) == 2:
        print 'usage: process_addrprobe.py <altcoin_name>'
        sys.exit(1)

    global ALTCOIN, ADDRPROBE_TIMESTAMPS, DATA_DIR
    ALTCOIN = sys.argv[1]
    assert ALTCOIN in ('bitcoin','litecoin')

    DATA_DIR = 'addrprobe-data/addrprobe-%s' % (ALTCOIN,)
    ADDRPROBE_TIMESTAMPS = []

    # Preload bitcoin experiment timestamps?
    if ALTCOIN == 'bitcoin':
        # START_ADDR = 1413850620
        START_ADDR = (((1420642201/60)/240) * 240+17)*60 # Start time for connector-dreyfus
        ADDRPROBE_TIMESTAMPS = [START_ADDR]
        while True:
            # Add all the "fixed" timestamps from 2015-jan-march before daylight savings.
            next_timestamp = ADDRPROBE_TIMESTAMPS[-1] + 240*60
            if next_timestamp > 1425788220:
                break
            ADDRPROBE_TIMESTAMPS.append(next_timestamp)
        
    with psycopg2.connect("dbname=connector") as conn:
        cur = conn.cursor()
        # Ignore addrprobes more recent than 12 hours ago
        data = (ALTCOIN, datetime.fromtimestamp(1425788220), datetime.fromtimestamp(time.time() - 12*60*60))
        cur.execute("SELECT altcoin, ts FROM alt_addrprobe_experiments WHERE altcoin = %s AND ts > %s AND ts < %s ORDER BY ts", data)
        for _,date in cur.fetchall():
            print int(date.strftime("%s"))
            ADDRPROBE_TIMESTAMPS.append(int(date.strftime("%s")))

    for ts in ADDRPROBE_TIMESTAMPS:
        pass_one_and_two(ts)
    all_edges()

def main3():
    import sys
    if not len(sys.argv) == 2:
        print 'usage: process_addrprobe.py <timestamp>'
        sys.exit(1)
    ts = float(sys.argv[1].strip())
    if not ts in ADDRPROBE_TIMESTAMPS:
        print ts, 'doesn\'t seem like an addrprobe start time'
        sys.exit(1)

    # FIX
    os.environ['TZ'] = 'UTC' # This is only for the 2014 data, when the localtimes were UTC!
    time.tzset()
    relfn = '%s/addr-relevant-%s.pkl' % (DATA_DIR, datetime.strftime(datetime.fromtimestamp(ts), '%F-%s'))
    do_edges(relfn)

def main4():
    import sys
    if not len(sys.argv) == 2:
        print 'usage: process_addrprobe.py <addr.pkl>'
        sys.exit(1)
    global DATA_DIR
    fn = sys.argv[1]
    DATA_DIR = os.path.dirname(fn)
    do_edges(fn)


if __name__ == '__main__':
    try:
        __IPYTHON__
    except NameError:
        #main2()
        #main()
        #main3()
        main4()
