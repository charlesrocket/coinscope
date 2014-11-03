from bitcoin.messages import *
import logger
from cStringIO import StringIO
import json
import time
from datetime import datetime
import cPickle as pickle
from collections import defaultdict
import psql_version

def good_versions(source_id, manifest, block=326044):
    # Does this node have a good version and start time?
    # TODO: need to worry about sid 
    versions = psql_version.versions_for_nodes(sorted(manifest['floodset']))
    #versions = psql_version.versions_for_nodes_litton(source_id, sorted(manifest['floodset']))
    versions = [(handle, s) for handle,_,s in [eval(c) for c, in versions]]
    d = {}
    import re
    allver = set()
    for h,v in versions:
        subver = re.findall('strSubVer=(.*) ', v)[0]
        # We're only interested in validating against satoshi 0.8.x+
        goodver = 'Satoshi:' in subver and ':0.7.' not in subver and ':0.6' not in subver
        allver.add(subver)
        # Special exception: Go away getaddr.bitnodes.io!
        notgetaddr = '148.251.238.178' not in v
        # other suspect ips for false positives: 
        # bitcoin ruby '144.76.183.77' 
        # '128.199.208.133' '188.165.239.82'
        height = int(re.findall('nStartingHeight=([-\d]+)\)', v)[0])
        good = goodver and height >= block and notgetaddr
        d[h] = good, height, subver
    return d

def good_getdata(manifest, logs):
    global hasflood
    hasflood = defaultdict(lambda:False)
    hasmarker = defaultdict(lambda:False)
    floodcheck = Hash(manifest['parents'][-1].decode('hex'))
    marker = Hash(manifest['markers'][-1].decode('hex'))    
    for log in logs:
        msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))

        if msg.command == 'getdata' and not log.is_sender:
            hashes = set(m.hash for m in msg.inv)
            if floodcheck in hashes:
                # Check a node responded with getdata when we invblocked it
                if manifest['time'] + 120 >= log.timestamp:
                    hasflood[log.handle_id] = True
            if marker in hashes:
                # Check a node responded with getdata when we probed it
                hasmarker[log.handle_id] = True
    gd = set([n for n in hasflood if hasmarker[n]])
    return gd

def process_experiment(manifest, logs):
    # First, look for transactions sent
    global sent, recvd, getdata
    getdata = defaultdict(lambda:set())
    floodcheck = Hash(manifest['parents'][-1].decode('hex'))
    marker = Hash(manifest['markers'][-1].decode('hex'))

    hasflood = defaultdict(bool)
    getdataflood = defaultdict(bool)
    getdatamarker = defaultdict(bool)

    global markers, parents
    markers = [Hash(ser.decode('hex')) for ser in manifest['markers'][:]]
    parents = [Hash(ser.decode('hex')) for ser in manifest['parents'][:]]

    inferred = defaultdict(set)
    dmap = dict(manifest['nodes'])

    global txreceived
    txreceived = defaultdict(set)

    hasreject = defaultdict(bool)

    for log in logs:
        msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))

        if msg.command == 'reject':
            if log.handle_id in manifest['floodset']:
                print 'floodset reject found', log.handle_id, msg
            if log.handle_id in manifest['testset']:
                print 'testset reject found', log.handle_id, msg
            hasreject[log.handle_id] = True

        if msg.command == 'tx' and not log.is_sender:
            h = Hash(msg.tx.serialize())
            txreceived[log.handle_id].add(h)

        if msg.command == 'getdata' and not log.is_sender:
            hashes = set(m.hash for m in msg.inv)
            if floodcheck in hashes:
                # Check a node responded with getdata when we invblocked it
                if manifest['time'] + 120 >= log.timestamp:
                    getdataflood[log.handle_id] = True

            if marker in hashes:
                # Check a node responded with getdata when we probed it
                getdatamarker[log.handle_id] = True

            if marker in hashes:
                for i,m in enumerate(markers[:-1]):
                    if m not in hashes: 
                        #print 'Connection to', manifest['testset'][i], ' inferred', log.handle_id, dmap[log.handle_id], len(hashes)
                        #inferred[manifest['testset'][i]].add(tuple(dmap[log.handle_id]))
                        inferred[manifest['testset'][i]].add(log.handle_id)
            getdata[log.handle_id].add(msg)

    # Interpret the sanity checks
    for n in manifest['floodset']:
        if txreceived[n] == set([parents[-1]]): hasflood[n] = True
        if txreceived[n] and not txreceived[n] == set([parents[-1]]):
            print 'node', n, 'has too many!'

    tls = set([_[0].split(',')[1][:-1] for _ in psql_version.addr_for_nodes(manifest['testset'])])
    fls = set([_[0].split(',')[1][:-1] for _ in psql_version.addr_for_nodes(manifest['floodset'])])
    dupset = tls.intersection(fls)


    # Collect the stats
    floodset_stats = {}
    for n in manifest['floodset']:
        floodset_stats[n] = dict(getdataflood=getdataflood[n], getdatamarker=getdatamarker[n], hasflood=hasflood[n], hasreject=hasreject[n])

    testset_stats = {}
    for n in manifest['testset']:
        ind = manifest['testset'].index(n)
        hasparent = False
        hasmarker = False
        haswrongparent = False
        for m in txreceived[n]:
            if m in markers and markers.index(m) == ind: hasparent = True
            if m in parents and parents.index(m) == ind: hasmarker = True
            if m == parents[-1]: hasflood[n] = True
            if m in parents and parents.index(m) != ind: haswrongparent = True
        testset_stats[n] = dict(hasparent=hasparent, hasmarker=hasmarker, haswrongparent=haswrongparent, hasflood=hasflood[n])
        print dmap[n], n, testset_stats[n]

    return testset_stats, floodset_stats, inferred

def test_crossval():
    testset_stats, floodset_stats, inferred = process_experiment(manifest, logs)

    sid = logs[0].source_id

    global versions
    versions = good_versions(sid, manifest)
    gv = set([k for k in versions if versions[k][0]])
    print len(gv), 'nodes have good starting heights and version string'

    # if floodset_stats[g]['hasflood'] and
    gd = [g for g in floodset_stats if floodset_stats[g]['getdataflood'] and floodset_stats[g]['getdatamarker']]
    print len(gd), 'nodes responded to getdata and have flood'

    global good
    good = gv.intersection(gd)
    good = gd
    print len(good), 'nodes are "good"'

    dmap = dict(manifest['nodes']) # Map handles to ip
    goodips = set([dmap[k][0] for k in good])
    global ipmap
    ipmap = defaultdict(set)
    for k,v in dmap.iteritems():
        ipmap[v[0]].add(k)

    import groundtruth
    peers = groundtruth.gtpeers(manifest['time'])
    for k in peers:
        try:
            handle = [h for h in dmap if dmap[h][0] == k and not testset_stats[h]['haswrongparent']][0] # FIXME: this only picks the first handle! but there are often dupes
        except IndexError:
            try:
                handle = [h for h in dmap if dmap[h][0] == k][0]
            except IndexError:
                continue
        except KeyError:
            continue
        print 'processing peer', k, handle
        stable,transient = peers[k]
        print 'stable', len(stable), 'transient', len(transient)
        sgood = set([s for s in stable if s in goodips])
        tgood = set([t for t in transient if t in goodips])
        print 'stable good', len(sgood), 'transient good', len(tgood)

        inferredgood = [dmap[h][0] for h in inferred[handle] if dmap[h][0] in goodips]
        print len(inferredgood), 'edges inferred'

        TP = set(inferredgood).intersection(tgood)
        FP = set(inferredgood).difference(tgood)
        FN = set(sgood).difference(inferredgood)
        print 'TP:', len(TP), 'FP:', len(FP), 'FN:', len(FN)
        print 'FP', FP
        print 'FN', FN

def process_all():
    import glob

    logfiles = sorted(glob.glob('./logs_txprobe-*.pkl'))
    import re
    ts = [re.findall('logs_txprobe-(.*).pkl', s)[0] for s in logfiles]
    for t in ts:
        global manifest
        global logs
        print 'Timestamp:', t
        manifest = json.load(open('experiment_logs/experiment_txprobe_gt-%s.json' % t))
        logs = pickle.load(open('./logs_txprobe-%s.pkl' % t))
        test_crossval()

def fetch_probes(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ts FROM txprobe_targets");
    return set([_[0] for _ in cur.fetchall()])

def process_all_psql():
    import psycopg2
    import glob
    global conn
    from datetime import datetime
    #conn = psycopg2.connect(database="connector", user="litton", password="osnoknom", host="localhost", port=7575)
    conn = psycopg2.connect("dbname=connector")
    cur = conn.cursor()

    probes = fetch_probes(conn)

    logfiles = sorted(glob.glob('./logs_txprobe-*.pkl'))
    import re
    ts = [re.findall('logs_txprobe-(.*).pkl', s)[0] for s in logfiles]
    for t in ts:
        global manifest
        global logs
        print 'Timestamp:', t
        timestamp = datetime.fromtimestamp(float(t.split('-')[-1]))
        if timestamp in probes: 
            print 'skipping'
            continue

        manifest = json.load(open('experiment_logs/experiment_txprobe_gt-%s.json' % t))
        logs = pickle.load(open('./logs_txprobe-%s.pkl' % t))
        global inferred
        testset_stats, floodset_stats, inferred = process_experiment(manifest, logs)

        if not logs: continue
        sid = logs[0].source_id


        # Source set
        for source in manifest['testset']:
            query = 'INSERT INTO txprobe_sources (sid, ts, handle, hasparent, hasmarker, haswrongparent, hasflood) VALUES (%s, %s, %s, %s, %s, %s, %s)'
            stats = testset_stats[source]
            data = (sid, timestamp, source, stats['hasparent'], stats['hasmarker'], stats['haswrongparent'], stats['hasflood'])
            cur.execute(query, data)

        # Target set 
        for target in manifest['floodset']:
            query = 'INSERT INTO txprobe_targets (sid, ts, handle, getdataflood, getdatamarker, hasflood, hasreject) VALUES (%s, %s, %s, %s, %s, %s, %s)'
            stats = floodset_stats[target]
            data = (sid, timestamp, target, stats['getdataflood'], stats['getdatamarker'], stats['hasflood'], stats['hasreject'])
            cur.execute(query, data)

        # Edges
        for source in manifest['testset']:
            for target in inferred[source]:
                query = 'INSERT INTO txprobe_edges (sid, ts, source, target) VALUES (%s, %s, %s, %s)'
                data = (sid, timestamp, source, target)
                cur.execute(query, data)

        conn.commit()
        
if __name__ == '__main__':
    try:
        __IPYTHON__
    except NameError:
        process_all()
