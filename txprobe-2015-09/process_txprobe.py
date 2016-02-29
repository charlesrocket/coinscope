from binascii import hexlify, unhexlify
from bitcoin.core import *
from bitcoin.net import *
from bitcoin.core.key import *
from bitcoin.core.script import *
from bitcoin.core.scripteval import *
from bitcoin import base58
from bitcoin.messages import *
import shutil
import json
import logger
import os
import glob
from cStringIO import StringIO
import time
from datetime import datetime
import cPickle as pickle
from collections import defaultdict
#import psql_version
import numpy as np
import psql_groundtruth

from bitcoin import SelectParams
SelectParams('testnet')

gt_ips = ['54.152.175.55',
          '54.172.15.152',
          '54.172.11.64',
          '54.165.251.192',
          '54.152.83.116']

good_satoshis=[
 '/libbitcoin:2.0/',
 '/libbitcoin:2.10.0/',
 '/Satoshi:0.10.0/',
 '/Satoshi:0.10.0/opennodes.org:0.1/',
 '/Satoshi:0.10.1/',
 '/Satoshi:0.10.2/',
 '/Satoshi:0.10.99/',
 '/Satoshi:0.11.0/',
 '/Satoshi:0.11.99/',
# '/Satoshi:0.8.4/',
# '/Satoshi:0.8.5/',
# '/Satoshi:0.8.6/',
 '/Satoshi:0.9.0/',
 '/Satoshi:0.9.1/',
 '/Satoshi:0.9.2/',
 '/Satoshi:0.9.2.1/',
 '/Satoshi:0.9.3/',
 '/Satoshi:0.9.4/',
 '/Satoshi:0.9.5/',
 '/Satoshi:0.9.99/',
 '/Satoshi RBF:0.10.2/',
]


# Prepare the experiment messages
def gettx(txhex): return CTransaction.deserialize(txhex.decode('hex'))
def gethash(tx): return tx.GetHash()

def nine_chart((stable,transient,negatives),
               (detected_positives, detected_neutral, detected_negatives),
               tag=None):

    TP = detected_positives.intersection(stable)
    sP = detected_positives.intersection(transient)
    FP = detected_positives.intersection(negatives)

    Fs = detected_neutral.intersection(stable)
    ss = detected_neutral.intersection(transient)
    Ts = detected_neutral.intersection(negatives)

    FN = detected_negatives.intersection(stable)
    sN = detected_negatives.intersection(transient)
    TN = detected_negatives.intersection(negatives)

    if tag is not None:
        for ip in TP: frq['TP'+tag][ip] += 1
        for ip in sP: frq['sP'+tag][ip] += 1
        for ip in FP: frq['FP'+tag][ip] += 1
        for ip in Fs: frq['Fs'+tag][ip] += 1
        for ip in ss: frq['ss'+tag][ip] += 1
        for ip in Ts: frq['Ts'+tag][ip] += 1
        for ip in FN: frq['FN'+tag][ip] += 1
        for ip in sN: frq['sN'+tag][ip] += 1
        for ip in TN: frq['TN'+tag][ip] += 1

    return np.array(((len(TP), len(Fs), len(FN)),
                     (len(sP), len(ss), len(sN)),
                     (len(FP), len(Ts), len(TN))))

def print_chart(chart):
    ((TP, Fs, FN),
     (sP, ss, sN),
     (FP, Ts, TN)) = chart

    print 'TP: %8d \t Fs: %8d \t FN: %8d' % (TP, Fs, FN)
    print 'sP: %8d \t ss: %8d \t sN: %8d' % (sP, ss, sN)
    print 'FP: %8d \t Ts: %8d \t TN: %8d' % (FP, Ts, TN)



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
    floodcheck = Hash(manifest['flood'].decode('hex'))
    getdata_key = Hash(manifest['getdata'].decode('hex'))    
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

def process_experiment_fn(fn):
    global manifest, testset_stats, floodset_stats
    manifest = json.load(open('./manifests/manifest-'+fn))
    dmap = defaultdict(lambda:'0.0.0.0', [(int(hid),ip) for hid,ip in manifest['nodes'].iteritems()])
    logs = pickle.load(open('./manifests/logs-'+fn+'.pkl'))
    testset_stats,floodset_stats,inferred = process_experiment(manifest, logs)

    testset = set()
    # Filter the testset
    for tgt in map(dmap.get, manifest['testset']):
        if tgt in ('128.8.124.7',): continue # Exclude CoinScope as a connection
        if tgt in ('192.99.46.190',): continue # Known non-compliant node
        testset.add(tgt)

    global positives, transient, dpos
    global negatives, dneg
    positives = set()
    transient = set()
    negatives = set()
    for ip in gt_ips:
        gt = psql_groundtruth.lookup_groundtruth(ip, manifest['time'], '10 minutes', '10 minutes')
        pos = set()
        trns= set()
        for tgt in gt.keys():
            if tgt not in testset: continue
            subver = gt[tgt][1]
            startheight = gt[tgt][4]
            print tgt, startheight
            count = gt[tgt][0] # number of times in this interval we found this gt
            if tgt == '54.200.177.203': 
                print '54.200.177.203 found', startheight
            if count < 2: 
                trns.add(tgt)
            elif startheight < 570000:  # Appropriate for ~Sep20 2015 experiments
                if tgt == '54.200.177.203': print '54.200.177.203 startheight'
                trns.add(tgt)
            # Filter bad versions
            elif 'bitcoinj' in subver.lower() or 'bitcoinruby' in subver.lower() or 'dain' in subver.lower() or 'xt' in subver.lower() or subver not in good_satoshis:
                trns.add(tgt)
            else:
                pos.add(tgt)
        neg = set(testset).difference(pos).difference(trns)
        for tgt in pos: positives.add((ip,tgt))
        for tgt in trns:transient.add((ip,tgt))
        for tgt in neg: negatives.add((ip,tgt))

    dpos = set()
    dneg = set()
    # Add the real detections
    for src,tgts in inferred.iteritems():
        for tgt in tgts:
            dpos.add((dmap[tgt],dmap[src]))

    # Check which negatives are *hard* negatives
    rmap = {}
    for hid,ip in dmap.iteritems(): rmap[ip] = hid
    for src,tgt in positives.union(negatives).difference(dpos):
        # src is in floodset/probeset, tgt is in testset
        fs = floodset_stats[rmap[src]]
        ts = testset_stats[rmap[tgt]]
        if ts['hasreject']: continue
        if fs['hasreject']: continue
        if not fs['hasflood']:
            print 'gtip doesn\'t have flood:', src
            continue
        if not ts['hasparent']: continue
        if not ts['hasmarker']: continue
        if ts['hasflood']: continue
        dneg.add((src,tgt))

    soft = positives.union(negatives).difference(dpos.union(dneg))
    chart = nine_chart((positives,transient,negatives), (dpos,soft,dneg), tag='tx')
    print_chart(chart)
    return chart

    # FIXME: should check for duplicates at this point

def process_experiment(manifest, logs):
    # First, look for transactions sent
    global sent, recvd, getdata
    getdata = defaultdict(lambda:set())
    floodcheck = Hash(manifest['flood'].decode('hex'))
    getdata_key = lx(manifest['getdata_key'])

    hasflood = defaultdict(bool)
    getdataflood = defaultdict(bool)
    getdatamarker = defaultdict(bool)

    global markers, parents
    markers = [Hash(ser.decode('hex')) for ser in manifest['markers'][:]]
    parents = [Hash(ser.decode('hex')) for ser in manifest['parents'][:]]

    inferred = defaultdict(set)
    dmap = defaultdict(lambda:'0.0.0.0', [(int(hid),ip) for hid,ip in manifest['nodes'].iteritems()])

    global txreceived
    txreceived = defaultdict(set)

    hasreject = defaultdict(bool)

    for log in logs:
        msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))

        if msg.command == 'reject':
            #if log.handle_id in manifest['floodset']:
            #    print 'floodset reject found', log.handle_id, msg
            #if log.handle_id in manifest['testset']:
            #    print 'testset reject found', log.handle_id, msg
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

            if getdata_key in hashes:
                if log.handle_id not in manifest['probeset']: continue
                # Check a node responded with getdata when we probed it
                getdatamarker[log.handle_id] = True

                if log.handle_id in manifest['probeset']:
                    n = log.handle_id
                    #print 'gtip detection', dmap[n], len(msg.inv)

                for i,m in enumerate(markers):
                    if m not in hashes:
                        print 'Connection to', manifest['testset'][i], dmap[manifest['testset'][i]], ' inferred', log.handle_id, dmap[log.handle_id], len(hashes)
                        inferred[manifest['testset'][i]].add(log.handle_id)
            getdata[log.handle_id].add(msg)

    # Interpret the sanity checks
    for n in manifest['floodset']:
        if txreceived[n] == set([floodcheck]): hasflood[n] = True
        if txreceived[n] and not txreceived[n] == set([floodcheck]):
            print 'node', n, 'has too many!'

    #tls = set([_[0].split(',')[1][:-1] for _ in psql_version.addr_for_nodes(manifest['testset'])])
    #fls = set([_[0].split(',')[1][:-1] for _ in psql_version.addr_for_nodes(manifest['floodset'])])
    #dupset = tls.intersection(fls)


    # Collect the stats
    floodset_stats = {}
    for n in manifest['floodset']:
        floodset_stats[n] = dict(getdataflood=getdataflood[n], getdatamarker=getdatamarker[n], hasflood=hasflood[n], hasreject=hasreject[n])
        #if n in manifest['probeset']:
            #print 'probeset', dmap[n], n, floodset_stats[n]
    #for n in manifest['floodset']:
    #    print 'floodset', dmap[n], n, floodset_stats[n]

    testset_stats = {}
    for ind,n in enumerate(manifest['testset']):
        hasparent = False
        hasmarker = False
        haswrongparent = False
        for m in txreceived[n]:
            if m in markers and markers.index(m) == ind: hasparent = True
            if m in parents and parents.index(m) == ind: hasmarker = True
            if m == floodcheck: hasflood[n] = True
            if m in parents and parents.index(m) != ind: haswrongparent = True
        testset_stats[n] = dict(hasparent=hasparent, hasmarker=hasmarker, haswrongparent=haswrongparent, hasflood=hasflood[n], hasreject=hasreject[n])
        #print dmap[n], n, testset_stats[n]

    return testset_stats, floodset_stats, inferred


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
