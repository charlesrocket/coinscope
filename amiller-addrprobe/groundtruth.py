# Groundtruth 
# 
# Covers groundtruth runs from July 1 2015 to August 10
#

import psql_groundtruth
import networkx as nx
import time
import re
from collections import defaultdict
from dateutil.parser import parse as parse_date
from datetime import datetime
import cPickle as pickle

DATA_DIR = 'addrprobe-BOTH'

gt_ips = [ 
 '52.1.224.48',
 '54.173.131.86',
 '52.4.199.113',
 '54.165.205.116',
 '54.172.207.196',
]

# Parse date example:
# parse_date('Oct 15 2014')

good_satoshis = [
 '/Satoshi:0.6.2/',
 '/Satoshi:0.6.3/',
 '/Satoshi:0.7.0.3/',
 '/Satoshi:0.7.1/',
 '/Satoshi:0.7.2/',
 '/Satoshi:0.7.3.3/',
 '/Satoshi:0.8.0/',
 '/Satoshi:0.8.1/',
 '/Satoshi:0.8.1.99/',
 '/Satoshi:0.8.2.2/',
 '/Satoshi:0.8.3/',
 '/Satoshi:0.8.4/',
 '/Satoshi:0.8.4/Eligius:3/',
 '/Satoshi:0.8.5/',
 '/Satoshi:0.8.6/',
 '/Satoshi:0.8.7/',
 '/Satoshi:0.8.8/',
 '/Satoshi:0.8.99/',
 '/Satoshi:0.9.0/',
 '/Satoshi:0.9.1/',
 '/Satoshi:0.9.2/',
 '/Satoshi:0.9.2.1/',
 '/Satoshi:0.9.2.1/opennodes.org:0.1/',
 '/Satoshi:0.9.2/opennodes.org:0.1/',
 '/Satoshi:0.9.3/',
 '/Satoshi:0.9.4/',
 '/Satoshi:0.9.5/',
 '/Satoshi:0.9.99/',
 '/Satoshi RBF:0.10.2/',
 '/Satoshi RBF:0.11.0/ljr:20150711/',
 '/Satoshi:0.10.0/',
 '/Satoshi:0.10.0/ljr:20150220/',
 '/Satoshi:0.10.0/opennodes.org:0.1/',
 '/Satoshi:0.10.0/zsulocal:0.10/',
 '/Satoshi:0.10.1/',
 '/Satoshi:0.10.1/ljr:20150220/',
 '/Satoshi:0.10.1/ljr:20150428/',
 '/Satoshi:0.10.1/mining.bitcoinaffiliatenetwork.com amsterdam2:0.10.2/',
 '/Satoshi:0.10.1/mining.bitcoinaffiliatenetwork.com amsterdam3:0.10.2/',
 '/Satoshi:0.10.1/mining.bitcoinaffiliatenetwork.com nyiix2:0.10.2/',
 '/Satoshi:0.10.1/mining.bitcoinaffiliatenetwork.com sydney:0.10.1/',
 '/Satoshi:0.10.1/opennodes.org:0.1/',
 '/Satoshi:0.10.2/',
 '/Satoshi:0.10.99/',
 '/Satoshi:0.11.0/',
 '/Satoshi:0.11.0/ljr:20150711/',
 '/Satoshi:0.11.0/mining.bitcoinaffiliatenetwork.com seattle:0.11.0/',
 '/Satoshi:0.11.99/',
 '/Satoshi:0.11.99/Gangnam Style:v1.00XL',
 '/Satoshi:0.11.99/Gangnam Style:v1.01XL',
]

def infer_edges_with_groundtruth(fn):
    dat = '-'.join(fn.split('.')[-2].split('-')[-4:])
    ts = int(dat.split('-')[-1])
    relfn = '%s/addr-relevant-%s.pkl' % (DATA_DIR, datetime.strftime(datetime.fromtimestamp(ts), '%F-%s'))
    print relfn
    ra = pickle.load(open(relfn))

    # This method is similer to infer_edges,
    # except it records 'all the squares' for edges involving GT nodes

    #inference = dict( (gt_ip, defaultdict(lambda:defaultdict())) for gt_ip in gt_ips)
    g_dupl = nx.DiGraph()
    g_late = nx.DiGraph()
    g_infer = nx.DiGraph()

    for src in ra:
        src_ip = src[0]
        for ts in ra[src]:
            d = list(dict(sorted(ra[src][ts])).iteritems())
            # Filter by <2h
            for tgt,logtime in d:
                tgt_ip = tgt[0]
                if not tgt_ip in gt_ips and not src_ip in gt_ips: continue
                # Filter by unique
                if len(d) == 1:
                    if logtime - 60*(60*2-20) > ts:
                        g_late.add_edge(src_ip, tgt_ip)
                    else:
                        g_infer.add_edge(src_ip,tgt_ip)
                else:
                    g_dupl.add_edge(src_ip, tgt_ip)
    return g_infer, g_dupl, g_late

def all_gt_2(fns):
    gtdata = defaultdict(lambda: {}) # By gt_ip
    for fn in fns:
        print fn
        timestamp = int(re.findall('-(\d+).gexf', fn)[0])

        g_i, g_d, g_l = infer_edges_with_groundtruth(fn)

        gt = {}
        for gt_ip in gt_ips:
            gt[gt_ip] = psql_groundtruth.lookup_groundtruth(gt_ip, timestamp)

        relfn = '%s/gt2015-%s.pkl' % (DATA_DIR, datetime.strftime(datetime.fromtimestamp(timestamp), '%F-%s'))
        pickle.dump((gt, g_i, g_d, g_l), open(relfn,'w'))


def all_gt(fns):
    gtdata = defaultdict(lambda: {}) # By gt_ip
    for fn in fns:
        print fn
        timestamp = int(re.findall('-(\d+).gexf', fn)[0])
        g = nx.read_gexf(fn)
        g_dual = g.reverse()
        for gt_ip in gt_ips:
            gt = psql_groundtruth.lookup_groundtruth(gt_ip, timestamp)

            out_edges = g.edges("('%s', 8333)" % gt_ip)
            out_detected = set([eval(_[1])[0] for _ in out_edges])

            in_edges = g_dual.edges("('%s', 8333)" % gt_ip)
            in_detected = set([eval(_[1])[0] for _ in in_edges])

            gtdata[gt_ip][timestamp] = dict(
                out_detected=out_detected,
                in_detected=in_detected,
                gt=gt,
                )
    return gtdata

def compare(fn, gt_ip):
    timestamp = int(re.findall('-(\d+).gexf', fn)[0])
    gt = psql_groundtruth.lookup_groundtruth(gt_ip, timestamp)
    g = nx.read_gexf(fn)
    return compare_groundtruth(g, gt, gt_ip)

# Compare a directed graph 
def compare_groundtruth(g, gt, gt_ip):
    edges = g.edges("('%s', 8333)" % gt_ip)
    detected = set([eval(_[1])[0] for _ in edges])
    transient = set([k for k,v in gt.iteritems()])
    stable = set([k for k,v in gt.iteritems() if int(v[0]) > 30])

    stable_outbound = set(k for k in stable if not gt[k][3])
    stable_satoshi = set(k for k in stable if gt[k][1] in good_satoshis)
    stable_good  = stable_outbound.union(stable_satoshi)

    print detected, transient
    print 'Detected', len(detected)
    print 'Stable:', len(stable)
    print 'Stable[satoshi]:', len(stable_satoshi)
    print 'Stable[outbound]:', len(stable_outbound)
    print 'Stable[good]:', len(stable_good)
    print 'Transient:', len(transient)
    print 'TP:', len(detected.intersection(transient))
    print 'FP:', len(detected.difference(transient))
    print 'FN[stable]:', len(stable.difference(detected))
    print 'FN[outbound]:', len(stable_outbound.difference(detected))
    print 'FN[satoshi]:', len(stable_satoshi.difference(detected))
    print 'FN[good]:', len(stable_good.difference(detected))
    for k in stable_good.difference(detected):
        print k, gt[k]

# July 17 - August 8 experiment files
experiment_files =  \
['addrprobe-BOTH/addrprobe-2015-07-17-1437131824.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-17-1437146223.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-17-1437160622.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-17-1437175021.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-17-1437189420.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-17-1437189514.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-18-1437203854.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-18-1437218253.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-18-1437232652.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-18-1437247051.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-18-1437261450.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-18-1437275849.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-19-1437290248.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-19-1437304647.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-19-1437319046.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-19-1437333446.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-19-1437347845.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-19-1437362244.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-20-1437376643.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-20-1437391042.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-20-1437405442.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-20-1437419841.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-20-1437434240.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-20-1437448639.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-21-1437463038.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-21-1437477437.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-21-1437491836.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-21-1437506235.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-21-1437520634.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-21-1437535033.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-22-1437549432.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-22-1437563831.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-22-1437578230.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-22-1437592629.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-22-1437607028.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-22-1437621427.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-23-1437635826.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-23-1437650225.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-23-1437664624.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-23-1437679023.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-23-1437693422.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-23-1437707821.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-24-1437722220.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-24-1437722306.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-24-1437736646.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-24-1437751045.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-24-1437765444.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-24-1437779843.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-24-1437794242.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-25-1437808641.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-25-1437823040.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-25-1437837439.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-25-1437851838.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-25-1437866237.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-25-1437880636.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-26-1437895035.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-26-1437909434.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-26-1437923833.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-26-1437938233.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-26-1437952632.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-26-1437967031.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-27-1437981430.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-27-1437995829.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-27-1438010228.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-27-1438024627.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-27-1438039026.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-27-1438053425.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-28-1438067824.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-28-1438082223.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-28-1438096622.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-28-1438111021.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-28-1438125420.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-28-1438125493.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-28-1438139833.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-29-1438154232.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-29-1438168631.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-29-1438183030.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-29-1438197429.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-29-1438211828.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-29-1438226227.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-30-1438240626.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-30-1438255025.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-30-1438269424.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-30-1438283823.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-30-1438298222.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-30-1438312621.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-31-1438327020.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-31-1438327091.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-31-1438341431.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-31-1438355830.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-31-1438370229.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-31-1438384628.gexf',
 'addrprobe-BOTH/addrprobe-2015-07-31-1438399027.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-01-1438413426.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-01-1438427825.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-01-1438442224.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-01-1438456623.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-01-1438471022.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-01-1438485421.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-02-1438499820.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-02-1438499888.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-02-1438514228.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-02-1438528627.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-02-1438543026.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-02-1438557425.gexf',
 'addrprobe-BOTH/addrprobe-2015-08-02-1438571824.gexf',]
