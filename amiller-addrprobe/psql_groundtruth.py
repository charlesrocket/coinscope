from __future__ import division
import gzip
import pylzma as lzma
import json
import glob
import psycopg2
from datetime import datetime
from dateutil.parser import parse as parse_date
import re
import time

#gtlabels = """
# zt5YCvNG50cLCcmj,netmine-gt-10,54.187.48.148
# JwNCQQAU8gGVzxFy,netmine-gt-11,54.187.173.83
# ofHPuUFVj6ka9FPf,netmine-gt-12,54.69.82.165
# 1OdGvFxAZsqx7loW,petertodd,162.243.165.105
# sc8CBpaWKbeuDLsp,luke-jr,192.3.11.20
#""".split()

# As of March 29 (version before 0.10)
gtlabels = """
U7wuYAe3V8xGQCQN,netmine-gt-1,52.5.228.129,
rpZUUKxtETrXHzxn,netmine-gt-2,52.5.115.208,
IJryCKmV2s9GmKnE,netmine-gt-3,52.5.194.119,
AcnvfVEK8rxGPIqv,netmine-gt-4,52.4.56.33,
X3fujPTOUSF7lNwB,netmine-gt-5,52.5.228.119,
""".split()

# As of July 29 ()
gtlabels = """
U7wuYAe3V8xGQCQN,netmine-gt-1,
rpZUUKxtETrXHzxn,netmine-gt-2,
IJryCKmV2s9GmKnE,netmine-gt-3,
AcnvfVEK8rxGPIqv,netmine-gt-4,
X3fujPTOUSF7lNwB,netmine-gt-5,
""".split()


def totimestamp(dt, epoch=datetime(1970,1,1)):
    td = dt - epoch
    # return td.total_seconds()
    return (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6 


gtlabels = set(x.split(',')[0] for x in gtlabels)
#gtlabels = dict([(lambda y:(y[0],y[2]))(x.split(',')) for x in gtlabels])

def within_ts(dirs, start, end):
    files = sorted(glob.glob('%s/*.gz' % dirs))
    if not files: return []

    def timestamp(fn):
        return int(fn.split('-')[-1].split('.')[0])
    
    return [fn for fn in files if start <= timestamp(fn) <= end]

def stable(dirs, target):
    raise NotImplemented # TODO: double check this method
    s = set()
    t = set()
    conntime = {}
    #conntime = 
    fns = within_ts(dirs,target-3600*5, target+3600*3)
    #print len(fns)
    #if len(fns): print (target-3600*5,fns[0]), (target+3600*2,fns[-1])
    for fn in fns:
        p = peers(fn)
        v = set(p.keys())
        # Remove all the nodes who reconnect during the experiment
        reconnected = set()
        for vi in v:
            if vi in conntime and p[vi] != conntime[vi]:
                reconnected.add(vi)
        v = v.difference(reconnected)
        conntime.update(p)
                
        if not t: 
            t = set(v)
            s = set(v)
        else: 
            s = set.intersection(s, set(v))
            t = set.union(t, set(v))
    return s, t

def peers(fn):
    try:
        f = gzip.open(fn)
        d = json.load(f)
    except IOError:
        f = lzma.LZMAFile(fn)
        d = json.loads(f.read())
    except ValueError:
        return dict()

    return d

"""
 ts       | timestamp without time zone | not null
 gt_ip    | inet                        | not null
 ip       | inet                        | not null
 port     | integer                     | not null
 inbound  | boolean                     | not null
 subver   | text                        | not null
 conntime | timestamp without time zone | not null
"""
def submit_peerinfo(cur, fn):
    timestamp = int(re.findall('peerinfo-(\d+)\.\d+\.gz$', fn)[0])
    ds = peers(fn)
    print len(ds)
    if not ds: return

    # Find at least one addrlocal?
    localip = None
    for d in ds:
        if not 'addrlocal' in d: continue
        _localip,_ = d['addrlocal'].split(':')
        if localip is None: localip = _localip
        else:
            if not localip == _localip:
                # Libbitcoin appears to cause us to report wrong addrlocal? Investigate!!!
                assert 'libbitcoin' in d['subver'] or 'bitcoinruby' in d['subver']
                print 'LIBBITCOIN!'
    if localip is None: return


    # Process the entries
    for d in ds:
        ts = datetime.fromtimestamp(timestamp)
        label = re.findall('/(\w+)/peerinfo', fn)[0]
        gt_ip = localip

        #gt_ip = gtlabels[label]
        ipv4,ipv6,port = re.findall('^(?:\[(.+)\]|(.+)):(\d+)$', d['addr'])[0]
        ip = ipv4 if ipv4 else ipv6
        inbound = d['inbound']
        subver = d['subver']
        conntime = datetime.fromtimestamp(d['conntime'])
        data = (ts, fn, gt_ip, ip, port, inbound, subver, conntime)
        query = 'INSERT INTO groundtruth (ts, filename, gt_ip, ip, port, inbound, subver, conntime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
        try:
            cur.execute(query, data)
        except psycopg2.IntegrityError:
            print "DUPLICATE:", fn
            return
    conn.commit()

def fetch_filenames():
    conn = psycopg2.connect("dbname=connector")
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT (filename) FROM groundtruth");
    return set([_[0] for _ in cur.fetchall()])

def submit_gt(fns):
    # fns can be built from within_ts
    global conn
    #conn = psycopg2.connect(database="connector", user="litton", password="osnoknom", host="localhost", port=7575)
    conn = psycopg2.connect("dbname=connector")
    filenames = fetch_filenames()
    global count
    count = 0
    for fn in sorted(fns):
        timestamp = datetime.fromtimestamp(int(re.findall('peerinfo-(\d+)\.\d+\.gz$', fn)[0]))
        if timestamp < parse_date('Oct 15 2014'): continue
        label = re.findall('/(\w+)/peerinfo', fn)[0]
        if label not in gtlabels: continue
        #gt_ip = gtlabels[label]

        #date = timestamp.strftime("%F %T")
        print fn
        #if timestamp in dates:
        if fn in filenames:
            print 'exists: continuing'
            continue

        cur = conn.cursor()
        submit_peerinfo(cur, fn)
        conn.commit()
        count += 1


def lookup_groundtruth(gt_ip, timestamp):
    conn = psycopg2.connect("dbname=connector")
    timestamp = datetime.fromtimestamp(timestamp).strftime('%F %T')
    # Look up for a multi-hour range around
    query = "SELECT ip, count(*), MIN(subver), bool_or(inbound), bool_and(inbound) FROM groundtruth WHERE gt_ip = '{gt_ip}' and ts >= (TIMESTAMP '{timestamp}' - interval '2 hours') and ts < (TIMESTAMP '{timestamp}' + interval '2 hours') GROUP BY ip"
    cur = conn.cursor()
    cur.execute(query.format(gt_ip=gt_ip, timestamp=timestamp))
    return dict((d[0], d[1:]) for d in cur.fetchall())
    

# Useful command to find the dates since July 1
# fns = within_ts('/scratch/groundtruth-soc1024/*/', totimestamp(parse_date('July 1 2015')), time.time())
