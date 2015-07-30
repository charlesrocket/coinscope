import gzip
import pylzma as lzma
import json
import glob
import psycopg2
from datetime import datetime
from dateutil.parser import parse as parse_date
import re

#gtlabels = """
# zt5YCvNG50cLCcmj,netmine-gt-10,54.187.48.148
# JwNCQQAU8gGVzxFy,netmine-gt-11,54.187.173.83
# ofHPuUFVj6ka9FPf,netmine-gt-12,54.69.82.165
# 1OdGvFxAZsqx7loW,petertodd,162.243.165.105
# sc8CBpaWKbeuDLsp,luke-jr,192.3.11.20
#""".split()

gtlabels = """
 U7wuYAe3V8xGQCQN,netmine-gt-1,52.5.228.129,
rpZUUKxtETrXHzxn,netmine-gt-2,52.5.115.208,
IJryCKmV2s9GmKnE,netmine-gt-3,52.5.194.119,
AcnvfVEK8rxGPIqv,netmine-gt-4,52.4.56.33,
X3fujPTOUSF7lNwB,netmine-gt-5,52.5.228.119,
""".split()

gtlabels = dict([(lambda y:(y[0],y[2]))(x.split(',')) for x in gtlabels])

def within_ts(dirs, start, end):
    files = sorted(glob.glob('%s/*.gz' % dirs))
    if not files: return []

    def timestamp(fn):
        return int(fn.split('-')[-1].split('.')[0])
    
    return [fn for fn in files if start <= timestamp(fn) <= end]

def stable(dirs, target):
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
    for d in ds:
        ts = datetime.fromtimestamp(timestamp)
        gt_ip = gtlabels[re.findall('uploads/(.+)/peerinfo', fn)[0]]
        ipv4,ipv6,port = re.findall('^(?:\[(.+)\]|(.+)):(\d+)$', d['addr'])[0]
        ip = ipv4 if ipv4 else ipv6
        inbound = d['inbound']
        subver = d['subver']
        conntime = datetime.fromtimestamp(d['conntime'])
        data = (ts, gt_ip, ip, port, inbound, subver, conntime)
        query = 'INSERT INTO groundtruth (ts, gt_ip, ip, port, inbound, subver, conntime) VALUES (%s, %s, %s, %s, %s, %s, %s)'
        cur.execute(query, data)

def fetch_dates(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT (ts, gt_ip) FROM groundtruth");
    return set([_[0] for _ in cur.fetchall()])

def submit_gt(fns):
    global conn
    #conn = psycopg2.connect(database="connector", user="litton", password="osnoknom", host="localhost", port=7575)
    conn = psycopg2.connect("dbname=connector")
    dates = fetch_dates(conn)
    global count
    count = 0
    for fn in sorted(fns):
        timestamp = datetime.fromtimestamp(int(re.findall('peerinfo-(\d+)\.\d+\.gz$', fn)[0]))
        if timestamp < parse_date('Oct 15 2014'): continue
        label = re.findall('uploads/(.+)/peerinfo', fn)[0]
        if label not in gtlabels: continue
        gt_ip = gtlabels[label]
        date = '("%s",%s)' % (timestamp.strftime("%F %T"), gt_ip)
        print fn, date
        if date in dates: 
            print 'exists: continuing'
            continue
        cur = conn.cursor()
        submit_peerinfo(cur, fn)
        conn.commit()
        count += 1
