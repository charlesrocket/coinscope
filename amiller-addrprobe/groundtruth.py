import networkx as nx
import parsers
import gzip
import glob

# Compare edge and 2hr-addr data with ground truth

# gtlabels = """
# sc8CBpaWKbeuDLsp,luke-jr,192.3.11.20
# 1OdGvFxAZsqx7loW,petertodd,162.243.165.105
# PdfNu27H9Hw0hhy9,nanotube,8.28.87.106
# cTlI5OuRqvYbnIEE,phantomcircuit,198.27.67.106
# FNJQp6iPaSGKtscC,midnightmagic,162.244.25.88
# U7wuYAe3V8xGQCQN,gtec2-1,54.84.88.123
# rpZUUKxtETrXHzxn,gtec2-2,54.86.44.124
# IJryCKmV2s9GmKnE,gtec2-3,54.187.175.94
# AcnvfVEK8rxGPIqv,gtec2-4,54.187.114.108
# X3fujPTOUSF7lNwB,gtec2-5,54.72.194.70
# """.split()

gtlabels = """
 zt5YCvNG50cLCcmj,netmine-gt-10,54.187.48.148
 JwNCQQAU8gGVzxFy,netmine-gt-11,54.187.173.83
 ofHPuUFVj6ka9FPf,netmine-gt-12,54.69.82.165
 1OdGvFxAZsqx7loW,petertodd,162.243.165.105
 sc8CBpaWKbeuDLsp,luke-jr,192.3.11.20
""".split()

gtlabels = [x.split(',') for x in gtlabels]

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
    import json
    import gzip
    import lzma
    try:
        f = gzip.open(fn)
        d = json.load(f)
    except IOError:
        f = lzma.LZMAFile(fn)
        d = json.loads(f.read())
    except ValueError:
        return dict()
    
    def is_ipv4(addr):
        import socket
        if addr == '127.0.0.1': return False
        try: 
            socket.inet_aton(addr);
            return True
        except: return False

    addrconntime = [(_['addr'].split(':')[0],int(_['conntime'])) for _ in d]
    addrconntime = [(addr,conn) for addr,conn in addrconntime if is_ipv4(addr)]
    return dict(addrconntime)

def peers_versions(fn):
    import json
    import gzip
    import lzma
    try:
        f = gzip.open(fn)
        d = json.load(f)
    except IOError:
        f = lzma.LZMAFile(fn)
        d = json.loads(f.read())
    except ValueError:
        return dict()
    
    def is_ipv4(addr):
        import socket
        if addr == '127.0.0.1': return False
        try: 
            socket.inet_aton(addr);
            return True
        except: return False

    addrconntime = [(_['addr'],_['subver'],_['inbound']) for _ in d]
    addrconntime = [(addr,(conn,subver,inbound)) for addr,conn,inbound in addrconntime if is_ipv4(addr)]
    return dict(addrconntime)

def gt_compare(g, timestamp):
    d = dict((k,v) for (k,_,v) in gtlabels)
    p = gtpeers(timestamp)
    good = [_[0] for _ in g.nodes()]
    for peer in d.values():
        s,t =  p[peer]
        #print 'stable:', len(s), 'transient:', len(t)
        s = s.intersection(good)
        t = t.intersection(good)
        #print 'stable:', len(s), 'transient:', len(t)
        g1 = set([_[1][0] for _ in g.edges((peer,8333))])
        #print 'inferred:', len(g1)
        TP = g1.intersection(t)
        FP = g1.difference(t)
        FN = s.difference(g1)
        print '%16s TP: %3d FP: %3d FN: %3d' % (peer, len(TP), len(FP), len(FN))
        for fn in FN:
            print fn, g.degree((fn,8333))
    
def gtpeers(timestamp):
    d = dict((k,v) for (k,_,v) in gtlabels)
    e = {}
    for label in d:
        s,t = stable('../data/uploads/%s/' % label, timestamp)
        e[d[label]] = s,t
    return e

def compare(g, p):
    if type(g) is nx.DiGraph:
        g_asym = g.to_undirected(False)
        g_sym = g.to_undirected(True)
    else:
        g_asym = g
        g_sym = g
    #print len(g_asym.edges()), len(g_sym.edges())
    print '\t\tGT:  x/y \t\t Asymmetric: (FP,  [afterremoval]/FNx/FNy, TPx/TPy) Symmetric: (FP,  FNx/FNy, TPx/TPy)'

    gtitems = sorted(gtrounds, key=lambda k:gtrounds[k])
    allbad = set()
    for k in flaggediters:
        allbad = allbad.union(set(probe_trials[k]))
    print len(allbad) 
    #print allbad
    #for k,(s,t) in p.iteritems():
    for k in gtitems:
        if k in allbad: continue
        if k not in g_asym: continue
        s,t = p[k]
        x = s = set([_ for _ in s if _ in g])
        y = t = set([_ for _ in t if _ in g])
        asym = set([_[1] for _ in g_asym.edges(k)])
        sym = set([_[1] for _ in g_sym.edges(k)])

        #  Asymmetric: (FP, FNx/FNy, TPx/TPy), Symmetric: (FP, FNx/FNy, TPx/TPy)?
        aFNxC_ = x.difference(asym).difference(set(allbad))
        aFNxC = len(aFNxC_)

        #print 'asym difference'
        #print x.difference(asym)

        aFP = len(asym.difference(y))
        aFNx = len(x.difference(asym))
        aFNy = len(y.difference(asym))
        aTPx = len(asym.intersection(x))
        aTPy = len(asym.intersection(y))
        sFP = len(sym.difference(y))
        sFNx = len(x.difference(sym))
        sFNy = len(y.difference(sym))
        sTPx = len(sym.intersection(x))
        sTPy = len(sym.intersection(y))

        print 'sFNx', x.difference(sym)
        #print 'aFP', asym.difference(y)

        if k in allbad:
            print k, 'flagged for error'

        if 0:
            print '% latex ip:', k
            print '(%d/%d) &   %d & ([%d]/%d/%d) & (%d/%d) & %d & (%d/%d) & (%d/%d)' % \
                (len(s), len(t), aFP, aFNxC, aFNx, aFNy, aTPx, aTPy, sFP, sFNx, sFNy, sTPx, sTPy)
        else:
            print '%15s\tIter[%2d]: GT:%3d/%3d\t Asymmetric: (%3d, [%3d]/%3d/%3d, %3d/%3d) Symmetric: (%3d, %3d/%3d, %3d/%3d) \\\\' % \
                (k, gtrounds[k], len(s), len(t), aFP, aFNxC, aFNx, aFNy, aTPx, aTPy, sFP, sFNx, sFNy, sTPx, sTPy)

        
        #print '%15s\tGT:%3d/%3d\t Detected:%3d/%3d\t  FN:%3d\t FP:%3d TP:%3d' %  \
        #    (k, len(s), len(t), len(asym), len(sym), len(s.difference(asym)),
        #     len(asym.difference(t)), len(asym.intersection(t)))
        #for c in list(set.difference(e,t))[:10]:
        #for c in list(set.intersection(e,s))[:10]:
        #    if c in d:
        #        print c, len(d[c]), len(set(_[1] for _ in d[c].values()))
        #print set.difference(set(v),set(e))

def compare_both(g_edge, g_noedge, p):
    nodes = set.union(set(g_edge),set(g_noedge))
    for k,(s,t) in p.iteritems():
        if k not in nodes:
            print 'skipping %s, not in "good"' % (k,)
            continue
        s = set([_ for _ in s if _ in nodes])
        t = set([_ for _ in t if _ in nodes])

        FP = set()
        FNx = set()
        FNy = set()
        TPx = set()
        TPy = set()
        TPs = set()
        g_edge_un = g_edge.to_undirected(False)
        for b,a in g_edge_un.edges(k):
            # False positive: detected, but not even in the union
            if a not in t: FP.add(a)
            if a in t: TPx.add(a)
            if a in s: TPy.add(a)
            if g_edge.has_edge(a,b) and g_edge.has_edge(b,a) and a in t:
                TPs.add(a)
        for a in s:
            # False Negative:
            if g_noedge.has_edge(k, a) and g_noedge.has_edge(a, k): FNx.add(a)
        for a in t:
            # False Negative:
            if g_noedge.has_edge(k, a) and g_noedge.has_edge(a, k): FNy.add(a)

        e = g_edge_un.edges(k)
        # False negative: 

        if 0: # latex dump
            print '% GT:x/y Detected FP FNx/FNy TPx/y (s)'
            print '%d/%d & %d & %d & %d/%d & %d/%d (%d) \\\\' %  \
            (len(s), len(t), len(e), len(FP), len(FNx), len(FNy), len(TPx), len(TPy), len(TPs))
        else:
            print '%15s\tGT:(%3d/%3d)\t Detected:%3d\t FP:%3d FN:%3d/%3d\t  TP:%3d/%d (%d)' %  \
                (k, len(s), len(t), len(e), len(FP), len(FNx), len(FNy), len(TPx), len(TPy), len(TPs))



def is_ipython():
    try: __IPYTHON__; return True
    except: return False

if __name__ == '__main__' and not is_ipython():
    import sys
    if len(sys.argv) < 3:
        print 'groundtruth for addrobe results'
        print 'usage: groundtruth.py <edges-*.7z file> <groundtruthlocation>'
    else:
        edgefile = sys.argv[1]
        gtdir = sys.argv[1]
        timestamp = parsers.date_from_edgefn(edgefile)
        p = gtpeers(timestamp)
        # substitute timefile
        timefile = edgefile.replace('edges-','time-').replace('.7z','')
        # find timestamp
        times = parsers.parse_times(timefile)
        good = parsers.parse_good(edgefile)
        g_edge, g_no_edge = parsers.parse_edges_7z(edgefile, times, good)
        compare_both(g_edge, g_no_edge, p)
