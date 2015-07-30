#!/usr/bin/python

import numpy as np
from collections import defaultdict
import matplotlib as mpl
import matplotlib.pyplot as pyplot
import matplotlib.patches as mpatches
import copy as copy
import sys
import random;

random.seed(20)



def get_keys(targets,data_f):
    global ct00


    ct00,my_mints = concern_graph(data_f, targets)
    pretty(ct00,my_mints)

def pick(file, K):
    possible = set();
    with open(file, 'r') as fp:
	for i,line in enumerate(fp):
	    src,tgt,report,ts = line.strip().split(',')
	    possible.add(tgt)
    return random.sample(possible, K)

def concern_graph(fn, good):
    import subprocess

    # Select some random nodes we're interested in
    concern = list(good)[:(140)]
    ct = dict((c,defaultdict(lambda:0)) for c in concern)
    mints = 1e50

    f = open(fn)
    f.next()
    f.next()
    for i,line in enumerate(f):
        src,tgt,report,ts = line.strip().split(',')
        report = int(report)
        ts = float(ts)
        mints = min(mints,ts)
        # Only care about reports about a node
        if tgt in concern:
            ct[tgt][report] += 1
        #if not i%300000: print i
    for k in ct: ct[k] = dict(ct[k])
    f.close()
    return ct, mints


def pretty(ct,mints):
    xs = []
    ys = []
    cs = []
    xraw = []
    ips = []
    dt = {}

    for k in ct:
        dt[k] = dict((ip,count) for ip,count in ct[k].iteritems() if count > 1)
    #ct = dt

    times = []
    for i,ip in list(enumerate(ct))[:(140)]:
        try:
            tmax = max(t for t in ct[ip] if ct[ip][t] > 9)
        except ValueError:
            tmax = max(t for t in ct[ip])


	it = []
	for report in ct[ip]:
	    it.append(report-tmax)

	times.append((i,max(it)))

    times.sort(key=lambda x : x[1])

    exclude = set()
    for t in times[:10]:
	exclude.add(t[0])

    for t in times[110:]:
	exclude.add(t[0])

    xraw = []
    for idx,ip in list(enumerate(ct))[:140]:
        try:
            tmax = max(t for t in ct[ip] if ct[ip][t] > 9)
        except ValueError:
            tmax = max(t for t in ct[ip])

	if idx not in exclude:
	    xraw.append([])
	    for report in ct[ip]:
		ips.append(ip)
		ys.append(len(xraw)-1)
		xs.append((report-tmax)/3600.)
		cs.append(ct[ip][report])
		xraw[-1].append(report-tmax)
	    if not xraw[-1]: xraw.pop()

    pyplot.clf()
    inds = np.argsort(map(max, xraw))
    tbl = copy.copy(inds)
    for i,y in enumerate(inds): tbl[y] = i
    inds = tbl
    for i in range(len(inds)):
        #pyplot.plot([-100,24],[i,i],color=(0.8,0.8,0.8),zorder=2)
        ys[i] = inds[y]
    xs = np.array(xs)
    #xs[np.array(cs) <= 2] = inf
    #xs = np.array(xs)[inds]
    #ys = np.array(ys)[inds]
    #cs = np.array(cs)[inds]
    pyplot.scatter(xs,tbl[ys],c=np.log10(cs),s=4*np.log2(cs)+1,zorder=3,vmin=0,vmax=4)

    yline = -4
    pyplot.plot([-100,100], [yline,yline], 'k')
    pyplot.ylim(-1, 100);
    xs = np.array(xs)
    #xs[np.array(cs) <= 2] = inf
    #scatter(xs,yline-4-np.array(ys)*2,c=np.log10(cs),s=4*np.log2(cs)+1,zorder=3,vmin=0,vmax=5)

    pyplot.xticks(np.arange(-100,100,4))
    pyplot.grid(axis='x')

    mx = np.percentile(xs,99.9)
    pyplot.xlim([-24,16])
    #xlim(mx-16, mx+1)
    bar = (mints-20*60)/3600.
    pyplot.plot([bar,bar],[min(ys)-1,max(ys)+1])
    ax = pyplot.gca()
    import matplotlib.dates
    #ax.xaxis.set_major_locator(matplotlib.dates.AutoDateLocator())
    #pyplot.title('Frequency of addr timestamps about nodes')
    pyplot.ylabel('Nodes (100 randomly chosen out of ~6k)')
    pyplot.xlabel('Time (hours since first time >10 nodes share same timestamp for target node)')

    pyplot.text(14, 93, "1", fontsize=14, weight=500, color='white', 
		horizontalalignment='center', verticalalignment='center', family='sans-serif')
    pyplot.plot(14, 93, 'or', markersize=18, markeredgecolor='red');

    pyplot.text(14, 96, "2", fontsize=14, weight=500, color='white', 
		horizontalalignment='center', verticalalignment='center', family='sans-serif')
    pyplot.plot(14, 96, 'or', markersize=18, markeredgecolor='red');


    fig1 = pyplot.gcf()
    pyplot.colorbar()

    pyplot.show()
    pyplot.draw()

    #fig1.savefig('bubble_plots.eps')

    #pyplot.tight_layout();
    #pyplot.show()
    #pyplot.draw()
    
    #fig1.savefig('bubble_plots.eps')
    #pyplot.close()


def main():
    argv = sys.argv
    if (len(argv) <= 1):
	sys.exit(0);
    elif (len(argv) == 2):
	observations = argv[1]
	target_nodes = pick(observations, 140)
    else:
	observations = argv[1]
	target_nodes = [];
	with open(argv[2], "r") as fp:
	    for lines in fp:
		target_nodes.append(lines.strip())

    get_keys(target_nodes,observations)

if __name__ == '__main__':
    try:
        __IPYTHON__
    except NameError:
        main()
