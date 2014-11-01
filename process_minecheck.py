from bitcoin.messages import *
import logger
from cStringIO import StringIO
import json
import time
from datetime import datetime
import cPickle as pickle
from collections import defaultdict
import psql_version
import requests
from BeautifulSoup import BeautifulSoup as BS
import glob

def find_winning_transaction(tx):
    assert len(tx.vin) == 1
    prevout = tx.vin[0].prevout
    global url
    url = 'https://blockchain.info/tx/%s?show_adv=true' % (prevout.hash[::-1].encode('hex'),)
    global bs
    bs = BS(requests.get(url).content)
    spentmap = []
    for a in bs.find(attrs={'class':'txtd'}).findChildren('a'):
        if 'Spent' in a.text:
            spentmap[-1] = int(a['href'].split('/')[-1])
        else:
            spentmap.append(None)
    if not spentmap[prevout.n]:
        print "bc.i doesn't think tx was spent"
        return
    url = 'https://blockchain.info/tx-index/%d?format=json' % (spentmap[prevout.n],)
    tx = json.loads(requests.get(url).content)
    print 'tx:', tx['hash']
    if 'block_height' in tx:
        print 'included in block:', tx['block_height']
        return tx['hash']
    else:
        return None

def process_minecheck(manifest, logs):
    # First, look for transactions sent
    global sent, recvd, getdata
    getdata = defaultdict(lambda:set())

    hasreject = defaultdict(bool)
    haswrongflake = defaultdict(bool)
    hasflake = defaultdict(bool)
    txreceived = defaultdict(set)

    global flakes
    flakes = [Hash(ser.decode('hex')) for ser in manifest['flakes']]

    # Find winning transaction in this list of flakes
    f = CTransaction.stream_deserialize(StringIO(manifest['flakes'][0].decode('hex')))
    winner = find_winning_transaction(f)

    for log in logs:
        msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))

        if msg.command == 'reject':
            print 'reject found', log.handle_id, msg
            hasreject[log.handle_id] = True

        if msg.command == 'tx' and not log.is_sender:
            h = Hash(msg.tx.serialize())
            txreceived[log.handle_id].add(h)

    # Interpret the sanity checks
    if 'cohorots' in manifest: manifest['cohorts'] = manifest['cohorots'] #whops
    for i,cohort in enumerate(manifest['cohorts']):
        #print 'cohort', i
        for n in cohort:
            if txreceived[n]:
                if txreceived[n] == set([flakes[i]]):
                    hasflake[n] = True
                elif flakes[i] in txreceived[n]:
                    pass
                    print 'node', n, 'has flake, but also others:', [flakes.index(_) if _ in flakes else '???' for _ in txreceived[n]]
                else:
                    pass
                    print 'node', n, 'has wrong flakes:', [flakes.index(_) if _ in flakes else '???' for _ in txreceived[n]]
        print i, 'hasflake:', len([n for n in cohort if hasflake[n]]), 'of', len(cohort), len([n for n in cohort if not txreceived[n]])

    #print [n for n in txreceived if flakes[88] in txreceived[n]]
    if winner:
        winners = manifest['cohorts'][flakes.index(winner.decode('hex')[::-1])]
        tagalongs = [n for n in txreceived if winner.decode('hex')[::-1] in txreceived[n] and n not in winners]
        return winners, tagalongs
    else:
        return None

def process_all_minechecks():
    global all_winners, all_tagalongs
    all_winners = []
    all_tagalongs = []
    logfiles = sorted(glob.glob('experiment_logs/logs_minecheck-*.pkl'))[-20:]
    import re
    ts = [re.findall('logs_minecheck-(.*).pkl', s)[0] for s in logfiles]
    for t in ts:
        global manifest
        global logs
        print 'Timestamp:', t
        timestamp = datetime.fromtimestamp(float(t.split('-')[-1]))

        manifest = json.load(open('experiment_logs/experiment_minecheck-%s.json' % t))
        logs = pickle.load(open('experiment_logs/logs_minecheck-%s.pkl' % t))
        try:
            winners, tagalongs = process_minecheck(manifest, logs)
            all_winners.append(winners)
            all_tagalongs.append(tagalongs)
        except Exception:
            pass

def frequency(xs):
    from collections import defaultdict
    d = defaultdict(int)
    for x in xs: d[x] += 1
    return dict(d)
