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
import time
import cPickle as pickle
from cStringIO import StringIO
from conntools import ConnectorSocket, read_logs_until
import socket

from bitcoin import SelectParams
SelectParams('testnet')

gt_ips = ['54.152.175.55',
          '54.172.15.152',
          '54.172.11.64',
          '54.165.251.192',
          '54.152.83.116']


# Prepare the experiment messages
def gettx(txhex): return CTransaction.deserialize(txhex.decode('hex'))
def gethash(tx): return tx.GetHash()


def purge_orphans(CLEANSER, CLEANSERS):
    import time
    sock = ConnectorSocket("/container_wide/connector-testnet/bitcoin_control")

    node_map = dict(sock.get_cxns())
    nodes = node_map.values()
    dmap = {}
    for ip, hid in node_map.iteritems(): dmap[hid] = ip

    nodes = [n for n in nodes if dmap[n] in gt_ips]
    print('GT Nodes:', nodes)

    rid_cleanser = sock.register_tx(CLEANSER)
    rid_cleansers = map(sock.register_tx, CLEANSERS)

    print "Sending orphans"
    for rid in rid_cleansers:
        sock.send_to_nodes(rid, nodes)

    print "Sending parent"
    sock.send_to_nodes(rid_cleanser, nodes)

    print "Done"

# ./payloads/24ad5877747097a5a6cfba14385037f9df9df6d801bc316ce0ed40a78fb6c490-10.json
def run_experiment(payload_fn='24ad5877747097a5a6cfba14385037f9df9df6d801bc316ce0ed40a78fb6c490-10.json'):
    import time
    sock = ConnectorSocket("/container_wide/connector-testnet/bitcoin_control")

    node_map = dict(sock.get_cxns())
    nodes = node_map.values()
    dmap = {}
    for ip, hid in node_map.iteritems(): dmap[hid] = ip

    probeset = [node_map[ip] for ip in gt_ips]
    
    print('Nodes:', nodes)

    # Read the payload, and 
    payload = json.load(open('./payloads/%s' % payload_fn))

    # Make up a fake hash to act as a getdata_key
    getdata_key = lx('9e7da7a0000000') + os.urandom(25)

    manifest = payload
    # Select len(parents) nodes to be test set
    testset = list(set(nodes).difference(probeset))
    import random
    random.shuffle(testset)
    testset = testset[:len(payload['parents'])]

    floodset = list(set(nodes).difference(testset))

    # Write the manifest
    manifest['getdata_key'] = b2lx(getdata_key)
    manifest['probeset'] = probeset
    manifest['testset'] = testset
    manifest['floodset'] = floodset
    manifest['nodes'] = dmap
    manifest['time'] = time.time()
    json.dump(manifest, open('./manifests/manifest-%d-%s' % (manifest['time'],payload_fn),'w'))

    PARENTS = map(gettx, manifest['parents'])
    MARKERS = map(gettx, manifest['markers'])
    FLOOD = gettx(manifest['flood'])
    CLEANSER = gettx(manifest['cleanser'])
    CLEANSERS = map(gettx, manifest['cleansers'])

    assert CLEANSERS[0].vin[0].prevout.hash == CLEANSER.GetHash()
    
    # Set up a reading thread
    from threading import Thread

    print('Setting up transactions')
    rid_invblock = sock.register_inv(map(gethash, PARENTS) + [gethash(FLOOD)])
    rid_invmarkers = sock.register_inv(map(gethash, MARKERS) + [getdata_key])
    rid_getdata = sock.register_getdata(map(gethash, MARKERS) + map(gethash, PARENTS) + [gethash(FLOOD), getdata_key])
    rid_markers = map(sock.register_tx, MARKERS)
    rid_parents = map(sock.register_tx, PARENTS)
    rid_flood = sock.register_tx(FLOOD)
    rid_cleanser = sock.register_tx(CLEANSER)
    rid_cleansers = map(sock.register_tx, CLEANSERS)

    print(rid_markers, rid_parents, rid_flood)

    print('Forking a thread to capture log messages')
    global logs
    logs = []
    deadline = time.time() + 180 # Wait for three minutes
    logsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    logsock.connect("/container_wide/connector-testnet/logger/clients/bitcoin_msg")

    def _read_logs():
        # Filter only messages we care about
        for log in read_logs_until(logsock, deadline):
            msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))
            if msg is None: continue
            if msg.command not in ('reject', 'tx', 'getdata', 'inv', 'notfound'): continue
            logs.append(log)

    t = Thread(target=_read_logs)
    t.start()

    # move this payload to the spent list
    shutil.move('./payloads/%s' % payload_fn, './spent/%d-%s' % (manifest['time'], payload_fn))

    def sleep(t):
        print('Sleeping %d seconds' % t)
        time.sleep(t)

    print "[Purge] Sending orphans"
    for rid in rid_cleansers:
        sock.send_to_nodes(rid, probeset)

    print "[Purge] Sending parent"
    sock.send_to_nodes(rid_cleanser, probeset)

    print('Beginning experiment')

    print('Step 1: inv blocking')
    # Let's block for 4 minutes
    for _ in range(2): sock.broadcast(rid_invblock)
    sleep(20)

    print('Step 2: send the flood')
    sock.send_to_nodes(rid_flood, floodset)
    print 'Letiting flood propagate to targetset and unreachable nodes'
    sleep(15)

    print('Step 3: send parents to each testset node')
    for n,parent in zip(testset,rid_parents):
        sock.send_to_nodes(parent, (n,))
    sleep(30)

    print('Step 4: Reading back to confirm')
    sock.broadcast(rid_getdata)

    print('Step 5: send the markers')
    for n,marker in zip(testset, rid_markers):
        sock.send_to_nodes(marker, (n,))
    sleep(30) #

    print('Step 6: read back')
    sock.send_to_nodes(rid_invmarkers, floodset)
    sock.broadcast(rid_getdata)

    print('Waiting for logs')
    t.join()
    pickle.dump(logs, open('./manifests/logs-%d-%s.pkl' % (manifest['time'],payload_fn),'w'), protocol=2)
