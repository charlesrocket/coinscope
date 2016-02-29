from binascii import hexlify, unhexlify
from bitcoin.core import *
from bitcoin.core.key import *
from bitcoin.core.script import *
from bitcoin.core.scripteval import *
from bitcoin import base58
from bitcoin.messages import *
import time
import sys
if sys.version_info.major < 3: from cStringIO import StringIO
else: from io import StringIO
import json
from threading import Thread
from collections import defaultdict

from imp import reload
import txtools; reload(txtools)
from txtools import *
from conntools import ConnectorSocket

from bitcoin.wallet import CBitcoinAddress, CBitcoinSecret

# Testnet addresses
from bitcoin import SelectParams
SelectParams('mainnet')
#SelectParams('testnet')

# Main address we'll use

# Randomly generated key on Sep 19, 2015
ec_secret = '794a5e9c348fd864cadade0a6dabfb0154b080baa74ba6244b4f0ed553e2674c'
#ec_secret = '0C28FCA386C7A227600B2FE50B7CAE11EC86D3BF1FBE471BE89827E19D72AA1D'
seckey = CBitcoinSecret.from_secret_bytes(unhexlify(ec_secret))
default_scriptPubKey = CScript([OP_DUP, OP_HASH160, Hash160(seckey.pub), OP_EQUALVERIFY, OP_CHECKSIG])
address = CBitcoinAddress.from_scriptPubKey(default_scriptPubKey)

prevout = COutPoint(lx('78aefb16f8702b5ddb677649811c87a0047a243bed15c99d53197adbdc1880e7'), 0)
input_value = 0.0004*COIN

# Constants
bigfee = 0.0003*COIN
#bigfee = 30000
mintxfee = 0.00001*COIN
dustlimit = 5460

def create_spend(inputs, input_vaue=0, fee=bigfee):
    for inp in inputs: assert isinstance(inp, ATxIn)
    tx = ATransaction()
    tx.vin = inputs

    nValue = input_value-fee
    txout = ATxOut(nValue, address.to_scriptPubKey())
    tx.append_txout(txout)

    tx.finalize()
    return tx
    
spend1 = create_spend([ATxIn.from_p2pkh(seckey, prevout)], input_value, bigfee)
spend2 = create_spend([ATxIn.from_p2pkh(seckey, prevout)], input_value, bigfee)

from conntools import ConnectorSocket, read_logs_until
import socket

def do_invblock(tx):
    import time
    sock = ConnectorSocket("/container_wide/connector-dreyfus/bitcoin_control")

    rid = sock.register_inv(20*[tx.GetHash()])
    print('Broadcasting inv')
    sock.broadcast(rid)

    print('hash:', b2lx(tx.GetHash()))
    print('Tx:', b2x(tx.serialize()))

    dmap = defaultdict(lambda:'unknown', [(hid,ip) for ip,hid in sock.get_cxns()])

    print('Forking a thread to capture log messages')
    global logs
    logs = []
    deadline = time.time() + 120 # Wait for two minutes
    logsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    logsock.connect("/container_wide/connector-dreyfus/logger/clients/bitcoin_msg")

    def _read_logs():
        # Filter only messages we care about
        for log in read_logs_until(logsock, deadline):
            msg = MsgSerializable.stream_deserialize(StringIO(log.bitcoin_msg))
            if msg is None: continue
            if msg.command not in ('reject', 'tx', 'getdata', 'inv', 'notfound'): continue
            if msg.command == 'inv' and not log.is_sender:
                hashes = set(m.hash for m in msg.inv)
                if tx.GetHash() in hashes:
                    print('Received matching inv from:', dmap[log.handle_id], log.handle_id)
            logs.append(log)

    #t = Thread(target=_read_logs)
    #t.start()
    #t.join()
    _read_logs()

def send_to_getaddr(tx):
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

    print("Sending orphans")
    for rid in rid_cleansers:
        sock.send_to_nodes(rid, nodes)

    print("Sending parent")
    sock.send_to_nodes(rid_cleanser, nodes)

    print("Done")
