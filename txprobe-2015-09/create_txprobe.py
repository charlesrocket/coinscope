from binascii import hexlify, unhexlify
from bitcoin.core import *
from bitcoin.core.key import *
from bitcoin.core.script import *
from bitcoin.core.scripteval import *
from bitcoin import base58
from bitcoin.messages import *
import time
#from cStringIO import StringIO
from io import StringIO
#from test_createtx import Transaction, void_coinbase, k, txpair_from_p2sh, get_txin_second
import logger
from imp import reload
import txtools; reload(txtools)
from txtools import *
from connector import *

from bitcoin.wallet import CBitcoinAddress, CBitcoinSecret

# Testnet addresses
from bitcoin import SelectParams
#SelectParams('mainnet')
SelectParams('testnet')

# Main address we'll use
# Incoming transaction from the testnet
# http://btc.blockr.io/api/v1/tx/raw/60c1f1a3160042152114e2bba45600a5045711c3a8a458016248acec59653471

ec_secret = '0C28FCA386C7A227600B2FE50B7CAE11EC86D3BF1FBE471BE89827E19D72AA1D'
seckey = CBitcoinSecret.from_secret_bytes(bytes.fromhex(ec_secret))
default_scriptPubKey = CScript([OP_DUP, OP_HASH160, Hash160(seckey.pub), OP_EQUALVERIFY, OP_CHECKSIG])
address = CBitcoinAddress.from_scriptPubKey(default_scriptPubKey)


# Constants
bigfee = 0.0001*COIN
mintxfee = 0.00001*COIN
dustlimit = 5460


def create_fanout(inputs, n, nValue, input_value=0, change_address=None, fee=0.0001*COIN):
    for inp in inputs: assert isinstance(inp, ATxIn)
    tx = ATransaction()
    tx.vin = inputs

    txouts = []
    for i in range(n):
        txout = ATxOut(nValue, address.to_scriptPubKey())
        tx.append_txout(txout)
        
    if input_value > 0:
        # Change is sent to the default as P2KH
        assert change_address is not None and isinstance(change_address,CBitcoinAddress)
        change = input_value - n*nValue - fee
        tx.vout.append(CTxOut(nValue=change, scriptPubKey=default_scriptPubKey))

    tx.finalize()

    txins = []
    for idx,txout in enumerate(tx.vout[:n]):
        prevout = COutPoint(tx.GetHash(), i)
        txins.append( ATxIn.from_p2pkh(seckey, prevout) )
    return tx, txins


# Testnet coins from faucet
input_0 = ATxIn.from_p2pkh(seckey, COutPoint(lx('25056a38e8cb69ce14701f851a6445e72f2e3bd210faf5e8f4c256a5735b1ec1'),0))
input_value = 0.35*COIN
tx,_ = create_fanout([input_0], 60, dustlimit+mintxfee, 0.35*COIN, address, bigfee)

input_a = ATxIn.from_p2pkh(seckey, COutPoint(lx('24ad5877747097a5a6cfba14385037f9df9df6d801bc316ce0ed40a78fb6c490'),6))
input_b = ATxIn.from_p2pkh(seckey, COutPoint(lx('24ad5877747097a5a6cfba14385037f9df9df6d801bc316ce0ed40a78fb6c490'),7))


def create_cleansers(input3, n, fraction=0.01):
    """
      CLEANSER:
         spends input3, has one output
      CLEANSERS[i]:
         all of these spend input3
         they should be "grinded" to have low tx id, to clear more room
    """
    # Cleanser
    CLEANSER = ATransaction()
    CLEANSER.vin = [input3]
    _cleanser_out,tx_cleanser_in = txpair_from_p2pkh(seckey, nValue=dustlimit)
    CLEANSER.append_txout(_cleanser_out)

    for i in range(n):
        # Try to find a low value
        pass

def create_txprobe(inputs, n):
    """
    Args:
         input should be exactly 3

    Creates several kinds of transactions:
      PARENT[i]:
         spends input1
         creates output p[i]
      MARKER[i]:
         spends input2, and p[i]
         creates output o[i] for recovery.
     
      FLOOD:
         spends input1, blocks parent[i]         
    """
    input1, input2 = inputs
    assert isinstance(input1, ATxIn)
    assert isinstance(input2, ATxIn)

    PARENTS = []
    ORPHANS = []
    for i in range(n):
        tx_parent = ATransaction()
        tx_parent.vin = [input1]

        _tx_parent_out,tx_parent_in = txpair_from_p2pkh(seckey, nValue=dustlimit)
        tx_parent.append_txout(_tx_parent_out)
        tx_parent.finalize()
        PARENTS.append(tx_parent)

        tx_orphan = ATransaction()
        tx_orphan.vin = [input2, tx_parent_in]
        _tx_orphan_out,tx_orphan_in = txpair_from_p2pkh(seckey, nValue=dustlimit*2-mintxfee)
        tx_orphan.append_txout(_tx_orphan_out)
        tx_orphan.finalize()
        ORPHANS.append(tx_orphan)

    # Flood transaction
    FLOOD = ATransaction()
    FLOOD.vin = [input1]
    _flood_out,tx_flood_in = txpair_from_p2pkh(seckey, nValue=dustlimit)
    FLOOD.append_txout(_flood_out)
    FLOOD.finalize()

    return PARENTS, ORPHANS, FLOOD

def test1():
    for i in range(6,60,2):
        input_a = ATxIn.from_p2pkh(seckey, COutPoint(lx('24ad5877747097a5a6cfba14385037f9df9df6d801bc316ce0ed40a78fb6c490'),i))
        input_b = ATxIn.from_p2pkh(seckey, COutPoint(lx('24ad5877747097a5a6cfba14385037f9df9df6d801bc316ce0ed40a78fb6c490'),i+1))
        PARENTS, ORPHANS, FLOOD = create_txprobe([input_a,input_b], 60)
        payload = make_payload(PARENTS, ORPHANS, FLOOD)
        fn = 'payloads/%s-%d.json' % ('24ad5877747097a5a6cfba14385037f9df9df6d801bc316ce0ed40a78fb6c490',i)
        json.dumps(payload, open(fn,'w'))
        print(fn)

def make_payload(PARENTS, ORPHANS, FLOOD):
    d = dict(parents=[b2x(tx.serialize()) for tx in PARENTS], 
             orphans=[b2x(tx.serialize()) for tx in ORPHANS],
             flood=b2x(FLOOD.serialize()))

def make_experiment2(path='./experiment2_payload.dat'):
    import time
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    #socket.create_connection
    sock.connect("/tmp/bitcoin_control")

    # Reset all the connections
    print('Resetting connections')
    n = 79
    cmsg = command_msg(commands.COMMAND_DISCONNECT, 0, [targets.BROADCAST])
    ser = cmsg.serialize()
    do_send(sock, ser)
    for i in range(1,n+1):
        msg = connect_msg('127.0.0.1', 8332+i, '0.0.0.0', 0)
        ser = msg.serialize()
        do_send(sock, ser)
    print('Connecting')
    time.sleep(2)

    nodes = list(get_cxns())
    print('Nodes:', nodes)

    import math
    sn = int(math.ceil(math.sqrt(n)))
    sched = schedule(range(n))
    print('sqrt(n):', sn)
    print('schedule:', len(sched))

    # 1. Create a setup transaction with enough inputs for 2 boosters per trial
    tx_setup = Transaction()
    tx_setup.vin = [get_txin_second()]
    tx_setup_ins = []
    for _ in sched:
        for _ in range(2):
            _out,_in = txpair_from_p2sh(nValue=0.01*COIN)
            tx_setup.append_txout(_out)
            tx_setup_ins.append(_in)
    tx_setup.finalize()

    # 1a. Add tx_setup to a block
    block = make_block()
    block.vtx.append(tx_setup._ctx)
    block.hashMerkleRoot = block.calc_merkle_root()

    PAYLOADS = []
    for i,(tgt,tst) in enumerate(sched):
        PARENTS, ORPHANS, FLOOD = create_txprobe(tx_setup_ins[2*i+0], tx_setup_ins[2*i+1], len(tgt))
        PAYLOADS.append((PARENTS, ORPHANS, FLOOD))
    return nodes, block, PAYLOADS


def check_logs(nodes, PARENTS, ORPHANS, FLOOD, logs):
    orphan_hashes = [Hash(o._ctx.serialize()) for o in ORPHANS]
    d = dict(zip(orphan_hashes, nodes))
    edges = set()
    for log in logs:
        if log.is_sender: continue
        msg = MsgSerializable.stream_deserialize(StringIO('\xf9'+log.bitcoin_msg))
        if msg.command != 'getdata': continue
        print(log.handle_id)
        connected = set(nodes)
        connected.remove(log.handle_id) # Remove self
        for i in msg.inv:
            connected.remove(d[i.hash])
        for i in connected:
            edges.add(tuple(sorted((log.handle_id-min(nodes)+1,i-min(nodes)+1))))
    for i,j in sorted(edges):
        print(i, '<->', j)
        yield(i,j)

def check_all_logs(nodes, PAYLOADS, logs):
    sched = schedule(nodes)
    edges = set()

    # First determine the edges to pay attention to
    d = {}
    expected = dict((n,[]) for n in nodes)
    assert(len(PAYLOADS) == len(sched))
    for (tgt,tst),(PARENTS,ORPHANS,_) in zip(sched,PAYLOADS):
        orphan_hashes = [Hash(o._ctx.serialize()) for o in ORPHANS]
        assert(len(orphan_hashes) == len(tgt))
        d.update(dict(zip(orphan_hashes, tgt)))
        for n in tst: expected[n] += orphan_hashes
    for n in nodes: expected[n] = set(expected[n])

    actual = dict((n,[]) for n in nodes)
    for log in logs:
        if log.is_sender: continue
        msg = MsgSerializable.stream_deserialize(StringIO('\xf9'+log.bitcoin_msg))
        if msg.command != 'getdata': continue
        for i in msg.inv:
            if i.hash in expected[log.handle_id]: 
                actual[log.handle_id].append(i.hash)
                
    for n in nodes: actual[n] = set(actual[n])

    for i in nodes:
        for h in expected[i]:
            j = d[h]
            if h not in actual[i]:
                edges.add(tuple(sorted((j-min(nodes)+1,i-min(nodes)+1))))

    for i,j in sorted(edges):
        print(i, '<->', j)
        yield(i,j)

def run_experiment2(nodes, block, PAYLOADS):
    import time

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    #socket.create_connection
    sock.connect("/tmp/bitcoin_control")

    # Set up a sending thread and queue
    from threading import Lock, Thread
    lock = Lock()

    # Helper functions
    def register_block(blk):
        m = msg_block()
        m.block = blk
        cmsg = bitcoin_msg(m.serialize())
        ser = cmsg.serialize()
        lock.acquire()
        do_send(sock, ser)
        rid = sock.recv(4)
        lock.release()
        rid, = unpack('>I', rid)  # message is now saved and can be sent to users with this id
        return rid

    def register_tx(tx):
        m = msg_tx()
        m.tx = tx._ctx
        cmsg = bitcoin_msg(m.serialize())
        ser = cmsg.serialize()
        lock.acquire()
        do_send(sock, ser)
        rid = sock.recv(4)
        lock.release()
        rid, = unpack('>I', rid)  # message is now saved and can be sent to users with this id
        return rid

    def register_inv(txs):
        m = msg_inv()
        for tx in txs:
            inv = CInv()
            inv.type = 1 # TX
            inv.hash = Hash(tx._ctx.serialize())
            m.inv.append(inv)
        cmsg = bitcoin_msg(m.serialize())
        ser = cmsg.serialize()
        lock.acquire()
        do_send(sock, ser)
        rid = sock.recv(4)
        lock.release()
        rid, = unpack('>I', rid)  # message is now saved and can be sent to users with this id
        return rid

    def broadcast(rid):
        cmsg = command_msg(commands.COMMAND_SEND_MSG, rid, (targets.BROADCAST,))
        ser = cmsg.serialize()
        lock.acquire()
        do_send(sock, ser)
        lock.release()

    def send_to_nodes(rid, nodes):
        cmsg = command_msg(commands.COMMAND_SEND_MSG, rid, nodes)
        ser = cmsg.serialize()
        lock.acquire()
        do_send(sock, ser)
        lock.release()

    # Run the experiment!
    print('Setup')
    broadcast(register_block(block))

    sched = schedule(nodes)
    global logs, all_logs
    all_logs = []
    print('Reading')
    logsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    logsock.connect("/tmp/logger/clients/bitcoin_msg")

    for (target_set, test_set), (PARENTS, ORPHANS, FLOOD) in zip(sched, PAYLOADS):
        def g(sets, inputs):
            (target_set, test_set) = sets
            (PARENTS, ORPHANS, FLOOD) = inputs
            print("Targets:", target_set)

            print('Step 1: inv blocking')
            broadcast(register_inv(PARENTS + [FLOOD]))
            time.sleep(1)

            print('Step 2: send the flood')
            send_to_nodes(register_tx(FLOOD), test_set)

            print('Step 3: prime the orphans')
            for n,orphan in zip(target_set,ORPHANS):
                send_to_nodes(register_tx(orphan), (n,))

            time.sleep(3) # Make sure the flood propagates

            print('Step 4: send parents')
            for n,parent in zip(target_set,PARENTS):
                send_to_nodes(register_tx(parent), (n,))
            time.sleep(10)

            print('Step 5: read back')
            send_to_nodes(register_inv(ORPHANS), test_set)
        Thread(target=g,args=((target_set, test_set), (PARENTS, ORPHANS, FLOOD))).start()
        #g()

    logs = []
    deadline = time.time() + 20
    def _read_logs():
        while(True):
            logsock.settimeout(deadline - time.time())
            try:
                length = logsock.recv(4, socket.MSG_WAITALL);
                length, = unpack('>I', length)
                logsock.settimeout(deadline - time.time())
                record = logsock.recv(length, socket.MSG_WAITALL)
            except socket.timeout: break
            log_type, timestamp, rest = logger.log.deserialize_parts(record)
            log = logger.type_to_obj[log_type].deserialize(timestamp, rest)
            logs.append(log)
            logsock.settimeout(None)
        print('Done')
    t = Thread(target=_read_logs)
    t.start()
    t.join()
