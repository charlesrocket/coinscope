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
import json

from imp import reload
import txtools; reload(txtools)
from txtools import *
from conntools import ConnectorSocket

from bitcoin.wallet import CBitcoinAddress, CBitcoinSecret

# Testnet addresses
from bitcoin import SelectParams
#SelectParams('mainnet')
SelectParams('testnet')

# Main address we'll use
# Incoming transaction from the testnet
# http://btc.blockr.io/api/v1/tx/raw/60c1f1a3160042152114e2bba45600a5045711c3a8a458016248acec59653471


# Randomly generated key on Sep 19, 2015
#ec_secret = '794a5e9c348fd864cadade0a6dabfb0154b080baa74ba6244b4f0ed553e2674c'

# Well known example private key (suitable for testnet, not real mainnet)
ec_secret = '0C28FCA386C7A227600B2FE50B7CAE11EC86D3BF1FBE471BE89827E19D72AA1D'
seckey = CBitcoinSecret.from_secret_bytes(unhexlify(ec_secret))
default_scriptPubKey = CScript([OP_DUP, OP_HASH160, Hash160(seckey.pub), OP_EQUALVERIFY, OP_CHECKSIG])
address = CBitcoinAddress.from_scriptPubKey(default_scriptPubKey)


# Constants
bigfee = 0.0001*COIN
#bigfee = 30000
mintxfee = 0.00001*COIN
dustlimit = 5460


def create_fanout(inputs, n, nValue, input_value=0, change_address=None, fee=mintxfee):
    for inp in inputs: assert isinstance(inp, ATxIn)
    tx = ATransaction()
    tx.vin = inputs

    txouts = []
    for i in range(n):
        txout = ATxOut(nValue, address.to_scriptPubKey())
        tx.append_txout(txout)
        
    change = input_value - n*nValue - fee
    if change > 0:
        # Change is sent to the default as P2KH
        assert change_address is not None and isinstance(change_address,CBitcoinAddress)
        tx.vout.append(CTxOut(nValue=change, scriptPubKey=default_scriptPubKey))

    tx.finalize()

    txins = []
    for idx,txout in enumerate(tx.vout[:n]):
        prevout = COutPoint(tx.GetHash(), i)
        txins.append( ATxIn.from_p2pkh(seckey, prevout) )
    return tx, txins


# Testnet coins from faucet
#input_0 = ATxIn.from_p2pkh(seckey, COutPoint(lx('24ad5877747097a5a6cfba14385037f9df9df6d801bc316ce0ed40a78fb6c490'),60))
#input_0 = ATxIn.from_p2pkh(seckey, COutPoint(lx('6125a5997d70b1c423cad698d54c3ad178df837006f0e89f71eaefa053ac40b2'),0))
#input_value = 0.34602400*COIN
input_value = 397600

#input_value = 34204800
#tx,_ = create_fanout([input_0], 60, dustlimit+mintxfee, input_value, address, bigfee)
#tx_fanout = lx('0b50c5f08f51781269bc00244fbcd967c29f945c30492ec0db85ba3baf8c624e')

# Fanout
input_0 = ATxIn.from_p2pkh(seckey, COutPoint(lx('8fca8aae4ae4b75882c7cac46b261f3cf4f31c6be280e3390aa2dfd75ae893be'),3))
print('Creating fanout for:', input_0.prevout)
tx,_ = create_fanout([input_0], 60, dustlimit+mintxfee, input_value, address, bigfee)
print(b2lx(tx.GetHash()))
tx_fanout = lx('c24adf0683a8adb3ae4921022eb68b7b8708b19d948e69facc98ee74bac26f29')

# Sources:
# txids = [_.split('-')[-1].split('.')[0] for _ in sorted(glob.glob('sources/*.hex'))]

def test2():
    for idx in range(4,60):
        input_0 = ATxIn.from_p2pkh(seckey, COutPoint(lx('8fca8aae4ae4b75882c7cac46b261f3cf4f31c6be280e3390aa2dfd75ae893be'),idx))
        print('Creating fanout for:', input_0.prevout)
        tx,_ = create_fanout([input_0], 60, dustlimit+mintxfee, input_value, address, bigfee)
        h = b2lx(tx.GetHash())
        print(h)
        import os
        with open('./source-%s.hex' % h,'w') as f:
            f.write(b2x(tx.serialize()))

def create_cleansers(input3, n=100, fraction=0.01):
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
    CLEANSER.finalize()

    CLEANSERS = []
    for i in range(n):
        tx_cleanser = ATransaction()
        _tx_cleanser_out,_ = txpair_from_p2pkh(seckey, nValue=dustlimit)
        tx_cleanser.append_txout(_tx_cleanser_out)
        while True:
            tx_cleanser.vin = [tx_cleanser_in]
            tx_cleanser.finalize()
            if tx_cleanser.GetHash()[-1] <= fraction*256: break
        CLEANSERS.append(tx_cleanser)
        print(i)
    return CLEANSER, CLEANSERS

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
    MARKERS = []
    for i in range(n):
        tx_parent = ATransaction()
        tx_parent.vin = [input1]

        _tx_parent_out,tx_parent_in = txpair_from_p2pkh(seckey, nValue=dustlimit)
        tx_parent.append_txout(_tx_parent_out)
        tx_parent.finalize()
        PARENTS.append(tx_parent)

        tx_marker = ATransaction()
        tx_marker.vin = [input2, tx_parent_in]
        _tx_marker_out,tx_marker_in = txpair_from_p2pkh(seckey, nValue=dustlimit*2-mintxfee)
        tx_marker.append_txout(_tx_marker_out)
        tx_marker.finalize()
        MARKERS.append(tx_marker)

    # Flood transaction
    FLOOD = ATransaction()
    FLOOD.vin = [input1]
    _flood_out,tx_flood_in = txpair_from_p2pkh(seckey, nValue=dustlimit)
    FLOOD.append_txout(_flood_out)
    FLOOD.finalize()

    return PARENTS, MARKERS, FLOOD

def test1(txid):
    for i in range(0,60,3):
        input_a = ATxIn.from_p2pkh(seckey, COutPoint(txid,i))
        input_b = ATxIn.from_p2pkh(seckey, COutPoint(txid,i+1))
        input_c = ATxIn.from_p2pkh(seckey, COutPoint(txid,i+2))

        PARENTS, MARKERS, FLOOD = create_txprobe([input_a,input_b], 60)
        CLEANSER, CLEANSERS = create_cleansers(input_c)
        assert CLEANSERS[0].vin[0].prevout.hash == CLEANSER.GetHash()

        payload = make_payload(PARENTS, MARKERS, FLOOD, CLEANSER, CLEANSERS)
        fn = 'payloads/%s-%02d.json' % (b2lx(txid),i)
        json.dump(payload, open(fn,'w'))
        print(fn)

def make_payload(PARENTS, MARKERS, FLOOD, CLEANSER, CLEANSERS):
    d = dict(parents=[b2x(tx.serialize()) for tx in PARENTS], 
             markers=[b2x(tx.serialize()) for tx in MARKERS],
             flood=b2x(FLOOD.serialize()),
             cleanser=b2x(CLEANSER.serialize()),
             cleansers=[b2x(tx.serialize()) for tx in CLEANSERS])
    return d
