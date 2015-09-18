# Create and sign a transaction with a bogus key

from binascii import hexlify, unhexlify
from bitcoin.core import *
from bitcoin.core.key import *
from bitcoin.core.script import *
from bitcoin.core.scripteval import *
from bitcoin import base58
from bitcoin.messages import *
from bitcoin.wallet import CBitcoinAddress, CBitcoinSecret

class ATransaction(CMutableTransaction):

    def append_txout(self, txout):
        assert isinstance(txout,ATxOut) and txout._tx is None and txout._idx is None
        txout._tx = self
        txout._idx = len(self.vout)
        self.vout.append(txout)

    def finalize(self):
        # Make sure all the txins are ready
        for txin in self.vin:
            txin.setprevout()

        for idx,txin in enumerate(self.vin):
            scriptSig = txin.sign(self, idx)
            self.vin[idx] = CTxIn(prevout=txin.prevout, scriptSig=scriptSig)
        self.GetHash()

class ATxOut(CMutableTxOut):
    def __init__(self, *args, **kwargs):
        super(ATxOut,self).__init__(*args, **kwargs)
        self._tx = None
        self._idx = None

    @property
    def prevout(self):
        assert self._tx is not None
        assert self._idx is not None
        return COutPoint(self._tx.GetHash(), self._idx)

class ATxIn(CMutableTxIn):
    @classmethod
    def from_p2pkh(cls, seckey, prevout=None):
        assert type(seckey) == CBitcoinSecret
        txin = ATxIn(prevout)

        scriptPubKey = CScript([OP_DUP, OP_HASH160, Hash160(seckey.pub), OP_EQUALVERIFY, OP_CHECKSIG])
        address = CBitcoinAddress.from_scriptPubKey(scriptPubKey)

        def sign(tx, idx):
            sighash = SignatureHash(scriptPubKey, tx, idx, SIGHASH_ALL)
            sig = seckey.sign(sighash) + bytes([SIGHASH_ALL])
            assert seckey.pub.verify(sighash, sig)
            assert len(sig) < OP_PUSHDATA1
            scriptSig = CScript([sig, seckey.pub])
            VerifyScript(scriptSig, scriptPubKey, tx, idx, ())
            return scriptSig

        txin.setprevout = lambda:None
        txin.sign = sign
        return txin


def txpair_from_p2pkh(seckey, nValue):
    assert type(seckey) == CBitcoinSecret
    scriptPubKey = CScript([OP_DUP, OP_HASH160, Hash160(seckey.pub), OP_EQUALVERIFY, OP_CHECKSIG])
    address = CBitcoinAddress.from_scriptPubKey(scriptPubKey)
    txout = ATxOut(nValue, scriptPubKey)
    txin = ATxIn.from_p2pkh(seckey)

    def setprevout():
        # Need to make sure we have an OutPoint
        #print('setprevout', txout.prevout)
        txin.prevout = txout.prevout

    def sign(tx, idx):
        sighash = SignatureHash(scriptPubKey, tx, idx, SIGHASH_ALL)
        sig = seckey.sign(sighash) + bytes([SIGHASH_ALL])
        assert seckey.pub.verify(sighash, sig)
        assert len(sig) < OP_PUSHDATA1
        scriptSig = CScript([sig, seckey.pub])
        VerifyScript(scriptSig, scriptPubKey, tx, idx, (SCRIPT_VERIFY_P2SH,))
        return scriptSig

    txin.setprevout = setprevout
    txin.sign = sign
    return txout, txin
    

def tx_from_CTransaction(ctx):
    """
    The base case (a Tx, TxIn, or TxOut with no predecessor) can only be a 
    transaction. It can't be a TxIn, since a signing a transaction requires
    loading the scriptPubKey from the underlying TxOut. It can't be a TxOut,
    since a TxOut is identified by the hash of the Tx it's contained in.
    """
    tx = Transaction()
    tx._ctx = ctx
    for idx,ctxout in enumerate(tx._ctx.vout):
        txout = TxOut(ctxout.scriptPubKey, ctxout.nValue)
        txout._idx = idx
        txout._tx = tx
        tx.vout.append(txout)
    return tx



def tx_coinbase(height):
    # Makes a coinbase transaction with a single input
    tx = Transaction()
    ctxin = CTxIn()
    ctxin.prevout.hash = 0
    ctxin.prevout.n = 0xffffffff
    # after v2, coinbase scriptsig must begin with height
    ctxin.scriptSig = CScript(chr(0x03) + struct.pack('<I', height)[:3])
    tx._ctx.vin.append(txin)
    return tx
