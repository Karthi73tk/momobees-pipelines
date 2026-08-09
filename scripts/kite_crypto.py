"""
kite_crypto.py
--------------
Python counterpart to the Next app's src/lib/rebalance/crypto.ts.

Decrypts the AES-256-GCM blobs written by the web app into
public.kite_profiles (kite_access_token_enc / kite_api_secret_enc).

Wire format (must stay byte-for-byte identical to crypto.ts):

    base64( iv[12] || authTag[16] || ciphertext )

The key comes from ENCRYPTION_KEY, a base64 string that decodes to exactly
32 bytes (the same secret the Next app uses). Note that Node appends the GCM
auth tag as a separate 16-byte field *before* the ciphertext, whereas
`cryptography`'s AESGCM.decrypt expects `ciphertext || tag`, so we reassemble.

Requires: cryptography>=42 (added to requirements.txt).
"""
import os
import base64

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_IV_LEN = 12
_TAG_LEN = 16


def _get_key() -> bytes:
    raw = os.environ.get("ENCRYPTION_KEY")
    if not raw:
        raise RuntimeError("Missing ENCRYPTION_KEY environment variable.")
    key = base64.b64decode(raw)
    if len(key) != 32:
        raise RuntimeError("ENCRYPTION_KEY must decode to exactly 32 bytes.")
    return key


def decrypt(blob: str) -> str:
    """Decrypt a base64 blob produced by crypto.ts `encrypt()`. Raises on any
    tampering / wrong key (AESGCM verifies the auth tag)."""
    key = _get_key()
    raw = base64.b64decode(blob)

    iv = raw[:_IV_LEN]
    tag = raw[_IV_LEN:_IV_LEN + _TAG_LEN]
    ciphertext = raw[_IV_LEN + _TAG_LEN:]

    plaintext = AESGCM(key).decrypt(iv, ciphertext + tag, None)
    return plaintext.decode("utf-8")
