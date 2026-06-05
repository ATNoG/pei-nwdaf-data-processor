"""
Homomorphic Encryption/Decryption transformers for the client SDK (sync versions).
"""
import base64
import logging
import os
import socket
import struct
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    import tenseal as ts
    _TENSEAL = True
except ImportError:
    _TENSEAL = False
    logger.warning("TenSEAL not available — homomorphic transformers will be no-ops")

_POLY_MODULUS_DEGREE = 8192
_COEFF_MOD_BIT_SIZES = [60, 55, 60]
_GLOBAL_SCALE = 2**55


def _build_context():
    ctx = ts.context(
        ts.SCHEME_TYPE.CKKS,
        poly_modulus_degree=_POLY_MODULUS_DEGREE,
        coeff_mod_bit_sizes=_COEFF_MOD_BIT_SIZES,
    )
    ctx.global_scale = _GLOBAL_SCALE
    ctx.generate_relin_keys()
    return ctx


class HomomorphicEncryptionTransformer:
    def __init__(self, fields: list[str], key_dir: str = "./fhe_keys"):
        self.fields = set(fields)
        self.key_dir = key_dir
        if _TENSEAL:
            self._context = self._load_or_create_context(key_dir)
            self.public_context_b64 = base64.b64encode(self._context.serialize()).decode()
        else:
            self._context = None
            self.public_context_b64 = None

    def _load_or_create_context(self, key_dir: str):
        Path(key_dir).mkdir(parents=True, exist_ok=True)
        public_path = os.path.join(key_dir, "ckks_public.bin")
        secret_path = os.path.join(key_dir, "ckks_secret.bin")

        if os.path.exists(public_path) and os.path.exists(secret_path):
            with open(public_path, "rb") as f:
                return ts.context_from(f.read())

        ctx = _build_context()
        with open(secret_path, "wb") as f:
            f.write(ctx.serialize(save_secret_key=True))
        ctx.make_context_public()
        with open(public_path, "wb") as f:
            f.write(ctx.serialize())
        return ctx

    @staticmethod
    def _to_float(value: Any) -> tuple[float, str]:
        if isinstance(value, bool):
            return float(value), "bool"
        if isinstance(value, int):
            return float(value), "int"
        if isinstance(value, float):
            return value, "float"
        if isinstance(value, str):
            try:
                return float(value), "str_float"
            except ValueError:
                pass
            try:
                packed = socket.inet_aton(value)
                return float(struct.unpack("!I", packed)[0]) / 4294967295.0, "ipv4"
            except (socket.error, OSError, struct.error):
                pass
            raise ValueError(f"Cannot convert string {value!r} to float for CKKS encryption.")
        raise ValueError(f"Cannot convert {type(value).__name__} to float for CKKS encryption.")

    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        if not _TENSEAL or self._context is None:
            return data
        result = data.copy()
        encrypted_fields = set(result.get("__fhe_encrypted_fields__") or [])

        for field in self.fields:
            if field not in result:
                continue
            try:
                val, original_type = self._to_float(result[field])
                enc = ts.ckks_vector(self._context, [val])
                result[field] = {
                    "__fhe__": True,
                    "ciphertext": base64.b64encode(enc.serialize()).decode(),
                    "scheme": "CKKS",
                    "original_type": original_type,
                }
                encrypted_fields.add(field)
            except Exception as e:
                logger.warning("Failed to encrypt field %r: %s — leaving plaintext", field, e)

        result["__fhe_context__"] = self.public_context_b64
        result["__fhe_encrypted_fields__"] = sorted(encrypted_fields)
        return result

    def __repr__(self) -> str:
        return f"HomomorphicEncryptionTransformer(fields={len(self.fields)}, key_dir={self.key_dir!r})"


class HomomorphicDecryptionTransformer:
    def __init__(self, fields: list[str], key_dir: str = "./fhe_keys"):
        self.fields = set(fields)
        self.key_dir = key_dir
        if _TENSEAL:
            self._secret_context = self._load_secret_context(key_dir)
        else:
            self._secret_context = None

    def _load_secret_context(self, key_dir: str):
        secret_path = os.path.join(key_dir, "ckks_secret.bin")
        if not os.path.exists(secret_path):
            raise FileNotFoundError(
                f"CKKS secret key not found at {secret_path}. "
                "Generate keys first by instantiating HomomorphicEncryptionTransformer."
            )
        with open(secret_path, "rb") as f:
            return ts.context_from(f.read())

    @staticmethod
    def _restore_type(val: float, original_type: str | None) -> Any:
        if original_type == "ipv4":
            try:
                packed = struct.pack("!I", int(round(val * 4294967295.0)))
                return socket.inet_ntoa(packed)
            except (struct.error, socket.error, OSError):
                return val
        if original_type == "int":
            return int(round(val))
        if original_type == "bool":
            return bool(round(val))
        return val

    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        if not _TENSEAL or self._secret_context is None:
            return data
        result = data.copy()
        encrypted_fields: list[str] = list(result.get("__fhe_encrypted_fields__") or [])
        still_encrypted: list[str] = []

        for field in self.fields:
            blob = result.get(field)
            if not isinstance(blob, dict) or not blob.get("__fhe__"):
                if field in encrypted_fields:
                    still_encrypted.append(field)
                continue
            try:
                ct_key = "ciphertext" if "ciphertext" in blob else "mean"
                raw = base64.b64decode(blob[ct_key])
                enc = ts.ckks_vector_from(self._secret_context, raw)
                decrypted_float = enc.decrypt()[0]
                result[field] = self._restore_type(decrypted_float, blob.get("original_type"))
            except Exception as e:
                logger.warning("Failed to decrypt field %r: %s — leaving encrypted", field, e)
                still_encrypted.append(field)
                continue

            if field in encrypted_fields:
                encrypted_fields.remove(field)

        if still_encrypted or encrypted_fields:
            result["__fhe_encrypted_fields__"] = sorted(set(still_encrypted) | set(encrypted_fields))
        else:
            result.pop("__fhe_encrypted_fields__", None)
            result.pop("__fhe_context__", None)

        return result

    def __repr__(self) -> str:
        return f"HomomorphicDecryptionTransformer(fields={len(self.fields)}, key_dir={self.key_dir!r})"
