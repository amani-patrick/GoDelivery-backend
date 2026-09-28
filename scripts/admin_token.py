"""Mint an ADMIN JWT for test harnesses — the same claims format the backend
issues (middleware.Claims: uid + role + RegisteredClaims, HS256).

The ADMIN user itself is seeded by scripts/seed_platform_treasury.sql; there
is no self-registration path for privileged roles (by design, that's RBAC).
Reads JWT_SECRET from backend/.env so the token is valid against the running
stack.
"""

import base64
import hashlib
import hmac
import json
import os
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(_HERE, "..", ".env")


def jwt_secret():
    with open(ENV_PATH) as f:
        for line in f:
            if line.startswith("JWT_SECRET="):
                return line.strip().split("=", 1)[1]
    raise RuntimeError("JWT_SECRET not found in backend/.env")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def mint_admin_token(secret: str = None, user_id: str = "platform-treasury",
                     hours: int = 2) -> str:
    secret = secret or jwt_secret()
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    claims = {
        "uid": user_id,
        "role": "ADMIN",
        "exp": now + hours * 3600,
        "iat": now,
    }
    h = _b64url(json.dumps(header, separators=(",", ":")).encode())
    p = _b64url(json.dumps(claims, separators=(",", ":")).encode())
    sig = hmac.new(secret.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest()
    return f"{h}.{p}.{_b64url(sig)}"


if __name__ == "__main__":
    print(mint_admin_token())
