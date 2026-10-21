from __future__ import annotations

import ssl
from importlib import resources

import httpx

_RUSSIAN_TRUSTED_ROOT_CA = "certs/russian_trusted_root_ca.crt"


def create_moex_ssl_context(*, trust_env: bool) -> ssl.SSLContext:
    """Create HTTPX's normal TLS context with the MOEX-required root added."""
    context = httpx.create_ssl_context(verify=True, trust_env=trust_env)
    certificate = resources.files("moexalgo").joinpath(_RUSSIAN_TRUSTED_ROOT_CA).read_text(encoding="ascii")
    context.load_verify_locations(cadata=certificate)
    return context
