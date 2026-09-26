"""
model3_federation.adapters.registry
-------------------------------------
Answers the actual question: "can one thing handle every VMS?"

No — a Milestone SDK call, an ONVIF SOAP call, and a REST+API-key call
are genuinely different protocols; nothing can paper over that without
lying about what it's doing. What CAN be one thing is the part that
was actually hardcoded and actually the problem: which adapter TYPES
exist, and what config each one needs, do not have to be Python code
baked into api/router.py's startup list. That's what this file is.

Every adapter type registers itself here once, declaring:
  - a stable `adapter_type` string (stored in vms_systems.adapter_type)
  - CONFIG_FIELDS: what the onboarding form needs to ask for
  - a constructor that takes (system_id, name, config: dict) and
    returns a working VMSAdapter — no adapter-specific code anywhere
    else in the app.

With this in place, "onboard a new VMS" becomes: pick a type that's
already registered (REST+API-key covers Windy and most cloud VMS/
webcam APIs; ONVIF covers the actual IP-camera industry standard that
Hikvision/Dahua/Axis/etc. all speak), fill in its config fields, hit
Test Connection, save. No redeploy. A genuinely new PROTOCOL (a vendor
SDK with no ONVIF/REST support, e.g. proprietary Milestone XProtect
like the simulated police adapter models) still needs a new adapter
class written once — that part can't be config-driven, any more than
a USB driver can be config-driven — but it only needs to be written
ONCE per protocol, not once per department/vendor that happens to
speak a protocol you already support.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from model3_federation.adapters.base import VMSAdapter
import socket
import ipaddress
from urllib.parse import urlparse

def resolve_safe_url(url: str) -> tuple[bool, str, str]:
    if not url:
        return True, "", ""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ["http", "https", "rtsp"]:
            return False, "", ""
        host = parsed.hostname
        if not host:
            return False, "", ""
        
        # Resolve the hostname to prevent DNS rebinding or obfuscated IPs
        ip = socket.gethostbyname(host)
        ip_obj = ipaddress.ip_address(ip)
        
        # Block private, loopback, and link-local ranges
        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
            return False, "", ""
        
        # Block cloud metadata specifically just in case
        if str(ip_obj) == "169.254.169.254":
            return False, "", ""
            
        # Rebuild URL with IP
        pinned_url = parsed._replace(netloc=f"{ip}:{parsed.port}" if parsed.port else ip).geturl()
        return True, pinned_url, host
    except Exception:
        return False, "", ""

def is_safe_url(url: str) -> bool:
    safe, _, _ = resolve_safe_url(url)
    return safe


@dataclass
class ConfigField:
    name: str                  # key in the config dict
    label: str                 # shown on the onboarding form
    type: str = "text"         # "text" | "password" | "number" | "url"
    required: bool = True
    default: Any = None
    help_text: str = ""


@dataclass
class AdapterTypeInfo:
    adapter_type: str
    display_name: str
    config_fields: list[ConfigField]
    factory: Callable[[str, str, dict], VMSAdapter]


_REGISTRY: dict[str, AdapterTypeInfo] = {}


def register_adapter_type(
    adapter_type: str,
    display_name: str,
    config_fields: list[ConfigField],
):
    """Class decorator. Put this on a VMSAdapter subclass that takes
    (system_id, name, config) in its __init__."""
    def _decorator(cls):
        def _factory(system_id: str, name: str, config: dict) -> VMSAdapter:
            return cls(system_id=system_id, name=name, config=config)

        _REGISTRY[adapter_type] = AdapterTypeInfo(
            adapter_type=adapter_type,
            display_name=display_name,
            config_fields=config_fields,
            factory=_factory,
        )
        cls.adapter_type = adapter_type
        return cls
    return _decorator


def list_adapter_types() -> list[dict]:
    """For GET /api/v3/adapter-types — drives the onboarding form's
    adapter-type dropdown and its per-type config fields."""
    return [
        {
            "adapter_type": info.adapter_type,
            "display_name": info.display_name,
            "config_fields": [
                {
                    "name": f.name,
                    "label": f.label,
                    "type": f.type,
                    "required": f.required,
                    "default": f.default,
                    "help_text": f.help_text,
                }
                for f in info.config_fields
            ],
        }
        for info in _REGISTRY.values()
    ]


def validate_config(adapter_type: str, config: dict) -> list[str]:
    """Returns a list of error strings (empty = valid)."""
    info = _REGISTRY.get(adapter_type)
    if info is None:
        return [f"Unknown adapter_type {adapter_type!r}. "
                f"Known types: {', '.join(_REGISTRY) or '(none registered)'}"]
    errors = []
    for f in info.config_fields:
        if f.required and not config.get(f.name):
            errors.append(f"Missing required field: {f.name}")
    return errors


def build_adapter(adapter_type: str, system_id: str, name: str, config: dict) -> VMSAdapter:
    info = _REGISTRY.get(adapter_type)
    if info is None:
        raise ValueError(f"Unknown adapter_type {adapter_type!r}")
    return info.factory(system_id, name, config)


def is_known_type(adapter_type: str) -> bool:
    return adapter_type in _REGISTRY
