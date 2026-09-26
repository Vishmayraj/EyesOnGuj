import os
import json
from typing import Optional, Dict, Any
from cryptography.fernet import Fernet
import logging

logger = logging.getLogger(__name__)

# Fetch the encryption key from environment variable.
# Should be a base64-encoded 32-byte key (e.g. Fernet.generate_key().decode())
ENCRYPTION_KEY = os.getenv("VMS_ENCRYPTION_KEY")

_fernet = None
if ENCRYPTION_KEY:
    try:
        _fernet = Fernet(ENCRYPTION_KEY.encode())
    except Exception as e:
        logger.error(f"Failed to initialize Fernet with VMS_ENCRYPTION_KEY: {e}")

def encrypt_config(config_dict: Dict[str, Any]) -> str:
    """Encrypt a dictionary into a JSON string, then wrap it for JSONB storage if encrypted."""
    config_str = json.dumps(config_dict)
    if _fernet:
        encrypted = _fernet.encrypt(config_str.encode()).decode()
        return json.dumps({"fernet": encrypted})
    
    # Fallback to plain JSON if no encryption key (e.g., local dev without key)
    # Warning: In production, lack of a key means credentials are saved in plaintext.
    return config_str

def decrypt_config(config_data: Any) -> Dict[str, Any]:
    """Decrypt a dictionary that might contain a 'fernet' key back to the original dictionary."""
    if not config_data or not isinstance(config_data, dict):
        return {}
        
    if _fernet and "fernet" in config_data:
        try:
            decrypted = _fernet.decrypt(config_data["fernet"].encode()).decode()
            return json.loads(decrypted)
        except Exception as e:
            logger.error(f"Failed to decrypt config: {e}")
            return {}
            
    # If not encrypted or no key, just return the dict
    return config_data
