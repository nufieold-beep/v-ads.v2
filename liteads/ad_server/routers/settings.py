from fastapi import APIRouter
from typing import Dict, Any
import yaml
import os
import threading
from pathlib import Path

router = APIRouter(prefix="/api/v1/settings", tags=["settings"])

config_path = Path("configs/base.yaml")

def load_config() -> Dict[str, Any]:
    if not config_path.exists():
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def save_config(config: Dict[str, Any]):
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

@router.get("/")
async def get_settings():
    return load_config()

@router.put("/")
async def update_settings(new_settings: Dict[str, Any]):
    current_config = load_config()
    # Deep update can get complicated, so we'll just merge top-level keys
    for k, v in new_settings.items():
        if isinstance(v, dict) and isinstance(current_config.get(k), dict):
            current_config[k].update(v)
        else:
            current_config[k] = v
    save_config(current_config)
    return {"status": "ok", "message": "Settings updated. You must restart the server for changes to apply."}

@router.post("/restart")
async def restart_server():
    """Immediately kills the Python process. Docker 'unless-stopped' will naturally restart the container and apply configurations instantly."""
    def kill_process():
        os._exit(1)
    
    # Delay termination slightly to allow HTTP response back to the client
    threading.Timer(1.0, kill_process).start()
    return {"status": "restarting", "message": "Server restarting in 1 second. Please wait..."}

