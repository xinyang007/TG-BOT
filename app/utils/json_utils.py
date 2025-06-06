import json
from typing import Any, Dict
from enum import Enum


def safe_json_dumps(obj: Any, **kwargs) -> str:
    def default_serializer(obj):
        if isinstance(obj, Enum):
            return obj.value
        if hasattr(obj, '__dict__'):
            return {k: v for k, v in obj.__dict__.items() if not k.startswith('_')}
        return str(obj)

    return json.dumps(obj, default=default_serializer, ensure_ascii=False, **kwargs)


def safe_bot_status(bot_instance) -> Dict:
    if not bot_instance:
        return {"error": "bot_instance is None"}

    try:
        return {
            "bot_id": str(getattr(bot_instance, 'bot_id', 'unknown')),
            "status": getattr(bot_instance.status, 'value', str(getattr(bot_instance, 'status', 'unknown'))),
            "health_score": getattr(bot_instance, 'health_score', 100),
            "consecutive_failures": getattr(bot_instance, 'consecutive_failures', 0),
            "request_count": getattr(bot_instance, 'request_count', 0),
            "last_error": getattr(bot_instance, 'last_error', None),
            "last_heartbeat": getattr(bot_instance, 'last_heartbeat', 0),
        }
    except Exception as e:
        return {"error": f"serialization_failed: {str(e)}"}