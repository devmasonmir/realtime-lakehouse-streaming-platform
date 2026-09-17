"""Mock CDC Transactional Producer."""
import json
import time
import uuid

def generate_cdc_event(entity_id: str, op: str = "INSERT") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_STATUS_CHANGED",
        "source_system": "order_service_pg",
        "payload": {
            "order_id": entity_id,
            "status": "SHIPPED",
            "amount": 189.50,
            "op": op
        },
        "timestamp_ms": int(time.time() * 1000),
        "is_deleted": (op == "DELETE")
    }

if __name__ == "__main__":
    print(json.dumps(generate_cdc_event("ord_8821"), indent=2))
