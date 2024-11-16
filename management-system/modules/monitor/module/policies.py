# Политики безопасности
policies = (
    {"src": "com-mobile", "dst": "profile-client"},
    {"src": "profile-client", "dst": "com-mobile"},
    {"src": "profile-client", "dst": "manage-drive"},
    {"src": "profile-client", "dst": "bank-pay"},
    {"src": "manage-drive", "dst": "profile-client"},
    {"src": "bank-pay", "dst": "profile-client"},
    {"src": "manage-drive", "dst": "verify"},
    {"src": "verify", "dst": "auth"},
    {"src": "auth", "dst": "sender-car"},
    {"src": "receiver-car", "dst": "control-drive"},
    {"src": "control-drive", "dst": "sender-car"},
    {"src": "control-drive", "dst": "manage-drive"}
)


def check_operation(event_id, event_details) -> bool:
    """ Проверка возможности совершения обращения. """
    src: str = event_details.get("source")
    dst: str = event_details.get("deliver_to")

    if not all((src, dst)):
        return False

    print(f"[info] checking policies for event {event_id}, {src}->{dst}")

    return {"src": src, "dst": dst} in policies
