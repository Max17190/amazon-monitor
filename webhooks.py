import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv("endpoint.env")


@dataclass(frozen=True)
class WebhookTarget:
    name: str
    url: str
    role_id: str = ""


LEGACY_WEBHOOK_MAPPINGS = [
    ("BLINK_FNF", "BLINK_FNF_WEBHOOK_URL", "BLINK_FNF_CHANNEL_ID"),
    ("BLINK_MONITORS", "BLINK_MONITORS_WEBHOOK_URL", "BLINK_MONITORS_CHANNEL_ID"),
    ("MATT_FNF", "MATT_FNF_WEBHOOK_URL", "MATT_FNF_CHANNEL_ID"),
]


def load_webhook_targets(env=None):
    """Build named Discord webhook targets from environment variables."""
    env = os.environ if env is None else env
    targets = {}

    for key, value in env.items():
        if not key.startswith("WEBHOOK_") or not key.endswith("_URL"):
            continue

        name = key[len("WEBHOOK_") : -len("_URL")]
        url = str(value).strip()
        if not name or not url:
            continue

        role_id = str(env.get(f"WEBHOOK_{name}_ROLE_ID", "")).strip()
        targets[name] = WebhookTarget(name=name, url=url, role_id=role_id)

    for name, webhook_key, role_key in LEGACY_WEBHOOK_MAPPINGS:
        url = str(env.get(webhook_key, "")).strip()
        if not url or name in targets:
            continue

        role_id = str(env.get(role_key, "")).strip()
        targets[name] = WebhookTarget(name=name, url=url, role_id=role_id)

    if not targets:
        logging.warning("No Discord webhook targets configured")

    return targets


WEBHOOK_TARGETS = load_webhook_targets()
WEBHOOK_CONFIG = {
    target.url: target.role_id for target in WEBHOOK_TARGETS.values() if target.role_id
}
WEBHOOK_URLS = [target.url for target in WEBHOOK_TARGETS.values()]
