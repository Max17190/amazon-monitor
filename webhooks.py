import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv("endpoint.env")


VALID_WEBHOOK_KINDS = ("discord", "generic")


@dataclass(frozen=True)
class WebhookTarget:
    name: str
    url: str
    role_id: str = ""
    kind: str = "discord"


def _parse_kind(env, name):
    raw = str(env.get(f"WEBHOOK_{name}_KIND", "")).strip().lower()
    if not raw:
        return "discord"
    if raw not in VALID_WEBHOOK_KINDS:
        logging.warning(
            "Unknown WEBHOOK_%s_KIND=%r; falling back to 'discord'", name, raw
        )
        return "discord"
    return raw


def load_webhook_targets(env=None):
    """Build named webhook targets (Discord or generic HTTP) from environment variables."""
    env = os.environ if env is None else env
    targets = {}

    for key, value in env.items():
        if not key.startswith("WEBHOOK_") or not key.endswith("_URL"):
            continue

        name = key[len("WEBHOOK_") : -len("_URL")]
        url = str(value).strip()
        if not name or not url:
            continue

        kind = _parse_kind(env, name)
        role_id = str(env.get(f"WEBHOOK_{name}_ROLE_ID", "")).strip()
        targets[name] = WebhookTarget(name=name, url=url, role_id=role_id, kind=kind)

    if not targets:
        logging.warning("No webhook targets configured")

    return targets


WEBHOOK_TARGETS = load_webhook_targets()
