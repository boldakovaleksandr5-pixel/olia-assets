"""Telegram listener: booking parser + outgoing message sender for Olya CRM."""
import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from telethon import TelegramClient
from telethon.sessions import StringSession

from telegram_booking_parser import offered_slots, selected_slot

ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "state.json"
REVIEW_FILE = ROOT / "review.jsonl"
BASE_URL = os.environ.get("CRM_BASE_URL", "https://olia-dashboard-x7k9m2.vercel.app").rstrip("/")
WORKER_SECRET = os.environ["CRM_WORKER_SECRET"]
SESSION_PATH = Path(os.environ.get("TG_SESSION_PATH", "/home/agent/.tg-userbot/session-agent.string"))
EXPECTED_USER_ID = int(os.environ.get("TG_EXPECTED_USER_ID", "367140409"))
API_ID = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("telethon").setLevel(logging.WARNING)


def load_state():
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {"dialogs": {}}


def save_state(state):
    target = STATE_FILE.with_suffix(".tmp")
    target.write_text(json.dumps(state, ensure_ascii=False))
    target.chmod(0o600)
    target.replace(STATE_FILE)


def crm(method, path, body=None):
    with requests.Session() as session:
        session.trust_env = False
        response = session.request(method, BASE_URL + path,
                                   headers={"Authorization": "Bearer " + WORKER_SECRET},
                                   json=body, timeout=70)
        try:
            data = response.json()
        except ValueError:
            data = {"error": "CRM returned a non-JSON response"}
        return response.status_code, data


async def process_messages(me, user, username, telegram_id, dialog_state, messages, state):
    for message in messages:
        text, sent_at = message.message or "", message.date.isoformat()
        if message.out:
            offers = offered_slots(text, sent_at)
            confirmation = re.search(r"\b(фиксирую|зафиксировал|зафиксировала|закреплен[аоы]?|записал[аи]?|встреча за вами)\b", text.lower())
            if offers and not confirmation:
                dialog_state["offers"] = offers
                dialog_state["offered_at"] = sent_at
            elif confirmation:
                dialog_state["offers"] = []
        elif text and dialog_state.get("offers") and dialog_state.get("offered_at"):
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(dialog_state["offered_at"])).total_seconds()
            selected = selected_slot(text, dialog_state["offers"], sent_at) if age < 86400 else None
            if selected:
                body = {**selected, "username": username, "telegramUserId": telegram_id,
                        "sourceEvent": f"{me.id}:{user.id}:{message.id}"}
                code, result = await asyncio.to_thread(crm, "POST", "/api/crm/telegram-booking", body)
                if code >= 500:
                    raise RuntimeError("Booking API unavailable; message will be retried")
                logging.info("booking message=%s status=%s manager=%s review=%s", message.id, code,
                             result.get("manager", ""), result.get("review", False))
                if code == 200:
                    dialog_state["offers"] = []
                else:
                    with REVIEW_FILE.open("a") as review:
                        review.write(json.dumps({**body, "error": result.get("error")}, ensure_ascii=False) + "\n")
        dialog_state["cursor"] = message.id
        save_state(state)


async def poll(client, me, state):
    status, data = await asyncio.to_thread(crm, "GET", "/api/crm/telegram-booking")
    if status != 200:
        raise RuntimeError(f"CRM contacts failed: {status}")
    contacts = data.get("contacts", [])
    by_id, by_username = {}, {}
    for contact in contacts:
        telegram_id, username = str(contact.get("telegramUserId", "")), str(contact.get("username", "")).lower()
        if telegram_id:
            by_id.setdefault(telegram_id, []).append(contact)
        if username:
            by_username.setdefault(username, []).append(contact)
    async for dialog in client.iter_dialogs(limit=500):
        user = dialog.entity
        username = str(getattr(user, "username", "") or "").lower()
        telegram_id = str(user.id)
        id_matches, username_matches = by_id.get(telegram_id, []), by_username.get(username, []) if username else []
        matched = {contact.get("leadId"): contact for contact in (id_matches or username_matches)}
        if not dialog.is_user or getattr(user, "bot", False) or len(matched) != 1:
            continue
        contact = next(iter(matched.values()))
        key = str(user.id)
        dialog_state = state["dialogs"].get(key)
        if dialog_state is None:
            submitted = datetime.fromisoformat(str(contact.get("submittedAt") or "").replace("Z", "+00:00"))
            if submitted.tzinfo is None:
                submitted = submitted.replace(tzinfo=ZoneInfo("Europe/Moscow"))
            cutoff = max(datetime.now(timezone.utc) - timedelta(hours=48), submitted.astimezone(timezone.utc) - timedelta(minutes=10))
            recent = await client.get_messages(user, limit=100)
            messages = [message for message in reversed(recent) if message.date >= cutoff]
            dialog_state = {"cursor": messages[0].id - 1 if messages else (dialog.message.id if dialog.message else 0), "offers": []}
            state["dialogs"][key] = dialog_state
            save_state(state)
        else:
            messages = await client.get_messages(user, min_id=dialog_state["cursor"], limit=100, reverse=True)
        await process_messages(me, user, username, telegram_id, dialog_state, messages, state)


async def process_outgoing_queue(client):
    """Fetch pending sends from CRM API and send via Telethon."""
    status, data = await asyncio.to_thread(crm, "GET", "/api/crm/pending-sends")
    if status != 200:
        logging.warning("pending-sends GET failed: %s", status)
        return
    for item in data.get("sends", []):
        tg_id = item.get("telegram_user_id")
        username = item.get("tg_username")
        target = int(tg_id) if tg_id and tg_id.isdigit() else (f"@{username}" if username else None)
        if not target:
            await asyncio.to_thread(crm, "PATCH", "/api/crm/pending-sends",
                                    {"id": item["id"], "status": "error", "error": "no target"})
            continue
        try:
            await client.send_message(target, item["text"])
            await asyncio.to_thread(crm, "PATCH", "/api/crm/pending-sends",
                                    {"id": item["id"], "status": "sent"})
            logging.info("outgoing sent lead=%s target=%s", item.get("lead_name"), target)
        except Exception as e:
            await asyncio.to_thread(crm, "PATCH", "/api/crm/pending-sends",
                                    {"id": item["id"], "status": "error", "error": str(e)})
            logging.warning("outgoing send failed lead=%s: %s", item.get("lead_name"), e)


async def main():
    state = load_state()
    client = TelegramClient(StringSession(SESSION_PATH.read_text().strip()), API_ID, API_HASH)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("Telegram authorization required")
        me = await client.get_me()
        if me.id != EXPECTED_USER_ID:
            raise RuntimeError("Wrong Telegram account")
        logging.info("verified Telegram account id=%s", me.id)
        cycle = 0
        while True:
            try:
                await poll(client, me, state)
            except Exception as error:
                logging.warning("polling failed: %s", error)
            # Check outgoing queue every other cycle (~60 sec)
            if cycle % 2 == 0:
                try:
                    await process_outgoing_queue(client)
                except Exception as error:
                    logging.warning("outgoing queue error: %s", error)
            cycle += 1
            await asyncio.sleep(30)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
