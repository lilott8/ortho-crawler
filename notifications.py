"""Daily notification job for the Icon & Saints data layer.

Independent of the crawl cadence (PRD §6): a once-a-day pass that finds events
due today and notifies the users following the affected saint/icon.

  * Recurring events (feast_day / nameday / veneration_day) match on MM-DD.
  * One-off events (new_icon_added) match on today's full date; these are
    written by the icon pipeline when a new icon is approved for a followed saint.

Actual delivery (push/email) is out of scope (PRD §8) and assumed to live behind
the injected ``dispatch`` callable; the default just logs. Per-user/per-event
de-duplication is enforced so re-running the job the same day is safe, while a
yearly-recurring event still fires again next year.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Awaitable, Callable, Optional

from storage import Storage

log = logging.getLogger("ortho_scraper.notifications")

# A dispatcher takes (user_id, event_dict) and delivers the notification.
Dispatcher = Callable[[int, dict], Awaitable[None]]


async def _log_dispatch(user_id: int, event: dict) -> None:
    """Default dispatcher: log the notification (delivery infra is out of scope)."""
    log.info("notify user=%s | %s for %s #%s (date %s)",
             user_id, event.get("event_type"), event.get("target_type"),
             event.get("target_id"), event.get("event_date"))


async def run_daily_notifications(db: Storage,
                                  dispatch: Optional[Dispatcher] = None,
                                  today: Optional[date] = None) -> dict:
    """Run one daily notification pass. Returns a small stats dict."""
    dispatch = dispatch or _log_dispatch
    # UTC to match the UTC-stamped notifications_sent rows (same-day dedup) and
    # the UTC event_date the icon pipeline writes for new_icon_added.
    today = today or datetime.now(timezone.utc).date()
    today_md = today.strftime("%m-%d")
    today_iso = today.isoformat()

    log.info("Notification job for %s (MM-DD %s)...", today_iso, today_md)

    # Make sure feast/veneration events exist for any dates populated since the
    # last run (these are mostly NULL at launch — PRD open risk §7).
    created = await db.sync_recurring_events()
    if created:
        log.info("Materialized %d new recurring event(s) from saints/icons.", created)

    recurring = await db.due_recurring_events(today_md)
    new_icons = await db.due_new_icon_events(today_iso)
    events = recurring + new_icons
    log.info("Due today: %d recurring + %d new-icon = %d event(s).",
             len(recurring), len(new_icons), len(events))

    sent = skipped = 0
    for event in events:
        followers = await db.get_followers(event["target_type"], event["target_id"])
        for user_id in followers:
            if await db.already_notified(user_id, event["id"], today_iso):
                skipped += 1
                continue
            await dispatch(user_id, event)
            await db.record_notification(user_id, event["id"])
            sent += 1

    log.info("Notification job done: %d sent, %d already-notified (skipped).", sent, skipped)
    return {"events": len(events), "sent": sent, "skipped": skipped, "created_events": created}
