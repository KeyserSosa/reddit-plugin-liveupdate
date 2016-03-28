import collections
import datetime

from pylons import app_globals as g

from r2.lib import amqp, websockets, utils
from r2.lib.db import tdb_cassandra
from r2.models.query_cache import CachedQueryMutator

from reddit_liveupdate.models import (
    ActiveVisitorsByLiveUpdateEvent,
    LiveUpdateEvent,
    LiveUpdateActivityHistoryByEvent,
)
from reddit_liveupdate.queries import get_active_events


ACTIVITY_FUZZING_THRESHOLD = 100


def update_activity():
    events = {}
    event_counts = collections.Counter()
    events = LiveUpdateEvent._all()

    for chunk in in_chunks(events, size=100):
        count = 0
        event_context_id = "LiveUpdateEvent_" + event._id

        try:
            count = c.activity_service.count_activity(
        except tdb_cassandra.TRANSIENT_EXCEPTIONS as e:
            g.log.warning("Failed to fetch activity count for %r: %s",
                          event_id, e)
            continue

        try:
            LiveUpdateActivityHistoryByEvent.record_activity(event_id, count)
        except tdb_cassandra.TRANSIENT_EXCEPTIONS as e:
            g.log.warning("Failed to update activity history for %r: %s",
                          event_id, e)

        is_fuzzed = False
        if count < ACTIVITY_FUZZING_THRESHOLD:
            count = utils.fuzz_activity(count)
            is_fuzzed = True

        try:
            event = LiveUpdateEvent.update_activity(event_id, count, is_fuzzed)
        except tdb_cassandra.TRANSIENT_EXCEPTIONS as e:
            g.log.warning("Failed to update event activity for %r: %s",
                          event_id, e)
        else:
            events[event_id] = event
            event_counts[event_id] = count

        websockets.send_broadcast(
            "/live/" + event_id,
            type="activity",
            payload={
                "count": count,
                "fuzzed": is_fuzzed,
            },
        )

    top_event_ids = [event_id for event_id, count in event_counts.most_common(1000)]
    top_events = [events[event_id] for event_id in top_event_ids]
    query_ttl = datetime.timedelta(days=3)
    with CachedQueryMutator() as m:
        m.replace(get_active_events(), top_events, ttl=query_ttl)

    # ensure that all the amqp messages we've put on the worker's queue are
    # sent before we allow this script to exit.
    amqp.worker.join()
