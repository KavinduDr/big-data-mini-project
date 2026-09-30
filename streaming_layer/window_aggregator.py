"""Shared tumbling-window aggregation logic for the fleet speed layer.

This module is the single source of truth for "speed layer" metrics and is used
by both stream processors:

* ``streaming_layer/spark_streaming_job.py`` -> Kafka consumer (Docker mode)
* ``run_standalone.py``                      -> in-memory bus (zero Docker)

Two correctness problems present in the original per-layer implementations are
fixed here, in one place:

1. **Event counts were reported as vehicle counts.** Every 2s heartbeat was
   counted into ``active``/``idle``/``enroute``, so a 10s window over a 25
   vehicle fleet reported ~125 "vehicles" and the idle ratio was computed
   against that inflated denominator. The aggregator tracks the *latest* known
   state per vehicle and reports distinct vehicles by status (<= fleet size).
2. **Cumulative trip fare was summed on every heartbeat.** ``fare`` is the
   running total for the current trip, so summing it on each tick over-reported
   live earnings by roughly (window size / interval). The aggregator remembers
   the highest fare already acknowledged per ``trip_id`` and books only the
   *delta*, giving true revenue-per-window, including trips that span windows.
"""

import logging
import time

logger = logging.getLogger("FleetWindowAggregator")


def _empty_zone_stats():
    return {
        "active": 0,
        "idle": 0,
        "enroute": 0,
        "trips": 0,
        "fare_sum": 0.0,
        "speed_sum": 0.0,
        "count": 0,
    }


class FleetWindowAggregator:
    """Accumulates telemetry events and exposes per-zone window aggregates."""

    def __init__(
        self,
        window_size_sec=10,
        idle_threshold_sec=60,
        alert_cooldown_sec=60,
        max_alert_cache=10000,
        clock=time.time,
    ):
        self.window_size_sec = window_size_sec
        self.idle_threshold_sec = idle_threshold_sec
        self.alert_cooldown_sec = alert_cooldown_sec
        self.max_alert_cache = max_alert_cache
        self._clock = clock

        # Latest known state per vehicle (bounded by fleet size)
        self._vehicle_latest = {}
        # Highest fare already acknowledged per trip_id (evicted when trip ends)
        self._trip_fare_seen = {}
        # Vehicles / trips observed since the last flush
        self._window_vehicles = set()
        self._window_trip_zones = {}
        # Last alert timestamp per vehicle (bounded)
        self._last_alerted = {}
        # Per-zone running totals for the open window
        self._zones = {}

        self.events_processed = 0
        self.invalid_events = 0
        self.alerts_triggered = 0

    # ------------------------------------------------------------------ ingest
    def add_event(self, event):
        """Feeds one telemetry event.

        Returns an alert payload dict when the idle-threshold rule fires,
        otherwise ``None``.
        """
        veh_id = event.get("vehicle_id")
        if not veh_id:
            self.invalid_events += 1
            return None

        try:
            speed = float(event.get("speed") or 0.0)
            fare = float(event.get("fare") or 0.0)
            idle_sec = int(event.get("idle_duration_sec") or 0)
        except (TypeError, ValueError):
            self.invalid_events += 1
            return None

        zone = event.get("grid_zone") or "Unknown"
        status = event.get("status") or "idle"
        trip_id = event.get("trip_id")
        self.events_processed += 1

        previous = self._vehicle_latest.get(veh_id)
        if previous and previous.get("trip_id") != trip_id:
            # The vehicle moved on: release the fare watermark so the dict
            # cannot grow without bound in a long-running pipeline.
            self._trip_fare_seen.pop(previous.get("trip_id"), None)

        self._vehicle_latest[veh_id] = {
            "status": status,
            "zone": zone,
            "speed": speed,
            "idle_duration_sec": idle_sec,
            "trip_id": trip_id,
            "driver_id": event.get("driver_id"),
            "fare": fare,
        }
        self._window_vehicles.add(veh_id)

        if status == "on_trip" and trip_id:
            # Book only the revenue delta since the previous heartbeat.
            acknowledged = self._trip_fare_seen.get(trip_id, 0.0)
            if fare > acknowledged:
                self._zones.setdefault(zone, _empty_zone_stats())["fare_sum"] += fare - acknowledged
                self._trip_fare_seen[trip_id] = fare
            self._window_trip_zones[trip_id] = zone
        else:
            self._trip_fare_seen.pop(trip_id, None)

        return self._check_idle_alert(veh_id, event, zone, status, idle_sec)

    # -------------------------------------------------------------- windowing
    def flush(self, window_start=None, window_end=None):
        """Closes the current window and returns ``{zone: stats}``.

        ``stats`` keys are unchanged from the original implementations
        (``active``/``idle``/``enroute``/``trips``/``fare_sum``/``speed_sum``/
        ``count``) so existing writers keep working, but the semantics are now
        "distinct vehicles by latest status observed in the window" plus genuine
        revenue deltas.

        ``window_start`` / ``window_end`` are accepted for call-site symmetry
        with the previous API and are not required by the computation.
        """
        zones = self._zones
        self._zones = {}

        for veh_id in self._window_vehicles:
            state = self._vehicle_latest.get(veh_id)
            if not state:
                continue
            stats = zones.setdefault(state["zone"], _empty_zone_stats())
            if state["status"] == "on_trip":
                stats["active"] += 1
            elif state["status"] == "enroute":
                stats["enroute"] += 1
            else:
                stats["idle"] += 1
            stats["speed_sum"] += state["speed"]
            stats["count"] += 1

        for _trip_id, zone in self._window_trip_zones.items():
            if zone in zones:
                zones[zone]["trips"] += 1

        self._window_vehicles = set()
        self._window_trip_zones = {}
        return zones

    # ----------------------------------------------------------------- alerts
    def _check_idle_alert(self, veh_id, event, zone, status, idle_sec):
        if status != "idle" or idle_sec < self.idle_threshold_sec:
            return None

        now = self._clock()
        if now - self._last_alerted.get(veh_id, 0.0) < self.alert_cooldown_sec:
            return None

        self._last_alerted[veh_id] = now
        self._prune_alert_cache()
        self.alerts_triggered += 1
        message = (
            f"Vehicle {veh_id} idle for {idle_sec}s in zone {zone} "
            f"(threshold: {self.idle_threshold_sec}s)."
        )
        return {
            "vehicle_id": veh_id,
            "driver_id": event.get("driver_id"),
            "grid_zone": zone,
            "alert_type": "EXCESSIVE_IDLE",
            "idle_duration_sec": idle_sec,
            "alert_message": message,
        }

    def _prune_alert_cache(self):
        if len(self._last_alerted) <= self.max_alert_cache:
            return
        # Defensive guard against unbounded growth with high-cardinality ids:
        # keep only the most recently alerted vehicles.
        keep = sorted(self._last_alerted.items(), key=lambda kv: kv[1], reverse=True)
        self._last_alerted = dict(keep[: max(self.max_alert_cache // 2, 1)])
        logger.warning("Alert cooldown cache pruned to %d entries.", len(self._last_alerted))
