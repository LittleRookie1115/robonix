#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Publish simulated TIAGo health and monitor sensor topic freshness."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import threading
import time

from robonix_api import Err, Ok, Primitive
from robonix_api.ros import RosBackend


tiago_health = Primitive(id="tiago_health", namespace="robonix/primitive/health")

import health_pb2  # noqa: E402


@dataclass(frozen=True)
class HealthSettings:
    scenario: str = "normal"
    variant: str = "lite"
    interval_s: float = 0.5
    sensor_timeout_s: float = 2.0
    fault_file: str = ""
    battery_percent: float = 82.0
    voltage: float = 24.8
    remaining_s: int = 10800

    @classmethod
    def from_config(cls, cfg: dict) -> "HealthSettings":
        """Validate lifecycle config and return one immutable runtime setup."""
        scenario = str(cfg.get("scenario", "normal")).strip().lower()
        if scenario != "normal":
            raise ValueError(
                f"unsupported scenario '{scenario}'; only 'normal' is implemented"
            )
        variant = str(cfg.get("variant", "lite")).strip().lower() or "lite"
        if variant not in {"lite", "full"}:
            raise ValueError(
                f"unsupported TIAGo variant '{variant}'; choose 'lite' or 'full'"
            )
        interval_s = float(cfg.get("interval_s", 0.5))
        if interval_s <= 0:
            raise ValueError("interval_s must be greater than zero")
        sensor_timeout_s = float(cfg.get("sensor_timeout_s", 2.0))
        if sensor_timeout_s <= 0:
            raise ValueError("sensor_timeout_s must be greater than zero")
        fault_file = str(cfg.get("fault_file", "")).strip()
        battery_percent = float(cfg.get("battery_percent", 82.0))
        if not 0.0 <= battery_percent <= 100.0:
            raise ValueError("battery_percent must be between 0 and 100")
        voltage = float(cfg.get("voltage", 24.8))
        remaining_s = int(cfg.get("remaining_s", 10800))
        return cls(
            scenario=scenario,
            variant=variant,
            interval_s=interval_s,
            sensor_timeout_s=sensor_timeout_s,
            fault_file=fault_file,
            battery_percent=battery_percent,
            voltage=voltage,
            remaining_s=remaining_s,
        )


_settings = HealthSettings()
_stop = threading.Event()
_sensor_lock = threading.Lock()
_last_sensor_sample: dict[str, float] = {}
_sensor_subscriptions = []
_sensor_monitor_started_at: float | None = None
_MONITORED_SENSORS = frozenset({"body/head_camera", "body/hokuyo_lidar"})


def _record_sensor_sample(component_id: str):
    """Create a ROS callback that records this sensor's monotonic sample time."""
    def callback(_message) -> None:
        with _sensor_lock:
            _last_sensor_sample[component_id] = time.monotonic()

    return callback


def _sensor_status(component_id: str, timeout_s: float) -> bool | None:
    """Read sensor freshness without holding the sample lock during reporting."""
    with _sensor_lock:
        last_seen = _last_sensor_sample.get(component_id)
        monitor_started_at = _sensor_monitor_started_at
    return _sensor_report_status(
        last_seen,
        monitor_started_at,
        time.monotonic(),
        timeout_s,
    )


def _sensor_report_status(
    last_seen: float | None,
    monitor_started_at: float | None,
    now: float,
    timeout_s: float,
) -> bool | None:
    """Keep startup unknown until a sample arrives or the grace period expires."""
    if last_seen is None:
        if monitor_started_at is not None and now - monitor_started_at > timeout_s:
            return False
        return None
    return _sensor_sample_is_fresh(last_seen, now, timeout_s)


def _sensor_sample_is_fresh(
    last_seen: float | None,
    now: float,
    timeout_s: float,
) -> bool:
    return last_seen is not None and 0.0 <= now - last_seen <= timeout_s


def _read_injected_faults(fault_file: str) -> set[str]:
    """Read local demo faults; invalid content marks both monitored sensors offline."""
    if not fault_file:
        return set()
    try:
        payload = json.loads(Path(fault_file).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return set()
    except (OSError, UnicodeError, json.JSONDecodeError):
        return set(_MONITORED_SENSORS)

    offline = payload.get("offline") if isinstance(payload, dict) else None
    if (
        not isinstance(offline, list)
        or any(not isinstance(component, str) for component in offline)
        or not set(offline).issubset(_MONITORED_SENSORS)
    ):
        return set(_MONITORED_SENSORS)

    return set(offline)


def _current_health_state() -> "health_pb2.HealthState":
    """Combine sensor freshness with the local demonstration fault overrides."""
    injected_faults = _read_injected_faults(_settings.fault_file)
    return build_health_state(
        _settings,
        camera_online=(
            False
            if "body/head_camera" in injected_faults
            else _sensor_status("body/head_camera", _settings.sensor_timeout_s)
        ),
        lidar_online=(
            False
            if "body/hokuyo_lidar" in injected_faults
            else _sensor_status("body/hokuyo_lidar", _settings.sensor_timeout_s)
        ),
    )


def _reading(
    name: str,
    *,
    temp_c: float = -1.0,
    voltage: float = -1.0,
    current_a: float = -1.0,
    battery_percent: float = -1.0,
) -> "health_pb2.SensorReading":
    """Create one reading with explicit unavailable sentinels."""
    return health_pb2.SensorReading(
        name=name,
        temp_c=temp_c,
        voltage=voltage,
        current_a=current_a,
        battery_percent=battery_percent,
    )


def _control(name: str, value: float) -> "health_pb2.SensorReading":
    return _reading(name, current_a=value)


def _sensor_readings(
    component_id: str,
    temp_c: float,
    online: bool | None,
) -> list["health_pb2.SensorReading"]:
    """Omit unknown sensors and encode explicit online or offline readings."""
    if online is None:
        return []
    return [
        _reading(component_id, temp_c=temp_c),
        _control(f"{component_id}/online", 1.0 if online else 0.0),
        _control(f"{component_id}/error", 0.0),
    ]


def _full_variant_readings(settings: HealthSettings) -> list["health_pb2.SensorReading"]:
    """Return nominal arm and gripper readings for the full Webots model."""
    readings = [
        _reading("body/arm", temp_c=37.0),
        _control("body/arm/online", 1.0),
        _control("body/arm/error", 0.0),
    ]
    for joint_index in range(1, 8):
        component_id = f"body/arm/joint_{joint_index}"
        readings.extend(
            [
                _reading(
                    component_id,
                    temp_c=38.0 + joint_index * 0.4,
                    voltage=settings.voltage,
                    current_a=0.35,
                ),
                _control(f"{component_id}/enabled", 1.0),
                _control(f"{component_id}/communication_ok", 1.0),
                _control(f"{component_id}/error", 0.0),
            ]
        )
    readings.extend(
        [
            _reading("body/arm/gripper", temp_c=38.0),
            _control("body/arm/gripper/online", 1.0),
            _control("body/arm/gripper/error", 0.0),
            _reading(
                "body/arm/gripper/actuator",
                temp_c=38.0,
                voltage=settings.voltage,
                current_a=0.2,
            ),
            _control("body/arm/gripper/actuator/enabled", 1.0),
            _control("body/arm/gripper/actuator/communication_ok", 1.0),
            _control("body/arm/gripper/actuator/error", 0.0),
        ]
    )
    return readings


def build_health_state(
    settings: HealthSettings,
    *,
    camera_online: bool | None = True,
    lidar_online: bool | None = True,
) -> "health_pb2.HealthState":
    """Build one health frame using Soma component paths."""
    readings = [
        _reading("body", temp_c=36.0),
        _reading("body/base", temp_c=34.0),
        _reading(
            "body/base/left_wheel",
            temp_c=38.0,
            voltage=settings.voltage,
            current_a=0.7,
        ),
        _reading("body/base/left_wheel/driver_temp", temp_c=41.0),
        _control("body/base/left_wheel/enabled", 1.0),
        _control("body/base/left_wheel/communication_ok", 1.0),
        _control("body/base/left_wheel/error", 0.0),
        _reading(
            "body/base/right_wheel",
            temp_c=38.5,
            voltage=settings.voltage,
            current_a=0.7,
        ),
        _reading("body/base/right_wheel/driver_temp", temp_c=41.5),
        _control("body/base/right_wheel/enabled", 1.0),
        _control("body/base/right_wheel/communication_ok", 1.0),
        _control("body/base/right_wheel/error", 0.0),
        _reading(
            "body/base/battery",
            temp_c=32.0,
            voltage=settings.voltage,
            current_a=0.0,
            battery_percent=settings.battery_percent,
        ),
        _control("body/base/battery/online", 1.0),
        _control("body/base/battery/error", 0.0),
        *_sensor_readings("body/head_camera", 42.0, camera_online),
        *_sensor_readings("body/hokuyo_lidar", 39.0, lidar_online),
        _reading("body/audio", temp_c=35.0),
        _control("body/audio/online", 1.0),
        _control("body/audio/error", 0.0),
    ]
    if settings.variant == "full":
        readings.extend(_full_variant_readings(settings))
    readings.append(_control("body/state", 0.0))
    return health_pb2.HealthState(
        voltage=settings.voltage,
        charging=False,
        remaining_s=settings.remaining_s,
        readings=readings,
    )


@tiago_health.grpc("robonix/primitive/health/state")
def get_health_state(_request) -> "health_pb2.GetHealthState_Response":
    """Return the latest simulated health frame."""
    return health_pb2.GetHealthState_Response(state=_current_health_state())


@tiago_health.grpc("robonix/primitive/health/stream")
def stream_health_state(_request, context):
    """Yield simulated health at the configured interval until disconnected."""
    while context.is_active() and not _stop.is_set():
        yield _current_health_state()
        _stop.wait(_settings.interval_s)


@tiago_health.on_init
def init(cfg):
    """Watch sensor topics and publish their availability with each health frame."""
    global _settings, _sensor_monitor_started_at, _sensor_subscriptions
    try:
        _settings = HealthSettings.from_config(cfg)
    except (TypeError, ValueError) as exc:
        return Err(str(exc))
    _stop.clear()
    camera_topic = str(cfg.get("camera_rgb_topic", "/head_front_camera/rgb/image_raw"))
    lidar_topic = str(cfg.get("lidar_scan_topic", "/scanner_normalized"))
    with _sensor_lock:
        _last_sensor_sample.clear()
        _sensor_monitor_started_at = time.monotonic()
    try:
        _sensor_subscriptions = [
            tiago_health.create_subscription(
                "internal/camera_health_watch",
                topic=camera_topic,
                msg_type="sensor_msgs/msg/Image",
                callback=_record_sensor_sample("body/head_camera"),
                qos="best_effort",
                declare=False,
            ),
            tiago_health.create_subscription(
                "internal/lidar_health_watch",
                topic=lidar_topic,
                msg_type="sensor_msgs/msg/LaserScan",
                callback=_record_sensor_sample("body/hokuyo_lidar"),
                qos="best_effort",
                declare=False,
            ),
        ]
    except Exception as exc:
        return Err(f"cannot monitor TIAGo sensor topics: {exc}")
    print(
        "[tiago_health] initialized "
        f"scenario={_settings.scenario} variant={_settings.variant} "
        f"interval_s={_settings.interval_s} "
        f"sensor_timeout_s={_settings.sensor_timeout_s}",
        flush=True,
    )
    return Ok()


@tiago_health.on_shutdown
def shutdown():
    """Release sensor readers before provider teardown."""
    _stop.set()
    backend = RosBackend.get()
    for subscription in _sensor_subscriptions:
        try:
            backend.node.destroy_subscription(subscription)
        except Exception as exc:
            print(f"[tiago_health] could not destroy sensor subscription: {exc}", flush=True)
    _sensor_subscriptions.clear()
    backend.shutdown()
    return Ok()


if __name__ == "__main__":
    tiago_health.run()
