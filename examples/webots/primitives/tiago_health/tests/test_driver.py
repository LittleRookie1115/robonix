#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Focused tests for the deterministic Webots health profile."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tiago_health import driver
from tiago_health.driver import (
    HealthSettings,
    _read_injected_faults,
    _sensor_report_status,
    _sensor_sample_is_fresh,
    build_health_state,
)


class HealthDriverTests(unittest.TestCase):
    def test_normal_profile_contains_nominal_component_values(self):
        """The default frame covers every Soma component without fault codes."""
        state = build_health_state(HealthSettings())
        readings = {reading.name: reading for reading in state.readings}
        self.assertAlmostEqual(state.voltage, 24.8, places=5)
        self.assertEqual(readings["body/state"].current_a, 0.0)
        self.assertEqual(readings["body/base/left_wheel/error"].current_a, 0.0)
        self.assertEqual(readings["body/base/right_wheel/communication_ok"].current_a, 1.0)
        self.assertAlmostEqual(
            readings["body/base/battery"].battery_percent, 82.0, places=5
        )

    def test_sensor_loss_is_reported_as_offline(self):
        """Explicit sample loss produces offline controls for both sensors."""
        state = build_health_state(
            HealthSettings(),
            camera_online=False,
            lidar_online=False,
        )
        readings = {reading.name: reading for reading in state.readings}

        self.assertEqual(readings["body/head_camera/online"].current_a, 0.0)
        self.assertEqual(readings["body/hokuyo_lidar/online"].current_a, 0.0)

    def test_sensor_sample_freshness_obeys_timeout(self):
        """Accept only monotonic samples within the configured freshness window."""
        self.assertFalse(_sensor_sample_is_fresh(None, 5.0, 2.0))
        self.assertTrue(_sensor_sample_is_fresh(3.0, 5.0, 2.0))
        self.assertFalse(_sensor_sample_is_fresh(2.99, 5.0, 2.0))
        self.assertFalse(_sensor_sample_is_fresh(5.1, 5.0, 2.0))

    def test_sensor_without_initial_sample_is_unknown_during_startup_grace(self):
        """Startup grace omits health until a sample or timeout is observed."""
        self.assertIsNone(_sensor_report_status(None, 2.0, 3.0, 2.0))
        self.assertFalse(_sensor_report_status(None, 2.0, 4.1, 2.0))
        self.assertIsNone(_sensor_report_status(None, None, 4.1, 2.0))

        state = build_health_state(
            HealthSettings(),
            camera_online=None,
            lidar_online=None,
        )
        names = {reading.name for reading in state.readings}
        self.assertNotIn("body/head_camera/online", names)
        self.assertNotIn("body/hokuyo_lidar/online", names)

    def test_fault_file_injects_and_clears_one_sensor_without_restarting(self):
        """Local fault updates affect only the selected sensor and can recover."""
        with tempfile.TemporaryDirectory() as directory:
            fault_file = Path(directory) / "faults.json"
            settings = HealthSettings.from_config({"fault_file": str(fault_file)})
            with (
                patch.object(driver, "_settings", settings),
                patch.object(driver, "_sensor_status", return_value=True),
            ):
                fault_file.write_text(
                    json.dumps({"offline": ["body/head_camera"]}),
                    encoding="utf-8",
                )
                faulted = driver._current_health_state()
                faulted_readings = {
                    reading.name: reading for reading in faulted.readings
                }
                self.assertEqual(
                    faulted_readings["body/head_camera/online"].current_a,
                    0.0,
                )
                self.assertEqual(
                    faulted_readings["body/hokuyo_lidar/online"].current_a,
                    1.0,
                )

                fault_file.write_text('{"offline":[]}', encoding="utf-8")
                recovered = driver._current_health_state()
                recovered_readings = {
                    reading.name: reading for reading in recovered.readings
                }
                self.assertEqual(
                    recovered_readings["body/head_camera/online"].current_a,
                    1.0,
                )

    def test_invalid_fault_file_fails_closed_for_monitored_sensors(self):
        """Reject unsupported component overrides by marking both sensors offline."""
        with tempfile.TemporaryDirectory() as directory:
            fault_file = Path(directory) / "faults.json"
            fault_file.write_text('{"offline":["body/base"]}', encoding="utf-8")

            self.assertEqual(
                _read_injected_faults(str(fault_file)),
                {"body/head_camera", "body/hokuyo_lidar"},
            )

    def test_shutdown_destroys_sensor_subscriptions_and_stops_ros(self):
        """Teardown releases subscriptions and stops the shared ROS backend."""
        class FakeNode:
            def __init__(self):
                self.destroyed = []

            def destroy_subscription(self, subscription):
                self.destroyed.append(subscription)

        class FakeBackend:
            def __init__(self):
                self.node = FakeNode()
                self.stopped = False

            def shutdown(self):
                self.stopped = True

        previous_subscriptions = list(driver._sensor_subscriptions)
        subscriptions = [object(), object()]
        backend = FakeBackend()
        driver._sensor_subscriptions[:] = subscriptions
        try:
            with patch.object(driver.RosBackend, "get", return_value=backend):
                driver.shutdown()
            self.assertEqual(backend.node.destroyed, subscriptions)
            self.assertTrue(backend.stopped)
            self.assertEqual(driver._sensor_subscriptions, [])
        finally:
            driver._sensor_subscriptions[:] = previous_subscriptions
            driver._stop.clear()

    def test_unknown_scenario_is_rejected(self):
        """Reserved scenarios fail initialization until their data is implemented."""
        with self.assertRaisesRegex(ValueError, "only 'normal' is implemented"):
            HealthSettings.from_config({"scenario": "wheel_fault"})

    def test_full_profile_contains_arm_and_gripper(self):
        """Full TIAGo reports every described arm joint and its gripper."""
        settings = HealthSettings.from_config({"variant": "full"})
        state = build_health_state(settings)
        readings = {reading.name: reading for reading in state.readings}
        for joint_index in range(1, 8):
            component_id = f"body/arm/joint_{joint_index}"
            self.assertIn(component_id, readings)
            self.assertEqual(readings[f"{component_id}/enabled"].current_a, 1.0)
        self.assertIn("body/arm/gripper", readings)
        self.assertEqual(readings["body/arm/gripper/error"].current_a, 0.0)
        self.assertIn("body/arm/gripper/actuator", readings)
        self.assertEqual(
            readings["body/arm/gripper/actuator/enabled"].current_a,
            1.0,
        )

    def test_lite_profile_omits_nonexistent_arm(self):
        """Default TIAGo Lite telemetry never invents arm components."""
        state = build_health_state(HealthSettings())
        self.assertFalse(
            any(reading.name.startswith("body/arm") for reading in state.readings)
        )

    def test_unknown_variant_is_rejected(self):
        """Variant typos fail initialization instead of desynchronizing Soma."""
        with self.assertRaisesRegex(ValueError, "choose 'lite' or 'full'"):
            HealthSettings.from_config({"variant": "tiago++"})


if __name__ == "__main__":
    unittest.main()
