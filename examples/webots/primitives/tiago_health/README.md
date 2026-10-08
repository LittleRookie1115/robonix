<!-- SPDX-License-Identifier: MulanPSL-2.0 -->

# TIAGo simulated health primitive

This Webots package provides `robonix/primitive/health/state` and
`robonix/primitive/health/stream`. It publishes simulated telemetry for the
TIAGo base, wheels, battery, camera, lidar, and audio component paths declared
in `examples/webots/soma.yaml`. Camera RGB and lidar scan subscriptions update
their `online` readings; if samples stop for `sensor_timeout_s`, the next
health frame reports that component offline. Before the first sample, it omits
the component reading so Soma reports `UNKNOWN` rather than assuming startup
means the sensor is online. The `full` variant also reports all seven arm
joints plus the parallel gripper and its actuator declared in `soma.full.yaml`.
For a live demonstration, `fault_file` enables a local-only camera/lidar
fault switch; no Atlas capability or RPC is added.

Configuration is delivered through the primitive entry in
`robonix_manifest.yaml`:

```yaml
config:
  variant: lite
  scenario: normal
  interval_s: 0.5
  sensor_timeout_s: 2.0
  fault_file: /tmp/robonix-tiago-health-faults.json
  camera_rgb_topic: /head_front_camera/rgb/image_raw
  lidar_scan_topic: /scanner_normalized
  battery_percent: 82.0
  voltage: 24.8
  remaining_s: 10800
```

`variant` accepts `lite` (default) or `full`. It must match the Webots and
Soma variant selected by the example launch scripts.

`sensor_timeout_s` is the maximum age of camera and lidar ROS samples. Vitals
marks the component `STALE` if Soma stops refreshing its health report and
passes the resulting state to Pilot.

`fault_file` is optional. When configured, the health provider rereads it for
each frame. It accepts only camera and lidar component IDs:

```json
{"offline":["body/head_camera"]}
```

The provided host-side helper atomically changes the file inside the Webots
container:

```bash
bash examples/webots/primitives/tiago_health/scripts/inject_fault.sh camera
bash examples/webots/primitives/tiago_health/scripts/inject_fault.sh lidar
bash examples/webots/primitives/tiago_health/scripts/inject_fault.sh normal
```

The first two commands report the selected device offline while Webots keeps
running. `normal` clears injected faults; real sensor-topic timeouts remain
monitored. The next health frame reflects the change, then Soma and Vitals
publish it for Pilot. Malformed or unsupported fault-file contents fail closed
by marking both monitored sensors offline. `scenario` remains `normal`; this
fault switch is controlled by the local file instead.
