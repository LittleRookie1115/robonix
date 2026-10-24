# SPDX-License-Identifier: MulanPSL-2.0
"""Regression tests for Webots Soma export ownership and contracts."""

import unittest
from pathlib import Path

try:
    import yaml
except ModuleNotFoundError:
    yaml = None


ROOT = Path(__file__).resolve().parents[1] / "examples" / "webots"


def _load_yaml(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _exports(component):
    """Keep ownership attached while walking root and nested component exports."""
    for export in component.get("exports", []):
        yield component["id"], export
    for child in component.get("components", []):
        yield from _exports(child)


@unittest.skipIf(yaml is None, "PyYAML is not installed")
class WebotsSomaExportsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Read both robot profiles without requiring remote package caches."""
        deployment = _load_yaml(ROOT / "robonix_manifest.yaml")
        cls.providers = {
            entry["name"]: entry
            for section in ("primitive", "service", "skill")
            for entry in deployment.get(section, [])
        }
        cls.robots = {
            name: _load_yaml(ROOT / name)["robot"]
            for name in ("soma.yaml", "soma.full.yaml")
        }

    def test_export_provider_ids_exist_in_deployment(self):
        """Every export must name the deployed instance, not its package alias."""
        for profile, robot in self.robots.items():
            for component_id, export in _exports(robot):
                with self.subTest(profile=profile, component=component_id):
                    self.assertIn(export["provider_id"], self.providers.keys())

    def test_component_contracts_match_local_packages(self):
        """Expose the provided hardware contracts, excluding lifecycle control."""
        for profile, robot in self.robots.items():
            for component_id, export in _exports(robot):
                provider = self.providers[export["provider_id"]]
                if "path" not in provider:
                    continue
                package = _load_yaml(
                    ROOT / provider["path"] / "package_manifest.yaml"
                )
                provided = {item["name"] for item in package["capabilities"]}
                exported = {item["path"] for item in export["capabilities"]}
                with self.subTest(profile=profile, component=component_id):
                    self.assertEqual(
                        exported, provided - {"robonix/lifecycle/driver"}
                    )

    def test_exports_keep_root_and_component_ownership(self):
        """Root services must not replace ownership by physical components."""
        for profile, robot in self.robots.items():
            owners = {
                export["provider_id"]: component_id
                for component_id, export in _exports(robot)
            }
            with self.subTest(profile=profile):
                self.assertEqual(
                    owners,
                    {
                        "tiago_health": robot["id"],
                        "nav2": robot["id"],
                        "explore": robot["id"],
                        "tiago_chassis": "base",
                        "tiago_camera": "head_camera",
                        "tiago_lidar": "hokuyo_lidar",
                        "audio_driver": "audio",
                    },
                )


if __name__ == "__main__":
    unittest.main()
