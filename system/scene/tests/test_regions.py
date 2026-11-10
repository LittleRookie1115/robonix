import asyncio
import math
from types import SimpleNamespace

import numpy as np
import pytest

from scene_service import mcp_tools
from scene_service.robot_geometry import RobotFootprint


class _Store:
    map_id = "3f_demo"

    def list(self):
        return [
            SimpleNamespace(
                annotation_id="anno.315",
                kind="region",
                name="room 315",
                points=[(0.0, 0.0), (2.0, 0.0), (2.0, 3.0), (0.0, 3.0)],
                theta=0.5,
                stale=False,
                stale_reason="",
                updated_at=123.0,
            ),
            SimpleNamespace(
                annotation_id="anno.poi",
                kind="poi",
                name="charging spot",
                points=[(1.0, 1.0)],
                theta=None,
                stale=False,
                stale_reason="",
                updated_at=124.0,
            ),
        ]


def test_list_regions_returns_regions_and_pois_with_goal_region_ids_and_full_geometry():
    mcp_tools.attach_annotation_store(_Store())
    response = asyncio.run(mcp_tools.list_regions(mcp_tools.ListRegions_Request()))

    assert response.map_id == "3f_demo"
    region, poi = response.regions
    assert region.id == "scene.region.anno.315"
    assert region.kind == "region"
    assert region.name == "room 315"
    assert region.points_xy == [0.0, 0.0, 2.0, 0.0, 2.0, 3.0, 0.0, 3.0]
    assert region.stale is False
    assert (poi.id, poi.kind, poi.points_xy) == ("scene.poi.anno.poi", "poi", [1.0, 1.0])


def test_list_regions_reports_annotation_store_unavailable():
    mcp_tools.attach_annotation_store(None)
    with pytest.raises(RuntimeError, match="annotation store is unavailable"):
        asyncio.run(mcp_tools.list_regions(mcp_tools.ListRegions_Request()))


class _Registry:
    async def snapshot(self):
        return {}


def test_list_objects_keeps_region_entries_for_v1_compatibility():
    mcp_tools.attach_state(registry=_Registry())
    mcp_tools.attach_annotation_store(_Store())
    response = asyncio.run(mcp_tools.list_objects(mcp_tools.ListObjects_Request()))
    assert [obj.id for obj in response.objects] == ["scene.region.anno.315"]


class _StaleStore(_Store):
    def list(self):
        regions = super().list()
        regions[0].stale = True
        regions[0].stale_reason = "map epoch changed"
        return regions


def test_goal_region_rejects_stale_geometry_before_navigation():
    mcp_tools.attach_annotation_store(_StaleStore())
    response = asyncio.run(
        mcp_tools.goal_region(mcp_tools.GoalRegion_Request(region_id="scene.region.anno.315"))
    )
    assert response.reachable is False
    assert "is stale" in response.reason
    assert "map epoch changed" in response.reason


class _Hub:
    """A 4 m free grid around the map origin with one occupied cell at (2.0, 2.0)."""

    def has(self, name):
        return name == "occupancy_grid"

    def latest(self, _name):
        data = np.zeros((80, 80), dtype=np.int8)
        data[60, 60] = 100
        return SimpleNamespace(
            header=SimpleNamespace(frame_id="map"),
            info=SimpleNamespace(width=80, height=80, resolution=0.05,
                                 origin=SimpleNamespace(position=SimpleNamespace(x=-1.0, y=-1.0))),
            data=data.tobytes(),
        ), 0.0, 1


class _Geometry:
    def current(self):
        return RobotFootprint(points=((-0.2, -0.2), (0.2, -0.2), (0.2, 0.2), (-0.2, 0.2)),
                              base_frame="base", inscribed_radius_m=0.2, circumscribed_radius_m=0.29)


class _PoiStore(_Store):
    def list(self):
        annotations = super().list()
        annotations[1].theta = 1.5
        return annotations + [SimpleNamespace(
            annotation_id="anno.wall", kind="poi", name="by the wall", points=[(2.1, 2.0)],
            theta=None, stale=False, stale_reason="", updated_at=125.0)]


def test_goal_region_returns_a_poi_pose_only_where_the_robot_fits():
    mcp_tools.attach_state(registry=_Registry(), hub=_Hub(), robot_geometry=_Geometry())
    mcp_tools.attach_annotation_store(_PoiStore())

    def goal(reference):
        return asyncio.run(mcp_tools.goal_region(mcp_tools.GoalRegion_Request(region_id=reference)))

    spot = goal("charging spot")
    assert (spot.reachable, spot.x, spot.y, spot.yaw) == (True, 1.0, 1.0, 1.5)
    pose = spot.goal.pose
    assert spot.goal.header.frame_id == "map"
    assert (pose.position.x, pose.position.y) == (1.0, 1.0)
    assert 2.0 * math.atan2(pose.orientation.z, pose.orientation.w) == pytest.approx(1.5)
    assert goal("scene.poi.anno.poi").reachable is True
    blocked = goal("by the wall")
    assert blocked.reachable is False
    assert "does not fit" in blocked.reason
    assert blocked.goal.header.frame_id == ""


class _LateRegistry:
    """Nothing for the first two looks, then a cup: perception catching up with a new view."""

    def __init__(self):
        self.looks = 0

    async def snapshot(self):
        self.looks += 1
        if self.looks <= 2:
            return {}
        cup = SimpleNamespace(
            object_id="scene.object.cup", label="cup", caption="", missing=False, attributes={},
            pose=SimpleNamespace(x=1.0, y=1.0, z=0.8, yaw=0.0, frame_id="map"),
            bbox=SimpleNamespace(size_x=0.1, size_y=0.1, size_z=0.1, frame_id="map"),
            observation_count=3, last_seen=0.0)
        return {cup.object_id: cup}


def test_find_waits_for_its_answer_to_settle_only_as_long_as_settle_s_allows(monkeypatch):
    monkeypatch.setattr(mcp_tools, "_FIND_POLL_S", 0.01)
    monkeypatch.setattr(mcp_tools, "_FIND_STABLE_S", 0.03)
    mcp_tools.attach_annotation_store(_Store())

    def find(settle_s, **constraints):
        registry = _LateRegistry()
        mcp_tools.attach_state(registry=registry)
        request = mcp_tools.Find_Request(settle_s=settle_s, **constraints)
        return asyncio.run(mcp_tools.find(request)), registry.looks

    at_once, looks = find(0.0, label="cup")
    assert (at_once.objects, at_once.detail, looks) == ([], "no object matches 'cup'", 1)
    settled, looks = find(1.0, label="cup")
    # Seen on the third look, and unchanged for the settling time after it.
    assert [obj.id for obj in settled.objects] == ["scene.object.cup"] and 5 <= looks < 20
    assert find(0.005, label="cup")[0].objects == []


def test_find_takes_a_region_by_its_id_as_well_as_its_name():
    mcp_tools.attach_annotation_store(_Store())

    def find(region):
        registry = _LateRegistry()
        registry.looks = 2
        mcp_tools.attach_state(registry=registry)
        found = asyncio.run(mcp_tools.find(mcp_tools.Find_Request(region=region)))
        return [obj.id for obj in found.objects]

    assert find("room 315") == find("scene.region.anno.315") == ["scene.object.cup"]
    assert find("scene.poi.anno.poi") == []
