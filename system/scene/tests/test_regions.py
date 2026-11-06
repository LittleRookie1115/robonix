import asyncio
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
    assert goal("scene.poi.anno.poi").reachable is True
    blocked = goal("by the wall")
    assert blocked.reachable is False
    assert "does not fit" in blocked.reason
