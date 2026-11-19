# SPDX-License-Identifier: MulanPSL-2.0
"""Object merging preserves observations and exported masks."""
import numpy as np
import pytest

pytest.importorskip("open3d")
from scene_service.ingest import perception_cg_numpy as cg


def test_merge_retains_only_first_mask(monkeypatch):
    class Cloud:
        points = np.ones((4, 3))

        def __iadd__(self, other):
            return self

    monkeypatch.setattr(cg, "process_pcd", lambda cloud, *args: cloud)
    monkeypatch.setattr(cg, "get_bounding_box", lambda *args: type("Box", (), {})())
    first = np.ones((4, 4), bool)
    obj = {"mask": [first], "num_detections": 1, "pcd": Cloud(),
           "clip_ft": np.ones(512, np.float32)}
    for _ in range(100):
        other = {"mask": [np.zeros((4, 4), bool)], "num_detections": 1,
                 "pcd": Cloud(), "clip_ft": np.ones(512, np.float32)}
        cg.merge_obj2_into_obj1(obj, other, .02, False, .02, 5, "axis_aligned")
    assert len(obj["mask"]) == 1
    assert obj["mask"][0] is first
    assert obj["num_detections"] == 101


def test_retained_mask_survives_map_export(tmp_path):
    import gzip
    import pickle
    import open3d as o3d
    from scene_service.ingest.cg_export import export_map_objects

    cloud = o3d.geometry.PointCloud()
    cloud.points = o3d.utility.Vector3dVector([[0, 0, 0], [1, 1, 1]])
    mask = np.ones((4, 4), bool)
    obj = {"mask": [mask], "pcd": cloud, "bbox": cloud.get_axis_aligned_bounding_box(),
           "clip_ft": np.ones(512, np.float32), "num_detections": 3}
    path = export_map_objects(cg.MapObjectList([obj]), "test", tmp_path, "ViT-B-32", "local")
    with gzip.open(path, "rb") as source:
        payload = pickle.load(source)
    exported = payload["objects"][0]
    assert len(exported["mask"]) == 1
    np.testing.assert_array_equal(exported["mask"][0], mask)
    assert exported["num_detections"] == 3
    assert "pcd_np" in exported


def test_overlap_merge_counts_each_source_once():
    import open3d as o3d

    objects = cg.MapObjectList()
    for i in range(3):
        cloud = o3d.geometry.PointCloud()
        cloud.points = o3d.utility.Vector3dVector(
            [[0, 0, 0], [1, 0, 0], [0, 1, 0], [0, 0, 1]])
        objects.append({"id": f"u{i}", "pcd": cloud, "clip_ft": np.array([1., 0.], np.float32),
                        "num_detections": 1, "image_idx": [i]})
    overlap = np.ones((3, 3), np.float32) - np.eye(3, dtype=np.float32)
    folded = list(objects)
    merged, indices = cg.merge_overlap_objects(
        .5, .5, 0., objects, overlap, .025, False, .02, 5, "axis_aligned")
    assert len(merged) == 1
    assert merged[0]["num_detections"] == 3
    assert sorted(merged[0]["image_idx"]) == [0, 1, 2]
    assert sorted(i for i in indices if i is not None) == [0]
    # Each folded cloud names the one it went into, which leads to the survivor.
    into = {o["id"]: o.get("folded_into") for o in folded}
    for uuid in into:
        while into.get(uuid):
            uuid = into[uuid]
        assert uuid == merged[0]["id"]


def test_unmatched_detection_is_appended_without_copying():
    detection = {"id": "new-track"}
    objects = cg.MapObjectList([{"id": "existing"}])
    result = cg.merge_detections_to_objects(
        .025, False, .02, 5, "axis_aligned",
        detection_list=cg.DetectionList([detection]), objects=objects,
        agg_sim=np.array([[-np.inf]], np.float32))
    assert result is objects
    assert result[-1] is detection


def _cube(centre, side, cls):
    import open3d as o3d
    cloud = o3d.geometry.PointCloud()
    grid = np.linspace(-side / 2, side / 2, 4)
    cloud.points = o3d.utility.Vector3dVector(
        [np.add(centre, (x, y, z)) for x in grid for y in grid for z in grid])
    cloud.colors = o3d.utility.Vector3dVector(np.full((64, 3), .5))
    return {"pcd": cloud, "clip_ft": np.array([1., 0.], np.float32), "num_detections": 1,
            "image_idx": [0], "class_name": cls}


@pytest.mark.parametrize("small, merged", [(0.1, False), (0.9, True)])
def test_overlap_merge_folds_classes_only_between_furniture(small, merged):
    # An item inside a table's cloud stays an item; a cabinet read as a shelf folds.
    objects = cg.MapObjectList([_cube((0, 0, 0), small, "cornflakes" if small < .4 else "cabinet"),
                                _cube((0, 0, 0), 1.0, "table" if small < .4 else "shelf")])
    overlap = np.ones((2, 2), np.float32) - np.eye(2, dtype=np.float32)
    out, _ = cg.merge_overlap_objects(.5, .5, 0., objects, overlap, .025, False, .02, 5,
                                      "axis_aligned", cross_class_min_extent=.4)
    assert (len(out) == 1) is merged

