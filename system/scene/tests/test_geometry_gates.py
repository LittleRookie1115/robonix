from types import SimpleNamespace

import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("scipy")

from scene_service.ingest.geometry_gates import KnownGround, fraction_on_known_ground  # noqa: E402


class _Hub:
    def __init__(self, msg):
        self._msg = msg

    def has(self, name):
        return name == "occupancy_grid"

    def latest(self, name):
        return self._msg, 1.0, 1


def test_unseen_cells_near_a_looked_at_cell_count_as_seen():
    # 1 m x 1 m at 5 cm: the west half was scanned, the east half never was.
    grid = np.full((20, 20), -1, dtype=np.int16)
    grid[:, :10] = 0
    info = SimpleNamespace(height=20, width=20, resolution=0.05,
                           origin=SimpleNamespace(position=SimpleNamespace(x=0.0, y=0.0)))
    known = KnownGround(_Hub(SimpleNamespace(info=info, data=grid.ravel().tolist()))).current()

    # A counter top just past the scanned edge, and a spot half a metre beyond it.
    assert fraction_on_known_ground([[0.72, 0.5]], known) == 1.0
    assert fraction_on_known_ground([[0.97, 0.5]], known) == 0.0
