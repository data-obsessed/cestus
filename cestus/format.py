from typing import Dict, List


class CestusEntity:
    def __init__(self, frame: Dict[str, List], stats: Dict[str, float]):
        self._stats = stats
        self._frame = frame

    @property
    def columns(self):
        return [str(k) for k in self._frame.keys()]

    @property
    def stats(self):
        return self._stats

    @stats.setter
    def stats(self, value):
        self._stats = value

    def select(self, attributes: List[str] | str = "*"):
        returned_columns = self.columns if attributes == "*" else attributes
        return [[a for a in self._frame.get(attr)] for attr in returned_columns]
