import os

from cestus.core import CestusReader, CestusWriter

schema = {"a": 0, "b": "", "c": 0.0, "d": bool()}
source_data = [[1, "a", 0.0, True], [3, "f", 1.0, False]]


def test_write_read():
    path = "/tmp/test_write.cestus"
    CestusWriter(path).write(source_data, schema)
    cestus_data = CestusReader(path).read().select("*")
    transposed_cestus = [row for row in zip(*cestus_data)]
    for i in range(len(source_data)):
        for j in range(len(source_data[i])):
            assert source_data[i][j] == transposed_cestus[i][j]
    os.remove(path)


def test_column_filter():
    path = "/tmp/test_write.cestus"
    CestusWriter(path).write(source_data, schema)
    filtered = CestusReader(path).read(columns=["a", "d"])
    os.remove(path)
    assert (
        "a" in filtered.columns
        and "d" in filtered.columns
        and filtered.select(["a", "d"]) == [[1, 3], [True, False]]
    )


def test_stats_per_column():
    path = "/tmp/test_write.cestus"
    CestusWriter(path).write(source_data, schema)
    stats = CestusReader(path).read().stats
    assert "a" in stats


if __name__ == "__main__":
    test_column_filter()
    test_stats_per_column()
