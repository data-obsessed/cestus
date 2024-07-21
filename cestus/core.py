import struct
from typing import List, Dict, Any
import blosc
import numpy as np
from collections import defaultdict
import orjson
from cestus.format import CestusEntity

_magic_number = b'CCF6'


class CestusReader:

    def __init__(self, path: str):
        self.path = path

    def read(self, columns: List[str] = None) -> CestusEntity:
        with open(self.path, 'rb') as file:
            magic = file.read(4)
            if magic != _magic_number:
                raise ValueError("Invalid file format")
            metadata_len = struct.unpack('I', file.read(4))[0]
            metadata_bytes = file.read(metadata_len)
            metadata = orjson.loads(metadata_bytes.decode('utf-8'))
            if not columns:
                columns = metadata['columns']
            data = {}
            for col_name, col_type in zip(metadata['columns'], metadata['types']):
                block_len = struct.unpack('I', file.read(4))[0]
                if col_name not in columns:
                    file.seek(block_len, 1)
                    continue
                compressed_data = file.read(block_len)
                encoded_data = blosc.decompress(compressed_data)
                data[col_name] = self._decode_column(encoded_data, col_type)
            stat_block_len = struct.unpack('I', file.read(4))[0]
            stat_bytes = file.read(stat_block_len)
            stats = orjson.loads(stat_bytes.decode('utf-8'))
        return CestusEntity(data, stats)

    @staticmethod
    def _decode_column(encoded_data, dt_column):
        match dt_column:
            case float():
                return [struct.unpack('f', encoded_data[i:i + 4])[0] for i in range(0, len(encoded_data), 4)]
            case str():
                return encoded_data.decode('utf-8').split('\0')[:-1]
            case bool():
                return [struct.unpack('?', encoded_data[i:i + 1])[0] for i in range(0, len(encoded_data), 1)]
            case int():
                return [struct.unpack('i', encoded_data[i:i + 4])[0] for i in range(0, len(encoded_data), 4)]
            case _:
                raise ValueError("Unsupported data type")


class CestusWriter:
    _stats = defaultdict(dict)

    def __init__(self, path: str):
        self.path = path

    def write(self, data, schema: Dict[str, Any], chunk_size: int = 5000):
        with open(self.path, 'wb') as f:
            _column_stats = {str(k): {} for k in schema.keys()}
            f.write(_magic_number)
            metadata = {
                'columns': [str(k) for k in schema.keys()],
                'types': [v for v in schema.values()]
            }
            metadata_bytes = orjson.dumps(metadata)
            f.write(struct.pack('I', len(metadata_bytes)))
            f.write(metadata_bytes)
            for col_meta, col_data in zip(schema.items(), zip(*data)):
                col_name, col_dt = col_meta
                encoded_data = self._encode_column(col_data, col_dt, col_name)
                compressed_data = blosc.compress(encoded_data)
                length = len(compressed_data)
                f.write(struct.pack('I', length))
                f.write(compressed_data)
            stat_bytes = orjson.dumps(self._stats)
            f.write(struct.pack('I', len(stat_bytes)))
            f.write(stat_bytes)

    def _calc_stats(self, column: List[str], column_name):
        stats_name = ['min', 'max', 'mean', 'std', 'average']
        np_function = [np.min, np.max, np.std, np.average, np.median]
        for name, np_function in zip(stats_name, np_function):
            self._stats[column_name][name] = np_function([float(v) if v != '' else 0.0 for v in column]).tolist()

    def _encode_column(self, column, column_dt, column_name):
        if not isinstance(column_dt, str):
            self._calc_stats(column, column_name)
        match column_dt:
            case bool():
                return b''.join(struct.pack('?', v) for v in column)
            case int():
                return b''.join(struct.pack('i', v) for v in column)
            case float():
                return b''.join(struct.pack('f', float(v) if v != '' else 0.0) for v in column)
            case str():
                return b''.join(v.encode('utf-8') + b'\0' for v in column)
            case _:
                raise ValueError("Unsupported data type")
