#!/usr/bin/env python3
"""把 samples/ 下的示例 CSV 发布进数据湖（演示用；重复执行会重复入批）。

用法（容器内）：python scripts/seed_demo.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import service  # noqa: E402
from app.config import Config  # noqa: E402

# (站点, 样例文件, 各鸟种列声明类型)
SAMPLES = [
    ("青湾监测站", "station_qingwan_2023.csv", ["uint64", "uint64"]),
    ("青湾监测站", "station_qingwan_2024.csv", ["uint64", "uint64"]),
    ("芦苇荡监测站", "station_luweidang_2023.csv", ["text", "uint64"]),  # 触发「白鹭」提升为 text
]


def main():
    cfg = Config()
    service.init_storage(cfg)
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for station, fname, types in SAMPLES:
        path = os.path.join(root, "samples", fname)
        r = service.publish(cfg, station, fname, path, types)
        print(f"published {fname}: batch={r['batch_id'][:8]} rows={r['n_rows']} changes={r['changes']}")


if __name__ == "__main__":
    main()
