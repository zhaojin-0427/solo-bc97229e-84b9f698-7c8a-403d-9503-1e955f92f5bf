import os


class Config:
    """数据湖存储布局：DATA_DIR/{catalog.db, parquet/, staging/}"""

    def __init__(self, data_dir=None):
        base = data_dir or os.environ.get("DATA_DIR") or os.path.join(os.getcwd(), "data")
        self.data_dir = os.path.abspath(base)
        self.db_path = os.path.join(self.data_dir, "catalog.db")
        self.parquet_dir = os.path.join(self.data_dir, "parquet")
        self.staging_dir = os.path.join(self.data_dir, "staging")
        # 默认启动后台补建线程；自检脚本可关闭以便精确控制补建时机
        self.dup_backfill_enabled = True
