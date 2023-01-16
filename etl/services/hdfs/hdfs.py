from etl.services.hdfs import BaseHDFS


class HDFSSerivce(BaseHDFS):
    def __init__(self) -> None:
        super().__init__()

    def is_exists(self, path: str):
        return True if self.client.status(path, strict=False) else False
