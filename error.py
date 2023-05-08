

class DoException(Exception):
    """基础异常"""

    def __init__(self, msg, *args: object) -> None:
        super().__init__(*args)
        self.msg = msg


class ConfigureException(DoException):
    """用户配置异常"""

    def __init__(self, msg, *args: object) -> None:
        super().__init__(msg, *args)


class DataException(DoException):
    """数据读写异常"""

    def __init__(self, msg, *args: object) -> None:
        super().__init__(msg, *args)
