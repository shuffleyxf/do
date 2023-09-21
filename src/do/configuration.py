from dataclasses import dataclass, field

from base import Storage, RetryStrategy, DefaultStrategy, TaskType
from error import ConfigureException
from storage.memory import MemoryStorage


@dataclass
class Configuration:
    """
    全局配置类
    """
    task_type: TaskType = field(default=TaskType.Idempotent)
    max_retry: int = field(default=-1)
    storage: Storage = field(default=MemoryStorage())
    strategy: RetryStrategy = field(default=DefaultStrategy())

    def configure(self, task_type: TaskType = None,
                  storage: Storage = None,
                  max_retry: int = None,
                  namer_cls: type = None) -> None:
        """配置全局参数

        Args:
            task_type (TaskType, optional): 任务类型.
            storage (Storage, optional): 存储模式.
            max_retry (int, optional): 最大重试次数.
            namer_cls (type, optional): 命名生成器.
        """
        try:
            kwargs = locals().copy()
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            for name, val in kwargs.items():
                self.__setattr__(name, val)
        except Exception as e:
            raise ConfigureException(f"Configure error!") from e


configuration = Configuration()
configure = configuration.configure