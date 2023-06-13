from dataclasses import dataclass, field
from storage.base import Storage, TaskType
from storage.memory import MemoryStorage
from storage import set_storage
from error import ConfigureException

from model import Runner


@dataclass
class DoContext:
    """上下文"""
    registry: dict = field(default_factory=dict)
    task_type: TaskType = field(default=TaskType.Idempotent)    # 任务类型，默认为幂等的
    max_retry: int = field(default=-1)  # 最大重试次数，默认无限重试
    storage: Storage = field(default=MemoryStorage())    # 存储类型，默认基于内存

    def __post_init__(self):
        """同步更新全局存储器"""
        set_storage(self.storage)


context = DoContext()   # 上下文对象


def configure(task_type: TaskType = None,
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
            context.__setattr__(name, val)
        context.__post_init__()
    except Exception as e:
        raise ConfigureException(f"Configure error!") from e


def register(name: str, runner: callable) -> None:
    """
    注册任务执行器

    Args:
        name (str): 名字
        runner (callable): 任务执行函数
    """
    context.registry[name] = runner


def get_runner(name: str) -> Runner:
    """获取任务执行器

    Args:
        name (str): 名字

    Returns:
        Runner: 任务执行器
    """
    return context.registry.get(name)
