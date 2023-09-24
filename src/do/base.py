import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum


class TaskState(IntEnum):
    """任务状态"""
    Success = 0  # 成功
    Failed = 1  # 失败
    Stopped = 2    # 停止
    Interrupted = 3  # 中断


class TaskType(IntEnum):
    """任务类型"""
    Idempotent = 1  # 幂等，幂等的函数重试没有副作用
    NonIdempotent = 0   # 非幂等，非幂等函数重试可能存在副作用，需要通过TryNext异常传入恰当的参数


class FuncType(IntEnum):
    FUNC = 1  # 函数类型
    METHOD = 2  # 方法类型
    AUTO_CHECK = 3  # 自动识别（但不保证准确）


ANY_TIME = -1   # 任何时候


@dataclass
class FailedTask:
    INIT_ID = -1   # 初始ID

    """任务数据类"""
    task_id: int    # 任务唯一标识
    task_type: TaskType  # 任务类型
    task_name: str  # 任务名
    task_args: list  # 任务变长参数
    task_kwargs: dict   # 任务关键字参数
    runner_name: str    # 任务执行器名
    retry_count: int    # 当前重试次数
    max_retry: int  # 最大重试次数
    create_time: float  # 创建时间
    update_time: float  # 更新时间
    next_run_time: float    # 下次执行时间
    state: TaskState    # 任务状态

    def __gt__(self, other):
        return self.task_id < other.task_id

    def __str__(self):
        return (f'(id={self.task_id}, name={self.task_name}, runner_name={self.runner_name},'
                f' retry_count={self.retry_count})')


class Storage(ABC):
    """存储器"""
    @abstractmethod
    def take(self) -> FailedTask:
        """返回一个执行失败的任务

        Returns:
            FailedTask: 失败任务
        """
        pass

    @abstractmethod
    def put(self, task: FailedTask) -> None:
        """新增失败任务

        Args:
            task (FailedTask): 失败任务
        """
        pass

    @abstractmethod
    def remove(self, task_id: int) -> None:
        """根据ID移除失败任务

        Args:
            task_id (int): 任务ID
        """
        pass

    @abstractmethod
    def all(self) -> [FailedTask]:
        """
        Returns: 返回记录的所有任务
        """
        pass

    @abstractmethod
    def get_next(self) -> [FailedTask]:
        """
        Returns: 返回最近需要执行的任务
        """
        pass


class Runner:
    """任务运行器"""

    def __init__(self, func: callable) -> None:
        """初始化

        Args:
            func (callable): 执行函数
        """
        self.func = func

    def run(self, *args, **kwargs) -> object:
        """执行任务

        Returns:
            object: 任务执行结果
        """
        return self.func(*args, **kwargs)


class BaseNamer(ABC):
    """
    任务名生成器
    """
    @abstractmethod
    def gen(self, func: callable, args: list, kwargs: dict) -> str:
        """根据函数名和传参生成任务名

        Args:
            func (callable): 任务执行函数
            args (list): 变长参数
            kwargs (dict): 关键字参数
        """
        pass


class DefaultNamer(BaseNamer):
    def gen(self, func: callable, args: list, kwargs: dict) -> str:
        """默认任务名生成器，和函数名相同

        Args:
            func (callable): 任务执行函数_
            args (list): 变长参数
            kwargs (dict): 关键字参数

        Returns:
            str: 任务名
        """
        return func.__name__


class RetryStrategy(ABC):
    """
    重试策略
    """
    @abstractmethod
    def next_run_time(self, task: FailedTask) -> float:
        """
        是否立即重试
        Args:
            task: 失败任务

        Returns: 是则返回True，否则返回False

        """
        pass


class DefaultStrategy(RetryStrategy):
    """
    默认重试策略：立即重试
    """
    def next_run_time(self, task: FailedTask) -> float:
        return ANY_TIME


class IntervalStrategy(RetryStrategy):
    """
    间隔型重试策略：每隔一段时间重试
    """
    def __init__(self, interval: float):
        """
        Args:
            interval: 重试间隔，单位为秒
        """
        self._interval = interval

    def next_run_time(self, task: FailedTask) -> float:
        return time.time() + self._interval

