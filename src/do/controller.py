import time
from abc import ABC, abstractmethod

from storage import FailedTask, ANY_TIME


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


class RetryController:
    """
    重试控制器
    该类负责控制现在是否允许任务进行重试
    """
    def __init__(self):
        self.strategy_dict: dict = dict()
        self.default_strategy: RetryStrategy = DefaultStrategy()

    def next_run_time(self, task: FailedTask) -> float:
        if task.task_name in self.strategy_dict:
            strategy = self.strategy_dict[task.task_name]
        else:
            strategy = self.default_strategy
        return strategy.next_run_time(task)

    def register_strategy(self, task_name: str, strategy: RetryStrategy):
        """
        注册重试策略到控制器
        Args:
            task_name: 任务名
            strategy: 重试策略
        """
        self.strategy_dict[task_name] = strategy
