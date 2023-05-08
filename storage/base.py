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


@dataclass
class FailedTask:
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
    state: TaskState    # 任务状态


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
    def gen_id(self) -> int:
        """生成唯一id

        Returns:
            int: 任务标识
        """
        pass

    @abstractmethod
    def remove(self, task_id: int) -> None:
        """根据ID移除失败任务

        Args:
            task_id (int): 任务ID
        """
        pass
