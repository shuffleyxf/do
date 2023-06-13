import copy
import inspect
import traceback
from enum import IntEnum
from functools import partial, wraps
from threading import Condition

from do_log import info, exception
from storage.base import FailedTask, TaskType
from storage import take_failed_task, new_failed_task, task_failed, \
    task_success, task_interrupted
from context import get_runner, register
from context import context as _context
from model import DefaultNamer, Runner
from error import DoException

_META_KEY = 'kEfrwsLmeo9QDQK4qUZuJv0ZL8yMLdIA'   # 元数据键名
_cond: Condition = Condition()  # 条件量


def _wait() -> None:
    with _cond:
        _cond.wait()


def _notify() -> None:
    with _cond:
        _cond.notify()


def _redo(task: FailedTask) -> None:
    """重试执行任务
    重试时会将元数据放入关键字参数字典中

    Args:
        task (FailedTask): 失败的任务
    """
    runner = get_runner(task.runner_name)
    if runner is None:
        info(f'runner {task.runner_name} not found, stop retry {task.task_name}.')
        task_interrupted(task)
        return

    args, kwargs = task.task_args, task.task_kwargs
    kwargs[_META_KEY] = task
    try:
        runner.run(*args, **kwargs)
    except Exception:
        exception(f'failed to redo task-{task.task_name}.')
        pass


def main_loop() -> None:
    """主循环
    不断重试获取失败任务并执行
    """
    while True:
        task = take_failed_task()
        if task is None:
            _wait()
        else:
            _redo(task)


class TryNext(Exception):
    """重试异常，用于非幂等函数的重试"""

    def __init__(self, *args: list, **kwargs: dict) -> None:
        self.args = args
        self.kwargs = kwargs


def _is_pure_function(func: callable) -> bool:
    """
    Args:
        func: 输入函数
    Returns: 若是纯函数返回True，否则返回False
    """
    module = inspect.getmodule(func)
    return module is not None and hasattr(module, func.__name__)


class FuncType(IntEnum):
    FUNC = 1  # 函数类型
    METHOD = 2  # 方法类型
    AUTO_CHECK = 3  # 自动识别（但不保证准确）


def do(func: callable = None, func_type: FuncType = FuncType.AUTO_CHECK,
       task_type: TaskType = None,
       runner_name: str = '', namer_cls: type = DefaultNamer,
       max_retry: int = 0) -> callable:
    """函数装饰器
    该装饰器所装饰的函数终将执行成功(除非主动放弃)
    如果func是类的方法，不应该直接装饰，应该采用语句更新方法引用。

    Args:
        func (callable): 装饰函数
        func_type: 函数类型
        task_type (TaskType): 任务类型
        runner_name (str, optional): 任务运行器名字，默认为函数名
        namer_cls (type, optional): 任务名生成器类型，默认所有DefaultNamer.
        max_retry (int, optional): 最大重试次数
    """
    if func is None:
        return partial(do, task_type=task_type, runner_name=runner_name,
                       namer_cls=namer_cls, max_retry=max_retry)

    if not runner_name:
        runner_name = func.__name__

    def _first_do(args: tuple, kwargs: dict) -> FailedTask:
        nonlocal runner_name, task_type, max_retry
        local_task_type = _context.task_type if task_type is None else task_type
        local_max_retry = _context.max_retry if max_retry == 0 else max_retry
        namer = namer_cls()
        task_args = copy.copy(args)
        if func_type == FuncType.METHOD or (func_type == FuncType.AUTO_CHECK and not _is_pure_function(func)):
            task_args = task_args[1:]
        task_name = namer.gen(func, args, kwargs)
        return new_failed_task(task_name, local_task_type, task_args, kwargs, runner_name, local_max_retry)

    @wraps(func)
    def wrapper(*args, **kwargs) -> object:
        if _META_KEY not in kwargs:
            task = _first_do(args, kwargs)
        else:
            task = kwargs.get(_META_KEY)
            kwargs.pop(_META_KEY)   # 移除元数据

        try:
            result = func(*args, **kwargs)
        except TryNext as e:
            task.task_args = e.args
            task.task_kwargs = e.kwargs
            info(f'non-idempotent task-{task.task_name} failed for the {task.retry_count+1}th time.')
            task_failed(task)
            _notify()
            raise
        except Exception:
            if task.task_type == TaskType.Idempotent:
                task.task_kwargs = kwargs
                info(f'idempotent task-{task.task_name} failed for the {task.retry_count + 1}th time.')
                task_failed(task)
                _notify()
            else:
                info(f"task-{task.task_name} is not idempotent.")
            raise
        else:
            info(f'task-{task.task_name} success.')
            task_success(task.task_id)
            return result

    runner = Runner(wrapper)
    register(runner_name, runner)
    return wrapper

