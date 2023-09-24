import copy
import inspect
import logging
from functools import partial, wraps

from actuator import actuator, MetaProcessor
from configuration import configuration, configure
from base import FuncType, TaskType, DefaultNamer, RetryStrategy, FailedTask, Runner
from do_log import info, configure_logger
import storage_helper
task_info = storage_helper.task_info
start = actuator.start


class TryNext(Exception):
    """重试异常，用于非幂等函数的重试"""

    def __init__(self, *args: list, **kwargs: dict) -> None:
        self.args = args
        self.kwargs = kwargs


def do(func: callable = None,
       func_type: FuncType = FuncType.AUTO_CHECK,
       task_type: TaskType = None,
       runner_name: str = '',
       namer_cls: type = DefaultNamer,
       max_retry: int = 0,
       retry_strategy: RetryStrategy = None) -> callable:
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
        retry_strategy (RetryStrategy): 重试策略
    """
    if func is None:
        return partial(do, task_type=task_type, runner_name=runner_name,
                       namer_cls=namer_cls, max_retry=max_retry, retry_strategy=retry_strategy)

    if not runner_name:
        runner_name = func.__name__

    if retry_strategy is not None:
        actuator.register_strategy(runner_name, retry_strategy)

    def _is_pure_function() -> bool:
        module = inspect.getmodule(func)
        return module is not None and hasattr(module, func.__name__)

    def _first_do(args: tuple, kwargs: dict) -> FailedTask:
        nonlocal runner_name, task_type, max_retry
        local_task_type = configuration.task_type if task_type is None else task_type
        local_max_retry = configuration.max_retry if max_retry == 0 else max_retry
        namer = namer_cls()
        task_args = copy.copy(args)
        if func_type == FuncType.METHOD or (func_type == FuncType.AUTO_CHECK and not _is_pure_function()):
            task_args = task_args[1:]
        task_name = namer.gen(func, args, kwargs)
        return storage_helper.new_failed_task(task_name, local_task_type, task_args, kwargs, runner_name, local_max_retry)

    @wraps(func)
    def wrapper(*args, **kwargs) -> object:
        if MetaProcessor.exists_meta(kwargs):
            task = MetaProcessor.take_task(kwargs)
        else:
            task = _first_do(args, kwargs)

        try:
            result = func(*args, **kwargs)
        except TryNext as e:
            task.task_args = e.args
            task.task_kwargs = e.kwargs
            info(f'non-idempotent task-{task.task_name} failed for the {task.retry_count+1}th time.')
            actuator.handle_failed_task(task)
            raise
        except Exception:
            if task.task_type == TaskType.Idempotent:
                task.task_kwargs = kwargs
                info(f'idempotent task-{task.task_name} failed for the {task.retry_count + 1}th time.')
                actuator.handle_failed_task(task)
            else:
                info(f"task-{task.task_name} is not idempotent.")
            raise
        else:
            info(f'task-{task.task_name} success.')
            storage_helper.task_success(task.task_id)
            return result

    runner = Runner(wrapper)
    actuator.register_runner(runner_name, runner)
    return wrapper


def start_do(block: bool=False, log_level: int = logging.ERROR, log_file: bool=False):
    """
    启动入口
    Args:
        block: 是否阻塞
        log_level: 日志等级
        log_file: 是否开启文件日志

    """
    actuator.start(block=block)
    configure_logger(level=log_level, log_file=log_file)