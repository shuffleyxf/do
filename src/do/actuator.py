import time
from threading import Condition, Thread

from configuration import configuration
from base import RetryStrategy
from do_log import info, exception, error
from base import FailedTask
from storage_helper import task_interrupted, take_failed_task, next_failed_task, task_failed


class MetaProcessor:
    """
    元数据处理器
    """
    _META_KEY = 'kEfrwsLmeo9QDQK4qUZuJv0ZL8yMLdIA'  # 元数据键名

    @staticmethod
    def add_meta(task: FailedTask, kwargs: dict):
        kwargs[MetaProcessor._META_KEY] = task

    @staticmethod
    def take_task(kwargs: dict):
        task = kwargs[MetaProcessor._META_KEY]
        kwargs.pop(MetaProcessor._META_KEY)
        return task

    @staticmethod
    def exists_meta(kwargs: dict) -> bool:
        return MetaProcessor._META_KEY in kwargs


class DoActuator:
    """
    重试执行器
    """

    def __init__(self):
        self._runner_registry = dict()
        self._strategy_registry = dict()
        self._cond = Condition()  # 条件量

    def _wait(self, timeout: float = None) -> None:
        with self._cond:
            self._cond.wait(timeout)

    def _notify(self) -> None:
        with self._cond:
            self._cond.notify()


    def _redo(self, task: FailedTask) -> None:
        """重试执行任务
        重试时会将元数据放入关键字参数字典中

        Args:
            task (FailedTask): 失败的任务
        """
        runner = self._runner_registry.get(task.runner_name)
        if runner is None:
            error(f'runner not found, stop retry {task}.')
            task_interrupted(task)
            return

        args, kwargs = task.task_args, task.task_kwargs
        MetaProcessor.add_meta(task, kwargs)
        try:
            runner.run(*args, **kwargs)
        except Exception:
            error(f'Task failed: {task}')

    def _main_loop(self) -> None:
        """主循环
        不断重试获取失败任务并执行
        """
        while True:
            try:
                now = time.time()
                task = take_failed_task()
                if task is not None:
                    self._redo(task)
                else:
                    next_task = next_failed_task()
                    if next_task is not None:
                        wait_time = next_task.next_run_time - now
                        self._wait(wait_time)
                    else:
                        self._wait()
            except Exception:
                exception("Main loop crash!")


    def start(self, block: bool=True) -> None:
        """启动do机制

        Args:
            block (bool, optional): 是否阻塞. 默认为True.
        """
        if block:
            self._main_loop()
        else:
            Thread(name='do-worker', target=self._main_loop, daemon=True).start()

    def _next_run_time(self, task: FailedTask) -> float:
        if task.task_name in self._strategy_registry:
            strategy = self._strategy_registry[task.task_name]
        else:
            strategy = configuration.strategy
        return strategy.next_run_time(task)

    def handle_failed_task(self, task: FailedTask):
        """
        失败任务处理
        Args:
            task: 失败的任务
        """
        info(f'New FailedTask: str({task})')
        next_run_time = self._next_run_time(task)
        task.next_run_time = next_run_time
        task_failed(task)
        self._notify()

    def register_strategy(self, task_name: str, strategy: RetryStrategy):
        """
        注册重试策略到控制器
        Args:
            task_name: 任务名
            strategy: 重试策略
        """
        self._strategy_registry[task_name] = strategy

    def register_runner(self, name: str, runner: callable) -> None:
        """
        注册任务执行器

        Args:
            name (str): 名字
            runner (callable): 任务执行函数
        """
        self._runner_registry[name] = runner


actuator = DoActuator()








