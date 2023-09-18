import time
import traceback

from do_log import exception
from storage.base import FailedTask, Storage, TaskState, TaskType, ANY_TIME

_storage: Storage = None    # 存储类


def set_storage(storage: Storage):
    """设置全局存储器

    Args:
        storage (Storage): 全局存储器
    """
    global _storage
    _storage = storage


def new_failed_task(task_name: str, task_type: TaskType, task_args: list, task_kwargs: dict,
                    runner_name: str, max_retry: int) -> FailedTask:
    """创建新的失败任务

    Args:
        task_name (str): 任务名
        task_type (TaskType):任务类型
        task_args (list): 任务变长参数
        task_kwargs (dict): 任务关键字参数
        runner_name (str): 任务运行器名
        max_retry (int): 最大重试次数

    Returns:
        FailedTask: 失败任务对象
    """
    return FailedTask(task_id=FailedTask.INIT_ID,
                      task_name=task_name,
                      task_type=task_type,
                      task_args=task_args,
                      task_kwargs=task_kwargs,
                      runner_name=runner_name,
                      retry_count=0,
                      max_retry=max_retry,
                      create_time=time.time(),
                      update_time=time.time(),
                      next_run_time=ANY_TIME,
                      state=TaskState.Failed)


def task_failed(task: FailedTask) -> None:
    """任务失败

    Args:
        task (FailedTask): 失败任务
    """
    task.update_time = time.time()
    if task.next_run_time == ANY_TIME:
        task.next_run_time = time.time()
    if task.max_retry != 0 and task.retry_count == task.max_retry:
        task.state = TaskState.Stopped
    else:
        task.retry_count += 1
        task.state = TaskState.Failed
    try:
        _storage.put(task)
    except Exception:
        exception(f'failed to save task-{task.task_name}.')
        raise


def task_interrupted(task: FailedTask) -> None:
    """任务中断

    Args:
        task (FailedTask): 失败任务
    """
    task.update_time = time.time()
    task.state = TaskState.Interrupted
    _storage.put(task)


def take_failed_task() -> FailedTask:
    """获取需要重试的失败任务

    Returns:
        FailedTask: 待重试失败任务
    """
    return _storage.take()


def next_failed_task() -> FailedTask:
    """
    Returns: 返回下一个待执行的任务
    """
    return _storage.get_next()


def task_success(task_id: int) -> None:
    """任务成功

    Args:
        task_id (int): 任务ID
    """
    if task_id != FailedTask.INIT_ID:
        _storage.remove(task_id)


def task_info() -> [dict]:
    """
    Returns: 返回字典类型的任务信息
    """
    info_list = []
    for task in _storage.all():
        info_list.append(task.__dict__.copy())
    return info_list
