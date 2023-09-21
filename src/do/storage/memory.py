import time
import queue
from threading import RLock

from base import FailedTask, Storage, TaskState
from error import DataException


class QueryablePriorityQueue(queue.PriorityQueue):
    """
    可查询的优先队列
    """
    def peek(self) -> FailedTask:
        try:
            task = self.get_nowait()[1]
            self.put_task(task)
            return task
        except queue.Empty:
            return None

    def put_task(self, task: FailedTask):
        elem = (task.next_run_time, task)
        print(elem)
        self.put_nowait((task.next_run_time, task))

    def get_task(self) -> FailedTask:
        return self.get_nowait()[1]


class MemoryStorage(Storage):
    """基于内存的任务存储器
    底层数据结构是一个字典+阻塞队列
    """
    _id = 1     # 自增ID

    def __init__(self, max_size=0) -> None:
        super().__init__()
        self._queue = QueryablePriorityQueue(maxsize=max_size) # 小顶堆
        self._db = dict()
        self._lock = RLock()

    def take(self) -> FailedTask:
        try:
            task = self._queue.peek()
            if task is not None and task.next_run_time <= time.time():
                return self._queue.get_task()
        except queue.Empty:
            return None

    def put(self, task: FailedTask) -> None:
        if task.task_id == FailedTask.INIT_ID:
            task.task_id = self._gen_id()
        self._db[task.task_id] = task
        try:
            if task.state == TaskState.Failed:
                self._queue.put_task(task)
        except queue.Full:
            raise DataException("queue already full！")

    def _gen_id(self) -> int:
        with self._lock:
            take_id = self._id
            self._id += 1
            return take_id

    def remove(self, task_id: int) -> None:
        if task_id in self._db:
            self._db.pop(task_id)

    def all(self) -> [FailedTask]:
        return [task for task in self._db.values()]

    def get_next(self) -> [FailedTask]:
        return self._queue.peek()
