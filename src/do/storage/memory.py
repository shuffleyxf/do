from queue import Queue
import queue
from storage.base import FailedTask, Storage, TaskState
from error import DataException


class MemoryStorage(Storage):
    """基于内存的任务存储器
    底层数据结构是一个字典+阻塞队列
    """
    _id = 1     # 自增ID

    def __init__(self, max_size=0) -> None:
        super().__init__()
        self._queue = Queue(maxsize=max_size)
        self._db = dict()

    def take(self) -> FailedTask:
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None

    def put(self, task: FailedTask) -> None:
        self._db[task.task_id] = task
        try:
            if task.state == TaskState.Failed:
                self._queue.put_nowait(task)
        except queue.Full:
            raise DataException("队列已满！")

    def gen_id(self) -> int:
        take_id = self._id
        self._id += 1
        return take_id

    def remove(self, task_id: int) -> None:
        if task_id in self._db:
            self._db.pop(task_id)

    def all(self) -> [FailedTask]:
        return [task for task in self._db.values()]
