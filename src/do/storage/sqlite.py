import json
import sqlite3
from contextlib import contextmanager
from sqlite3 import Cursor
from typing import Union

from storage import Storage, FailedTask, TaskState, TaskType
from do_log import debug


class SqliteStorage(Storage):
    """基于sqlite的任务存储器"""
    _TB_NAME = 'failed_task'    # 表名
    _PK = 'task_id' # 主键名

    def __init__(self, db='do.db') -> None:
        super().__init__()
        self._db = db
        self._init_db()

    @contextmanager
    def _new_conn(self):
        conn = sqlite3.connect(self._db)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.close()
            raise

    def _init_db(self):
        with self._new_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self._TB_NAME}`(
                task_id INTEGER PRIMARY KEY,
                task_type INTEGER,
                task_name TEXT,
                task_args TEXT,
                task_kwargs TEXT,
                runner_name TEXT,
                retry_count INTEGER,
                max_retry INTEGER,
                create_time FLOAT,
                update_time FLOAT,
                state INTEGER
            )
            """)

    @staticmethod
    def _to_data_dict(task: FailedTask) -> dict:
        return {
            'task_type': int(task.task_type),
            'task_name': task.task_name,
            'task_args': json.dumps({'task_args': task.task_args}),
            'task_kwargs': json.dumps(task.task_kwargs),
            'runner_name': task.runner_name,
            'retry_count': task.retry_count,
            'max_retry': task.max_retry,
            'create_time': task.create_time,
            'update_time': task.update_time,
            'state': int(task.state)
        }

    @staticmethod
    def _to_task(data_dict: dict) -> FailedTask:
        return FailedTask(
            task_id=data_dict.get('task_id'),
            task_type=TaskType(data_dict.get('task_type')),
            task_name=data_dict.get('task_name'),
            task_args=json.loads(data_dict.get('task_args'))['task_args'],
            task_kwargs=json.loads(data_dict.get('task_kwargs')),
            runner_name=data_dict.get('runner_name'),
            retry_count=data_dict.get('retry_count'),
            max_retry=data_dict.get('max_retry'),
            create_time=data_dict.get('create_time'),
            update_time=data_dict.get('update_time'),
            state=TaskState(data_dict.get('state'))
        )

    @staticmethod
    def _execute_sql(cursor: Cursor, *args):
        debug(f'sqlite execute sql: {args}.')
        cursor.execute(*args)

    def _select(self, cursor: Cursor, condition: str = "") -> [FailedTask]:
        keys = ['task_id', 'task_type', 'task_name', 'task_args', 'task_kwargs', 'runner_name',
                'retry_count', 'max_retry', 'create_time', 'update_time', 'state']
        select_sql = f"SELECT "
        for k in keys:
            select_sql += f"{k}, "
        select_sql = f'{select_sql.rstrip(", ")} FROM {self._TB_NAME} {condition}'
        self._execute_sql(cursor, select_sql)
        values = cursor.fetchall()
        task_list = list()
        data_dict = dict()
        for value in values:
            for i, k in enumerate(keys):
                data_dict[k] = value[i]
            task_list.append(self._to_task(data_dict))
        return task_list

    def take(self) -> Union[FailedTask, None]:
        with self._new_conn() as conn:
            cursor = conn.cursor()
            task_list = self._select(cursor, f"""
                WHERE state = 1
                ORDER BY UPDATE_TIME
                LIMIT 1
            """)
            if task_list:
                return task_list[0]
            return None

    def put(self, task: FailedTask) -> None:
        with self._new_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
            SELECT * FROM `{self._TB_NAME}`
            WHERE task_id = {task.task_id}
            """)
            rows = cursor.fetchall()
            exists = len(rows) > 0
            data_dict = self._to_data_dict(task)
            values = list()
            if exists:
                update_sql = f"UPDATE `{self._TB_NAME}` SET "
                for k in ['task_name', 'task_args', 'task_kwargs', 'retry_count', 'update_time', 'state']:
                    update_sql += f"{k} = ?, "
                    values.append(data_dict.get(k))
                update_sql = update_sql.rstrip(", ")
                self._execute_sql(cursor, update_sql, values)
            else:
                insert_sql = f"INSERT INTO `{self._TB_NAME}`("
                key_counter = 0
                for k, v in data_dict.items():
                    if k == self._PK:
                        continue
                    insert_sql += f"{k}, "
                    values.append(data_dict.get(k))
                    key_counter += 1
                insert_sql = f"{insert_sql.rstrip(', ')}) VALUES(" \
                             + (key_counter - 1) * "?, " \
                             + "?)"
                self._execute_sql(cursor, insert_sql, values)

    def remove(self, task_id: int) -> None:
        with self._new_conn() as conn:
            cursor = conn.cursor()
            self._execute_sql(cursor, f"DELETE FROM `{self._TB_NAME}` WHERE {self._PK} = {task_id}")

    def all(self) -> [FailedTask]:
        with self._new_conn() as conn:
            cursor = conn.cursor()
            return self._select(cursor)
