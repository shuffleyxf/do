import time

import pytest

from api import *
from controller import IntervalStrategy
from model import BaseNamer
from storage import TaskType
from confest import start_do, keep_check
from storage.memory import MemoryStorage
from storage.sqlite import SqliteStorage


class TestDo1:
    """
    测试基于do的自动重试
    """
    data = dict()

    def get_66(self):
        self.data['counter'] += 1
        if self.data['counter'] != 66:
            raise Exception("not 66")
        self.data['66'] = True

    def do_get_66(self):
        self.data['counter-do'] += 1
        if self.data['counter-do'] != 66:
            raise Exception("not 66")
        self.data['66-do'] = True

    def test_case(self, start_do):
        self.do_get_66 = do(self.do_get_66)

        self.data.update({
            '66': False,
            'counter': 0,
            '66-do': False,
            'counter-do': 0
        })

        assert self.data['66'] is False
        assert self.data['66-do'] is False

        with pytest.raises(Exception):
            self.get_66()
        with pytest.raises(Exception):
            self.do_get_66()
        keep_check(lambda : self.data['66-do'] is True)

        assert self.data['66'] is False
        assert self.data['66-do'] is True


class TestDo2:
    """
    测试基于do的有限重试
    """
    data = dict()

    def get_66(self):
        self.data['counter'] += 1
        if self.data['counter'] != 66:
            raise Exception("not 66")
        self.data['66'] = True

    def do_get_66(self):
        self.data['counter-do'] += 1
        if self.data['counter-do'] != 66:
            raise Exception("not 66")
        self.data['66-do'] = True

    def test_case(self, start_do):
        self.do_get_66 = do(self.do_get_66, max_retry=10)

        self.data.update({
            '66': False,
            'counter': 0,
            '66-do': False,
            'counter-do': 0
        })

        assert self.data['66'] is False
        assert self.data['66-do'] is False

        with pytest.raises(Exception):
            self.get_66()
        with pytest.raises(Exception):
            self.do_get_66()

        with pytest.raises(Exception):
            keep_check(lambda : self.data['66-do'] is True, max_time=10)

        assert self.data['66'] is False
        assert self.data['66-do'] is False
        assert self.data['counter-do'] == 11


class TestDo3:
    """
    测试基于do的非幂等任务处理
    """
    data = dict()

    def do_get_66(self):
        self.data['counter-do'] += 1
        if self.data['counter-do'] != 66:
            raise Exception("not 66")
        self.data['66-do'] = True

    def test_case(self, start_do):
        self.do_get_66 = do(self.do_get_66, task_type=TaskType.NonIdempotent)

        self.data.update({
            '66-do': False,
            'counter-do': 0
        })

        assert self.data['66-do'] is False

        with pytest.raises(Exception):
            self.do_get_66()

        assert self.data['66-do'] is False
        assert self.data['counter-do'] == 1


class TestDo4:
    """
    测试基于do的自定义运行器
    """
    data = dict()

    def do_get_66(self):
        self.data['counter-do'] += 1
        if self.data['counter-do'] != 66:
            raise Exception("not 66")
        self.data['66-do'] = True

    def do_10_get_66(self):
        self.data['counter-do'] += 1
        if self.data['counter-do'] != 66:
            raise Exception("not 66")
        self.data['66-do'] = False

    def test_case(self, start_do):
        self.do_get_66 = do(self.do_get_66, runner_name="do_10_get_66")
        self.do_10_get_66 = do(self.do_10_get_66)

        self.data.update({
            '66-do': False,
            'counter-do': 0
        })

        assert self.data['66-do'] is False

        with pytest.raises(Exception):
            self.do_get_66()
        keep_check(lambda : self.data['counter-do'] == 66)

        assert self.data['66-do'] is False
        assert self.data['counter-do'] == 66


class TestDo5:
    """
    测试基于do的自定义任务命名器
    """
    class CustomizeNamer(BaseNamer):
        def gen(self, func: callable, args: list, kwargs: dict) -> str:
            return "CustomTask"

    def do_never_success(self):
        raise Exception("failed")

    def do_never_success_custom(self):
        raise Exception("failed")

    def test_case(self, start_do):
        self.do_never_success = do(self.do_never_success)
        self.do_never_success_custom = do(self.do_never_success_custom, namer_cls=self.CustomizeNamer)

        with pytest.raises(Exception):
            self.do_never_success()
        with pytest.raises(Exception):
            self.do_never_success_custom()

        task_name_list = [task.get('task_name') for task in task_info()]
        assert "do_never_success" in task_name_list
        assert "CustomTask" in task_name_list


class TestDo6:
    """
    测试基于sqlite的内存存储器
    """
    data = dict()

    def get_66(self):
        self.data['counter'] += 1
        if self.data['counter'] != 66:
            raise Exception("not 66")
        self.data['66'] = True

    def do_get_66(self):
        self.data['counter-do'] += 1
        if self.data['counter-do'] != 66:
            raise Exception("not 66")
        self.data['66-do'] = True

    def test_case(self, start_do):
        self.do_get_66 = do(self.do_get_66)
        configure(storage=SqliteStorage())
        self.data.update({
            '66': False,
            'counter': 0,
            '66-do': False,
            'counter-do': 0
        })

        assert self.data['66'] is False
        assert self.data['66-do'] is False

        with pytest.raises(Exception):
            self.get_66()
        with pytest.raises(Exception):
            self.do_get_66()
        keep_check(lambda : self.data['66-do'] is True)

        assert self.data['66'] is False
        assert self.data['66-do'] is True


class TestDo7:
    """
    测试重试策略
    """
    counter_1 = 0
    counter_2 = 0

    def do_add(self):
        self.counter_1 += 1
        raise Exception()

    def do_add_per_second(self):
        self.counter_2 += 1
        raise Exception()

    def test_case(self, start_do):
        configure(storage=MemoryStorage())
        self.do_add = do(self.do_add)
        self.do_add_per_second = do(self.do_add_per_second, retry_strategy=IntervalStrategy(1))

        with pytest.raises(Exception):
            self.do_add()
        with pytest.raises(Exception):
            self.do_add_per_second()
        time.sleep(5)

        expect_count = 5
        margin = 1
        assert self.counter_1 > self.counter_2
        assert expect_count + margin >= self.counter_2 >= expect_count - margin

