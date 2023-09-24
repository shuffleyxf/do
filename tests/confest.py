import os
import sys
import time

import pytest

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(root_dir, 'src', 'do'))

from api import start_do as start


@pytest.fixture(scope="session")
def start_do():
    start(block=False)
    db_path = os.path.join(os.getcwd(), 'do.db')
    if os.path.isfile(db_path):
        os.remove(db_path)


def keep_check(condition: callable, max_time: float = 60, interval: float = 0.1):
    """
    持续检测，等待某个条件为真
    Args:
        condition: 条件检测函数
        max_time: 最大检测时长
        interval: 检测间隔

    Returns:

    """
    turn = int(max_time / interval)
    for x in range(turn):
        if condition():
            return
        time.sleep(interval)
    raise Exception("Condition still False.")