from threading import Thread
from context import configure
from actuator import do
from actuator import main_loop
from storage import task_info


def start(block=True) -> None:
    """启动do机制

    Args:
        block (bool, optional): 是否阻塞. 默认为True.
    """
    if block:
        main_loop()
    else:
        Thread(name='do工作线程', target=main_loop, daemon=True).start()
