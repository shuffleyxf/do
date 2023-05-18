from threading import Thread
import context
import actuator

configure = context.configure
do = actuator.do


def start(block=True) -> None:
    """启动do机制

    Args:
        block (bool, optional): 是否阻塞. 默认为True.
    """
    if block:
        actuator.main_loop()
    else:
        Thread(name='do工作线程', target=actuator.main_loop, daemon=True).start()
