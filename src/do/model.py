from abc import ABC, abstractmethod


class Runner:
    """任务运行器"""

    def __init__(self, func: callable) -> None:
        """初始化

        Args:
            func (callable): 执行函数
        """
        self.func = func

    def run(self, *args, **kwargs) -> object:
        """执行任务

        Returns:
            object: 任务执行结果
        """
        return self.func(*args, **kwargs)


class BaseNamer(ABC):
    """
    任务名生成器
    """
    @abstractmethod
    def gen(self, func: callable, args: list, kwargs: dict) -> str:
        """根据函数名和传参生成任务名

        Args:
            func (callable): 任务执行函数
            args (list): 变长参数
            kwargs (dict): 关键字参数
        """
        pass


class DefaultNamer(BaseNamer):
    def gen(self, func: callable, args: list, kwargs: dict) -> str:
        """默认任务名生成器，和函数名相同

        Args:
            func (callable): 任务执行函数_
            args (list): 变长参数
            kwargs (dict): 关键字参数

        Returns:
            str: 任务名
        """
        return func.__name__
