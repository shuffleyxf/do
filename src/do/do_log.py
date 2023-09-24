import logging
import os

from error import DoException

_DEFAULT_FORMAT = '%(asctime)s %(levelname)s %(message)s'
_DEFAULT_PATH = os.path.join(os.path.expanduser("~"), 'do.log')

def _init_logger():
    """日志初始化
    Returns: 日志类
    """
    logger = logging.getLogger('do')
    logger.propagate = False
    logger.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
    logger.addHandler(console)
    return logger


_logger: logging.Logger = _init_logger()


def _set_level(level: int):
    """设置日志等级

    Args:
        level: 日志等级

    """
    if level not in [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]:
        raise DoException(f"illegal log level: {level}")
    _logger.setLevel(level)
    for handler in _logger.handlers:
        handler.setLevel(level)


def _add_file_handler(file_path: str):
    """设置日志文件路径

    Args:
        file_path: 日志文件路径
    """
    file_handler = None
    for handler in _logger.handlers:
        if type(handler) == logging.FileHandler:
            file_handler = handler
            break
    if file_handler:
        _logger.removeHandler(file_handler)

    if file_path is not None:
        file_dir = os.path.dirname(file_path)
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)
        file_handler = logging.FileHandler(file_path, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
        file_handler.setLevel(_logger.level)
        _logger.addHandler(file_handler)


debug, info, warning, error, exception = _logger.debug, _logger.info, _logger.warning, _logger.error, _logger.exception


def configure_logger(level: int = logging.INFO,
                     log_file: bool = True,
                     log_file_path: str = _DEFAULT_PATH):
    """ 配置日志

    Args:
        level: 日志等级
        log_file: 是否开启日志日志
        log_file_path: 日志文件路径

    """
    if log_file:
        _add_file_handler(log_file_path)
    _set_level(level)

