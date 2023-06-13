import logging
import os

from error import DoException

content_format = '%(asctime)s %(levelname)s %(message)s'


def _init_logger():
    """日志初始化
    Returns: 日志类
    """
    logger = logging.getLogger('do')
    logger.propagate = False
    logger.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(content_format))
    logger.addHandler(console)
    return logger


_logger: logging.Logger = _init_logger()


def set_level(level: int):
    """设置日志等级

    Args:
        level: 日志等级

    """
    if level not in [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]:
        raise DoException(f"illegal log level: {level}")
    _logger.setLevel(level)
    for handler in _logger.handlers:
        handler.setLevel(level)


def set_log_file(file_path: str):
    """设置日志文件路径

    Args:
        file_path: 日志文件路径

    """
    file_dir = os.path.dirname(file_path)
    if not os.path.isdir(file_dir):
        os.makedirs(file_dir)
    file_handler = None
    for handler in _logger.handlers:
        if type(handler) == logging.FileHandler:
            file_handler = handler
            break
    if file_handler:
        _logger.removeHandler(file_handler)

    file_handler = logging.FileHandler(file_path, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(content_format))
    file_handler.setLevel(_logger.level)
    _logger.addHandler(file_handler)


debug, info, warning, error, exception = _logger.debug, _logger.info, _logger.warning, _logger.error, _logger.exception

