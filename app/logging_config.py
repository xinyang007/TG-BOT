import logging
import sys
from typing import Dict, Any
import json
from datetime import datetime
from .settings import settings


class StructuredFormatter(logging.Formatter):
    """结构化日志格式化器"""

    def format(self, record: logging.LogRecord) -> str:
        # 基础日志信息
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # 添加异常信息
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # 添加额外的上下文信息
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)

        # 添加用户ID（如果存在）
        if hasattr(record, 'user_id'):
            log_entry["user_id"] = record.user_id

        # 添加消息ID（如果存在）
        if hasattr(record, 'message_id'):
            log_entry["message_id"] = record.message_id

        # 添加操作类型（如果存在）
        if hasattr(record, 'operation'):
            log_entry["operation"] = record.operation

        return json.dumps(log_entry, ensure_ascii=False)


def setup_logging():
    """配置应用程序的日志系统"""

    # 根据环境设置日志级别
    log_level = getattr(settings, 'LOG_LEVEL', 'INFO')
    if hasattr(settings, 'DEBUG') and settings.DEBUG:
        log_level = 'DEBUG'

    level = getattr(logging, log_level.upper(), logging.INFO)

    # 创建根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # 使用结构化格式化器
    formatter = StructuredFormatter()
    console_handler.setFormatter(formatter)

    # 添加处理器到根日志器
    root_logger.addHandler(console_handler)

    # 为特定模块设置日志级别
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # 应用程序日志器
    app_logger = logging.getLogger("app")
    app_logger.setLevel(level)

    return app_logger


class LoggerAdapter(logging.LoggerAdapter):
    """自定义日志适配器，用于添加上下文信息"""

    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})

    def process(self, msg, kwargs):
        # 将额外的字段添加到记录中
        if 'extra' not in kwargs:
            kwargs['extra'] = {}

        # 合并适配器的额外字段
        kwargs['extra'].update(self.extra)

        return msg, kwargs

    def with_context(self, **context):
        """创建带有额外上下文的新适配器"""
        new_extra = self.extra.copy()
        new_extra.update(context)
        return LoggerAdapter(self.logger, new_extra)


def get_logger(name: str = None, **context) -> LoggerAdapter:
    """获取带有上下文的日志器"""
    logger = logging.getLogger(name or __name__)
    return LoggerAdapter(logger, context)


# 便利函数，用于在处理器中快速获取带上下文的日志器
def get_user_logger(user_id: int, operation: str = None) -> LoggerAdapter:
    """获取用户相关的日志器"""
    context = {"user_id": user_id}
    if operation:
        context["operation"] = operation
    return get_logger("app.user", **context)


def get_message_logger(message_id: int, chat_id: int = None, operation: str = None) -> LoggerAdapter:
    """获取消息相关的日志器"""
    context = {"message_id": message_id}
    if chat_id:
        context["chat_id"] = chat_id
    if operation:
        context["operation"] = operation
    return get_logger("app.message", **context)