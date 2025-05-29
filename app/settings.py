from functools import lru_cache
from pathlib import Path
from pydantic import Field, HttpUrl, validator, field_validator
from pydantic_settings import BaseSettings
import secrets
from enum import Enum
from typing import List


class LogLevel(str, Enum):
    """日志级别枚举"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class DatabaseType(str, Enum):
    """数据库类型枚举"""
    SQLITE = "sqlite"
    MYSQL = "mysql"


class Environment(str, Enum):
    """运行环境枚举"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"


class Settings(BaseSettings):
    """应用设置，从环境变量或 .env 文件加载"""

    # --- 基础配置 ---
    ENVIRONMENT: Environment = Field(
        default=Environment.DEVELOPMENT,
        description="运行环境 (development, testing, production)"
    )
    DEBUG: bool = Field(
        default=False,
        description="调试模式"
    )

    # --- 日志配置 ---
    LOG_LEVEL: LogLevel = Field(
        default=LogLevel.INFO,
        description="日志级别"
    )

    # --- Telegram Bot 配置 ---
    BOT_TOKEN: str = Field(
        ...,
        description="Telegram Bot API Token",
        min_length=45,  # Telegram Bot Token 标准长度检查
        max_length=50
    )

    # 客服支持话题所在的超级群组 ID
    SUPPORT_GROUP_ID: str = Field(
        ...,
        alias="GROUP_ID",
        description="Telegram Support Supergroup ID for customer support topics"
    )

    # --- 数据库设置 ---
    DB_KIND: DatabaseType = Field(
        default=DatabaseType.SQLITE,
        description="Database type (sqlite, mysql)"
    )

    # SQLite 配置
    DB_PATH: Path = Field(
        default=Path("data.db"),
        description="SQLite 数据库文件的路径 (仅用于 sqlite)"
    )

    # MySQL 配置
    DB_HOST: str = Field(
        default="localhost",
        description="Database host (仅用于 mysql)"
    )
    DB_PORT: int = Field(
        default=3306,
        description="Database port (仅用于 mysql)",
        ge=1,
        le=65535
    )
    DB_NAME: str = Field(
        default="telegram_bot",
        description="Database name (仅用于 mysql)"
    )
    DB_USER: str = Field(
        default="",
        description="Database user (仅用于 mysql)"
    )
    DB_PASSWORD: str = Field(
        default="",
        description="Database password (仅用于 mysql)"
    )
    DB_MAX_CONNECTIONS: int = Field(
        default=20,
        description="数据库连接池最大连接数",
        ge=1,
        le=100
    )
    DB_STALE_TIMEOUT: int = Field(
        default=3600,
        description="数据库连接池中连接被视为空闲过久的超时时间 (秒)",
        ge=60
    )

    # --- 安全设置 ---
    WEBHOOK_PATH: str = Field(
        default_factory=lambda: secrets.token_urlsafe(32),
        description="Telegram Webhook 端点的随机路径",
        min_length=32
    )

    # 应用的公共可访问基 URL
    PUBLIC_BASE_URL: HttpUrl = Field(
        ...,
        description="Application's public HTTPS base URL (e.g., https://your.domain.com)"
    )

    # --- 管理员配置 ---
    ADMIN_USER_IDS: List[int] = Field(
        default=[],
        description="允许执行特权命令的管理员 Telegram User ID 列表"
    )

    # --- 外部群组配置 ---
    EXTERNAL_GROUP_IDS: List[str] = Field(
        default=[],
        description="需要监听消息并转发到支持话题的外部群组 ID 列表"
    )

    # --- 功能开关 ---
    ENABLE_INPUT_VALIDATION: bool = Field(
        default=True,
        description="启用输入验证"
    )
    ENABLE_STRUCTURED_LOGGING: bool = Field(
        default=True,
        description="启用结构化日志"
    )
    ENABLE_ERROR_MONITORING: bool = Field(
        default=True,
        description="启用错误监控"
    )

    # --- 性能配置 ---
    REQUEST_TIMEOUT: int = Field(
        default=30,
        description="请求超时时间（秒）",
        ge=5,
        le=300
    )
    MAX_MESSAGE_LENGTH: int = Field(
        default=4096,
        description="最大消息长度（字符）",
        ge=1,
        le=4096
    )

    # --- 速率限制配置 ---
    RATE_LIMIT_ENABLED: bool = Field(
        default=True,
        description="启用速率限制"
    )
    RATE_LIMIT_REQUESTS: int = Field(
        default=20,
        description="速率限制：每分钟最大请求数",
        ge=1,
        le=100
    )
    RATE_LIMIT_WINDOW: int = Field(
        default=60,
        description="速率限制：时间窗口（秒）",
        ge=10,
        le=3600
    )

    # --- 验证器 ---
    @validator('EXTERNAL_GROUP_IDS', pre=True)
    @classmethod
    def parse_external_group_ids(cls, v):
        """解析外部群组ID列表"""
        if isinstance(v, list):
            return [str(item).strip() for item in v if str(item).strip()]
        if isinstance(v, (int, str)):
            return [str(id).strip() for id in str(v).split(',') if str(id).strip()]
        return []

    @validator('ADMIN_USER_IDS', pre=True)
    @classmethod
    def parse_admin_user_ids(cls, v):
        """解析管理员用户ID列表"""
        if isinstance(v, list):
            return [int(item) for item in v if str(item).strip().isdigit()]
        if isinstance(v, (int, str)):
            ids = []
            for id_str in str(v).split(','):
                id_str = id_str.strip()
                if id_str.isdigit():
                    ids.append(int(id_str))
            return ids
        return []

    @validator('BOT_TOKEN')
    @classmethod
    def validate_bot_token(cls, v):
        """验证Bot Token格式"""
        if not v:
            raise ValueError('Bot Token 不能为空')

        # 基础格式检查：应该包含冒号
        if ':' not in v:
            raise ValueError('Bot Token 格式无效')

        # 分割并检查两部分
        parts = v.split(':', 1)
        if len(parts) != 2 or not parts[0].isdigit() or len(parts[1]) < 35:
            raise ValueError('Bot Token 格式无效')

        return v

    @validator('PUBLIC_BASE_URL')
    @classmethod
    def validate_public_base_url(cls, v):
        """验证公共基础URL"""
        url_str = str(v)
        if not url_str.startswith('https://'):
            raise ValueError('PUBLIC_BASE_URL 必须使用 HTTPS')
        return v

    @validator('WEBHOOK_PATH')
    @classmethod
    def validate_webhook_path(cls, v):
        """验证Webhook路径安全性"""
        if len(v) < 32:
            raise ValueError('Webhook路径长度不足32位，存在安全风险')

        # 检查是否包含不安全字符
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Webhook路径包含不安全字符')

        return v

    @validator('DB_KIND')
    @classmethod
    def validate_db_config(cls, v, values):
        """验证数据库配置的完整性"""
        if v == DatabaseType.MYSQL:
            # MySQL 需要的必填字段
            required_fields = ['DB_HOST', 'DB_NAME', 'DB_USER']
            for field in required_fields:
                if field in values and not values[field]:
                    raise ValueError(f'使用 MySQL 时 {field} 不能为空')
        return v

    @validator('ENVIRONMENT')
    @classmethod
    def validate_environment_settings(cls, v, values):
        """根据环境验证相关设置"""
        if v == Environment.PRODUCTION:
            # 生产环境的额外检查
            if values.get('DEBUG', False):
                raise ValueError('生产环境不应启用调试模式')

            if values.get('LOG_LEVEL') == LogLevel.DEBUG:
                import warnings
                warnings.warn('生产环境建议使用 INFO 或更高级别的日志')

        return v

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "populate_by_name": True,  # Pydantic V2 中仍然使用这个名称
        "use_enum_values": True,
        "env_prefix": "",
        "extra": "ignore",  # 忽略额外字段
    }

    def is_production(self) -> bool:
        """检查是否为生产环境"""
        return self.ENVIRONMENT == Environment.PRODUCTION

    def is_development(self) -> bool:
        """检查是否为开发环境"""
        return self.ENVIRONMENT == Environment.DEVELOPMENT

    def get_db_url(self) -> str:
        """获取数据库连接URL"""
        if self.DB_KIND == DatabaseType.SQLITE:
            return f"sqlite:///{self.DB_PATH}"
        elif self.DB_KIND == DatabaseType.MYSQL:
            return f"mysql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        else:
            raise ValueError(f"不支持的数据库类型: {self.DB_KIND}")


@lru_cache
def get_settings() -> Settings:
    """获取应用设置实例 (使用 lru_cache 缓存)"""
    return Settings()


# 在模块加载时即加载设置
settings = get_settings()


# 设置验证：在导入时进行一些基本检查
def validate_settings_on_import():
    """导入时验证设置"""
    try:
        # 检查关键配置
        if not settings.BOT_TOKEN:
            raise ValueError("BOT_TOKEN 未设置")

        if not settings.SUPPORT_GROUP_ID:
            raise ValueError("SUPPORT_GROUP_ID 未设置")

        if not settings.PUBLIC_BASE_URL:
            raise ValueError("PUBLIC_BASE_URL 未设置")

        # 生产环境额外检查
        if settings.is_production():
            if len(settings.ADMIN_USER_IDS) == 0:
                import warnings
                warnings.warn("生产环境建议设置至少一个管理员用户ID")

    except Exception as e:
        import sys
        print(f"配置验证失败: {e}", file=sys.stderr)
        # 在开发环境可以选择不退出，生产环境应该退出
        if settings.is_production():
            sys.exit(1)


# 执行导入时验证
validate_settings_on_import()