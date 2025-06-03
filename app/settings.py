from functools import lru_cache
from pathlib import Path
from pydantic import Field, HttpUrl, validator, BaseModel
from pydantic_settings import BaseSettings
import secrets
from enum import Enum
from typing import List, Union, Dict, Any
import json


class BotConfig(BaseModel):
    """单个机器人配置"""
    token: str = Field(..., description="机器人Token")
    name: str = Field(..., description="机器人名称")
    priority: int = Field(default=1, description="优先级，数字越小优先级越高")
    enabled: bool = Field(default=True, description="是否启用")
    max_requests_per_minute: int = Field(default=20, description="每分钟最大请求数")

    @validator('token')
    @classmethod
    def validate_token(cls, v):
        """验证Token格式"""
        if not v:
            raise ValueError('机器人Token不能为空')

        if ':' not in v:
            raise ValueError('无效的机器人Token格式')

        parts = v.split(':', 1)
        if len(parts) != 2 or not parts[0].isdigit() or len(parts[1]) < 35:
            raise ValueError('无效的机器人Token格式')

        return v

    @validator('priority')
    @classmethod
    def validate_priority(cls, v):
        """验证优先级"""
        if v < 1:
            raise ValueError('优先级必须大于等于1')
        return v

    @validator('max_requests_per_minute')
    @classmethod
    def validate_max_requests(cls, v):
        """验证请求限制"""
        if v < 1 or v > 100:
            raise ValueError('每分钟最大请求数必须在1-100之间')
        return v


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

    # --- 多机器人配置 ---
    MULTI_BOT_ENABLED: bool = Field(
        default=False,
        description="是否启用多机器人模式"
    )

    BOT_CONFIGS: Union[List[BotConfig], str] = Field(
        default=[],
        description="机器人配置列表"
    )

    # --- 向后兼容的单机器人配置 ---
    BOT_TOKEN: str = Field(
        default="",
        description="主要机器人Token（向后兼容）",
        min_length=0  # 允许为空，因为可能使用多机器人配置
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

    # --- 消息队列配置 ---
    ENABLE_MESSAGE_QUEUE: bool = Field(
        default=False,
        description="启用消息队列（需要Redis）"
    )
    REDIS_URL: str = Field(
        default="redis://localhost:6379",
        description="Redis连接URL"
    )

    # --- 高级速率限制配置 ---
    ADVANCED_RATE_LIMIT_ENABLED: bool = Field(
        default=True,
        description="启用高级速率限制"
    )

    # --- 用户分组配置 ---
    PREMIUM_USER_IDS: List[int] = Field(
        default=[],
        description="高级用户 Telegram User ID 列表"
    )

    # --- 速率限制通知配置 ---
    ENABLE_RATE_LIMIT_NOTIFICATIONS: bool = Field(
        default=True,
        description="启用速率限制通知"
    )
    RATE_LIMIT_NOTIFICATION_LANGUAGE: str = Field(
        default="zh",
        description="通知语言 (zh=中文, en=英文)"
    )

    # 群聊限制时是否同时私信用户详细信息
    ALSO_NOTIFY_USER_PRIVATELY: bool = Field(
        default=False,
        description="群聊触发限制时是否同时私信用户详细信息（除了在群里通知）"
    )

    RATE_LIMIT_NOTIFICATION_COOLDOWN: int = Field(
        default=60,
        description="同一用户在同一聊天中的通知冷却时间（秒），防止通知刷屏",
        ge=10,
        le=300
    )
    # --- 消息协调配置 ---
    ENABLE_MESSAGE_COORDINATION: bool = Field(
        default=True,
        description="启用消息协调器（多机器人模式下的消息分发）"
    )

    MESSAGE_QUEUE_MAX_SIZE: int = Field(
        default=10000,
        description="消息队列最大大小",
        ge=100,
        le=100000
    )

    MESSAGE_PROCESSING_TIMEOUT: int = Field(
        default=300,
        description="消息处理超时时间（秒）",
        ge=30,
        le=3600
    )

    MESSAGE_MAX_RETRIES: int = Field(
        default=3,
        description="消息处理最大重试次数",
        ge=0,
        le=10
    )

    COORDINATION_LOCK_TIMEOUT: int = Field(
        default=60,
        description="分布式锁超时时间（秒）",
        ge=10,
        le=300
    )

    COORDINATION_CLEANUP_INTERVAL: int = Field(
        default=60,
        description="协调器清理任务间隔（秒）",
        ge=30,
        le=600
    )

    # --- 负载均衡配置 ---
    LOAD_BALANCER_ALGORITHM: str = Field(
        default="health_priority",
        description="负载均衡算法 (health_priority, weighted_round_robin, least_connections, priority_based)"
    )

    BOT_SELECTION_STRATEGY: str = Field(
        default="health_priority",
        description="机器人选择策略 (health_priority, load_based, random)"
    )

    # --- 消息优先级配置 ---
    ADMIN_MESSAGE_PRIORITY_BOOST: bool = Field(
        default=True,
        description="管理员消息是否获得优先级提升"
    )

    SUPPORT_GROUP_PRIORITY_BOOST: bool = Field(
        default=True,
        description="支持群组消息是否获得优先级提升"
    )

    PRIVATE_CHAT_PRIORITY: int = Field(
        default=2,
        description="私聊消息优先级 (1=低, 2=正常, 3=高, 4=紧急)",
        ge=1,
        le=4
    )

    GROUP_CHAT_PRIORITY: int = Field(
        default=1,
        description="群聊消息优先级 (1=低, 2=正常, 3=高, 4=紧急)",
        ge=1,
        le=4
    )

    # --- 故障恢复配置 ---
    BOT_HEALTH_CHECK_INTERVAL: int = Field(
        default=30,
        description="机器人健康检查间隔（秒）",
        ge=10,
        le=300
    )

    BOT_FAILURE_THRESHOLD: int = Field(
        default=3,
        description="机器人标记为故障的连续失败次数",
        ge=1,
        le=10
    )

    BOT_RECOVERY_CHECK_INTERVAL: int = Field(
        default=300,
        description="机器人恢复检查间隔（秒）",
        ge=60,
        le=3600
    )

    AUTO_FAILOVER_ENABLED: bool = Field(
        default=True,
        description="启用自动故障转移"
    )

    # --- 监控和告警配置 ---
    COORDINATION_MONITORING_ENABLED: bool = Field(
        default=True,
        description="启用协调器监控"
    )

    QUEUE_SIZE_ALERT_THRESHOLD: int = Field(
        default=1000,
        description="队列大小告警阈值",
        ge=10,
        le=50000
    )

    PROCESSING_DELAY_ALERT_THRESHOLD: int = Field(
        default=60,
        description="处理延迟告警阈值（秒）",
        ge=10,
        le=600
    )

    # --- 验证器 ---
    @validator('LOAD_BALANCER_ALGORITHM')
    @classmethod
    def validate_load_balancer_algorithm(cls, v):
        """验证负载均衡算法"""
        valid_algorithms = ["health_priority", "weighted_round_robin", "least_connections", "priority_based"]
        if v not in valid_algorithms:
            raise ValueError(f"负载均衡算法必须是以下之一: {valid_algorithms}")
        return v

    @validator('BOT_SELECTION_STRATEGY')
    @classmethod
    def validate_bot_selection_strategy(cls, v):
        """验证机器人选择策略"""
        valid_strategies = ["health_priority", "load_based", "random"]
        if v not in valid_strategies:
            raise ValueError(f"机器人选择策略必须是以下之一: {valid_strategies}")
        return v

    @validator('ENABLE_MESSAGE_COORDINATION')
    @classmethod
    def validate_coordination_dependencies(cls, v, values):
        """验证消息协调依赖"""
        if v and not values.get('MULTI_BOT_ENABLED', False):
            import warnings
            warnings.warn("启用消息协调但未启用多机器人模式，协调功能将不会工作")
        return v

    def get_coordination_config(self) -> Dict[str, Any]:
        """获取协调器配置"""
        return {
            "enabled": self.ENABLE_MESSAGE_COORDINATION,
            "queue_max_size": self.MESSAGE_QUEUE_MAX_SIZE,
            "processing_timeout": self.MESSAGE_PROCESSING_TIMEOUT,
            "max_retries": self.MESSAGE_MAX_RETRIES,
            "lock_timeout": self.COORDINATION_LOCK_TIMEOUT,
            "cleanup_interval": self.COORDINATION_CLEANUP_INTERVAL,
            "load_balancer_algorithm": self.LOAD_BALANCER_ALGORITHM,
            "bot_selection_strategy": self.BOT_SELECTION_STRATEGY
        }

    def get_priority_config(self) -> Dict[str, Any]:
        """获取优先级配置"""
        return {
            "admin_boost": self.ADMIN_MESSAGE_PRIORITY_BOOST,
            "support_group_boost": self.SUPPORT_GROUP_PRIORITY_BOOST,
            "private_chat_priority": self.PRIVATE_CHAT_PRIORITY,
            "group_chat_priority": self.GROUP_CHAT_PRIORITY
        }

    def get_monitoring_config(self) -> Dict[str, Any]:
        """获取监控配置"""
        return {
            "enabled": self.COORDINATION_MONITORING_ENABLED,
            "queue_alert_threshold": self.QUEUE_SIZE_ALERT_THRESHOLD,
            "delay_alert_threshold": self.PROCESSING_DELAY_ALERT_THRESHOLD,
            "health_check_interval": self.BOT_HEALTH_CHECK_INTERVAL,
            "failure_threshold": self.BOT_FAILURE_THRESHOLD,
            "recovery_check_interval": self.BOT_RECOVERY_CHECK_INTERVAL
        }

    def validate_coordination_configuration(self) -> List[str]:
        """验证协调配置并返回警告信息"""
        warnings = []

        # 检查协调器配置
        if self.MULTI_BOT_ENABLED and self.ENABLE_MESSAGE_COORDINATION:
            # 检查Redis配置
            if not self.REDIS_URL or self.REDIS_URL == "redis://localhost:6379":
                warnings.append("使用默认Redis配置，生产环境请配置专用Redis实例")

            # 检查队列大小
            if self.MESSAGE_QUEUE_MAX_SIZE < 1000:
                warnings.append("消息队列大小较小，可能影响高并发处理")

            # 检查超时配置
            if self.MESSAGE_PROCESSING_TIMEOUT < 60:
                warnings.append("消息处理超时时间较短，可能导致正常消息被标记为超时")

            # 检查机器人配置
            enabled_bots = self.get_enabled_bots()
            if len(enabled_bots) < 2:
                warnings.append("启用了消息协调但机器人数量少于2个，建议配置多个机器人")

            # 检查优先级配置
            if self.PRIVATE_CHAT_PRIORITY == self.GROUP_CHAT_PRIORITY:
                warnings.append("私聊和群聊消息优先级相同，建议区分优先级")

        elif self.ENABLE_MESSAGE_COORDINATION and not self.MULTI_BOT_ENABLED:
            warnings.append("启用了消息协调但未启用多机器人模式")

        return warnings

    # --- 验证器 ---
    @validator('BOT_CONFIGS', pre=True)
    @classmethod
    def parse_bot_configs(cls, v, values):
        """解析机器人配置"""
        # 如果是字符串，尝试解析为JSON
        if isinstance(v, str):
            if not v.strip():
                v = []
            else:
                try:
                    v = json.loads(v)
                except json.JSONDecodeError as e:
                    raise ValueError(f"BOT_CONFIGS JSON格式错误: {e}")

        # 如果是列表，验证每个配置
        if isinstance(v, list):
            if len(v) > 0:
                # 验证配置格式
                bot_configs = []
                for i, config in enumerate(v):
                    if isinstance(config, dict):
                        try:
                            bot_config = BotConfig(**config)
                            bot_configs.append(bot_config)
                        except Exception as e:
                            raise ValueError(f"机器人配置 {i + 1} 无效: {e}")
                    else:
                        raise ValueError(f"机器人配置 {i + 1} 必须是字典格式")
                return bot_configs

        # 如果没有配置多机器人，且有BOT_TOKEN，创建默认配置
        bot_token = values.get('BOT_TOKEN', '')
        if bot_token and not v:
            return [BotConfig(
                token=bot_token,
                name="主机器人",
                priority=1,
                enabled=True,
                max_requests_per_minute=20
            )]

        return v or []

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
    def validate_bot_token(cls, v, values):
        """验证Bot Token格式"""
        # 如果启用多机器人模式且有机器人配置，BOT_TOKEN可以为空
        multi_bot_enabled = values.get('MULTI_BOT_ENABLED', False)
        if multi_bot_enabled and not v:
            return v  # 允许为空

        if not v:
            raise ValueError('在单机器人模式下，BOT_TOKEN 不能为空')

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
        "populate_by_name": True,
        "use_enum_values": True,
        "env_prefix": "",
        "extra": "ignore",  # 忽略额外字段
    }

    def get_enabled_bots(self) -> List[BotConfig]:
        """获取启用的机器人列表"""
        if isinstance(self.BOT_CONFIGS, list):
            return [bot for bot in self.BOT_CONFIGS if bot.enabled]
        return []

    def get_primary_bot_token(self) -> str:
        """获取主要机器人Token"""
        if self.MULTI_BOT_ENABLED:
            enabled_bots = self.get_enabled_bots()
            if enabled_bots:
                # 返回优先级最高的机器人Token
                primary_bot = min(enabled_bots, key=lambda b: b.priority)
                return primary_bot.token

        return self.BOT_TOKEN

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

    def get_user_group(self, user_id: int) -> str:
        """获取用户组"""
        if user_id in self.ADMIN_USER_IDS:
            return "admin"
        elif user_id in getattr(self, 'PREMIUM_USER_IDS', []):
            return "premium"
        else:
            return "normal"

    def validate_configuration(self) -> List[str]:
        """验证配置并返回警告信息"""
        warnings = []

        # 检查机器人配置
        if self.MULTI_BOT_ENABLED:
            enabled_bots = self.get_enabled_bots()
            if not enabled_bots:
                warnings.append("启用了多机器人模式但没有可用的机器人配置")
            elif len(enabled_bots) == 1:
                warnings.append("多机器人模式只配置了一个机器人，建议配置多个以提供冗余")

            # 检查Token重复
            tokens = [bot.token for bot in enabled_bots]
            if len(tokens) != len(set(tokens)):
                warnings.append("发现重复的机器人Token")

        else:
            if not self.BOT_TOKEN:
                warnings.append("单机器人模式下未设置BOT_TOKEN")

        # 检查生产环境配置
        if self.is_production():
            if not self.ADMIN_USER_IDS:
                warnings.append("生产环境建议设置至少一个管理员用户ID")
            if self.DEBUG:
                warnings.append("生产环境不应启用调试模式")

        return warnings


@lru_cache
def get_settings() -> Settings:
    """获取应用设置实例 (使用 lru_cache 缓存)"""
    return Settings()


# 在模块加载时即加载设置
settings = get_settings()

# 更新验证函数
def validate_settings_on_import():
    """导入时验证设置（更新版本）"""
    try:
        # 原有的检查
        if settings.MULTI_BOT_ENABLED:
            enabled_bots = settings.get_enabled_bots()
            if not enabled_bots:
                raise ValueError("启用了多机器人模式但没有可用的机器人配置")
        else:
            if not settings.BOT_TOKEN:
                raise ValueError("单机器人模式下 BOT_TOKEN 未设置")

        if not settings.SUPPORT_GROUP_ID:
            raise ValueError("SUPPORT_GROUP_ID 未设置")

        if not settings.PUBLIC_BASE_URL:
            raise ValueError("PUBLIC_BASE_URL 未设置")

        # 验证配置并显示警告
        general_warnings = settings.validate_configuration()
        coordination_warnings = settings.validate_coordination_configuration()

        all_warnings = general_warnings + coordination_warnings

        if all_warnings:
            import sys
            for warning in all_warnings:
                print(f"配置警告: {warning}", file=sys.stderr)

        # 显示配置摘要
        if settings.MULTI_BOT_ENABLED:
            enabled_count = len(settings.get_enabled_bots())
            print(f"✅ 多机器人模式启用，配置了 {enabled_count} 个机器人")

            if settings.ENABLE_MESSAGE_COORDINATION:
                print(f"✅ 消息协调器已启用")
                coord_config = settings.get_coordination_config()
                print(f"   - 负载均衡算法: {coord_config['load_balancer_algorithm']}")
                print(f"   - 机器人选择策略: {coord_config['bot_selection_strategy']}")
                print(f"   - 队列最大大小: {coord_config['queue_max_size']}")
            else:
                print("⚠️ 消息协调器已禁用")
        else:
            print("✅ 单机器人模式")

    except Exception as e:
        import sys
        print(f"配置验证失败: {e}", file=sys.stderr)
        # 在开发环境可以选择不退出，生产环境应该退出
        if settings.is_production():
            sys.exit(1)


# 执行导入时验证
validate_settings_on_import()