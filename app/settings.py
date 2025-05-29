from functools import lru_cache
from pathlib import Path
from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings
import secrets


class Settings(BaseSettings):
    """应用设置，从环境变量或 .env 文件加载."""
    BOT_TOKEN: str = Field(..., description="Telegram Bot API Token")
    # 客服支持话题所在的超级群组 ID
    SUPPORT_GROUP_ID: str = Field(..., alias="GROUP_ID", description="Telegram Support Supergroup ID for customer support topics") # 更名为 SUPPORT_GROUP_ID

    # --- 移除翻译相关的设置 ---
    # XAI_API_KEY: str = Field(..., description="用于翻译的 XAI (或其他 LLM) API Key")
    # DEFAULT_USER_LANG: str = Field(default="en", description="用户未设置语言时的默认语言")
    # ADMIN_LANG_FOR_USER_MSG: str = Field(default="zh-CN", description="将用户消息翻译给管理员的目标语言")
    # ADMIN_LANGS: list[str] = Field(default=["zh", "en"], description="管理员回复假定的语言，如果用户语言是其中之一则跳过翻译")


    # --- 数据库设置 ---
    # 根据 DB_KIND 选择相应的驱动和配置
    DB_KIND: str = Field(default="sqlite", description="Database type (e.g., sqlite, mysql)")
    # SQLite 配置 (如果 DB_KIND 是 sqlite)
    DB_PATH: Path = Field(default=Path("data.db"), description="SQLite 数据库文件的路径 (仅用于 sqlite)")
    # MySQL 配置 (如果 DB_KIND 是 mysql)
    DB_HOST: str = Field(default="localhost", description="Database host (仅用于 mysql)")
    DB_PORT: int = Field(default=3306, description="Database port (仅用于 mysql)")
    DB_NAME: str = Field(..., description="Database name (仅用于 mysql)")
    DB_USER: str = Field(default=None, description="Database user (仅用于 mysql)")
    DB_PASSWORD: str = Field(default=None, description="Database password (仅用于 mysql)")
    DB_MAX_CONNECTIONS: int = Field(default=20, description="数据库连接池最大连接数")
    DB_STALE_TIMEOUT: int = Field(default=3600, description="数据库连接池中连接被视为空闲过久的超时时间 (秒)")


    # 安全设置: Webhook 路径的随机字符串
    WEBHOOK_PATH: str = Field(default_factory=lambda: secrets.token_urlsafe(32),
                              description="Telegram Webhook 端点的随机路径")

    # --- 应用的公共可访问基 URL ---
    PUBLIC_BASE_URL: HttpUrl = Field(...,
                                     description="Application's public HTTPS base URL (e.g., https://your.domain.com)")

    # --- 新增：管理员用户ID列表 ---
    ADMIN_USER_IDS: list[int] = Field(default=[],
                                      description="允许执行特权命令的管理员 Telegram User ID 列表 (逗号分隔)")

    # --- 新增: 外部群组配置 ---
    # Bot 需要监听消息并转发到支持话题的外部群组 ID 列表
    # 可以是逗号分隔的字符串，或者列表
    EXTERNAL_GROUP_IDS: list[str] = Field(default=[],
                                          description="List of external group IDs (as strings) to monitor for forwarding messages. Comma-separated in .env")

    # --- 添加自定义 validator 来处理 EXTERNAL_GROUP_IDS 的解析 ---
    # @field_validator 用于 Pydantic v2+
    # 导入 field_validator
    from pydantic import field_validator
    @field_validator('EXTERNAL_GROUP_IDS', mode='before') # 在字段类型验证前运行
    @classmethod
    def _parse_external_group_ids(cls, v):
        """将逗号分隔的字符串或单个数字解析为字符串列表."""
        # logger.debug(f"Validator received input for EXTERNAL_GROUP_IDS: {v} (type: {type(v)})") # Debugging line

        if isinstance(v, list):
            # 如果输入已经是列表，确保其中的元素是字符串
            return [str(item).strip() for item in v if str(item).strip()]
        if isinstance(v, (int, str)):
            # 如果是整数或字符串，转换为字符串并按逗号分割
            # 将每个部分转换为字符串并去除空白，过滤掉空字符串
            return [str(id).strip() for id in str(v).split(',') if str(id).strip()]
        # 如果是其他类型，返回空列表或抛出错误，取决于需求
        # logger.warning(f"EXTERNAL_GROUP_IDS validator received unexpected type: {type(v)}") # Debugging line
        return [] # 默认返回空列表，或者可以 raise ValueError("Invalid input type for EXTERNAL_GROUP_IDS")

    @field_validator('ADMIN_USER_IDS', mode='before')
    @classmethod
    def _parse_admin_user_ids(cls, v):
        """将逗号分隔的字符串或单个数字/字符串解析为整数列表。"""
        if isinstance(v, list):
            return [int(item) for item in v if str(item).strip()]
        if isinstance(v, (int, str)):
            return [int(id_str.strip()) for id_str in str(v).split(',') if id_str.strip()]
        return []


    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # 允许别名 GROUP_ID
        populate_by_name = True
        # 允许通过 env 变量覆盖列表，例如 EXTERNAL_GROUP_IDS="123,456,789"
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (
                env_settings,
                init_settings,
                file_secret_settings,
            )

        @classmethod
        def validate_assignment(cls, values):
             # 确保 EXTERNAL_GROUP_IDS 是列表
             if isinstance(values.get('EXTERNAL_GROUP_IDS'), str):
                 values['EXTERNAL_GROUP_IDS'] = [id.strip() for id in values['EXTERNAL_GROUP_IDS'].split(',') if id.strip()]
             return values


@lru_cache
def get_settings() -> Settings:
    """获取应用设置实例 (使用 lru_cache 缓存)."""
    return Settings()


# 在模块加载时即加载设置
settings = get_settings()

# END OF FILE settings.py