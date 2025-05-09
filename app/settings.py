from functools import lru_cache
from pathlib import Path
from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings
import secrets

class Settings(BaseSettings):
    """应用设置，从环境变量或 .env 文件加载."""
    BOT_TOKEN: str = Field(..., description="Telegram Bot API Token")
    GROUP_ID: str = Field(..., description="用于客户支持话题的 Telegram 群组 ID")
    XAI_API_KEY: str = Field(..., description="用于翻译的 XAI (或其他 LLM) API Key")

    # --- 数据库设置 (SQLite) ---
    # 使用指定的 SQLite 数据库文件路径
    DB_PATH: Path = Field(default=Path("data.db"), description="SQLite 数据库文件的路径")


    # 安全设置: Webhook 路径的随机字符串，防止 Token 暴露
    WEBHOOK_PATH: str = Field(default_factory=lambda: secrets.token_urlsafe(32),
                              description="Telegram Webhook 端点的随机路径")

    # --- 应用的公共可访问基 URL ---
    # 这是 Telegram 将更新发送到的 HTTPS 地址前缀。
    # 务必以 https:// 开头，并且不包含路径和末尾的斜杠 (/)。
    # 例如: https://your.domain.com 或 https://your.server.ip:443
    PUBLIC_BASE_URL: HttpUrl = Field(...,
                                     description="Application's public HTTPS base URL (e.g., https://your.domain.com)")

    # 翻译设置
    DEFAULT_USER_LANG: str = Field(default="en", description="用户未设置语言时的默认语言")
    ADMIN_LANG_FOR_USER_MSG: str = Field(default="zh-CN", description="将用户消息翻译给管理员的目标语言")
    # 管理员可能使用的语言。如果用户设定的语言不是这些，则尝试翻译管理员消息。
    ADMIN_LANGS: list[str] = Field(default=["zh", "en"], description="管理员回复假定的语言，如果用户语言是其中之一则跳过翻译")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # 允许环境变量覆盖字段
        # env_prefix = 'MYAPP_'


@lru_cache
def get_settings() -> Settings:
    """获取应用设置实例 (使用 lru_cache 缓存)."""
    return Settings()

# 在模块加载时即加载设置
settings = get_settings()