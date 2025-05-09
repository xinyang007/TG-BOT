import logging
from datetime import datetime, timezone
from peewee import (
    Model, SqliteDatabase, BigIntegerField, TextField,
    DateTimeField, AutoField, DoesNotExist, PeeweeException,
    ForeignKeyField, fn
)
from starlette.concurrency import run_in_threadpool # 用于异步执行同步 DB 操作
from .settings import settings # 使用加载的设置

logger = logging.getLogger(__name__)

# --- 数据库初始化 ---
# 使用指定的 SQLite 数据库文件路径
db = SqliteDatabase(settings.DB_PATH)
logger.info(f"使用 SQLite 数据库文件: {settings.DB_PATH}")


class BaseModel(Model):
    """提供数据库连接上下文的基础模型."""
    class Meta:
        database = db


# --- 模型定义 (基于提供并补充的模式，适配 SQLite) ---
class Conversation(BaseModel):
    """
    存储用户 ID、对应的群组话题 ID 和对话状态。
    基于提供的 conversations 表结构，并补充 bot 运行所需的 topic_id 和 status 字段。
    """
    # user_id 作为主键
    user_id = BigIntegerField(primary_key=True, help_text="Telegram User ID")

    # 添加 topic_id 回来，用于关联用户和群组话题。确保唯一。
    topic_id = BigIntegerField(unique=True, help_text="Telegram Group Topic Thread ID")

    # 添加 status 回来，用于管理对话状态 (open, pending, closed)。
    status = TextField(default="open", help_text="Conversation status (open, pending, closed)")

    # 用户偏好的语言设置 (BCP 47 格式)
    lang = TextField(null=True, help_text="Target language for user replies (BCP 47 format)")

    # --- 新增字段: 存储用户创建对话时的名字 ---
    user_first_name = TextField(null=True, help_text="用户创建对话时的名字")

    # first_seen 字段，使用带时区信息的 DateTimeField
    # 在 SQLite 中存储为 ISO8601 字符串，Peewee 会处理转换。
    first_seen = DateTimeField(default=lambda: datetime.now(timezone.utc), help_text="对话创建时间 (UTC)")


class Messages(BaseModel):
    """
    存储对话中的消息历史。
    基于提供的 messages 表结构。
    """
    # AutoField 在 SQLite 中映射为 INTEGER PRIMARY KEY AUTOINCREMENT
    id = AutoField(help_text="消息唯一 ID (AUTOINCREMENT)")

    # 外键关联到 Conversation.user_id
    # on_delete='CASCADE' 意味着如果一个对话被删除，其所有消息也会被删除。
    # field='user_id' 明确指向 Conversation 表中的 user_id 字段
    conv_id = ForeignKeyField(Conversation, field='user_id', backref='messages', on_delete='CASCADE', help_text="外键关联至 conversations(user_id)")

    # 消息方向：'in' (用户发给 bot) 或 'out' (bot 发给用户)
    dir = TextField(choices=[('in', 'in'), ('out', 'out')], help_text="消息方向 ('in' 用户 -> bot, 'out' bot -> 用户)")

    # 消息文本或 caption，允许为 null (例如只有图片没有 caption)
    body = TextField(null=True, help_text="消息文本或 caption")

    # Telegram 消息 ID，可以是用户私聊中的 ID 或群组话题中的 ID
    tg_mid = BigIntegerField(help_text="Telegram 消息 ID (在源聊天中)")

    # 消息创建时间，使用带时区信息的 DateTimeField
    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc), help_text="消息创建时间 (UTC)")


# --- BlackList 模型 (为拉黑功能保留) ---
class BlackList(BaseModel):
    """存储被拉黑的用户 ID."""
    user_id = BigIntegerField(primary_key=True, help_text="Telegram User ID")
    # 拉黑到期时间，null 表示永久拉黑，使用带时区信息的 DateTimeField
    until = DateTimeField(null=True, help_text="拉黑到期时间 (UTC). Null 表示永久拉黑.")


# --- 数据库连接和表管理 ---

def connect_db():
    """连接到数据库，如果它当前是关闭的."""
    if db.is_closed():
        try:
            db.connect()
            logger.info(f"数据库连接到 {settings.DB_PATH}")
        except Exception as e:
            logger.critical(f"连接数据库 {settings.DB_PATH} 失败: {e}", exc_info=True)
            # 根据重要性，可以在此处退出应用
            # import sys; sys.exit(1)


def close_db():
    """关闭数据库连接，如果它当前是打开的."""
    if not db.is_closed():
        try:
            db.close()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接时出错: {e}", exc_info=True)


def create_all_tables():
    """创建所有定义的数据库表，如果它们不存在的话."""
    try:
        # 创建 Conversation, Messages 和 BlackList 三张表
        db.create_tables([Conversation, Messages, BlackList], safe=True)
        logger.info("数据库表检查/创建完成")
    except Exception as e:
        logger.critical(f"创建数据库表失败: {e}", exc_info=True)
        # 同样，根据重要性，考虑此处是否是致命错误