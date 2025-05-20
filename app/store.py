import logging
from datetime import datetime, timezone
# 导入 Peewee 标准字段和 MySQL 特定类
from peewee import (
    Model, SqliteDatabase, BigIntegerField, TextField,
    DateTimeField, AutoField, DoesNotExist, PeeweeException,
    ForeignKeyField, fn,
    MySQLDatabase, # Standard MySQLDatabase
    CharField, # CharField for fixed-length strings
    CompositeKey, # CompositeKey for composite primary keys
    IntegerField # IntegerField for message count
)
# 导入 PyMySQLDatabase 如果您确定要用 playhouse 的特定版本，否则标准 MySQLDatabase 就够了


# 导入用于索引长度的 SQL 对象
from peewee import SQL # Import SQL object

from starlette.concurrency import run_in_threadpool
from .settings import settings

logger = logging.getLogger(__name__)

# --- 获取当前 UTC 时间（带时区信息）的辅助函数 ---
# 数据库存储 UTC 是最佳实践
# 使用 pytz 来获取带时区信息的 UTC 时间
import pytz # 导入 pytz
utc_tz = pytz.timezone('UTC')
def get_current_utc_time():
    """获取当前 UTC 时间 (带时区信息)."""
    return datetime.now(utc_tz)



# --- 获取当前北京时间（带时区信息）的辅助函数 ---
# 用于 Conversation 和 Messages 默认时间戳 (如果需要北京时间)
# 使用 pytz
beijing_tz = pytz.timezone('Asia/Shanghai') # 'Asia/Shanghai' 是北京/上海时区的标准名称

# --- 定义一个获取当前北京时间（带时区信息）的辅助函数 ---
def get_current_beijing_time():
    """获取当前北京时间 (带时区信息)."""
    return datetime.now(beijing_tz).replace(tzinfo=None)


# --- 数据库初始化 ---
db = None
if settings.DB_KIND == "mysql":
    try:
        # 使用导入的标准的 MySQLDatabase 类来创建连接实例
        # 当安装了 PyMySQL 驱动时，Peewee 的 MySQLDatabase 会自动使用 PyMySQL
        db = MySQLDatabase(
            settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            charset='utf8mb4',
            # 其他连接参数如果需要可以添加
        )
        logger.info(f"使用 MySQL 数据库: {settings.DB_USER}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")

    except Exception as e:
        logger.critical(f"初始化 MySQL 数据库连接失败: {e}", exc_info=True)
        import sys; sys.exit(1)

elif settings.DB_KIND == "sqlite":
     logger.info(f"使用 SQLite 数据库文件: {settings.DB_PATH}")
     db = SqliteDatabase(settings.DB_PATH)
else:
    logger.critical(f"未知的 DB_KIND 指定: {settings.DB_KIND}")
    import sys; sys.exit(1)


# 确保 db 对象已被初始化
if db is None:
     raise RuntimeError(f"数据库初始化失败，不支持的类型或配置错误: {settings.DB_KIND}")


class BaseModel(Model):
    """提供数据库连接上下文的基础模型."""
    class Meta:
        database = db
        # 如果 MySQL 表名需要指定引擎，可以在这里设置
        # engine = 'InnoDB'
        # 如果需要指定默认字符集和排序规则
        # table_settings = ['ENGINE=InnoDB', 'DEFAULT CHARSET=utf8mb4', 'COLLATE=utf8mb4_unicode_ci']


# --- 模型定义 (重构 Conversation, Messages, BlackList 和 新增 BindingID) ---

class Conversation(BaseModel):
    """
    存储对话发起实体 (用户或外部群组) 与其客服话题的关联。
    """
    # entity_id 作为主键的一部分，可以是用户ID或外部群组ID
    entity_id = BigIntegerField(help_text="对话实体ID (用户ID或外部群组ID)") # <--- 移除 primary_key=True

    # 实体类型: 'user' 或 'group'，改为 CharField 并指定长度，且非空 (作为联合主键一部分)
    entity_type = CharField(max_length=10, choices=[('user', 'user'), ('group', 'group')], help_text="实体类型 ('user' 或 'group')") # <--- 修改为 CharField

    topic_id = BigIntegerField(unique=True, null=True, help_text="关联的客服话题ID") # 话题ID可以为Null直到绑定完成

    status = TextField(default="pending", choices=[('pending', 'pending'), ('open', 'open'), ('closed', 'closed')], help_text="对话状态 (pending, open, closed)") # 默认状态 pending

    # 仅对 'user' 类型的实体有效，用户偏好的语言设置
    lang = TextField(null=True, help_text="用户偏好的语言设置 (BCP 47 格式)")

    # 存储实体名称 (用户名字或外部群组名字)，TextField 通常可以用于非索引列
    entity_name = TextField(null=True, help_text="实体名称 (用户名字或外部群组名字)")

    # --- 新增字段: 绑定相关的字段 ---
    custom_id = CharField(max_length=255, unique=True, null=True, help_text="绑定的自定义 ID") # 绑定的自定义 ID，允许为 Null，确保唯一性
    # is_verified 状态用于绑定过程: 'pending' (已创建记录，待绑定/验证), 'verified' (绑定完成)
    is_verified = TextField(default="pending", choices=[('pending', 'pending'), ('verified', 'verified')], help_text="绑定验证状态 (pending, verified)")

    # --- 新增字段: 记录绑定前消息数量限制 ---
    message_count_before_bind = IntegerField(default=0, help_text="绑定验证完成前接收的消息数量")


    # 首次看到时间，记录为 UTC 时间
    first_seen = DateTimeField(default=get_current_beijing_time, help_text="对话创建时间 (北京时间字面值)") # 使用 UTC 时间

    class Meta:
        # 定义联合主键由 entity_id 和 entity_type 组成
        primary_key = CompositeKey('entity_id', 'entity_type') # <--- 使用 CompositeKey 定义联合主键

        # 移除旧的索引定义，CompositeKey 会自动创建主键索引
        # indexes = (
        #     (('entity_id', 'entity_type'), True), # 联合唯一索引
        # )


class Messages(BaseModel):
    """
    存储对话中的消息历史。
    """
    # AutoField 在 SQLite 中映射为 INTEGER PRIMARY KEY AUTOINCREMENT, 在 MySQL 中映射为 BIGINT PRIMARY KEY AUTO_INCREMENT
    id = AutoField(help_text="消息唯一 ID (AUTOINCREMENT)")

    # --- 修正 Messages 模型，放弃数据库层面的 ForeignKey，只存储普通字段 ---
    # 外键关联到 conversations 表的实体 ID
    conv_entity_id = BigIntegerField(help_text="关联的对话实体ID (用户ID或外部群组ID)")
    conv_entity_type = CharField(max_length=10, help_text="关联的对话实体类型 ('user' 或 'group')") # 使用 CharField 并指定长度

    dir = TextField(choices=[('in', 'in'), ('out', 'out')], help_text="消息方向 ('in' 实体 -> bot, 'out' bot -> 实体)")

    sender_id = BigIntegerField(null=True, help_text="消息发送者 ID")
    sender_name = TextField(null=True, help_text="消息发送者名字")

    body = TextField(null=True, help_text="消息文本或 caption")

    tg_mid = BigIntegerField(help_text="Telegram 消息 ID (在源聊天中)")

    created_at = DateTimeField(default=get_current_beijing_time, help_text="对话创建时间 (北京时间字面值)")

    class Meta:
        # 在 Messages 表中，我们现在不使用数据库层面的外键约束，只使用普通索引
        # 为 (conv_entity_id, conv_entity_type) 组合添加索引，并为 conv_entity_type 指定索引长度 5
        # 使用 ((field1, (field2, length)), is_unique) 的标准语法
        # 修正: 确保引用字段时没有 Messages. 前缀
        indexes = (
            # 使用简单的字段列表格式
            (('conv_entity_id', 'conv_entity_type'), False),  # 组合索引
            (('created_at',), False),  # 创建时间索引
        )


# --- 新增: 预生成或管理的绑定 ID 表 ---
class BindingID(BaseModel):
    """存储预生成或可管理的自定义 ID."""
    custom_id = CharField(primary_key=True, max_length=255, help_text="自定义的唯一 ID") # 自定义的唯一 ID
    is_used = TextField(default="unused", choices=[('unused', 'unused'), ('pending', 'pending'), ('used', 'used')], help_text="ID 使用状态 (unused, pending, used)")
    # 可以添加其他字段，如 creation_date, expiration_date 等


class BlackList(BaseModel):
    """存储被拉黑的用户 ID."""
    user_id = BigIntegerField(primary_key=True)
    until = DateTimeField(null=True, help_text="拉黑到期时间 (UTC). Null 表示永久拉黑.")


# --- 数据库连接和表管理 ---
# ... (connect_db, close_db, create_all_tables 函数代码保持不变) ...

# 修正 connect_db, close_db, create_all_tables 的缩进，并增加 db 对象的检查
def connect_db():
    """连接到数据库如果它当前是关闭的."""
    # 在尝试连接前检查 db 对象是否已成功初始化且关闭
    if db and db.is_closed():
        try:
            db.connect()
            logger.info(f"数据库连接成功")
        except Exception as e:
            logger.critical(f"数据库连接失败: {e}", exc_info=True)
            # 致命错误，无法继续
            import sys; sys.exit(1)


def close_db():
    """关闭数据库连接如果它当前是打开的."""
    if db and not db.is_closed():
        try:
            db.close()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接时出错: {e}", exc_info=True)


def create_all_tables():
    """创建所有定义的数据库表，如果它们不存在的话."""
    # 确保 db 对象已初始化
    if db:
        try:
            # 检查当前使用的数据库是否支持某些特性（例如 MySQL 的 BIGINT 自动递增）
            # 对于 SQLite，AutoField 映射到 INTEGER PRIMARY KEY AUTOINCREMENT 是标准的。
            # 对于 MySQL，PyMySQLDatabase 的 AutoField 应该能正确映射到 BIGINT PRIMARY KEY AUTO_INCREMENT。
            # 如果遇到问题，可能需要根据实际使用的 MySQL 驱动和 Peewee 扩展进行调整。
            # 例如，可能需要使用 playhouse.mysql_ext 的 AutoField，或者在 Meta 中指定 auto_increment=True (但通常 AutoField 会自动处理)。

            # 创建 Conversation, Messages, BlackList 和 BindingID 四张表
            db.create_tables([Conversation, Messages, BlackList, BindingID], safe=True)
            logger.info("数据库表检查/创建完成")
        except Exception as e:
            logger.critical(f"创建数据库表失败: {e}", exc_info=True)
            # 致命错误，无法继续
            import sys; sys.exit(1)
    else:
         logger.error("数据库对象未初始化，无法创建表。")
         import sys; sys.exit(1)

# END OF FILE store.py