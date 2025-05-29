from pydantic import BaseModel, field_validator, Field
from typing import Optional, Union, Dict, Any, List
import html
import re
import logging

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """输入验证错误"""

    def __init__(self, message: str, field: str = None):
        self.message = message
        self.field = field
        super().__init__(message)


class TelegramMessage(BaseModel):
    """Telegram消息验证模型"""
    message_id: int
    from_user: Dict[str, Any] = Field(alias="from")
    chat: Dict[str, Any]
    text: Optional[str] = None
    caption: Optional[str] = None

    model_config = {
        "populate_by_name": True
    }

    @field_validator('text', 'caption', mode='before')
    @classmethod
    def sanitize_text_content(cls, v):
        if v is None:
            return v

        # 转换为字符串（防止非字符串类型）
        v = str(v)

        # HTML转义防止XSS
        v = html.escape(v)

        # 限制长度（Telegram限制）
        if len(v) > 4096:
            logger.warning(f"文本内容被截断，原长度: {len(v)}")
            v = v[:4093] + "..."

        return v

    @field_validator('message_id')
    @classmethod
    def validate_message_id(cls, v):
        if not isinstance(v, int) or v <= 0:
            raise ValueError('消息ID必须是正整数')
        return v

    def get_user_id(self) -> Optional[int]:
        """安全获取用户ID"""
        try:
            return self.from_user.get('id')
        except (KeyError, AttributeError):
            return None

    def get_chat_id(self) -> Optional[int]:
        """安全获取聊天ID"""
        try:
            return self.chat.get('id')
        except (KeyError, AttributeError):
            return None

    def get_user_name(self) -> str:
        """安全获取用户名"""
        try:
            first_name = self.from_user.get('first_name', '')
            last_name = self.from_user.get('last_name', '')
            username = self.from_user.get('username', '')

            if first_name or last_name:
                return f"{first_name} {last_name}".strip()
            elif username:
                return f"@{username}"
            else:
                return f"用户 {self.get_user_id()}"
        except (KeyError, AttributeError):
            return f"用户 {self.get_user_id()}"


class BindCommand(BaseModel):
    """绑定命令验证模型"""
    custom_id: str
    password: Optional[str] = None

    @field_validator('custom_id')
    @classmethod
    def validate_custom_id(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('自定义ID不能为空')

        # 去除首尾空格
        v = v.strip()

        # 长度检查
        if len(v) < 3:
            raise ValueError('自定义ID长度不能少于3个字符')
        if len(v) > 50:
            raise ValueError('自定义ID长度不能超过50个字符')

        # 格式检查：只允许字母、数字、下划线和横线
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('自定义ID只能包含字母、数字、下划线和横线')

        return v

    @field_validator('password')
    @classmethod
    def validate_password(cls, v):
        if v is None:
            return v

        if not isinstance(v, str):
            raise ValueError('密码必须是字符串')

        # 长度检查
        if len(v) > 128:
            raise ValueError('密码长度不能超过128个字符')

        # 简单的密码强度检查（可根据需求调整）
        if len(v) > 0 and len(v) < 4:
            raise ValueError('密码长度不能少于4个字符')

        return v

    @classmethod
    def parse_from_text(cls, text: str) -> 'BindCommand':
        """从文本解析绑定命令"""
        if not text or not isinstance(text, str):
            raise ValidationError("绑定命令不能为空")

        # 移除 /bind 前缀并分割参数
        text = text.strip()
        if not text.lower().startswith('/bind'):
            raise ValidationError("不是有效的绑定命令")

        # 分割命令和参数
        parts = text.split(maxsplit=2)

        if len(parts) < 2:
            raise ValidationError("绑定命令格式错误，缺少自定义ID")

        custom_id = parts[1]
        password = parts[2] if len(parts) > 2 else None

        try:
            return cls(custom_id=custom_id, password=password)
        except ValueError as e:
            raise ValidationError(str(e))


class UserInput(BaseModel):
    """用户输入通用验证"""
    user_id: int
    text: Optional[str] = None

    @field_validator('user_id')
    @classmethod
    def validate_user_id(cls, v):
        if not isinstance(v, int) or v <= 0:
            raise ValueError('用户ID必须是正整数')
        return v

    @field_validator('text')
    @classmethod
    def sanitize_text(cls, v):
        if v is None:
            return v

        # 转换为字符串并去除首尾空格
        v = str(v).strip()

        # 如果为空字符串，返回None
        if not v:
            return None

        # HTML转义
        v = html.escape(v)

        # 长度限制
        if len(v) > 4096:
            logger.warning(f"用户输入被截断，原长度: {len(v)}")
            v = v[:4093] + "..."

        return v


class WebhookUpdate(BaseModel):
    """Webhook更新验证模型"""
    update_id: int
    message: Optional[Dict[str, Any]] = None
    edited_message: Optional[Dict[str, Any]] = None
    callback_query: Optional[Dict[str, Any]] = None

    @field_validator('update_id')
    @classmethod
    def validate_update_id(cls, v):
        if not isinstance(v, int):
            raise ValueError('更新ID必须是整数')
        return v

    def get_message(self) -> Optional[Dict[str, Any]]:
        """获取有效的消息对象"""
        return (self.message or
                self.edited_message or
                (self.callback_query.get('message') if self.callback_query else None))

    def has_valid_message(self) -> bool:
        """检查是否包含有效消息"""
        return self.get_message() is not None


def validate_telegram_message(raw_message: Dict[str, Any]) -> TelegramMessage:
    """验证Telegram消息"""
    try:
        return TelegramMessage(**raw_message)
    except Exception as e:
        logger.error(f"消息验证失败: {e}")
        raise ValidationError(f"消息格式无效: {str(e)}")


def validate_webhook_update(raw_update: Dict[str, Any]) -> WebhookUpdate:
    """验证Webhook更新"""
    try:
        return WebhookUpdate(**raw_update)
    except Exception as e:
        logger.error(f"Webhook更新验证失败: {e}")
        raise ValidationError(f"更新格式无效: {str(e)}")


def validate_bind_command(text: str) -> BindCommand:
    """验证绑定命令"""
    try:
        return BindCommand.parse_from_text(text)
    except ValidationError:
        raise
    except Exception as e:
        logger.error(f"绑定命令验证失败: {e}")
        raise ValidationError(f"绑定命令格式错误: {str(e)}")


def safe_get_nested(data: Dict[str, Any], keys: List[str], default=None):
    """安全获取嵌套字典的值"""
    try:
        result = data
        for key in keys:
            result = result[key]
        return result
    except (KeyError, TypeError, AttributeError):
        return default


def sanitize_filename(filename: str) -> str:
    """清理文件名，移除潜在的危险字符"""
    if not filename:
        return "unnamed_file"

    # 移除路径分隔符和其他危险字符
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)

    # 限制长度
    if len(filename) > 255:
        name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
        filename = name[:250] + ('.' + ext if ext else '')

    return filename


def validate_chat_id(chat_id: Union[int, str]) -> int:
    """验证聊天ID"""
    try:
        chat_id_int = int(chat_id)
        if chat_id_int == 0:
            raise ValueError("聊天ID不能为0")
        return chat_id_int
    except (ValueError, TypeError):
        raise ValidationError(f"无效的聊天ID: {chat_id}")


def validate_message_id(message_id: Union[int, str]) -> int:
    """验证消息ID"""
    try:
        msg_id_int = int(message_id)
        if msg_id_int <= 0:
            raise ValueError("消息ID必须是正整数")
        return msg_id_int
    except (ValueError, TypeError):
        raise ValidationError(f"无效的消息ID: {message_id}")


# 输入验证装饰器
def validate_input(validator_func):
    """输入验证装饰器"""

    def decorator(func):
        async def wrapper(*args, **kwargs):
            try:
                # 这里可以根据需要添加具体的验证逻辑
                return await func(*args, **kwargs)
            except ValidationError as e:
                logger.error(f"输入验证失败: {e.message}", extra={'validation_error': str(e)})
                raise
            except Exception as e:
                logger.error(f"函数执行失败: {e}", exc_info=True)
                raise

        return wrapper

    return decorator