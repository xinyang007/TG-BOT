# app/handlers/commands.py

from ..settings import settings
from ..tg_utils import tg # 确保tg导入
from ..services.conversation_service import ConversationService
from ..logging_config import get_logger, get_user_logger
from ..validation import ValidationError
from typing import Optional # 导入 Optional

logger = get_logger("app.handlers.commands")

# 需要管理员权限的命令 - 移除 /setlang，新增 /create_id，保留 /set_password
PRIVILEGED_COMMANDS = {"/ban", "/close", "/unban", "/create_id", "/set_password"}


class CommandError(Exception):
    """命令执行错误"""

    def __init__(self, message: str, user_message: str = None):
        self.message = message
        self.user_message = user_message or message
        super().__init__(message)


async def handle_commands(tid: int, admin_sender_id: int | str, text: str, conv_service: ConversationService, specific_bot_token: Optional[str] = None):
    """
    处理管理员在客服支持群组话题中发送的命令

    Args:
        tid: 发送命令的消息线程 ID (话题 ID)
        admin_sender_id: 发送命令的管理员的 Telegram 用户 ID
        text: 完整的命令文本 (包括开头的 '/')
        conv_service: 用于业务逻辑的 ConversationService 实例
        specific_bot_token: 可选的，用于发送回复的机器人token。通常Admin命令回复可以不指定，让系统自动选择。
    """
    # 输入验证
    try:
        tid = int(tid)
        admin_sender_id = int(admin_sender_id)
        text = str(text).strip()

        if not text.startswith('/'):
            raise ValueError("无效的命令格式")

    except (ValueError, TypeError) as e:
        logger.error(
            "命令参数验证失败",
            extra={
                "topic_id": tid,
                "admin_id": admin_sender_id,
                "text": text,
                "validation_error": str(e)
            }
        )
        # 这里的错误通常不需要通知到 tg，因为它发生在参数解析阶段
        return

    # 使用管理员相关的日志器
    admin_logger = get_user_logger(admin_sender_id, "admin_command")

    # 解析命令
    cmd, *args = text.split()
    cmd = cmd.lower()
    parts = text.strip().split(maxsplit=2)

    arg1 = parts[1] if len(parts) > 1 else None
    arg2 = parts[2] if len(parts) > 2 else None

    admin_logger.info(
        "执行管理员命令",
        extra={
            "topic_id": tid,
            "command": cmd,
            "args_count": len(args),
            "has_arg1": arg1 is not None,
            "has_arg2": arg2 is not None
        }
    )

    try:
        # 权限检查
        if cmd in PRIVILEGED_COMMANDS:
            if admin_sender_id not in settings.ADMIN_USER_IDS:
                admin_logger.warning(
                    "非管理员尝试执行特权命令",
                    extra={"command": cmd}
                )
                await send_error_message(tid, f"抱歉，您没有权限执行 {cmd} 命令。", specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                return

        admin_logger.info("权限检查通过")

        # 获取话题关联的对话信息（对于需要的命令）
        conv = None
        entity_id_in_topic = None
        entity_type_in_topic = None

        commands_needing_conv = {"/close", "/ban"}
        if cmd == "/unban" and not args:
            commands_needing_conv.add("/unban")

        if cmd in commands_needing_conv:
            try:
                conv = await conv_service.get_conversation_by_topic(tid)
                if not conv:
                    admin_logger.warning(
                        "命令执行失败：话题未关联对话",
                        extra={"command": cmd}
                    )
                    await send_error_message(tid, "错误：此话题未关联对话实体，无法执行此命令。", specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                    return

                entity_id_in_topic = conv.entity_id
                entity_type_in_topic = conv.entity_type

                admin_logger.info(
                    "成功获取话题对应的对话实体",
                    extra={
                        "entity_type": entity_type_in_topic,
                        "entity_id": entity_id_in_topic
                    }
                )

            except Exception as e:
                admin_logger.error(
                    "获取话题对应对话失败",
                    extra={"command": cmd},
                    exc_info=True
                )
                await send_error_message(tid, "处理命令失败：无法获取对话实体信息。", specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                return

        # 执行具体命令
        await execute_command(
            cmd, arg1, arg2, tid, admin_sender_id, admin_logger,
            conv_service, conv, entity_id_in_topic, entity_type_in_topic, specific_bot_token # 传递 specific_bot_token
        )

    except CommandError as e:
        admin_logger.warning(
            "命令执行失败",
            extra={
                "command": cmd,
                "error_message": e.message
            }
        )
        await send_error_message(tid, e.user_message, specific_bot_token=specific_bot_token) # 传递 specific_bot_token

    except Exception as e:
        admin_logger.error(
            "命令执行异常",
            extra={"command": cmd},
            exc_info=True
        )
        await send_error_message(tid, "命令执行失败，请稍后重试。", specific_bot_token=specific_bot_token) # 传递 specific_bot_token


async def execute_command(cmd: str, arg1: str, arg2: str, tid: int, admin_sender_id: int,
                          admin_logger, conv_service: ConversationService, conv,
                          entity_id_in_topic: int, entity_type_in_topic: str, specific_bot_token: Optional[str] = None):
    """执行具体的命令逻辑"""

    if cmd == "/create_id":
        await handle_create_id_command(
            arg1, arg2, tid, admin_sender_id, admin_logger, conv_service, specific_bot_token # 传递 specific_bot_token
        )

    elif cmd == "/set_password":
        await handle_set_password_command(
            arg1, arg2, tid, admin_sender_id, admin_logger, conv_service, specific_bot_token # 传递 specific_bot_token
        )

    elif cmd == "/close":
        await handle_close_command(
            tid, admin_logger, conv_service, entity_id_in_topic, entity_type_in_topic, specific_bot_token # 传递 specific_bot_token
        )

    elif cmd == "/ban":
        await handle_ban_command(
            tid, admin_logger, conv_service, entity_id_in_topic, entity_type_in_topic, specific_bot_token # 传递 specific_bot_token
        )

    elif cmd == "/unban":
        await handle_unban_command(
            arg1, tid, admin_logger, conv_service, entity_id_in_topic, entity_type_in_topic, specific_bot_token # 传递 specific_bot_token
        )

    else:
        admin_logger.warning(f"未知命令: {cmd}")
        await send_error_message(
            tid,
            "未知命令，未发送给客户。\n可用命令:\n- /close: 关闭对话\n- /ban: 拉黑用户\n- /unban [<用户ID>]: 解除拉黑\n- /create_id <ID> [<密码>]: 创建新的绑定ID\n- /set_password <ID> [<新密码>]: 修改ID密码（会替换原密码）",
            specific_bot_token=specific_bot_token # 传递 specific_bot_token
        )


async def handle_set_password_command(custom_id: str, password: str, tid: int,
                                   admin_sender_id: int, admin_logger, conv_service: ConversationService, specific_bot_token: Optional[str] = None):
    """处理修改密码命令"""
    if not custom_id:
        raise CommandError(
            "用法错误：缺少自定义ID",
            "用法错误。\n修改密码: `/set_password <自定义ID> <新密码>`\n清除密码: `/set_password <自定义ID>` (密码部分留空)\n\n注意：此操作会替换原有密码。"
        )

    # 验证自定义ID格式
    if len(custom_id) < 3 or len(custom_id) > 50:
        raise CommandError(
            f"自定义ID长度无效: {len(custom_id)}",
            "自定义ID长度必须为3-50个字符。"
        )

    # 验证密码（如果提供）
    if password and len(password) > 128:
        raise CommandError(
            f"密码过长: {len(password)}",
            "密码长度不能超过128个字符。"
        )

    admin_logger.info(
        "修改自定义ID密码",
        extra={
            "custom_id": custom_id,
            "has_password": password is not None,
            "password_length": len(password) if password else 0
        }
    )

    success, message = await conv_service.set_binding_id_password(custom_id, password)

    reply_text = f"修改自定义ID '{custom_id}' 密码结果：\n{message}"
    if not success:
        reply_text = f"❗ 修改失败：\n{message}"

    await tg("sendMessage", {
        "chat_id": settings.SUPPORT_GROUP_ID,
        "message_thread_id": tid,
        "text": reply_text
    }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token


async def handle_create_id_command(custom_id: str, password: str, tid: int,
                                   admin_sender_id: int, admin_logger, conv_service: ConversationService, specific_bot_token: Optional[str] = None):
    """处理创建用户ID命令"""
    if not custom_id:
        raise CommandError(
            "用法错误：缺少自定义ID",
            "用法错误。\n创建ID: `/create_id <自定义ID> [<密码>]`\n例如: `/create_id user123` 或 `/create_id user123 password456`"
        )

    # 验证自定义ID格式
    if len(custom_id) < 4 or len(custom_id) > 50:
        raise CommandError(
            f"自定义ID长度无效: {len(custom_id)}",
            "自定义ID长度必须为4-50个字符。"
        )

    # 验证密码（如果提供）
    if password and not (4 < len(password) < 128):
        raise CommandError(
            f"密码长度不符合要求: {len(password)}",
            "密码长度必须在4到128个字符之间。"
        )

    admin_logger.info(
        "创建自定义ID",
        extra={
            "custom_id": custom_id,
            "has_password": password is not None,
            "password_length": len(password) if password else 0
        }
    )

    success, message = await conv_service.create_binding_id(custom_id, password)

    reply_text = f"创建自定义ID '{custom_id}' 结果：\n{message}"
    if not success:
        reply_text = f"❗ 创建失败：\n{message}"

    await tg("sendMessage", {
        "chat_id": settings.SUPPORT_GROUP_ID,
        "message_thread_id": tid,
        "text": reply_text
    }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token


async def handle_close_command(tid: int, admin_logger, conv_service: ConversationService,
                               entity_id: int, entity_type: str, specific_bot_token: Optional[str] = None):
    """处理关闭对话命令"""
    admin_logger.info(
        "关闭对话",
        extra={
            "entity_type": entity_type,
            "entity_id": entity_id
        }
    )

    await conv_service.close_conversation(tid, entity_id, entity_type, specific_bot_token=specific_bot_token) # 传递 specific_bot_token

    await tg("sendMessage", {
        "chat_id": settings.SUPPORT_GROUP_ID,
        "message_thread_id": tid,
        "text": f"对话已标记为关闭。关联实体: {entity_type} ID {entity_id}"
    }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token


async def handle_ban_command(tid: int, admin_logger, conv_service: ConversationService,
                             entity_id: int, entity_type: str, specific_bot_token: Optional[str] = None):
    """处理拉黑用户命令"""
    if entity_type != 'user':
        raise CommandError(
            f"ban命令不适用于{entity_type}类型实体",
            f"错误：/ban 命令仅适用于用户对话，此话题关联实体类型为 {entity_type} ID {entity_id}。"
        )

    admin_logger.info(
        "拉黑用户",
        extra={"user_id": entity_id}
    )

    await conv_service.ban_user(entity_id, specific_bot_token=specific_bot_token) # 传递 specific_bot_token

    await tg("sendMessage", {
        "chat_id": settings.SUPPORT_GROUP_ID,
        "message_thread_id": tid,
        "text": f"用户 {entity_id} 已被拉黑。"
    }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token


async def handle_unban_command(user_id_arg: str, tid: int, admin_logger, conv_service: ConversationService,
                               entity_id: int, entity_type: str, specific_bot_token: Optional[str] = None):
    """处理解除拉黑命令"""
    user_id_to_unban = None

    if user_id_arg:
        # 验证用户ID参数
        try:
            user_id_to_unban = int(user_id_arg)
            admin_logger.info(f"解除指定用户拉黑: {user_id_to_unban}")
        except ValueError:
            raise CommandError(
                f"无效的用户ID: {user_id_arg}",
                "无效的用户 ID。用法: /unban <用户ID> 或在用户对话话题中直接使用 /unban。"
            )
    else:
        # 使用当前话题关联的用户
        if entity_type != 'user':
            raise CommandError(
                f"unban命令在{entity_type}话题中需要用户ID参数",
                f"错误：/unban 命令仅适用于用户对话。此话题关联实体类型为 {entity_type}。用法: /unban <用户ID>。"
            )
        user_id_to_unban = entity_id
        admin_logger.info(f"解除当前话题用户拉黑: {user_id_to_unban}")

    success = await conv_service.unban_user(user_id_to_unban, specific_bot_token=specific_bot_token) # 传递 specific_bot_token

    if success:
        await tg("sendMessage", {
            "chat_id": settings.SUPPORT_GROUP_ID,
            "message_thread_id": tid,
            "text": f"用户 {user_id_to_unban} 已被解除拉黑。"
        }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
    else:
        await tg("sendMessage", {
            "chat_id": settings.SUPPORT_GROUP_ID,
            "message_thread_id": tid,
            "text": f"用户 {user_id_to_unban} 不在拉黑列表中或解除失败。"
        }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token


async def send_error_message(tid: int, message: str, specific_bot_token: Optional[str] = None):
    """发送错误消息到话题"""
    try:
        await tg("sendMessage", {
            "chat_id": settings.SUPPORT_GROUP_ID,
            "message_thread_id": tid,
            "text": message
        }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
    except Exception as e:
        logger.error(
            "发送错误消息失败",
            extra={
                "topic_id": tid,
                "error_message": message
            },
            exc_info=True
        )