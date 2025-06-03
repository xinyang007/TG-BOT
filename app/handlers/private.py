from starlette.concurrency import run_in_threadpool

from ..settings import settings
from ..store import Conversation
from ..tg_utils import tg, send_with_prefix
from ..services.conversation_service import ConversationService, MESSAGE_LIMIT_BEFORE_BIND
from ..logging_config import get_user_logger, get_logger
from ..validation import validate_bind_command, ValidationError, UserInput
from ..monitoring import record_message_processing, monitor_performance
from ..cache import CacheManager

logger = get_logger("app.handlers.private")


@monitor_performance("handle_private_message")
async def handle_private(msg: dict, conv_service: ConversationService):
    """处理用户发来的私聊消息"""
    # 基本信息提取和验证
    uid = msg["from"]["id"]
    user_first_name = msg["from"].get("first_name", f"用户 {uid}")
    message_id = msg.get("message_id")
    original_body = msg.get("text") or msg.get("caption")

    # 使用用户相关的日志器
    user_logger = get_user_logger(uid, "private_message")

    # 获取用户输入的原始文本
    raw_text_content = msg.get("text", "").strip()

    # 命令识别
    is_start_command = raw_text_content.lower() == "/start"
    is_bind_command = raw_text_content.lower().startswith("/bind ")
    is_bind_command_alone = raw_text_content.lower() == "/bind"
    is_bind_with_args_command = (
            raw_text_content.lower().startswith("/bind ") and
            len(raw_text_content.split(maxsplit=1)) > 1
    )

    user_logger.info(
        "处理私聊消息",
        extra={
            "message_id": message_id,
            "message_type": "command" if raw_text_content.startswith("/") else "text",
            "is_start": is_start_command,
            "is_bind": is_bind_command or is_bind_command_alone
        }
    )

    # --- 1. 输入验证 ---
    try:
        if original_body:
            user_input = UserInput(user_id=uid, text=original_body)
            # 使用验证后的清理文本
            original_body = user_input.text
    except Exception as e:
        user_logger.warning(
            "用户输入验证失败",
            extra={"validation_error": str(e)}
        )
        try:
            await tg("sendMessage", {
                "chat_id": uid,
                "text": "消息格式有误，请重新发送。"
            })
        except Exception:
            pass
        return

    # --- 2. 检查拉黑状态 ---
    try:
        is_banned = await conv_service.is_user_banned(uid)
        if is_banned:
            user_logger.info("用户被拉黑，停止处理")
            try:
                await tg("sendMessage", {
                    "chat_id": uid,
                    "text": "您当前无法发起新的对话。"
                })
            except Exception as e:
                user_logger.warning("发送拉黑通知失败", extra={"error": str(e)})
            return
    except Exception as e:
        user_logger.error("检查用户拉黑状态失败", exc_info=True)
        try:
            await tg("sendMessage", {
                "chat_id": uid,
                "text": "服务器错误，请稍后再试。"
            })
        except Exception:
            pass
        return

    # --- 3. 获取对话记录 ---
    try:
        conv = await conv_service.get_conversation_by_entity(uid, 'user')
    except Exception as e:
        user_logger.error("获取对话记录失败", exc_info=True)
        try:
            await tg("sendMessage", {
                "chat_id": uid,
                "text": "获取对话状态失败，请稍后再试。"
            })
        except Exception:
            pass
        return

    # --- 4. 处理绑定命令 ---
    if is_bind_command_alone:
        user_logger.info("处理 /bind 命令（无参数）")

        if conv and conv.is_verified == 'verified':
            user_logger.info("用户已绑定")
            try:
                await tg("sendMessage", {
                    "chat_id": uid,
                    "text": "您已经完成绑定，无需重复绑定。"
                })
            except Exception as e:
                user_logger.error("发送已绑定消息失败", exc_info=True)
        else:
            user_logger.info("发送绑定引导消息")
            message_text = (
                "好的，您准备绑定对话。\n"
                "请按照以下格式回复您的自定义ID和密码进行绑定：\n\n"
                "`/bind <您的自定义ID> [您的密码]`\n\n"
                "例如：\n"
                "`/bind anotherID PaSsWoRd` (如果此ID需要密码 `PaSsWoRd`)"
            )
            try:
                await tg("sendMessage", {
                    "chat_id": uid,
                    "text": message_text
                })
            except Exception as e:
                user_logger.error("发送绑定引导消息失败", exc_info=True)
        return

    elif is_bind_with_args_command:
        user_logger.info("处理 /bind 命令（带参数）")

        try:
            # 验证绑定命令
            bind_cmd = validate_bind_command(raw_text_content)

            user_logger.info(
                "绑定命令验证通过",
                extra={
                    "custom_id": bind_cmd.custom_id,
                    "has_password": bind_cmd.password is not None
                }
            )

            # 执行绑定
            success = await conv_service.bind_entity(
                entity_id=uid,
                entity_type='user',
                entity_name=user_first_name,
                custom_id=bind_cmd.custom_id,
                password=bind_cmd.password
            )

            user_logger.info(
                "绑定操作完成",
                extra={
                    "success": success,
                    "custom_id": bind_cmd.custom_id
                }
            )

        except ValidationError as e:
            user_logger.warning(
                "绑定命令验证失败",
                extra={"validation_error": e.message}
            )
            try:
                # 修复：使用更安全的消息格式，避免 Markdown 解析错误
                error_text = (
                    f"绑定格式错误：{e.message}\n\n"
                    f"请使用正确格式：\n"
                    f"/bind <自定义ID> [密码]\n\n"
                    f"例如：\n"
                    f"/bind myID123\n"
                    f"/bind myID123 myPassword"
                )
                await tg("sendMessage", {
                    "chat_id": uid,
                    "text": error_text
                    # 移除 parse_mode 避免格式解析错误
                })
            except Exception as send_error:
                user_logger.error("发送绑定错误消息失败", exc_info=True)
        except Exception as e:
            user_logger.error("绑定过程异常", exc_info=True)
            try:
                await tg("sendMessage", {
                    "chat_id": uid,
                    "text": "绑定过程中发生错误，请稍后重试或联系管理员。"
                })
            except Exception:
                pass
        return

    # --- 5. 处理对话状态和创建/重新开启逻辑 ---
    if not conv or not conv.topic_id:
        user_logger.info("创建初始对话和话题")
        try:
            conv = await conv_service.create_initial_conversation_with_topic(
                uid, 'user', user_first_name
            )
            if not conv or not conv.topic_id:
                user_logger.error("创建初始对话失败")
                try:
                    await tg("sendMessage", {
                        "chat_id": uid,
                        "text": "无法开始对话，请稍后再试或联系管理员。"
                    })
                except Exception:
                    pass
                return

            # 发送欢迎消息
            try:
                await tg("sendMessage", {
                    "chat_id": uid,
                    "text": (
                        "欢迎！您的客服对话已创建。\n"
                        f"为了更好地为您服务，请尽快使用 /bind <后台ID> [密码] 命令完成身份绑定。\n"
                        f"在绑定前，您最多可以发送 {MESSAGE_LIMIT_BEFORE_BIND} 条消息。"
                    )
                })
            except Exception as e:
                user_logger.warning("发送欢迎消息失败", extra={"error": str(e)})

        except Exception as e:
            user_logger.error("创建对话过程异常", exc_info=True)
            try:
                await tg("sendMessage", {
                    "chat_id": uid,
                    "text": "创建对话失败，请稍后重试。"
                })
            except Exception:
                pass
            return

    # --- 6. 处理未验证对话的消息限制 ---
    elif conv.is_verified != 'verified':
        user_logger.info("处理未验证对话")
        try:
            new_count, limit_reached = await conv_service.increment_message_count_and_check_limit(
                conv.entity_id, conv.entity_type
            )

            if limit_reached:
                user_logger.warning("未验证对话达到消息限制")
                await conv_service.close_conversation(
                    conv.topic_id, conv.entity_id, conv.entity_type
                )
                try:
                    await tg("sendMessage", {
                        "chat_id": uid,
                        "text": (
                            f"您的未验证对话已达到消息限制 ({MESSAGE_LIMIT_BEFORE_BIND}条)，"
                            f"对话已关闭。请先完成绑定：/bind <您的自定义ID>"
                        )
                    })
                except Exception:
                    pass
                return
            else:
                if not is_start_command:
                    try:
                        await tg("sendMessage", {
                            "chat_id": uid,
                            "text": (
                                f"您的对话仍需绑定。请发送 /bind <您的自定义ID>。"
                                f" ({new_count}/{MESSAGE_LIMIT_BEFORE_BIND} 条消息)"
                            )
                        })
                    except Exception:
                        pass

        except Exception as e:
            user_logger.error("处理消息限制检查异常", exc_info=True)
            return

    # --- 7. 处理已关闭的对话 ---
    elif conv.status == "closed":
        if not is_start_command and not is_bind_command:
            user_logger.info("重新开启已关闭的对话")
            try:
                await conv_service.reopen_conversation(
                    conv.entity_id, conv.entity_type, conv.topic_id
                )
                conv.status = "open"
            except Exception as e:
                user_logger.error("重新开启对话失败", exc_info=True)
                try:
                    await tg("sendMessage", {
                        "chat_id": uid,
                        "text": "无法重新开启对话，请稍后再试。"
                    })
                except Exception:
                    pass
                return
        else:
            if is_start_command:
                user_logger.info("在已关闭对话中处理 /start")
                if conv.is_verified != 'verified':
                    try:
                        await tg("sendMessage", {
                            "chat_id": uid,
                            "text": "您的对话已关闭但尚未绑定。请使用 /bind <您的自定义ID>。"
                        })
                    except Exception:
                        pass
                else:
                    try:
                        await tg("sendMessage", {
                            "chat_id": uid,
                            "text": "您的上一个对话已关闭。发送消息即可开启新对话。"
                        })
                    except Exception:
                        pass
                return

    # --- 8. 转发消息到客服话题 ---
    # 跳过命令消息的转发
    if not (is_start_command or is_bind_command or is_bind_command_alone):
        if conv and conv.topic_id:
            try:
                await send_with_prefix(
                    source_chat_id=uid,
                    dest_chat_id=settings.SUPPORT_GROUP_ID,
                    message_thread_id=conv.topic_id,
                    sender_name=user_first_name,
                    msg=msg,
                    conversation_service=conv_service,  # 添加这个参数
                    entity_id=uid,  # 添加这个参数
                    entity_type='user',  # 添加这个参数
                    entity_name=user_first_name  # 添加这个参数
                )
                user_logger.info(
                    "消息转发成功",
                    extra={"topic_id": conv.topic_id}
                )
            except Exception as e:
                user_logger.error("消息转发失败", exc_info=True)
                try:
                    await tg("sendMessage", {
                        "chat_id": uid,
                        "text": "消息发送失败，请稍后再试。"
                    })
                except Exception:
                    pass

    # --- 9. 记录消息 ---
    if conv and conv.topic_id:
        try:
            await conv_service.record_incoming_message(
                conv_id=conv.entity_id,
                conv_entity_type='user',
                sender_id=uid,
                sender_name=user_first_name,
                tg_mid=message_id,
                body=original_body
            )
            user_logger.debug("消息记录成功")
        except Exception as e:
            user_logger.error("记录消息失败", exc_info=True)

    user_logger.debug("私聊消息处理完成")