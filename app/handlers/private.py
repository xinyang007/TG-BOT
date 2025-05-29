import logging
from starlette.concurrency import run_in_threadpool

from ..settings import settings
from ..store import Conversation
from ..tg_utils import tg, send_with_prefix
from ..services.conversation_service import ConversationService, MESSAGE_LIMIT_BEFORE_BIND

logger = logging.getLogger(__name__)


async def handle_private(msg: dict, conv_service: ConversationService):
    """处理用户发来的私聊消息。"""
    uid = msg["from"]["id"]
    user_first_name = msg["from"].get("first_name", f"用户 {uid}")
    message_id = msg.get("message_id")
    original_body = msg.get("text") or msg.get("caption")

    # 获取用户输入的原始文本用于命令解析，不转小写，因为ID和密码可能区分大小写
    raw_text_content = msg.get("text", "").strip()

    is_start_command = msg.get("text", "").strip().lower() == "/start"
    is_bind_command = msg.get("text", "").strip().lower().startswith("/bind ")
    # is_bind_command_alone: 用户只发送了 "/bind" (不区分大小写)
    is_bind_command_alone = raw_text_content.lower() == "/bind"


    is_bind_with_args_command = raw_text_content.lower().startswith("/bind ") and \
                                len(raw_text_content.split(maxsplit=1)) > 1  # 确保 /bind 后面有内容

    logger.info(f"处理来自用户 {uid} 的私聊消息 {message_id}")
    if is_start_command:
        logger.info(f"用户 {uid} 发送了 /start 命令.")
    elif is_bind_command:
        logger.info(f"用户 {uid} 发送了 /bind 命令.")

    # --- 1. 检查拉黑状态 ---
    try:
        is_banned = await conv_service.is_user_banned(uid)
        if is_banned:
            logger.info(f"用户 {uid} 被拉黑，停止处理.")
            try:
                await tg("sendMessage", {"chat_id": uid, "text": "您当前无法发起新的对话。"})
            except Exception as e:
                logger.warning(f"发送拉黑通知给用户 {uid} 失败: {e}")
            return
    except Exception as e:
        logger.error(f"检查用户 {uid} 拉黑状态失败: {e}", exc_info=True)
        try:
            await tg("sendMessage", {"chat_id": uid, "text": "服务器错误，请稍后再试。"})
        except Exception as e_notify:
            logger.warning(f"发送服务器错误消息给用户 {uid} 失败: {e_notify}")
        return

    # --- 2. 获取对话记录 ---
    conv = await conv_service.get_conversation_by_entity(uid, 'user')

    # --- 3. 处理绑定命令 ---
    if is_bind_command_alone:
        # 先检查用户是否已经绑定验证通过
        logger.info(f"PRIVATE_HANDLER: 用户 {uid} 发送了 /bind (无参数)，检查绑定状态。")

        # 获取用户的对话记录（如果 conv 在上面还没有获取）
        if not conv:
            conv = await conv_service.get_conversation_by_entity(uid, 'user')

        if conv and conv.is_verified == 'verified':
            # 已经绑定验证通过，发送已绑定消息
            logger.info(f"用户 {uid} 已经绑定验证通过，发送已绑定消息。")
            try:
                await tg("sendMessage", {
                    "chat_id": uid,
                    "text": "您已经完成绑定，无需重复绑定。"
                })
            except Exception as e:
                logger.error(f"PRIVATE_HANDLER: 向用户 {uid} 发送已绑定消息失败: {e}", exc_info=True)
        else:
            # 未绑定或未验证，发送引导消息
            logger.info(f"用户 {uid} 未绑定或未验证，发送引导消息。")
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
                    "text": message_text,
                    "parse_mode": "Markdown"
                })
            except Exception as e:
                logger.error(f"PRIVATE_HANDLER: 向用户 {uid} 发送 /bind 引导消息失败: {e}", exc_info=True)
        return  # 处理完毕，无论已绑定还是未绑定

    elif is_bind_with_args_command:  # 用户发送了 /bind <ID> [密码]
        logger.info(f"PRIVATE_HANDLER: 用户 {uid} 发送了带参数的 /bind 命令。")

        # raw_text_content 是原始的、未转小写的用户输入，例如 "/bind MyCustomID MyPassword123"
        # parts[0] 将是 "/bind" (或 "/BiNd" 等，取决于用户输入，但我们用 startswith("/bind ") 判断了)
        # parts[1] 将是 "MyCustomID"
        # parts[2] (如果存在) 将是 "MyPassword123"
        command_parts = raw_text_content.split(maxsplit=2)

        custom_id = None
        password_provided = None  # 默认为 None，表示用户可能没有提供密码

        if len(command_parts) > 1:  # 至少有 "/bind" 和 "<自定义ID>"
            custom_id = command_parts[1]

        if len(command_parts) > 2:  # 有 "/bind", "<自定义ID>", 和 "<密码>"
            password_provided = command_parts[2]

        if not custom_id:  # 理论上不会发生，因为 is_bind_with_args_command 保证了 /bind 后面有内容
            logger.warning(f"PRIVATE_HANDLER: 用户 {uid} 发送的 /bind 命令解析自定义ID失败: '{raw_text_content}'")
            await tg("sendMessage",
                     {"chat_id": uid, "text": "绑定格式错误，未能解析自定义ID。请使用 `/bind <自定义ID> [密码]`"})
            return

        logger.info(
            f"PRIVATE_HANDLER: 用户 {uid} 尝试绑定 ID: '{custom_id}', 提供密码: '{'******' if password_provided else '未提供'}'")

        success = await conv_service.bind_entity(
            entity_id=uid,
            entity_type='user',
            entity_name=user_first_name,
            custom_id=custom_id,
            password=password_provided  # 将解析出的密码传递给 service
        )
        logger.info(f"PRIVATE_HANDLER: 用户 {uid} 绑定到自定义 ID '{custom_id}' 的尝试结果: {success}")
        # conv_service.bind_entity 内部会发送成功或失败的具体消息给用户
        return  # /bind <ID> [密码] 命令处理完毕

    # --- 4. 处理对话状态和创建/重新开启逻辑 ---
    # 如果没有对话记录，或者记录中没有 topic_id (表示这是一个非常旧的待处理状态或错误状态)
    if not conv or not conv.topic_id:
        logger.info(f"用户 {uid} 没有带话题的活动对话。正在创建初始对话和话题。")
        conv = await conv_service.create_initial_conversation_with_topic(uid, 'user', user_first_name)
        if not conv or not conv.topic_id:  # 确保 conv 和 topic_id 都有效
            logger.error(f"为用户 {uid} 创建初始对话/话题失败。")
            try:
                await tg("sendMessage", {"chat_id": uid, "text": "无法开始对话，请稍后再试或联系管理员。"})
            except Exception:
                pass
            return  # 关键失败

        # 新对话和话题已创建，状态为 'pending' 验证。
        # 提示绑定，但当前消息现在可以被转发。
        try:
            await tg("sendMessage", {
                "chat_id": uid,
                "text": (
                    "欢迎！您的客服对话已创建。\n"
                    f"为了更好地为您服务。\n 请尽快使用 /bind <后台ID> [密码] 命令完成身份绑定。\n"
                    f"在绑定前，您最多可以发送 {MESSAGE_LIMIT_BEFORE_BIND} 条消息。"
                )
            })
        except Exception:
            pass
        # 继续处理当前消息（转发和记录）

    # 对话存在 (conv 不为 None 且 conv.topic_id 不为 None)
    # 如果对话待验证 (但现在有话题了)
    elif conv.is_verified != 'verified':
        logger.info(f"用户 {uid} (话题 {conv.topic_id}) 的对话待验证。")
        new_count, limit_reached = await conv_service.increment_message_count_and_check_limit(conv.entity_id,
                                                                                              conv.entity_type)

        if limit_reached:
            logger.warning(f"用户 {uid} (话题 {conv.topic_id}) 未验证对话达到消息限制。正在关闭。")
            # 关闭对话（服务方法处理话题名称和数据库更新）
            # close_conversation 现在可以处理 topic_id 为 None 的情况，但这里 topic_id 应该存在
            await conv_service.close_conversation(conv.topic_id, conv.entity_id, conv.entity_type)
            # close_conversation 已包含用户通知
            try:  # 额外的特定通知
                await tg("sendMessage", {"chat_id": uid,
                                         "text": f"您的未验证对话已达到消息限制 ({MESSAGE_LIMIT_BEFORE_BIND}条)，对话已关闭。请先完成绑定：/bind <您的自定义ID>"})
            except Exception:
                pass
            # 不要记录或转发触发限制关闭的此消息。
            return
        else:
            # 未达到限制，但仍未验证。再次提示。
            if not is_start_command:  # 如果已提示，则不要在 /start 上再次提示。
                try:
                    await tg("sendMessage", {"chat_id": uid,
                                             "text": f"您的对话仍需绑定。请发送 /bind <您的自定义ID>。 ({new_count}/{MESSAGE_LIMIT_BEFORE_BIND} 条消息)"})
                except Exception:
                    pass
            # 继续处理当前消息（转发和记录）

    # 如果对话已关闭且用户发送消息（且不是 /start, /bind）
    elif conv.status == "closed":
        if not is_start_command and not is_bind_command:  # 仅在实际消息内容上重新开启
            logger.info(f"用户 {uid} 向已关闭的对话 (话题 {conv.topic_id}) 发送消息。正在重新开启。")
            try:
                await conv_service.reopen_conversation(conv.entity_id, conv.entity_type, conv.topic_id)
                conv.status = "open"  # 更新本地 conv 对象状态
                # 通知在 reopen_conversation 中。
                # 继续处理当前消息
            except Exception as e:
                logger.error(f"为用户 {uid} 重新开启对话失败: {e}", exc_info=True)
                try:
                    await tg("sendMessage", {"chat_id": uid, "text": "无法重新开启对话，请稍后再试。"})
                except Exception:
                    pass
                return  # 重新开启失败，停止。
        else:  # 是 /start 或 /bind 发送到已关闭的对话，让特定逻辑处理或忽略
            if is_start_command:
                logger.info(f"用户 {uid} 向已关闭的对话发送了 /start。如果未验证则提示绑定，否则仅确认。")
                if conv.is_verified != 'verified':
                    try:
                        await tg("sendMessage",
                                 {"chat_id": uid, "text": f"您的对话已关闭但尚未绑定。请使用 /bind <您的自定义ID>。"})
                    except Exception:
                        pass
                else:  # 已关闭且已验证
                    try:
                        await tg("sendMessage",
                                 {"chat_id": uid, "text": f"您的上一个对话已关闭。发送消息即可开启新对话。"})
                    except Exception:
                        pass
                return  # /start 在已关闭对话上的处理完毕。

    # 此时，conv 应为 'open' 或 'pending' (但状态为 'open')，且有 topic_id。
    # 如果是 'pending' 验证，则未超过消息限制。
    # 如果是 /start 导致创建/重新开启，则跳过转发。
    if is_start_command and (not conv or conv.is_verified != 'verified' or conv.status == 'closed'):
        # 这个条件可能需要调整，因为 conv 在上面已经被处理为有效对象了
        # 简化：如果 is_start_command 并且是首次交互（通过 conv.message_count_before_bind 或其他标志判断）
        # 或者，更简单：如果 is_start_command 刚刚导致了话题创建或重新开启，就不转发。
        # 我们在创建/重开后已经 fall through，所以这里可以信任 conv.status
        # 如果是 /start 且对话刚刚被创建/重开（状态变为 open），并且是未验证的，就不转发 /start
        was_just_opened_or_created = (
                    conv.status == "open" and conv.message_count_before_bind <= 1 and conv.is_verified != 'verified')  # 粗略判断
        if is_start_command and was_just_opened_or_created:
            logger.debug(f"跳过转发用户 {uid} 的 /start 命令，因为它用于发起/重新开启。")
            # 记录 /start 命令本身可能仍然有用
            if conv and conv.topic_id:  # 确保对话和话题有效
                try:
                    await conv_service.record_incoming_message(
                        conv_id=conv.entity_id,
                        conv_entity_type='user',
                        sender_id=uid,
                        sender_name=user_first_name,
                        tg_mid=message_id,
                        body=original_body
                    )
                except Exception as e:
                    logger.error(f"记录用户 {uid} 的入站 /start 命令 {message_id} 失败: {e}", exc_info=True)
            return  # 不转发 /start

    # 修改：所有命令消息都不转发
    elif not (is_start_command or is_bind_command or is_bind_command_alone):  # 不转发任何命令消息
        # --- 5. 复制消息到群组话题 ---
        if conv and conv.topic_id:  # 确保对话和话题有效
            try:
                await send_with_prefix(
                    source_chat_id=uid,
                    dest_chat_id=settings.SUPPORT_GROUP_ID,
                    message_thread_id=conv.topic_id,
                    sender_name=user_first_name,
                    msg=msg
                )
                logger.info(f"成功复制用户 {uid} 的消息 {message_id} 到话题 {conv.topic_id}")
            except Exception as e:
                logger.error(f"复制用户 {uid} 的消息 {message_id} 到话题 {conv.topic_id} 失败: {e}", exc_info=True)
                try:
                    await tg("sendMessage", {"chat_id": uid, "text": "消息发送失败，请稍后再试。"})
                except Exception as e_notify:
                    logger.warning(f"发送'消息发送失败'通知给用户 {uid} 失败: {e_notify}")

            # --- 6. 记录入站消息 ---
            try:
                await conv_service.record_incoming_message(
                    conv_id=conv.entity_id,
                    conv_entity_type='user',
                    sender_id=uid,
                    sender_name=user_first_name,
                    tg_mid=message_id,
                    body=original_body
                )
            except Exception as e:
                logger.error(f"记录用户 {uid} 的入站消息 {message_id} 失败: {e}", exc_info=True)
        else:
            logger.warning(f"用户 {uid} 的对话或话题无效，无法转发或记录消息 {message_id}。")

    logger.debug(f"完成处理用户 {uid} 的私聊消息 {message_id}")