# app/handlers/group.py

from ..settings import settings
from ..tg_utils import tg, copy_any, send_with_prefix # 确保tg导入
from ..services.conversation_service import ConversationService, MESSAGE_LIMIT_BEFORE_BIND
from .commands import handle_commands
from ..logging_config import get_logger
from ..monitoring import monitor_performance, record_message_processing
from ..cache import CacheManager
from typing import Optional # 导入 Optional

logger = get_logger("app.handlers.group")

@monitor_performance("handle_group_message")
async def handle_group(msg: dict, conv_service: ConversationService, specific_bot_token: Optional[str] = None):
    """处理支持群组聊天和外部群组的入站消息"""
    chat_id = msg.get("chat", {}).get("id")
    message_id = msg.get("message_id")
    sender_user = msg.get("from")
    sender_id = sender_user.get("id") if sender_user else None
    sender_name = sender_user.get("first_name", "未知用户") if sender_user else "未知用户"
    original_content = msg.get("text") or msg.get("caption")
    raw_text_content_group = msg.get("text", "").strip()

    logger.info(
        "处理群组消息",
        extra={
            "chat_id": chat_id,
            "chat_type": msg.get('chat', {}).get('type'),
            "message_id": message_id,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "specific_bot_token_present": specific_bot_token is not None
        }
    )

    # 检查消息来源
    if conv_service.is_support_group(str(chat_id)):
        # 消息来自客服支持群组
        tid = msg.get("message_thread_id")
        if not tid:
            logger.debug(f"忽略客服支持群组 {chat_id} 中非话题线程的消息 {message_id}")
            return

        logger.info(
            "处理客服支持群组话题消息",
            extra={
                "chat_id": chat_id,
                "topic_id": tid,
                "message_id": message_id,
                "sender_id": sender_id
            }
        )

        # 检查是否为服务消息
        is_content_message = any(msg.get(key) for key in
                                 ["text", "caption", "photo", "video", "sticker", "animation", "document", "audio",
                                  "voice", "contact", "location", "venue", "poll", "game", "invoice",
                                  "successful_payment", "passport_data"])
        if not is_content_message:
            logger.debug(f"检测到话题 {tid} 中的消息 {message_id} 可能为服务消息，跳过处理")
            return

        # 1. 处理命令
        if original_content and original_content.strip().startswith("/"):
            logger.info(f"在话题 {tid} 中检测到命令: '{original_content}'")
            # commands.py 中的 handle_commands 内部会调用 tg，因此不需要传递 specific_bot_token
            await handle_commands(tid, sender_id, original_content.strip(), conv_service)
            return

        # 2. 处理管理员回复
        conv = None
        try:
            conv = await conv_service.get_conversation_by_topic(tid)
            if not conv:
                logger.warning(f"收到非命令/服务消息 {message_id} 在话题 {tid} 中，但未找到关联对话")
                try:
                    await tg("sendMessage", {
                        "chat_id": settings.SUPPORT_GROUP_ID,
                        "message_thread_id": tid,
                        "text": "注意：此话题未关联对话实体，消息不会转发。"
                    }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                except Exception:
                    pass
                return

            if conv.status == "closed":
                logger.info(f"收到管理员消息 {message_id} 在已关闭的话题 {tid} 中。不转发")
                try:
                    await tg("sendMessage", {
                        "chat_id": settings.SUPPORT_GROUP_ID,
                        "message_thread_id": tid,
                        "text": "注意：此对话已标记为关闭，消息不会转发。"
                    }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                except Exception:
                    pass
                return
        except Exception as e:
            logger.error(f"处理消息 {message_id} 时，查找话题 {tid} 对应的对话失败: {e}", exc_info=True)
            try:
                await tg("sendMessage", {
                    "chat_id": settings.SUPPORT_GROUP_ID,
                    "message_thread_id": tid,
                    "text": "处理消息失败：无法获取对话实体信息，消息未转发。"
                }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
            except Exception as e_notify:
                logger.warning(f"发送'查找实体失败'消息到话题 {tid} 失败: {e_notify}")
            return

        # 3. 添加发送者名字后缀 (管理员回复)
        suffix = f"\n-- 发送者: {sender_name}"
        copy_params = {}
        current_text = msg.get("text")
        current_caption = msg.get("caption")

        if current_text is not None:
            copy_params["text"] = current_text + suffix
        elif current_caption is not None:
            copy_params["caption"] = current_caption + suffix

        # 4. 复制消息到实体聊天
        try:
            # copy_any 内部会调用 tg，因此需要传递 specific_bot_token
            await copy_any(
                src_chat_id=settings.SUPPORT_GROUP_ID,
                dst_chat_id=conv.entity_id,
                message_id=message_id,
                extra_params=copy_params,
                specific_bot_token=specific_bot_token # 传递 specific_bot_token
            )
            logger.info(f"成功复制话题 {tid} 中的消息 {message_id} 到实体 {conv.entity_type} ID {conv.entity_id}")
        except Exception as e:
            logger.error(
                f"复制话题 {tid} 中的消息 {message_id} 到实体 {conv.entity_type} ID {conv.entity_id} 失败: {e}",
                exc_info=True
            )
            try:
                await tg("sendMessage", {
                    "chat_id": settings.SUPPORT_GROUP_ID,
                    "message_thread_id": tid,
                    "text": f"❗ 复制消息失败，无法发送给实体 {conv.entity_type} ID {conv.entity_id}。\n原始消息: {(original_content or '')[:100]}..."
                }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
            except Exception as e_notify:
                logger.warning(f"发送'复制失败'通知到话题 {tid} 失败: {e_notify}")

        # 5. 记录出站消息
        if conv:
            try:
                await conv_service.record_outgoing_message(
                    conv_id=conv.entity_id,
                    conv_entity_type=conv.entity_type,
                    sender_id=sender_id,
                    sender_name=sender_name,
                    tg_mid=message_id,
                    body=original_content
                )
            except Exception as e:
                logger.error(f"记录出站消息失败: {e}", exc_info=True)

    else:
        # 消息来自外部群组
        logger.info(
            "处理外部群组消息",
            extra={
                "chat_id": chat_id,
                "chat_type": msg.get('chat', {}).get('type'),
                "message_id": message_id,
                "sender_id": sender_id
            }
        )

        # 检查是否为服务消息或机器人自己的消息
        is_content_message = any(msg.get(key) for key in
                                 ["text", "caption", "photo", "video", "sticker", "animation", "document", "audio",
                                  "voice", "contact", "location", "venue", "poll", "game", "invoice",
                                  "successful_payment", "passport_data"])
        if not is_content_message:
            logger.debug(f"检测到外部群组 {chat_id} 中的消息 {message_id} 可能为服务消息，跳过处理")
            return

        # 检查机器人自己的消息
        # 理论上这里的settings.BOT_TOKEN应该指代主token，但多机器人环境下，需要检查所有启用bot的token
        # 最稳妥的方式是检查from.id是否是任何一个bot的ID
        from ..bot_manager import get_bot_manager # 延迟导入
        bot_manager = await get_bot_manager()
        is_bot_message = False
        if bot_manager:
            for bot_instance in bot_manager.bots.values():
                bot_id_from_token = bot_instance.config.token.split(':')[0]
                if sender_id is not None and str(sender_id) == bot_id_from_token:
                    is_bot_message = True
                    break
        else: # 单机器人模式
            if sender_id is not None and str(sender_id) == settings.BOT_TOKEN.split(':')[0]:
                is_bot_message = True

        if is_bot_message:
            logger.debug(f"检测到外部群组 {chat_id} 中的消息 {message_id} 是 Bot 自己发的，跳过处理")
            return

        # 获取群组名称
        group_name = f"群组 {chat_id}"
        try:
            # getChat API 调用也需要传递 specific_bot_token
            chat_info = await tg("getChat", {"chat_id": chat_id}, specific_bot_token=specific_bot_token)
            group_name = chat_info.get("title", group_name)
        except Exception as e:
            logger.warning(f"获取外部群组 {chat_id} 名称失败: {e}", exc_info=True)

        # 处理 /bind 命令
        if raw_text_content_group.lower().startswith("/bind"):
            is_bind_alone = raw_text_content_group.lower() == "/bind"
            is_bind_with_args = (raw_text_content_group.lower().startswith("/bind ") and
                                len(raw_text_content_group.split(maxsplit=1)) > 1)

            if is_bind_alone:
                logger.info(f"外部群组 {chat_id} 发送了 /bind (无参数)，检查绑定状态")

                group_conv_for_bind_check = await conv_service.get_conversation_by_entity(chat_id, 'group')

                if group_conv_for_bind_check and group_conv_for_bind_check.is_verified == 'verified':
                    logger.info(f"群组 {chat_id} 已经绑定验证通过，发送已绑定消息")
                    try:
                        await tg("sendMessage", {
                            "chat_id": chat_id,
                            "text": "本群组已经完成绑定，无需重复绑定。"
                        }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                    except Exception as e:
                        logger.error(f"向群组 {chat_id} 发送已绑定消息失败: {e}", exc_info=True)
                else:
                    logger.info(f"群组 {chat_id} 未绑定或未验证，发送引导消息")
                    message_text = (
                        "好的，准备为本群组绑定对话。\n"
                        "请群管理员按照以下格式回复自定义ID和可选的密码进行绑定：\n\n"
                        "`/bind <群组专属自定义ID> [密码]`\n\n"
                        "例如：\n"
                        "`/bind groupXYZ` (如果此ID不需要密码)\n"
                        "`/bind ourGroup PaSs123` (如果此ID需要密码 `PaSs123`)"
                    )
                    try:
                        await tg("sendMessage", {
                            "chat_id": chat_id,
                            "text": message_text,
                            "parse_mode": "Markdown"
                        }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                        logger.info(f"成功向群组 {chat_id} 发送 /bind 引导消息")
                    except Exception as e:
                        logger.error(f"向群组 {chat_id} 发送 /bind 引导消息失败: {e}", exc_info=True)
                return

            elif is_bind_with_args:
                logger.info(f"外部群组 {chat_id} 发送了带参数的 /bind 命令")
                command_parts = raw_text_content_group.split(maxsplit=2)

                custom_id = None
                password_provided = None

                if len(command_parts) > 1:
                    custom_id = command_parts[1]
                if len(command_parts) > 2:
                    password_provided = command_parts[2]

                if not custom_id:
                    logger.warning(f"群组 {chat_id} 发送的 /bind 命令解析自定义ID失败: '{raw_text_content_group}'")
                    try:
                        await tg("sendMessage", {
                            "chat_id": chat_id,
                            "text": "绑定格式错误，未能解析自定义ID。请使用 `/bind <自定义ID> [密码]`",
                            "parse_mode": "Markdown"
                        }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                    except Exception as e:
                        logger.error(f"发送绑定格式错误消息失败: {e}")
                    return

                logger.info(
                    f"群组 {chat_id} ({group_name}) 尝试绑定 ID: '{custom_id}', 提供密码: '{'******' if password_provided else '未提供'}'")

                try:
                    success = await conv_service.bind_entity(
                        entity_id=chat_id,
                        entity_type='group',
                        entity_name=group_name,
                        custom_id=custom_id,
                        password=password_provided,
                        specific_bot_token=specific_bot_token # 传递 specific_bot_token
                    )
                    logger.info(f"群组 {chat_id} 绑定到自定义 ID '{custom_id}' 的结果: {success}")
                except Exception as e:
                    logger.error(f"群组 {chat_id} 绑定过程中发生异常: {e}", exc_info=True)
                    try:
                        await tg("sendMessage", {
                            "chat_id": chat_id,
                            "text": "绑定过程中发生错误，请稍后再试或联系管理员。"
                        }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                    except Exception:
                        pass
                return

        # 获取或创建群组对话实体
        group_conv = await conv_service.get_conversation_by_entity(chat_id, 'group')

        # 如果没有对话记录，或者记录中没有 topic_id
        if not group_conv or not group_conv.topic_id:
            logger.info(f"外部群组 {chat_id} ({group_name}) 没有带话题的活动对话。正在创建")
            group_conv = await conv_service.create_initial_conversation_with_topic(chat_id, 'group', group_name) # create_initial_conversation_with_topic 内部会使用 tg_caller，无需额外传递 specific_bot_token
            if not group_conv or not group_conv.topic_id:
                logger.error(f"为群组 {chat_id} 创建初始对话/话题失败")
                return

            # 新对话和话题已创建，状态为 'pending' 验证
            try:
                await tg("sendMessage", {
                    "chat_id": chat_id,
                    "text": (
                        f"欢迎！本群组的客服协助通道已创建。\n"
                        f"为了将本群组消息正确路由给客服，请群管理员使用 /bind <群组专属自定义ID> 命令完成绑定。\n"
                        f"在绑定前，本群组最多可以发送 {MESSAGE_LIMIT_BEFORE_BIND} 条消息给客服系统。"
                    )
                }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
            except Exception:
                pass

        # 处理未验证对话的消息限制
        elif group_conv.is_verified != 'verified':
            logger.info(f"外部群组 {chat_id} (话题 {group_conv.topic_id}) 的对话待验证")
            new_count, limit_reached = await conv_service.increment_message_count_and_check_limit(
                group_conv.entity_id, group_conv.entity_type) # 不发送消息，无需 specific_bot_token

            if limit_reached:
                logger.warning(f"外部群组 {chat_id} (话题 {group_conv.topic_id}) 未验证对话达到消息限制")
                await conv_service.close_conversation(group_conv.topic_id, group_conv.entity_id, group_conv.entity_type, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                try:
                    await tg("sendMessage", {
                        "chat_id": chat_id,
                        "text": f"本群组的未验证客服对话已达到消息限制 ({MESSAGE_LIMIT_BEFORE_BIND}条)，对话已关闭。请管理员先完成绑定：/bind <群组专属自定义ID>"
                    }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                except Exception:
                    pass
                return
            else:
                # 未达到限制，但仍未验证。再次提示（如果不是命令）
                if not (original_content and original_content.strip().startswith("/")):
                    try:
                        await tg("sendMessage", {
                            "chat_id": chat_id,
                            "text": f"本群组的客服对话仍需绑定。请管理员发送 /bind <群组专属自定义ID>。 ({new_count}/{MESSAGE_LIMIT_BEFORE_BIND} 条消息)"
                        }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                    except Exception:
                        pass

        # 处理已关闭的对话
        elif group_conv.status == "closed":
            if not (original_content and original_content.strip().startswith("/")):
                logger.info(f"来自外部群组 {chat_id} 的消息发送到已关闭的对话 (话题 {group_conv.topic_id})。正在重新开启")
                try:
                    await conv_service.reopen_conversation(group_conv.entity_id, group_conv.entity_type, group_conv.topic_id, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                    group_conv.status = "open"
                except Exception as e:
                    logger.error(f"为群组 {chat_id} 重新开启对话失败: {e}", exc_info=True)
                    try:
                        await tg("sendMessage", {"chat_id": chat_id, "text": "无法重新开启客服对话，请稍后再试。"}, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                    except Exception:
                        pass
                    return
            else:
                logger.debug(f"在已关闭的群组 {chat_id} 话题中收到命令 '{original_content}'。暂时忽略")
                return

        # 转发消息到客服支持话题
        if group_conv and group_conv.topic_id and group_conv.status == "open":
            group_name_for_prefix = group_name or f"群组 {chat_id}"
            sender_name_for_prefix = sender_name or f"用户 {sender_id}"

            try:
                await send_with_prefix(
                    source_chat_id=chat_id,
                    dest_chat_id=settings.SUPPORT_GROUP_ID,
                    message_thread_id=group_conv.topic_id,
                    sender_name=f"🏠{group_name_for_prefix} | 👤{sender_name_for_prefix}",
                    msg=msg,
                    conversation_service=conv_service,
                    entity_id=chat_id,
                    entity_type='group', # 应该是group
                    entity_name=group_name_for_prefix,
                    specific_bot_token=specific_bot_token # 传递 specific_bot_token
                )
                logger.info(f"成功复制外部群组 {chat_id} 的消息 {message_id} 到话题 {group_conv.topic_id}")
            except Exception as e:
                logger.error(f"复制外部群组 {chat_id} 的消息 {message_id} 到话题 {group_conv.topic_id} 失败: {e}", exc_info=True)
                try:
                    await tg("sendMessage", {
                        "chat_id": settings.SUPPORT_GROUP_ID,
                        "message_thread_id": group_conv.topic_id,
                        "text": f"❗ 从群组 {chat_id} ({group_name_for_prefix}) 复制消息失败。\n发送者: {sender_name_for_prefix}\n原始消息: {(original_content or '')[:100]}..."
                    }, specific_bot_token=specific_bot_token) # 传递 specific_bot_token
                except Exception as e_notify:
                    logger.warning(f"发送'复制失败'通知到话题 {group_conv.topic_id} 失败: {e_notify}")

            # 记录入站消息
            try:
                await conv_service.record_incoming_message(
                    conv_id=group_conv.entity_id,
                    conv_entity_type='group',
                    sender_id=sender_id,
                    sender_name=sender_name,
                    tg_mid=message_id,
                    body=original_content
                )
            except Exception as e:
                logger.error(f"记录外部群组 {chat_id} 的入站消息 {message_id} 失败: {e}", exc_info=True)
        else:
            logger.warning(
                f"外部群组 {chat_id} 的对话状态不允许转发。"
                f"topic_id: {group_conv.topic_id if group_conv else 'N/A'}, "
                f"status: {group_conv.status if group_conv else 'N/A'}"
            )