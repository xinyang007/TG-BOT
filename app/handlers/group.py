import logging
from ..settings import settings
from ..tg_utils import tg, copy_any, send_with_prefix
from ..services.conversation_service import ConversationService, MESSAGE_LIMIT_BEFORE_BIND
from .commands import handle_commands

logger = logging.getLogger(__name__)


async def handle_group(msg: dict, conv_service: ConversationService):
    """处理支持群组聊天和外部群组的入站消息。"""
    chat_id = msg.get("chat", {}).get("id")
    message_id = msg.get("message_id")
    sender_user = msg.get("from")
    sender_id = sender_user.get("id") if sender_user else None
    sender_name = sender_user.get("first_name", "未知用户") if sender_user else "未知用户"
    original_content = msg.get("text") or msg.get("caption")

    logger.info(
        f"处理来自聊天 {chat_id} (类型: {msg.get('chat', {}).get('type')}) 的消息 {message_id}, 发送者 {sender_id} ({sender_name})")

    # --- 检查消息来源 ---
    if conv_service.is_support_group(str(chat_id)):
        # --- 消息来自客服支持群组 ---
        tid = msg.get("message_thread_id")
        if not tid:
            logger.debug(f"忽略客服支持群组 {chat_id} 中非话题线程的消息 {message_id}.")
            return

        logger.info(f"处理客服支持群组 {chat_id} 中话题 {tid} 的消息 {message_id}，发送者 {sender_id} ({sender_name})")

        # --- 检查是否为服务消息 ---
        is_content_message = any(msg.get(key) for key in
                                 ["text", "caption", "photo", "video", "sticker", "animation", "document", "audio",
                                  "voice", "contact", "location", "venue", "poll", "game", "invoice",
                                  "successful_payment", "passport_data"])
        if not is_content_message:
            logger.debug(f"检测到话题 {tid} 中的消息 {message_id} 可能为服务消息，跳过处理。")
            return

        # --- 1. 处理命令 ---
        if original_content and original_content.strip().startswith("/"):
            logger.info(f"在话题 {tid} 中检测到命令: '{original_content}'")
            await handle_commands(tid, sender_id, original_content.strip(), conv_service)
            return

        # --- 2. 处理管理员回复 ---
        conv = None
        try:
            conv = await conv_service.get_conversation_by_topic(tid)
            if not conv:
                logger.warning(f"收到非命令/服务消息 {message_id} 在话题 {tid} 中，但未找到关联对话。忽略。")
                try:
                    await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                             "text": "注意：此话题未关联对话实体，消息不会转发。"})
                except Exception:
                    pass
                return

            if conv.status == "closed":
                logger.info(
                    f"收到管理员消息 {message_id} 在已关闭的话题 {tid} (实体 {conv.entity_type} ID {conv.entity_id}) 中。不转发。")
                try:
                    await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                             "text": "注意：此对话已标记为关闭，消息不会转发。"})
                except Exception:
                    pass
                return
        except Exception as e:
            logger.error(f"处理消息 {message_id} 时，查找话题 {tid} 对应的对话失败: {e}", exc_info=True)
            try:
                await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                         "text": "处理消息失败：无法获取对话实体信息，消息未转发。"})
            except Exception as e_notify:
                logger.warning(f"发送'查找实体失败'消息到话题 {tid} 失败: {e_notify}")
            return

        # --- 3. 添加发送者名字后缀 (管理员回复) ---
        suffix = f"\n-- 发送者: {sender_name}"
        # 构建 copy_params，正确处理 text 和 caption
        copy_params = {}
        current_text = msg.get("text")
        current_caption = msg.get("caption")

        if current_text is not None:  # 包括空字符串
            copy_params["text"] = current_text + suffix
        elif current_caption is not None:  # 包括空字符串
            copy_params["caption"] = current_caption + suffix
        # 如果都没有，但有其他媒体，则 suffix 不会添加，这是期望行为

        # --- 4. 复制消息到实体聊天 ---
        try:
            await copy_any(
                src_chat_id=settings.SUPPORT_GROUP_ID,  # 源是客服群
                dst_chat_id=conv.entity_id,  # 目标是关联的实体 (用户或群组)
                message_id=message_id,  # 要复制的消息 ID
                extra_params=copy_params  # 包含修改后文本/标题的参数
            )
            logger.info(f"成功复制话题 {tid} 中的消息 {message_id} 到实体 {conv.entity_type} ID {conv.entity_id}")
        except Exception as e:
            logger.error(
                f"复制话题 {tid} 中的消息 {message_id} 到实体 {conv.entity_type} ID {conv.entity_id} 失败: {e}",
                exc_info=True)
            try:
                await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID,
                                         "message_thread_id": tid,
                                         "text": f"❗ 复制消息失败，无法发送给实体 {conv.entity_type} ID {conv.entity_id}。\n原始消息: {(original_content or '')[:100]}..."})
            except Exception as e_notify:
                logger.warning(f"发送'复制失败'通知到话题 {tid} 失败: {e_notify}")

        # --- 5. 记录出站消息 ---
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
                logger.error(f"记录出站消息 for conv {conv.entity_id} (TG MID: {message_id}) 失败: {e}", exc_info=True)

    elif conv_service.is_external_group(chat_id):
        # --- 消息来自需要监听的外部群组 ---
        logger.info(
            f"处理来自外部群组 {chat_id} (类型: {msg.get('chat', {}).get('type')}) 的消息 {message_id}, 发送者 {sender_id} ({sender_name})")

        # --- 检查是否为服务消息或机器人自己的消息 ---
        is_content_message = any(msg.get(key) for key in
                                 ["text", "caption", "photo", "video", "sticker", "animation", "document", "audio",
                                  "voice", "contact", "location", "venue", "poll", "game", "invoice",
                                  "successful_payment", "passport_data"])
        if not is_content_message:
            logger.debug(f"检测到外部群组 {chat_id} 中的消息 {message_id} 可能为服务消息，跳过处理。")
            return
        if sender_id is not None and str(sender_id) == settings.BOT_TOKEN.split(':')[0]:
            logger.debug(f"检测到外部群组 {chat_id} 中的消息 {message_id} 是 Bot 自己发的，跳过处理。")
            return

        # --- 获取群组名称 ---
        group_name = f"群组 {chat_id}"
        try:
            chat_info = await tg("getChat", {"chat_id": chat_id})
            group_name = chat_info.get("title", group_name)
        except Exception as e:
            logger.warning(f"获取外部群组 {chat_id} 名称失败: {e}", exc_info=True)

        # --- 获取或创建群组对话实体 ---
        group_conv = await conv_service.get_conversation_by_entity(chat_id, 'group')

        # 处理绑定命令 (群组内)
        if original_content and original_content.strip().lower().startswith("/bind "):
            logger.info(f"在外部群组 {chat_id} 检测到 /bind 命令。")
            args = original_content.strip().split(maxsplit=1)
            if len(args) > 1:
                custom_bind_id = args[1]
                logger.info(f"外部群组 {chat_id} ({group_name}) 尝试使用ID进行绑定: {custom_bind_id}")
                # 假设群组内的 /bind 命令由管理员发出，代表整个群组进行绑定
                success = await conv_service.bind_entity(chat_id, 'group', group_name, custom_bind_id)
                logger.info(f"群组 {chat_id} 绑定到 {custom_bind_id} 尝试结果: {success}")
            else:
                try:
                    await tg("sendMessage", {"chat_id": chat_id, "text": "用法: /bind <群组专属自定义ID>"})
                except Exception:
                    pass
            return  # /bind 命令处理完毕

        # 如果没有对话记录，或者记录中没有 topic_id
        if not group_conv or not group_conv.topic_id:
            logger.info(f"外部群组 {chat_id} ({group_name}) 没有带话题的活动对话。正在创建。")
            group_conv = await conv_service.create_initial_conversation_with_topic(chat_id, 'group', group_name)
            if not group_conv or not group_conv.topic_id:
                logger.error(f"为群组 {chat_id} 创建初始对话/话题失败。")
                # 不在群组中发送失败通知，避免干扰
                return

            # 新对话和话题已创建，状态为 'pending' 验证。
            # 提示群组进行绑定。
            try:
                await tg("sendMessage", {
                    "chat_id": chat_id,  # 发送到外部群组
                    "text": (
                        f"欢迎！本群组的客服协助通道已创建。\n"
                        f"为了将本群组消息正确路由给客服，请群管理员使用 /bind <群组专属自定义ID> 命令完成绑定。\n"
                        f"在绑定前，本群组最多可以发送 {MESSAGE_LIMIT_BEFORE_BIND} 条消息给客服系统。"
                    )
                })
            except Exception:
                pass
            # 继续处理当前消息

        # 对话存在 (group_conv 不为 None 且 group_conv.topic_id 不为 None)
        elif group_conv.is_verified != 'verified':
            logger.info(f"外部群组 {chat_id} (话题 {group_conv.topic_id}) 的对话待验证。")
            new_count, limit_reached = await conv_service.increment_message_count_and_check_limit(group_conv.entity_id,
                                                                                                  group_conv.entity_type)

            if limit_reached:
                logger.warning(f"外部群组 {chat_id} (话题 {group_conv.topic_id}) 未验证对话达到消息限制。正在关闭。")
                await conv_service.close_conversation(group_conv.topic_id, group_conv.entity_id, group_conv.entity_type)
                try:
                    await tg("sendMessage", {"chat_id": chat_id,
                                             "text": f"本群组的未验证客服对话已达到消息限制 ({MESSAGE_LIMIT_BEFORE_BIND}条)，对话已关闭。请管理员先完成绑定：/bind <群组专属自定义ID>"})
                except Exception:
                    pass
                return
            else:
                # 未达到限制，但仍未验证。再次提示（如果不是命令）。
                if not (original_content and original_content.strip().startswith("/")):
                    try:
                        await tg("sendMessage", {"chat_id": chat_id,
                                                 "text": f"本群组的客服对话仍需绑定。请管理员发送 /bind <群组专属自定义ID>。 ({new_count}/{MESSAGE_LIMIT_BEFORE_BIND} 条消息)"})
                    except Exception:
                        pass
                # 继续处理消息

        elif group_conv.status == "closed":
            # 如果群组向已关闭的对话发送消息，重新开启它。
            # 通常不因群组成员发送的命令而重新开启。
            if not (original_content and original_content.strip().startswith("/")):
                logger.info(
                    f"来自外部群组 {chat_id} 的消息发送到已关闭的对话 (话题 {group_conv.topic_id})。正在重新开启。")
                try:
                    await conv_service.reopen_conversation(group_conv.entity_id, group_conv.entity_type,
                                                           group_conv.topic_id)
                    group_conv.status = "open"  # 更新本地 conv 对象状态
                except Exception as e:
                    logger.error(f"为群组 {chat_id} 重新开启对话失败: {e}", exc_info=True)
                    try:
                        await tg("sendMessage", {"chat_id": chat_id, "text": "无法重新开启客服对话，请稍后再试。"})
                    except Exception:
                        pass
                    return
            else:  # 是已关闭群组话题中的命令，通常忽略或按需特别处理
                logger.debug(f"在已关闭的群组 {chat_id} 话题中收到命令 '{original_content}'。暂时忽略。")
                return

        # 此时，group_conv 应为 'open'，有 topic_id。
        # 如果是 'pending' 验证，则未超过消息限制。
        # 将消息从外部群组转发到支持话题。

        # --- 添加发送者和群组信息前缀 ---
        # (这里的 group_name 应该使用上面获取的，而不是 group_conv.entity_name，因为 group_conv 可能刚创建)
        group_name_for_prefix = group_name or f"群组 {chat_id}"
        sender_name_for_prefix = sender_name or f"用户 {sender_id}"
        # prefix = f"🏠 {group_name_for_prefix} | 👤 {sender_name_for_prefix}:\n" # send_with_prefix 会处理前缀

        # --- 5. 复制消息到客服支持话题 ---
        if group_conv and group_conv.topic_id:  # 确保对话和话题有效
            try:
                await send_with_prefix(
                    source_chat_id=chat_id,
                    dest_chat_id=settings.SUPPORT_GROUP_ID,
                    message_thread_id=group_conv.topic_id,
                    sender_name=f"🏠{group_name_for_prefix} | 👤{sender_name_for_prefix}",  # 构建完整前缀传递给 sender_name
                    msg=msg
                )
                logger.info(f"成功复制外部群组 {chat_id} 的消息 {message_id} 到话题 {group_conv.topic_id}")
            except Exception as e:
                logger.error(f"复制外部群组 {chat_id} 的消息 {message_id} 到话题 {group_conv.topic_id} 失败: {e}",
                             exc_info=True)
                try:
                    await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID,
                                             "message_thread_id": group_conv.topic_id,
                                             "text": f"❗ 从群组 {chat_id} ({group_name_for_prefix}) 复制消息失败。\n发送者: {sender_name_for_prefix}\n原始消息: {(original_content or '')[:100]}..."})
                except Exception as e_notify:
                    logger.warning(f"发送'复制失败'通知到话题 {group_conv.topic_id} 失败: {e_notify}")

            # --- 6. 记录入站消息 ---
            try:
                await conv_service.record_incoming_message(
                    conv_id=group_conv.entity_id,
                    conv_entity_type='group',
                    sender_id=sender_id,
                    sender_name=sender_name,  # 记录原始发送者名字
                    tg_mid=message_id,
                    body=original_content
                )
            except Exception as e:
                logger.error(f"记录外部群组 {chat_id} 的入站消息 {message_id} 失败: {e}", exc_info=True)
        else:
            logger.warning(f"外部群组 {chat_id} 的对话或话题无效，无法转发或记录消息 {message_id}。")

    else:
        # --- 消息来自既不是支持群组也不是外部群组的聊天 ---
        logger.info(f"消息来自外部群组 {chat_id}。按外部群组逻辑处理。")
        # 1. 跳过服务消息或机器人自己的消息
        is_content_message = any(msg.get(key) for key in
                                 ["text", "caption", "photo", "video", "sticker", "animation", "document", "audio",
                                  "voice", "contact", "location", "venue", "poll", "game", "invoice",
                                  "successful_payment", "passport_data"])
        if not is_content_message:
            logger.debug(f"检测到外部群组 {chat_id} 中的消息 {message_id} 可能为服务消息，跳过处理。")
            return
        if sender_id is not None and str(sender_id) == settings.BOT_TOKEN.split(':')[0]:
            logger.debug(f"检测到外部群组 {chat_id} 中的消息 {message_id} 是 Bot 自己发的，跳过处理。")
            return

        # 2. 获取群组名称
        group_name = f"群组 {chat_id}"
        try:
            chat_info = await tg("getChat", {"chat_id": chat_id})
            group_name = chat_info.get("title", group_name)
        except Exception as e:
            logger.warning(f"获取外部群组 {chat_id} 名称失败: {e}", exc_info=True)

        # 3. 获取或创建群组对话实体
        group_conv = await conv_service.get_conversation_by_entity(chat_id, 'group')

        # 4. 处理群组内的 /bind 命令 (如果需要，或者由管理员在私聊或客服话题中为群组绑定)
        if original_content and original_content.strip().lower().startswith("/bind "):
            logger.info(f"在外部群组 {chat_id} 检测到 /bind 命令。")
            args = original_content.strip().split(maxsplit=1)
            if len(args) > 1:
                custom_bind_id = args[1]
                logger.info(f"外部群组 {chat_id} ({group_name}) 尝试使用ID进行绑定: {custom_bind_id}")
                success = await conv_service.bind_entity(chat_id, 'group', group_name, custom_bind_id)
                logger.info(f"群组 {chat_id} 绑定到 {custom_bind_id} 尝试结果: {success}")
            else:
                try:
                    await tg("sendMessage", {"chat_id": chat_id, "text": "用法: /bind <群组专属自定义ID>"})
                except Exception:
                    pass
            return  # /bind 命令处理完毕

        # 5. 如果没有对话记录，或者记录中没有 topic_id，则创建初始对话和话题
        if not group_conv or not group_conv.topic_id:
            logger.info(f"外部群组 {chat_id} ({group_name}) 没有带话题的活动对话。正在创建。")
            group_conv = await conv_service.create_initial_conversation_with_topic(chat_id, 'group', group_name)
            if not group_conv or not group_conv.topic_id:  # 再次检查确保成功
                logger.error(f"为群组 {chat_id} 创建初始对话/话题失败。")
                return

            try:
                await tg("sendMessage", {
                    "chat_id": chat_id,
                    "text": (
                        f"欢迎！本群组的客服协助通道已创建。\n"
                        f"为了将本群组消息正确路由给客服，请群管理员使用 /bind <群组专属自定义ID> 命令完成绑定。\n"
                        f"在绑定前，本群组最多可以发送 {MESSAGE_LIMIT_BEFORE_BIND} 条消息给客服系统。"
                    )
                })
            except Exception:
                pass
            # 继续处理当前消息

        # 6. 处理未验证对话的消息限制和状态
        elif group_conv.is_verified != 'verified':  # 对话存在，有话题，但未验证
            logger.info(f"外部群组 {chat_id} (话题 {group_conv.topic_id}) 的对话待验证。")
            new_count, limit_reached = await conv_service.increment_message_count_and_check_limit(group_conv.entity_id,
                                                                                                  group_conv.entity_type)
            if limit_reached:
                logger.warning(f"外部群组 {chat_id} (话题 {group_conv.topic_id}) 未验证对话达到消息限制。正在关闭。")
                await conv_service.close_conversation(group_conv.topic_id, group_conv.entity_id, group_conv.entity_type)
                try:
                    await tg("sendMessage", {"chat_id": chat_id,
                                             "text": f"本群组的未验证客服对话已达到消息限制 ({MESSAGE_LIMIT_BEFORE_BIND}条)，对话已关闭。请管理员先完成绑定：/bind <群组专属自定义ID>"})
                except Exception:
                    pass
                return
            else:
                if not (original_content and original_content.strip().startswith("/")):
                    try:
                        await tg("sendMessage", {"chat_id": chat_id,
                                                 "text": f"本群组的客服对话仍需绑定。请管理员发送 /bind <群组专属自定义ID>。 ({new_count}/{MESSAGE_LIMIT_BEFORE_BIND} 条消息)"})
                    except Exception:
                        pass
                # 继续处理消息

        elif group_conv.status == "closed":  # 对话已关闭
            logger.info(
                f"GROUP_HANDLER: 群组 {chat_id} 对话已关闭。当前 group_conv: entity_id={group_conv.entity_id if group_conv else 'N/A'}, type={group_conv.entity_type if group_conv else 'N/A'}, topic_id={group_conv.topic_id if group_conv else 'N/A'}, status={group_conv.status if group_conv else 'N/A'}, verified={group_conv.is_verified if group_conv else 'N/A'}")  # 调试日志
            # 检查消息是否为命令，如果不是命令，则尝试重新打开
            if not (original_content and original_content.strip().startswith("/")):
                logger.info(f"GROUP_HANDLER: 群组 {chat_id} 发送非命令消息到已关闭对话。尝试重新开启。")
                try:
                    # 调用 service 的 reopen_conversation 方法
                    # 需要传递正确的 topic_id，这个 topic_id 应该是 group_conv 中存储的关闭前的客服话题 ID
                    if group_conv and group_conv.topic_id:  # 确保 group_conv 和 topic_id 有效
                        await conv_service.reopen_conversation(group_conv.entity_id, group_conv.entity_type,
                                                               group_conv.topic_id)
                        # **关键：在 handler 层面也更新 group_conv 的状态**
                        # 这样后续的转发逻辑才能正确判断对话已开启
                        group_conv.status = "open"
                        logger.info(
                            f"GROUP_HANDLER: 群组 {chat_id} 对话已调用 reopen_conversation 并本地更新状态为 'open'. 新 group_conv.status: {group_conv.status}")
                        # 重新开启成功后，当前这条消息应该被继续处理并转发
                    else:
                        logger.error(
                            f"GROUP_HANDLER: 群组 {chat_id} 对话已关闭，但无法获取有效的 topic_id 来重新开启。group_conv: {group_conv}")
                        # 也许通知群组或管理员
                        return  # 无法重新开启，则不继续

                except Exception as e:
                    logger.error(f"GROUP_HANDLER: 为群组 {chat_id} 重新开启对话失败: {e}", exc_info=True)
                    try:
                        await tg("sendMessage", {"chat_id": chat_id, "text": "无法重新开启客服对话，请稍后再试。"})
                    except Exception:
                        pass
                    return  # 重新开启失败，则不继续处理当前消息
                # 如果重新开启成功，代码会继续往下执行到转发逻辑
            else:  # 如果是命令
                logger.debug(f"GROUP_HANDLER: 群组 {chat_id} 已关闭对话中的命令 '{original_content}'。忽略。")
                return  # 不重新开启，也不转发命令

            # 7. 转发消息到客服话题并记录
            # 确保这里能正确判断 group_conv.status == "open"
        logger.info(
            f"GROUP_HANDLER: 准备转发前检查群组 {chat_id}。group_conv.topic_id: {group_conv.topic_id if group_conv else 'N/A'}, group_conv.status: {group_conv.status if group_conv else 'N/A'}")
        if group_conv and group_conv.topic_id and group_conv.status == "open":
            # ... (原有的 send_with_prefix 和 record_incoming_message 逻辑)
            group_name_for_prefix = group_name or f"群组 {chat_id}"  # group_name 在前面已获取
            sender_name_for_prefix = sender_name or f"用户 {sender_id}"
            try:
                await send_with_prefix(
                    source_chat_id=chat_id,
                    dest_chat_id=settings.SUPPORT_GROUP_ID,
                    message_thread_id=group_conv.topic_id,
                    sender_name=f"🏠{group_name_for_prefix} | 👤{sender_name_for_prefix}",
                    msg=msg
                )
                logger.info(
                    f"GROUP_HANDLER: 成功复制外部群组 {chat_id} 的消息 {message_id} 到话题 {group_conv.topic_id}")
            except Exception as e:
                logger.error(
                    f"GROUP_HANDLER: 复制外部群组 {chat_id} 的消息 {message_id} 到话题 {group_conv.topic_id} 失败: {e}",
                    exc_info=True)
                # ... (错误通知) ...

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
                logger.error(f"GROUP_HANDLER: 记录外部群组 {chat_id} 的入站消息 {message_id} 失败: {e}", exc_info=True)
        else:
            logger.warning(
                f"GROUP_HANDLER: 外部群组 {chat_id} 的对话状态不允许转发。topic_id: {group_conv.topic_id if group_conv else 'N/A'}, status: {group_conv.status if group_conv else 'N/A'}")