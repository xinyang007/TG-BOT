import logging
# 导入其他模块所需组件
from ..settings import settings # 访问设置以获取 group_id 等
from ..tg_utils import tg # Telegram API 工具
from ..services.conversation_service import ConversationService # 导入服务层

logger = logging.getLogger(__name__)

async def handle_commands(tid: int, admin_sender_id: int, text: str, conv_service: ConversationService):
    """
    处理管理员在群组话题中发送的命令。

    Args:
        tid: 发送命令的消息线程 ID (话题 ID).
        admin_sender_id: 发送命令的管理员的 Telegram 用户 ID.
        text: 完整的命令文本 (包括开头的 '/').
        conv_service: 用于业务逻辑的 ConversationService 实例.
    """
    cmd, *args = text.split()
    cmd = cmd.lower() # 命令不区分大小写

    logger.info(f"管理员 {admin_sender_id} 在话题 {tid} 执行命令: '{text}'")

    # --- 获取话题关联用户的辅助逻辑 ---
    # 大部分命令需要话题关联的用户 ID。
    # 如果命令需要话题关联的用户，在此处尝试获取。
    # /start 是给用户在私聊中使用的，理论上不在这里处理。
    # /unban 在没有参数时需要话题关联用户。
    # /tag 命令已被移除
    conv = None
    user_id_in_topic = None
    # 需要话题关联对话记录的命令列表 (移除了 /tag)
    commands_needing_conv_lookup = ("/close", "/ban", "/setlang")

    # 特殊处理 /unban，它可能需要参数，或者作用于当前话题用户 (新的默认行为)
    if cmd == "/unban" and not args:
        # 如果是 /unban 且没有参数，则需要当前话题关联的用户 ID
        commands_needing_conv_lookup = ("/unban",) # 临时将 /unban 加入需要对话的列表，仅当无参数时
    elif cmd in ("/close", "/ban", "/setlang"): # 移除了 /tag
        pass # 这些命令始终需要话题关联的用户，保持 commands_needing_conv_lookup 不变
    # 注意：用户在私聊中发送的 /start 命令会在 private handler 中处理，不经过这里。

    if cmd in commands_needing_conv_lookup:
         try:
             conv = await conv_service.get_conversation_by_topic(tid)
             if not conv:
                  logger.warning(f"在话题 {tid} 中收到命令 '{text}'，但未找到关联对话。管理员: {admin_sender_id}")
                  # 修正 chat_id 参数，使用 settings.GROUP_ID
                  await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "错误：此话题未关联用户对话，无法执行此命令。"})
                  return # 如果查找对话失败，停止处理
             user_id_in_topic = conv.user_id
             logger.debug(f"命令目标用户 ID (从话题 {tid}): {user_id_in_topic}")
             logger.info(f"成功检索到话题 {tid} 的对话: 用户 ID {user_id_in_topic}")

         except Exception as e: # 捕获从 service/DB 查询中可能发生的错误
             logger.error(f"处理命令 '{text}' 时，检索话题 {tid} 对话失败: {e}", exc_info=True)
             # 修正 chat_id 参数，使用 settings.GROUP_ID
             await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_thread_id": tid, "text": "处理命令失败：无法获取对话信息。"})
             return


    # --- 命令处理逻辑 ---

    if cmd == "/close":
        # user_id_in_topic 在上面已经检查并保证存在
        try:
            # Service handles DB update and notifies user/updates topic name
            await conv_service.close_conversation(tid, user_id_in_topic)
            # Service notifies user and updates topic name. Just confirm to admin.
            # 修正 chat_id 参数，使用 settings.GROUP_ID
            await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": f"对话已标记为关闭。关联用户: {user_id_in_topic}"})
            logger.info(f"话题 {tid} (用户 {user_id_in_topic}) 由管理员 {admin_sender_id} 关闭")
        except Exception as e:
             logger.error(f"执行 /close 命令失败，话题 {tid}: {e}", exc_info=True)
             # 修正 chat_id 参数，使用 settings.GROUP_ID
             await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "关闭话题失败。"})


    elif cmd == "/ban":
        # user_id_in_topic 在上面已经检查并保证存在
        try:
            # Service handles DB update and notifies user
            await conv_service.ban_user(user_id_in_topic)
            # Service notifies user. Just confirm to admin.
            # 修正 chat_id 参数，使用 settings.GROUP_ID
            await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": f"用户 {user_id_in_topic} 已被拉黑。"})
            logger.info(f"用户 {user_id_in_topic} 在话题 {tid} 中由管理员 {admin_sender_id} 拉黑")
        except Exception as e:
             logger.error(f"执行 /ban 命令失败，用户 {user_id_in_topic}: {e}", exc_info=True)
             # 修正 chat_id 参数，使用 settings.GROUP_ID
             await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "拉黑用户失败。"})


    elif cmd == "/unban":
        # 新逻辑: 如果有参数，解除指定 ID 拉黑；如果没有参数，解除当前话题关联用户拉黑。
        user_id_to_unban = None
        if args:
            # 从参数获取用户 ID
            try:
                user_id_to_unban = int(args[0])
                logger.debug(f"/unban 命令指定用户 ID: {user_id_to_unban}")
            except ValueError:
                 logger.warning(f"管理员 {admin_sender_id} 在话题 {tid} 中使用 /unban 提供了无效 ID: '{args[0]}'")
                 # 修正 chat_id 参数，使用 settings.GROUP_ID
                 await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "无效的用户 ID。用法: /unban <用户ID> 或在话题中直接使用 /unban 解除当前用户拉黑。"})
                 return # 无效参数，停止处理
        else:
            # 没有参数，使用当前话题关联的用户 ID
            # user_id_in_topic 在上面检查 commands_needing_conv_lookup 时已经获取（因为无参数时 /unban 会被加入检查列表）
            if user_id_in_topic is None:
                 # 如果没有参数，但话题也没有关联用户，提示错误
                 # 修正 chat_id 参数，使用 settings.GROUP_ID
                 await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "错误：此话题未关联用户对话，且未提供用户 ID 参数。无法执行解除拉黑。用法: /unban <用户ID> 或在话题中直接使用 /unban。"})
                 return
            user_id_to_unban = user_id_in_topic
            logger.debug(f"/unban 命令无参数，使用当前话题关联用户 ID: {user_id_to_unban}")


        # 执行解除拉黑逻辑 (user_id_to_unban 现在确定有值，除非上面返回了)
        if user_id_to_unban is not None:
            try:
                # Service handles DB update and user notification
                success = await conv_service.unban_user(user_id_to_unban)
                if success:
                     # 管理员反馈
                     # 修正 chat_id 参数，使用 settings.GROUP_ID
                     await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": f"用户 {user_id_to_unban} 已被解除拉黑。"})
                     logger.info(f"用户 {user_id_to_unban} 由管理员 {admin_sender_id} 解除拉黑 (在话题 {tid} 执行).")
                     # 用户通知在 service.unban_user 中完成
                else:
                     # 管理员反馈
                     # 修正 chat_id 参数，使用 settings.GROUP_ID
                     await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": f"未找到用户 {user_id_to_unban} 在拉黑列表中。"})
                     logger.info(f"解除拉黑命令失败: 用户 {user_id_to_unban} 未在拉黑列表中 (由管理员 {admin_sender_id} 在话题 {tid} 执行).")
            except Exception as e:
                 logger.error(f"执行 /unban 命令失败，用户 {user_id_to_unban}: {e}", exc_info=True)
                 # 管理员反馈
                 # 修正 chat_id 参数，使用 settings.GROUP_ID
                 await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "解除拉黑失败。"})


    # --- /tag 命令已移除 ---
    # elif cmd == "/tag":
    #     pass # 此分支已被移除


    elif cmd == "/setlang":
        # user_id_in_topic 在上面已经检查并保证存在
        if not args:
             # 管理员反馈：没有参数
             # 修正 chat_id 参数，使用 settings.GROUP_ID
             await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "用法: /setlang <语言码> (例如: en, zh-CN, fr)"})
             logger.warning(f"管理员 {admin_sender_id} 在话题 {tid} 中使用 /setlang 未提供参数.")
             return

        try:
            new_lang = args[0].strip()[:10].lower() # 限制长度并转小写
            # 服务层处理 DB 更新并通知用户
            await conv_service.set_user_language(tid, user_id_in_topic, new_lang)
            # 服务层已通知用户。在此处向管理员确认。
            # 修正 chat_id 参数，使用 settings.GROUP_ID
            await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": f"用户 {user_id_in_topic} 的目标语言已更新为: {new_lang}"})
            logger.info(f"话题 {tid} (用户 {user_id_in_topic}) 语言设置为 '{new_lang}'，由管理员 {admin_sender_id} 执行")
        except Exception as e:
             logger.error(f"执行 /setlang 命令失败，话题 {tid}: {e}", exc_info=True)
             # 管理员反馈：异常时给出反馈
             # 修正 chat_id 参数，使用 settings.GROUP_ID
             await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "设置语言失败。"})


    else:
         # 未知命令
         logger.warning(f"管理员 {admin_sender_id} 在话题 {tid} 中发送未知命令: '{text}'")
         # 管理员反馈：未知命令时给出反馈 (移除了 /tag 的用法)
         await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "未知命令。\n可用命令: /close, /ban, /unban [<用户ID>], /setlang <语言码>"})