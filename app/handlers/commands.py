import logging
# 导入其他模块所需组件
from ..settings import settings  # 访问设置以获取 support_group_id 等
from ..tg_utils import tg  # Telegram API 工具
from ..services.conversation_service import ConversationService  # 导入服务层

logger = logging.getLogger(__name__)

# --- 定义需要管理员权限的命令 ---
PRIVILEGED_COMMANDS = {"/ban", "/close", "/unban","/set_password","/setlang"}  # 您可以按需调整


async def handle_commands(tid: int, admin_sender_id: int | str, text: str, conv_service: ConversationService):
    """
    处理管理员在客服支持群组话题中发送的命令。

    Args:
        tid: 发送命令的消息线程 ID (话题 ID).
        admin_sender_id: 发送命令的管理员的 Telegram 用户 ID.
        text: 完整的命令文本 (包括开头的 '/').
        conv_service: 用于业务逻辑的 ConversationService 实例.
    """
    cmd, *args = text.split()
    cmd = cmd.lower()  # 命令不区分大小写
    parts = text.strip().split(maxsplit=2)  # /cmd, arg1, arg2_and_onwards

    logger.info(f"管理员 {admin_sender_id} 在话题 {tid} 执行命令: '{text}'")

    arg1 = None
    arg2 = None  # 对于 /set_password，arg2 是密码 (可能为空或包含空格)

    if len(parts) > 1:
        arg1 = parts[1]
    if len(parts) > 2:
        arg2 = parts[2]  # 密码部分，保留原始大小写和空格

    # --- 权限检查 ---
    # 将 admin_sender_id 转为 int 类型进行比较
    try:
        sender_id_int = int(admin_sender_id)
    except ValueError:
        logger.warning(f"无效的管理员发送者ID格式: {admin_sender_id} (来自话题 {tid})。拒绝执行命令 '{text}'。")
        # 可以在话题中回复一个通用错误，但不建议暴露过多细节
        # await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid, "text": "命令执行失败：内部错误。"})
        return

    if cmd in PRIVILEGED_COMMANDS:  # 检查是否是需要权限的命令
        if sender_id_int not in settings.ADMIN_USER_IDS:
            logger.warning(f"用户 {sender_id_int} (非管理员) 尝试在话题 {tid} 执行特权命令 '{text}'。已拒绝。")
            try:
                await tg("sendMessage", {
                    "chat_id": settings.SUPPORT_GROUP_ID,
                    "message_thread_id": tid,
                    "text": f"抱歉，您没有权限执行 {cmd} 命令。"
                })
            except Exception as e:
                logger.error(f"发送权限不足通知到话题 {tid} 失败: {e}")
            return  # 如果没有权限，直接返回

    logger.info(f"权限检查通过: 管理员 {sender_id_int} 在话题 {tid} 执行命令: '{text}'")

    # --- 获取话题关联的实体和实体 ID 的辅助逻辑 ---
    # 大部分命令需要话题关联的实体和实体 ID。
    # /unban 在没有参数时需要话题关联的用户实体。
    # /tag 命令已被移除
    conv = None
    entity_id_in_topic = None
    entity_type_in_topic = None

    # 需要话题关联对话记录的命令列表
    commands_needing_conv_lookup = ("/close", "/ban", "/setlang")  # 移除了 /tag

    # 特殊处理 /unban，它可能需要参数，或者作用于当前话题关联的用户实体
    # 如果是 /unban 且没有参数，则需要当前话题关联的用户实体
    if cmd == "/unban" and not args:
        commands_needing_conv_lookup = ("/unban",)  # 临时将 /unban 加入需要对话的列表，仅当无参数时

    elif cmd in ("/close", "/ban", "/setlang"):  # 移除了 /tag
        pass  # 这些命令始终需要话题关联的对话记录

    # 注意：用户在私聊或外部群组中发送的 /start 命令会在 private/group handler 中处理，不经过这里。

    # 如果命令需要话题关联的对话记录，则进行查找
    if cmd in commands_needing_conv_lookup:
        try:
            conv = await conv_service.get_conversation_by_topic(tid)
            if not conv:
                logger.warning(f"在话题 {tid} 中收到命令 '{text}'，但未找到关联对话。管理员: {admin_sender_id}")
                # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
                await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                         "text": "错误：此话题未关联对话实体，无法执行此命令。"})
                return  # 如果查找对话失败，停止处理
            entity_id_in_topic = conv.entity_id
            entity_type_in_topic = conv.entity_type
            logger.debug(f"命令目标实体: 类型 {entity_type_in_topic} ID {entity_id_in_topic} (来自话题 {tid})")
            logger.info(f"成功检索到话题 {tid} 的对话实体: 类型 {entity_type_in_topic} ID {entity_id_in_topic}")

        except Exception as e:  # 捕获从 service/DB 查询中可能发生的错误
            logger.error(f"处理命令 '{text}' 时，检索话题 {tid} 对应对话实体失败: {e}", exc_info=True)
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_thread_id": tid,
                                     "text": "处理命令失败：无法获取对话实体信息。"})
            return

    # --- 命令处理逻辑 ---

    # --- 命令处理逻辑 ---
    if cmd == "/set_password":
        custom_id_to_set = arg1
        new_password_to_set = arg2  # 可能是 None (如果只提供了ID)，也可能是空字符串或实际密码

        if not custom_id_to_set:
            await tg("sendMessage", {
                "chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                "text": "用法错误。\n设置密码: `/set_password <自定义ID> <新密码>`\n清除密码: `/set_password <自定义ID>` (密码部分留空)"
            })
            return

        logger.info(
            f"COMMANDS: 管理员 {sender_id_int} 尝试为ID '{custom_id_to_set}' 设置密码。提供的密码: '{'******' if new_password_to_set else '将清除密码'}'")

        success, message = await conv_service.set_binding_id_password(custom_id_to_set, new_password_to_set)

        reply_text = f"为自定义ID '{custom_id_to_set}' 操作密码结果：\n{message}"
        if not success:
            reply_text = f"❗ 操作失败：\n{message}"

        await tg("sendMessage", {
            "chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
            "text": reply_text
        })
        return  # /set_password 命令处理完毕

    if cmd == "/close":
        # conv, entity_id_in_topic, entity_type_in_topic 在上面已检查并保证存在
        try:
            # Service handles DB update and notifies entity/updates topic name
            await conv_service.close_conversation(tid, entity_id_in_topic, entity_type_in_topic)
            # Service notifies entity and updates topic name. Just confirm to admin.
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                     "text": f"对话已标记为关闭。关联实体: {entity_type_in_topic} ID {entity_id_in_topic}"})
            logger.info(
                f"话题 {tid} (实体 {entity_type_in_topic} ID {entity_id_in_topic}) 由管理员 {admin_sender_id} 关闭")
        except Exception as e:
            logger.error(f"执行 /close 命令失败，话题 {tid}: {e}", exc_info=True)
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage",
                     {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid, "text": "关闭话题失败。"})


    elif cmd == "/ban":
        # conv, entity_id_in_topic, entity_type_in_topic 在上面已检查并保证存在
        # /ban 命令仅适用于用户实体
        if entity_type_in_topic != 'user':
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                     "text": f"错误：/ban 命令仅适用于用户对话，此话题关联实体类型为 {entity_type_in_topic} ID {entity_id_in_topic}。"})
            logger.warning(
                f"管理员 {admin_sender_id} 在话题 {tid} 中对非用户实体使用 /ban 命令 (类型: {entity_type_in_topic} ID: {entity_id_in_topic}).")
            return

        try:
            # Service handles DB update and notifies user
            await conv_service.ban_user(entity_id_in_topic)  # ban_user 方法仍然只接受 user_id
            # Service notifies user. Just confirm to admin.
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                     "text": f"用户 {entity_id_in_topic} 已被拉黑。"})
            logger.info(f"用户 {entity_id_in_topic} 在话题 {tid} 中由管理员 {admin_sender_id} 拉黑")
        except Exception as e:
            logger.error(f"执行 /ban 命令失败，用户 {entity_id_in_topic}: {e}", exc_info=True)
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage",
                     {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid, "text": "拉黑用户失败。"})


    elif cmd == "/unban":
        # 新逻辑: 如果有参数，解除指定 ID 拉黑；如果没有参数，解除当前话题关联用户实体拉黑。
        user_id_to_unban = None
        if args:
            # 从参数获取用户 ID
            try:
                user_id_to_unban = int(args[0])
                logger.debug(f"/unban 命令指定用户 ID: {user_id_to_unban}")
            except ValueError:
                logger.warning(f"管理员 {admin_sender_id} 在话题 {tid} 中使用 /unban 提供了无效 ID: '{args[0]}'")
                # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
                await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                         "text": "无效的用户 ID。用法: /unban <用户ID> 或在用户对话话题中直接使用 /unban 解除当前用户拉黑。"})
                return  # 无效参数，停止处理
        else:
            # 没有参数，使用当前话题关联的用户实体 ID
            # conv, entity_id_in_topic, entity_type_in_topic 在上面检查 commands_needing_conv_lookup 时已经获取
            if entity_type_in_topic != 'user':
                # 如果没有参数，但话题关联的不是用户实体，提示错误
                # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
                await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                         "text": f"错误：/unban 命令仅适用于用户对话。此话题关联实体类型为 {entity_type_in_topic}。用法: /unban <用户ID> 或在用户对话话题中直接使用 /unban。"})
                logger.warning(
                    f"管理员 {admin_sender_id} 在话题 {tid} 中对非用户实体使用无参数 /unban (类型: {entity_type_in_topic}).")
                return
            user_id_to_unban = entity_id_in_topic
            logger.debug(f"/unban 命令无参数，使用当前话题关联用户实体 ID: {user_id_to_unban}")

        # 执行解除拉黑逻辑 (user_id_to_unban 现在确定有值，并且是用户 ID，除非上面返回了)
        if user_id_to_unban is not None:
            try:
                # Service handles DB update and user notification
                await conv_service.unban_user(user_id_to_unban)  # unban_user 方法仍然只接受 user_id
                # Service notifies user. Just confirm to admin.
                # 管理员反馈
                # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
                await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                         "text": f"用户 {user_id_to_unban} 已被解除拉黑。"})
                logger.info(f"用户 {user_id_to_unban} 由管理员 {admin_sender_id} 解除拉黑 (在话题 {tid} 执行).")
                # 用户通知在 service.unban_user 中完成
            except Exception as e:
                logger.error(f"执行 /unban 命令失败，用户 {user_id_to_unban}: {e}", exc_info=True)
                # 管理员反馈
                # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
                await tg("sendMessage",
                         {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid, "text": "解除拉黑失败。"})


    # --- /tag 命令已移除 ---
    # elif cmd == "/tag":
    #     pass # 此分支已被移除

    elif cmd == "/setlang":
        # conv, entity_id_in_topic, entity_type_in_topic 在上面已检查并保证存在
        # /setlang 命令仅适用于用户实体
        if entity_type_in_topic != 'user':
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                     "text": f"错误：/setlang 命令仅适用于用户对话，此话题关联实体类型为 {entity_type_in_topic} ID {entity_id_in_topic}。"})
            logger.warning(
                f"管理员 {admin_sender_id} 在话题 {tid} 中对非用户实体使用 /setlang 命令 (类型: {entity_type_in_topic} ID: {entity_id_in_topic}).")
            return

        if not args:
            # 管理员反馈：没有参数
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                     "text": "用法: /setlang <语言码> (例如: en, zh-CN, fr)"})
            logger.warning(f"管理员 {admin_sender_id} 在话题 {tid} 中使用 /setlang 未提供参数.")
            return

        try:
            new_lang = args[0].strip()[:10].lower()  # 限制长度并转小写
            # Service handles DB update and notifies user
            # set_user_language 方法需要话题 ID 和用户 ID
            await conv_service.set_user_language(tid, entity_id_in_topic, new_lang)
            # Service notifies user. Just confirm to admin.
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                     "text": f"用户 {entity_id_in_topic} 的目标语言已更新为: {new_lang}"})
            logger.info(
                f"话题 {tid} (用户 {entity_id_in_topic}) 语言设置为 '{new_lang}'，由管理员 {admin_sender_id} 执行")
        except Exception as e:
            logger.error(f"执行 /setlang 命令失败，话题 {tid}: {e}", exc_info=True)
            # 管理员反馈：异常时给出反馈
            # 修正 chat_id 参数，使用 settings.SUPPORT_GROUP_ID
            await tg("sendMessage",
                     {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid, "text": "设置语言失败。"})


    else:
        # 未知命令
        logger.warning(f"管理员 {admin_sender_id} 在话题 {tid} 中发送未知命令: '{text}' ")
        # 管理员反馈：未知命令时给出反馈 (移除了 /tag 的用法)
        await tg("sendMessage", {"chat_id": settings.SUPPORT_GROUP_ID, "message_thread_id": tid,
                                 "text": "未知命令,未发送给客户。\n可用命令: /close, /ban, /unban [<用户ID>], /setlang <语言码>"})

# END OF FILE handlers/commands.py