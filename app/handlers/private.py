import logging
# 导入所需组件
from ..settings import settings # 访问设置
from ..tg_utils import tg, copy_any # Telegram API 工具
from ..translate import translate # 翻译工具
from ..services.conversation_service import ConversationService # 导入服务层

logger = logging.getLogger(__name__)

async def handle_private(msg: dict, conv_service: ConversationService):
    """
    处理用户发来的私聊消息。

    Args:
        msg: Telegram 消息更新字典.
        conv_service: 用于业务逻辑的 ConversationService 实例.
    """
    uid = msg["from"]["id"] # 用户 ID
    user_first_name = msg["from"].get("first_name", f"用户 {uid}") # 用户名字
    message_id = msg.get("message_id") # 消息 ID
    # 获取消息内容，用于记录和翻译
    original_body = msg.get("text") or msg.get("caption")

    # 检查消息是否是 /start 命令 (仅检查文本消息)
    is_start_command = msg.get("text", "").strip().lower() == "/start"

    logger.info(f"处理来自用户 {uid} 的私聊消息 {message_id}")
    if is_start_command:
        logger.info(f"用户 {uid} 发送了 /start 命令.")

    # --- 1. 检查拉黑状态 ---
    try:
        logger.debug(f"检查用户 {uid} 拉黑状态...")
        is_banned = await conv_service.is_user_banned(uid)
        logger.debug(f"用户 {uid} 拉黑状态结果: {is_banned}")
        if is_banned:
            logger.info(f"用户 {uid} 被拉黑，停止处理.")
            # 可选通知用户 (服务层在拉黑时可能已通知，此处作为防御性措施)
            try:
                 await tg("sendMessage", {"chat_id": uid, "text": "您当前无法发起新的对话，如有疑问请联系管理员。"})
            except Exception as e:
                 logger.warning(f"发送拉黑通知给用户 {uid} 失败: {e}")
            return # 如果用户被拉黑，这里就直接返回了
        logger.debug(f"用户 {uid} 未被拉黑.")
    except Exception as e: # 捕获拉黑检查本身可能发生的错误
         logger.error(f"检查用户 {uid} 拉黑状态失败: {e}", exc_info=True)
         # 如果拉黑检查失败 (例如 DB 错误)，为了安全起见，此处停止处理此消息。
         try:
              await tg("sendMessage", {"chat_id": uid, "text": "服务器错误，请稍后再试。"})
         except Exception as e_notify:
              logger.warning(f"发送服务器错误消息给用户 {uid} 失败: {e_notify}")
         return


    # --- 2. 获取对话记录 ---
    logger.debug(f"尝试获取用户 {uid} 的对话记录...")
    conv = None
    try:
        conv = await conv_service.get_conversation_by_user(uid)
        logger.debug(f"获取用户 {uid} 对话记录结果: {conv}")
    except Exception as e: # 捕获获取对话可能发生的错误
        logger.error(f"获取用户 {uid} 对话记录失败: {e}", exc_info=True)
        try:
             await tg("sendMessage", {"chat_id": uid, "text": "服务器错误，请稍后再试。"})
        except Exception as e_notify:
             logger.warning(f"发送服务器错误消息给用户 {uid} 失败: {e_notify}")
        return # 获取对话失败，停止处理


    # --- 3. 处理对话状态和创建/重新开启对话 ---

    # 如果没有找到对话记录 (conv is None)，则创建新的对话
    if not conv:
        logger.info(f"用户 {uid} 没有进行中的对话记录。正在创建新的对话。")
        # 保持当前逻辑：用户发送任何消息，如果没有对话，就创建新的。
        # 这样更符合客服 Bot 的直觉。
        try:
             # 服务层处理创建新的话题和对话记录，并通知用户
             conv = await conv_service.create_first_conversation(uid, user_first_name) # create_first_conversation 现在会保存用户名字和更新话题名称
             logger.info(f"成功为用户 {uid} 创建新对话，话题 {conv.topic_id}") # 如果创建成功，打印此日志
             # 代码将继续执行到下面的翻译和复制部分。

        except Exception as e: # 捕获服务层创建对话可能发生的错误 (服务层已通知用户)
             logger.error(f"为用户 {uid} 创建新对话失败 (在创建块内): {e}", exc_info=True) # 添加标记说明错误位置
             return # 创建对话失败，停止处理

    # 如果找到了对话记录 (conv 不是 None)，则根据状态处理
    elif conv.status == "closed": # 如果对话状态是 "closed"
        logger.info(f"收到用户 {uid} 发送的消息 {message_id}，其对话 (话题 {conv.topic_id}) 已关闭。")

        # --- 问题 1: 用户发送任意消息时重新开启 ---
        # 如果用户有 closed 状态的对话记录，无论发 /start 还是其他消息，都尝试重新开启。
        logger.info(f"用户 {uid} 对话已关闭，尝试重新开启对话 {conv.topic_id}.")
        try:
            # 重新开启对话 (状态设为 open)，并更新话题名称
            # 调用服务方法重新开启
            await conv_service.reopen_conversation(conv.user_id, conv.topic_id) # reopen_conversation 现在会在 service 中通知用户和更新话题名称

            # conv 对象此时可能还未更新状态，但我们知道服务层正在处理更新。
            # 在当前 handler 实例中更新状态标记，用于后续逻辑判断
            conv.status = "open"

            # 用户通知在 service.reopen_conversation 中完成。

            # --- 修改点 ---
            # 如果希望用户在收到通知后必须再发一条消息才会被转发，
            # 在这里重新开启成功后，直接返回，不再继续处理当前消息。
            logger.info(f"对话已重新开启，通知用户再次发送消息。停止处理当前消息 {message_id}.")
            return # <--- 重新开启成功后，不再转发当前消息，而是返回

        except Exception as e: # 捕获重新开启对话可能发生的错误
            logger.error(f"重新开启用户 {uid} 对话 (话题 {conv.topic_id}) 失败: {e}", exc_info=True)
            # 如果重新开启失败，通知用户 (服务层已尝试，这里作为二次确认或补充)
            try:
                 await tg("sendMessage", {"chat_id": uid, "text": "无法重新开启对话，请稍后再试。"})
            except Exception as e_notify:
                 logger.warning(f"发送'重新开启失败'消息给用户 {uid} 失败: {e_notify}")
            return # 重新开启失败，停止处理当前消息

    # 如果对话状态是 "open" 或 "pending" (conv 存在且状态不是 closed)
    # 且用户发送的是 /start 命令 (可选处理)
    # 如果你不想转发 /start 消息本身，可以在这里添加 return
    elif is_start_command: # 注意这里是 elif，它与 if not conv 和 elif conv.status == "closed" 对齐
         try:
              await tg("sendMessage", {"chat_id": uid, "text": "您的对话已经在进行中。请直接发送您的问题。"})
         except Exception as e:
              logger.warning(f"发送'对话进行中'消息给用户 {uid} 失败: {e}")
         # 如果是 /start 命令且对话已 open/pending，我们通常不转发 /start 本身作为对话内容
         logger.debug(f"用户 {uid} 发送 /start 且对话已存在 open/pending 状态。跳过消息转发。")
         return # <--- 发送了 /start 命令且对话已 open/pending，不再转发当前消息


    # 如果代码执行到这里，conv 应该是一个有效的 Conversation 对象，状态为 "open" 或 "pending"。
    # 这是用户发送了一条**非 /start 命令**，且对话处于 open/pending 状态的情况。
    # 此时，conv 对象都已经被正确获取或创建/更新。
    # 翻译和复制逻辑应该紧随其后。

    logger.debug(f"准备为用户 {uid} 处理翻译和复制消息 (话题 {conv.topic_id})")


    # --- 4. 机器翻译 (用户 -> 管理员) ---
    # 尝试将用户消息的文本或 caption 翻译成管理员的目标语言 (中文)
    # original_body 在函数开头已获取
    admin_target_lang = settings.ADMIN_LANG_FOR_USER_MSG
    msg_to_copy = msg.copy() # 创建消息字典的副本进行修改

    # 仅在有内容且内容长度大于某个阈值，并且不像命令或特殊标记时进行翻译
    # 不再依赖 conv.lang 进行跳过判断
    if original_body and len(original_body) > 5 and not original_body.strip().startswith(('/', '[', '【', '（')):
         logger.debug(f"正在为用户 {uid} 的消息 {message_id} (内容: '{original_body[:50]}...') 尝试翻译到管理员语言 '{admin_target_lang}'")
         try:
             cn_translation = await translate(original_body, admin_target_lang)
             if cn_translation and cn_translation.strip() != original_body.strip(): # 避免翻译结果与原始内容完全相同 (可能表示翻译失败或内容是目标语言)
                 translation_text = f"\n———\n💬机翻: {cn_translation}"
                 # 将翻译结果添加到 text 或 caption 中，取决于原始内容在哪里
                 if "text" in msg_to_copy:
                     msg_to_copy["text"] = msg_to_copy.get("text", "") + translation_text
                 elif "caption" in msg_to_copy:
                      # 确保 caption 字段存在
                     msg_to_copy["caption"] = msg_to_copy.get("caption", "") + translation_text
                 logger.debug(f"成功将用户 {uid} 的消息 {message_id} 翻译成了 {admin_target_lang}")
             else:
                  logger.debug(f"用户 {uid} 的消息 {message_id} 翻译到 {admin_target_lang} 结果为空或与原文相同，跳过添加翻译注释。")
         except Exception as e:
             logger.warning(f"用户 {uid} 的消息 {message_id} 翻译失败 (用户 -> 管理员) 到 {admin_target_lang}: {e}", exc_info=True)
             translation_text = "\n———\n💬机翻失败"
             if "text" in msg_to_copy:
                 msg_to_copy["text"] = msg_to_copy.get("text", "") + translation_text
             elif "caption" in msg_to_copy:
                 msg_to_copy["caption"] = msg_to_copy.get("caption", "") + translation_text


    # --- 5. 复制消息到群组话题 ---
    # 这个 try 块应该在确保 conv 有效后执行
    try:
        # 将修改后的 msg_to_copy 的 text 和 caption 显式传递给 copy_any
        await copy_any(uid, settings.GROUP_ID, message_id,
                       {"message_thread_id": conv.topic_id,
                        "text": msg_to_copy.get("text"),
                        "caption": msg_to_copy.get("caption")
                       })
        logger.info(f"成功复制用户 {uid} 的消息 {message_id} 到话题 {conv.topic_id}")
    except Exception as e:
        logger.error(f"复制用户 {uid} 的消息 {message_id} 到话题 {conv.topic_id} 失败: {e}", exc_info=True)
        # 通知用户他们的消息未能发送到支持频道
        try:
             await tg("sendMessage", {"chat_id": uid, "text": "消息发送失败，请稍后再试。"})
        except Exception as e_notify:
             logger.warning(f"发送'消息发送失败'通知给用户 {uid} 失败: {e_notify}")

    # --- 6. 记录入站消息 ---
    # 消息转发 (尝试) 成功后，将原始消息内容记录到数据库
    # 即使转发失败，我们通常也想记录用户尝试发送了什么消息
    # 这个 if conv: 检查是多余的，因为代码到这里 conv 应该总是有效，但留着也无害
    if conv:
        try:
            await conv_service.record_incoming_message(conv_id=conv.user_id, tg_mid=message_id, body=original_body)
        except Exception as e: # 记录消息失败是一个非关键错误，只需日志记录
            logger.error(f"记录用户 {uid} 的入站消息 {message_id} 失败: {e}", exc_info=True)


# END OF FILE handlers/private.py