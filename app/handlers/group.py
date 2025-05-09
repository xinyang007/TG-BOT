import logging
# 导入所需组件
from ..settings import settings # 访问设置
from ..tg_utils import tg, copy_any # Telegram API 工具
from ..translate import translate # 翻译工具
from ..services.conversation_service import ConversationService # 导入服务层
from .commands import handle_commands # 导入命令处理器

logger = logging.getLogger(__name__)

async def handle_group(msg: dict, conv_service: ConversationService):
    """
    处理支持群组聊天中的入站消息。
    包括命令处理和将管理员回复转发给用户。

    Args:
        msg: Telegram 消息更新字典.
        conv_service: 用于业务逻辑的 ConversationService 实例.
    """
    tid = msg.get("message_thread_id") # 话题 ID
    # 仅处理话题线程内的消息
    if not tid:
        logger.debug(f"忽略聊天 {msg.get('chat',{}).get('id')} 中非话题线程的消息 {msg.get('message_id')}.")
        return

    message_id = msg.get("message_id") # 消息 ID
    admin_sender = msg.get("from") # 发送消息/命令的管理员对象
    admin_sender_id = admin_sender.get("id") if admin_sender else "N/A" # 发送者 ID
    admin_sender_name = admin_sender.get("first_name", "未知管理员") if admin_sender else "未知管理员" # 发送者名字
    original_content = msg.get("text") or msg.get("caption") # 要翻译的内容，不含管理员前缀

    logger.info(f"处理来自管理员 {admin_sender_id} ({admin_sender_name}) 在话题 {tid} 中的群组消息 {message_id}")

    # --- 检查是否为服务消息 ---
    # 服务消息通常没有 text, caption, photo, video 等字段
    # 可以通过检查这些内容字段是否存在来判断
    if not (msg.get("text") or msg.get("caption") or msg.get("photo") or msg.get("video") or msg.get("sticker") or msg.get("animation") or msg.get("document") or msg.get("audio") or msg.get("voice") or msg.get("contact") or msg.get("location") or msg.get("venue") or msg.get("poll") or msg.get("game") or msg.get("invoice") or msg.get("successful_payment") or msg.get("passport_data")):
         # 这可能是服务消息，例如话题创建消息
         logger.debug(f"检测到话题 {tid} 中的消息 {message_id} 可能为服务消息，跳过处理。")
         return # 跳过处理服务消息

    # --- 1. 处理命令 ---
    # 命令通常位于文本消息的开头
    # 在检查是否为服务消息之后再检查命令
    if original_content and original_content.strip().startswith("/"): # 检查原始内容是否以 / 开头
        logger.info(f"在话题 {tid} 中检测到命令: '{original_content}'")
        # handle_commands 函数会根据需要检索话题关联的用户 ID
        # 并处理命令逻辑，包括向管理员发送命令执行结果反馈
        await handle_commands(tid, admin_sender_id, original_content.strip(), conv_service)
        return # 如果是命令，停止处理消息内容


    # --- 2. 处理管理员回复 (如果不是命令或服务消息) ---
    # 查找与此话题线程关联的用户对话
    conv = None
    try:
        conv = await conv_service.get_conversation_by_topic(tid)
        if not conv:
             logger.warning(f"收到非命令/服务消息 {message_id} 在话题 {tid} 中，但未找到关联对话。忽略。")
             # 可选地在话题中通知管理员: "此话题未关联用户对话。"
             # try: await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "注意：此话题未关联用户对话，消息不会转发给用户。"}) except Exception: pass
             return # 忽略与任何用户对话不关联的话题中的消息

        # 检查对话是否已关闭。如果已关闭，不转发管理员回复给用户。
        if conv.status == "closed":
             logger.info(f"收到管理员消息 {message_id} 在已关闭的话题 {tid} (用户 {conv.user_id}) 中。不转发给用户。")
             # 也许给管理员发送一个温和的提示？
             # try: await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "注意：此对话已标记为关闭，消息不会转发给用户。"}) except Exception: pass
             return # 如果对话已关闭，不转发消息内容

    except Exception as e: # 捕获查找对话可能发生的错误
        logger.error(f"处理消息 {message_id} 时，查找话题 {tid} 对应的对话失败: {e}", exc_info=True)
        # 如果无法获取用户 ID，就无法转发。
        try:
            # 修正 chat_id 参数，使用 settings.GROUP_ID
            await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "处理消息失败：无法获取对话信息，消息未转发。"})
        except Exception as e_notify:
            logger.warning(f"发送'查找对话失败'消息到话题 {tid} 失败: {e_notify}")
        return


    # --- 3. 机器翻译 (管理员 -> 用户) ---
    # 尝试将管理员消息的文本或 caption 翻译成用户的目标语言
    user_target_lang = conv.lang # 从对话对象中获取用户目标语言
    # original_content 在函数开头已获取

    msg_to_copy = msg.copy() # 创建消息字典的副本进行修改

    # --- 问题 4: 添加管理员名字到消息内容 ---
    # 在原始内容前添加管理员名字
    admin_prefix = f"👤 {admin_sender_name}:\n"
    # 只有当原始内容存在时才添加前缀
    if original_content:
        if "text" in msg_to_copy and msg_to_copy.get("text") is not None:
            msg_to_copy["text"] = admin_prefix + msg_to_copy["text"]
        elif "caption" in msg_to_copy and msg_to_copy.get("caption") is not None:
            msg_to_copy["caption"] = admin_prefix + msg_to_copy["caption"]


    # --- 问题 5: 翻译判断和执行 ---
    # 仅在以下情况下尝试翻译:
    # - 用户目标语言已设置 (不是 None 且不是空字符串)
    # - 用户目标语言不在管理员常用的语言列表 settings.ADMIN_LANGS 中
    # - 消息有内容 (original_content)
    # - 内容长度大于某个阈值且不像命令/标记
    # 如果用户目标语言是 zh 或 en，或者未设置，将不会进行管理员消息到用户语言的翻译。
    if user_target_lang and user_target_lang.strip() and user_target_lang.lower() not in [lang.lower() for lang in settings.ADMIN_LANGS] and original_content: # 将 settings.ADMIN_LANGS 中的语言转小写再比较，增强健壮性
         if len(original_content) > 5 and not original_content.strip().startswith(('/', '[', '【', '（')):
            logger.debug(f"正在为话题 {tid} 中的消息 {message_id} (内容: '{original_content[:50]}...') 尝试翻译到用户语言 '{user_target_lang}'")
            try:
                # 翻译原始内容 (不含管理员前缀)
                translated_text = await translate(original_content, user_target_lang)
                if translated_text and translated_text.strip() != original_content.strip(): # 避免翻译结果与原始内容完全相同
                    translation_note = f"\n———\n💬机翻: {translated_text}"
                    # 将翻译结果添加到 text 或 caption 中，添加到 管理员前缀 + 原始内容 之后
                    if "text" in msg_to_copy:
                        msg_to_copy["text"] = msg_to_copy.get("text", "") + translation_note
                    elif "caption" in msg_to_copy:
                        msg_to_copy["caption"] = msg_to_copy.get("caption", "") + translation_note
                    logger.debug(f"成功将话题 {tid} 中的消息 {message_id} 翻译给了用户.")
                else:
                     logger.debug(f"管理员消息 {message_id} 翻译到 {user_target_lang} 结果为空或与原文相同，跳过添加翻译注释。")

            except Exception as e:
                logger.warning(f"管理员消息 {message_id} 翻译失败 (管理员 -> 用户) 到语言 '{user_target_lang}': {e}", exc_info=True)
                translation_note = "\n———\n💬机翻失败"
                if "text" in msg_to_copy:
                    msg_to_copy["text"] = msg_to_copy.get("text", "") + translation_note
                elif "caption" in msg_to_copy:
                    msg_to_copy["caption"] = msg_to_copy.get("caption", "") + translation_note

        # --- 4. 复制消息到用户的私聊 ---
    try:
            # 将修改后的 msg_to_copy 的 text 和 caption 显式传递
            # 这些内容现在包含了管理员前缀和可能的翻译注释
            # copy_any 内部会调用 tg()
        await copy_any(settings.GROUP_ID, conv.user_id, message_id,
                           {"text": msg_to_copy.get("text"),
                            "caption": msg_to_copy.get("caption")
                           })
        logger.info(f"成功复制话题 {tid} 中的消息 {message_id} 给用户 {conv.user_id}")
    except Exception as e:
            # 捕获 copy_any 失败的异常
            logger.error(f"复制话题 {tid} 中的消息 {message_id} 给用户 {conv.user_id} 失败: {e}", exc_info=True)
            # 在话题中通知管理员，消息发送给用户失败
            try:
                # 修正 chat_id 参数，使用 settings.GROUP_ID
                await tg("sendMessage", {"chat_id": settings.GROUP_ID, "message_thread_id": tid, "text": "发送给用户失败，请检查日志。"})
            except Exception as e_notify:
                 # 捕获发送通知失败的异常
                 logger.warning(f"发送'发送给用户失败'通知到话题 {tid} 失败: {e_notify}")

    # --- 5. 记录出站消息 ---
    # 消息转发 (尝试) 成功后，将原始消息内容记录到数据库
    # 记录原始内容，不包含管理员前缀和翻译注释
    # original_body 在函数开头已获取
    if conv: # 确保对话对象存在
        try:
            # 记录原始内容 (不包含管理员前缀和翻译注释)
            await conv_service.record_outgoing_message(conv_id=conv.user_id, tg_mid=message_id, body=original_content)
        except Exception as e: # 记录消息失败是一个非关键错误，只需日志记录
            logger.error(f"记录用户 {conv.user_id} 的出站消息 {message_id} (来自话题 {tid}) 失败: {e}", exc_info=True)