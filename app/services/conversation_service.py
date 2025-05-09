import logging
from datetime import datetime, timezone
from peewee import DoesNotExist, PeeweeException # 导入特定的 Peewee 异常
from starlette.concurrency import run_in_threadpool # 用于异步执行同步 DB 操作

# 导入模型和 tg 工具函数，包括新增的 Messages 模型
from ..store import Conversation, Messages, BlackList
from ..tg_utils import tg # 导入 tg 工具函数
from ..settings import settings # 导入设置

logger = logging.getLogger(__name__)

# --- 定义对话状态对应的标记 (Emoji) ---
# 根据您的需求更新标记，closed 使用 ✅ 标记
STATUS_EMOJIS = {
    "open": "🟢",
    "pending": "🟡", # 即使不通过 /tag 设置，也可以在 DB 中作为标记使用
    "closed": "✅", # 灰色圆点或✅标记表示关闭
    "resolved": "☑️", # 例如
}

# --- 定义默认话题名称格式 ---
# 移除状态标记，状态标记将通过 _build_topic_name 方法添加
DEFAULT_TOPIC_NAME_FORMAT = "{name} ({user_id})"

class ConversationService:
    """
    处理与对话、用户、消息相关的核心业务逻辑。
    与数据库模型交互，并发送一些用户可见的消息。
    """
    def __init__(self, group_id: str, tg_func):
        """
        使用必要的依赖初始化服务。

        Args:
            group_id: 用于话题的 Telegram 群组 ID。
            tg_func: 用于与 Telegram API 交互的异步函数。
        """
        self.group_id = group_id
        self.tg = tg_func

    # --- 辅助方法：构建话题名称 ---
    def _build_topic_name(self, user_first_name: str | None, user_id: int, status: str) -> str:
        """根据用户名字、ID 和状态构建话题名称，状态标记放在名称前面."""
        # 获取状态对应的 emoji，如果状态不在 STATUS_EMOJIS 中则不添加标记
        status_emoji = STATUS_EMOJIS.get(status, "")
        # 使用保存的用户名字或默认名字 (如果名字不存在或为 None)
        name_part = user_first_name or f"User {user_id}"
        # 构建话题名称格式： 状态标记 用户名字 (用户ID)
        # 如果有 emoji，在 emoji 后加个空格
        emoji_prefix = f"{status_emoji} " if status_emoji else ""
        # 确保 user_id 是字符串以便格式化
        return f"{emoji_prefix}{name_part} ({user_id})".strip()


    # --- 用户 / 对话管理 ---

    async def is_user_banned(self, user_id: int) -> bool:
        """检查用户当前是否被拉黑."""
        try:
            # 使用线程池执行同步的数据库操作
            ban_entry: BlackList = await run_in_threadpool(BlackList.get_or_none, user_id=user_id)
            if ban_entry:
                is_permanent = ban_entry.until is None
                # 使用带时区信息的 datetime 进行比较
                is_expired = ban_entry.until is not None and ban_entry.until <= datetime.now(timezone.utc)
                if is_permanent or not is_expired:
                    logger.info(f"用户 {user_id} 当前被拉黑 (永久: {is_permanent}, 到期时间: {ban_entry.until})")
                    return True
                else:
                    logger.info(f"用户 {user_id} 的拉黑已过期.")
                    # Optional: automatically remove expired ban entry
                    # await run_in_threadpool(ban_entry.delete_instance)
            return False
        except PeeweeException as e:
            logger.error(f"数据库错误：检查用户 {user_id} 拉黑状态失败: {e}", exc_info=True)
            # 决定在数据库检查失败时如何处理拉黑状态 - 为了安全起见，可能假设用户未被拉黑？
            # 或者如果拉黑检查的数据库连接是关键的，重新抛出异常。此处返回 False。
            return False # 假设数据库检查失败时用户未被拉黑


    async def get_conversation_by_user(self, user_id: int) -> Conversation | None:
        """检索用户的对话记录，无论其状态如何."""
        try:
            # 使用线程池执行同步的数据库操作
            # 此处不再根据状态过滤，直接返回找到的记录
            conv: Conversation = await run_in_threadpool(Conversation.get_or_none, user_id=user_id)
            if not conv:
                 logger.debug(f"未找到用户 {user_id} 的对话记录")
            else:
                 # 记录找到的对话的状态
                 logger.debug(f"找到用户 {user_id} 的对话记录: 话题 {conv.topic_id}, 状态 {conv.status}")
            return conv
        except PeeweeException as e:
            logger.error(f"数据库错误：获取用户 {user_id} 对话失败: {e}", exc_info=True)
            raise # 重新抛出

    # create_first_conversation 方法保持不变，用于处理用户首次联系时的情况 (conv is None)

    async def create_first_conversation(self, user_id: int, user_first_name: str) -> Conversation:
        """
        为一个用户创建新的话题和初始对话记录。
        此方法应仅在确认该用户没有现有对话记录时调用。

        Args:
            user_id: 用户的 Telegram ID.
            user_first_name: 用户的名字，用于话题名称.

        Returns:
            新创建的 Conversation 模型实例.

        Raises:
            Exception: 如果话题或对话记录创建失败.
            ValueError: 如果该用户已存在对话记录 (表示调用逻辑错误).
        """
        # 再次检查以防 handler 逻辑有误
        existing_conv = await self.get_conversation_by_user(user_id)
        if existing_conv:
             logger.error(f"尝试为用户 {user_id} 创建初始对话，但记录已存在 (话题 {existing_conv.topic_id}).")
             raise ValueError(f"用户 {user_id} 的对话记录已存在") # 指示调用逻辑错误

        logger.info(f"正在为用户 {user_id} ({user_first_name}) 创建新话题和对话记录")
        try:
            # 构建初始话题名称 (状态为 open)
            initial_status = "open"
            # 使用 _build_topic_name 方法构建话题名称
            topic_name = self._build_topic_name(user_first_name, user_id, initial_status)

            # 在群组聊天中创建话题
            topic_data = await self.tg("createForumTopic",
                                        {"chat_id": self.group_id,
                                         "name": topic_name}) # 使用构建好的名称
            topic_id = topic_data["message_thread_id"]
            logger.info(f"为用户 {user_id} 创建了 Telegram 话题 {topic_id} 名称为 '{topic_name}'")

            # 创建数据库记录 (状态默认为 "open"，保存用户名字)
            # user_id 是主键，如果此处成功执行，表明之前确实没有记录
            conv = await run_in_threadpool(Conversation.create, user_id=user_id, topic_id=topic_id, user_first_name=user_first_name, status=initial_status)
            logger.info(f"为用户 {user_id} 创建了 DB 记录，话题 {topic_id}")

            # 通知用户对话已创建
            try:
                await self.tg("sendMessage", {"chat_id": user_id, "text": "已为您创建新的对话，请耐心等待客服回复。"})
            except Exception as e:
                 logger.warning(f"发送'对话已创建'消息给用户 {user_id} 失败: {e}")

            return conv

        except Exception as e:
            # 捕获话题创建或 DB 记录创建过程中的错误
            logger.error(f"为用户 {user_id} 创建初始对话失败: {e}", exc_info=True)
            # 通知用户创建失败
            try:
                await self.tg("sendMessage", {"chat_id": user_id, "text": "无法创建对话，请稍后再试。"})
            except Exception as e_notify:
                logger.warning(f"发送'创建失败'消息给用户 {user_id} 失败: {e_notify}")
            raise # 重新抛出原始异常以指示失败


    async def get_conversation_by_topic(self, topic_id: int):
        """根据话题 ID 查找对话."""
        try:
            # 使用线程池执行同步的数据库操作
            conv: Conversation = await run_in_threadpool(Conversation.get_or_none, topic_id=topic_id)
            if not conv:
                 logger.debug(f"未找到话题 ID {topic_id} 对应的对话")
            return conv
        except PeeweeException as e:
            logger.error(f"数据库错误：获取话题 {topic_id} 对话失败: {e}", exc_info=True)
            raise # 重新抛出


    async def close_conversation(self, topic_id: int, user_id: int):
        """将对话状态设置为 'closed' 并更新话题名称."""
        try:
            # 获取 Conversation 记录以获取用户名字和当前状态
            conv_entry: Conversation = await run_in_threadpool(Conversation.get_or_none, user_id=user_id)
            if not conv_entry:
                 logger.warning(f"关闭话题 {topic_id} (用户 {user_id}) 时未找到对话记录。")
                 return # 如果记录都不存在，就没法更新状态和话题名称

            # 使用线程池执行同步的数据库操作，更新状态
            new_status = "closed"
            updated_count = await run_in_threadpool(Conversation.update(status=new_status).where(Conversation.topic_id == topic_id).execute)

            if updated_count > 0:
                 logger.info(f"将话题 {topic_id} (用户 {user_id}) 的对话状态设置为 '{new_status}'.")
                 # 通知用户对话已结束
                 try:
                     await self.tg("sendMessage", {"chat_id": user_id, "text": "您的客服对话已结束，感谢咨询！如果您需要新的帮助，请发送新消息，我们将为您开启新的对话。（请勿回复此回复）"})
                 except Exception as e:
                     logger.warning(f"发送'对话已结束'消息给用户 {user_id} 失败: {e}")

                 # 更新话题名称在群组聊天中
                 try:
                      # 使用保存的用户名字和新的状态构建话题名称
                      topic_name = self._build_topic_name(conv_entry.user_first_name, user_id, new_status)
                      await self.tg("editForumTopic",
                                    {"chat_id": self.group_id,
                                     "message_thread_id": topic_id,
                                     "name": topic_name}) # 设置为 closed 状态的话题名称
                      logger.debug(f"更新话题 {topic_id} 名称为 '{topic_name}'")

                 except Exception as e:
                      logger.warning(f"更新话题 {topic_id} 名称为 'closed' 失败: {e}")

            else:
                 logger.warning(f"在话题 {topic_id} 中的 /close 命令未能找到匹配的对话记录来更新状态.")
                 # 如果未找到对话，不会通知用户，只在日志和管理员回复中提示
                 # 管理员通知在 handler caller 中处理

        except PeeweeException as e:
            logger.error(f"数据库错误：为话题 {topic_id} 设置状态为 'closed' 失败: {e}", exc_info=True)
            raise # 重新抛出


    async def ban_user(self, user_id: int):
        """Bans a user by adding them to the blacklist."""
        try:
            # 使用线程池执行同步的数据库操作
            await run_in_threadpool(BlackList.insert(user_id=user_id).on_conflict_replace().execute)
            logger.info(f"用户 {user_id} 已添加到拉黑列表.")

            # 通知用户已被拉黑
            try:
                await self.tg("sendMessage", {"chat_id": user_id, "text": "已被拉黑，无法继续会话"})
            except Exception as e:
                 logger.warning(f"发送'已被拉黑'消息给用户 {user_id} 失败: {e}")

        except PeeweeException as e:
            logger.error(f"数据库错误：拉黑用户 {user_id} 失败: {e}", exc_info=True)
            raise # 重新抛出


    async def unban_user(self, user_id_to_unban: int):
        """
        解除用户拉黑，并向用户发送通知。

        Args:
            user_id_to_unban: 要解除拉黑的用户 ID.

        Returns:
            bool: 如果用户在拉黑列表中并被成功移除，返回 True，否则返回 False.
        """
        try:
            # 使用线程池执行同步的数据库操作
            deleted_count = await run_in_threadpool(BlackList.delete().where(BlackList.user_id == user_id_to_unban).execute)
            if deleted_count > 0:
                logger.info(f"用户 {user_id_to_unban} 已从拉黑列表中移除.")
                # --- 新需求: 通知用户已解除拉黑 ---
                try:
                    await self.tg("sendMessage", {"chat_id": user_id_to_unban, "text": "您的账号已被解除拉黑。现在可以继续发起新的对话了。"})
                except Exception as e:
                     logger.warning(f"发送解除拉黑通知给用户 {user_id_to_unban} 失败: {e}")
                # --- 通知结束 ---
                return True
            else:
                logger.info(f"尝试解除拉黑用户 {user_id_to_unban}，但在拉黑列表中未找到.")
                return False
        except PeeweeException as e:
            logger.error(f"数据库错误：解除拉黑用户 {user_id_to_unban} 失败: {e}", exc_info=True)
            raise # 重新抛出


    async def set_user_language(self, topic_id: int, user_id: int, lang_code: str):
        """设置用户对话的目标语言，并更新话题名称（可选，保持现有状态标记）.

        Args:
            topic_id: 话题线程 ID.
            user_id: 用户的 Telegram ID.
            lang_code: 目标语言代码.
        """
        try:
            # 使用线程池执行同步的数据库操作
            updated_count = await run_in_threadpool(Conversation.update(lang=lang_code).where(Conversation.topic_id == topic_id).execute)

            if updated_count > 0:
                 logger.info(f"话题 {topic_id} (用户 {user_id}) 的目标语言设置为 '{lang_code}'.")
                 # 可选: 通知用户语言已更改
                 try:
                      await self.tg("sendMessage", {"chat_id": user_id, "text": f"您的客服对话语言已设置为: {lang_code}。管理员的消息将尝试翻译到此语言。"})
                 except Exception as e:
                      logger.warning(f"发送'语言已设置'消息给用户 {user_id} 失败: {e}")

                 # --- 可选：更新话题名称以反映语言变化 (保持原状态标记) ---
                 # 这需要获取当前话题的状态，并使用 _build_topic_name 方法重新构建名称。
                 # 获取当前状态和名字
                 # conv_entry: Conversation = await run_in_threadpool(Conversation.get_or_none, topic_id=topic_id)
                 # if conv_entry:
                 #      topic_name = self._build_topic_name(conv_entry.user_first_name, conv_entry.user_id, conv_entry.status)
                 #      try:
                 #           await self.tg("editForumTopic", {"chat_id": self.group_id, "message_thread_id": topic_id, "name": topic_name})
                 #           logger.debug(f"更新话题 {topic_id} 名称以反映语言变化：'{topic_name}'")
                 #      except Exception as e:
                 #           logger.warning(f"更新话题 {topic_id} 名称失败 (设置语言后): {e}")

            else:
                 logger.warning(f"在话题 {topic_id} 中的 /setlang 命令未能找到匹配的对话记录来更新语言.")
                 # 管理员通知在 handler caller 中处理

        except PeeweeException as e:
            logger.error(f"数据库错误：为话题 {topic_id} 设置语言失败: {e}", exc_info=True)
            raise # 重新抛出


    # --- 移除 update_conversation_status 方法，因为 /tag 被移除 ---
    # async def update_conversation_status(...):
    #     pass


    # --- 新增方法: 重新开启对话 ---
    async def reopen_conversation(self, user_id: int, topic_id: int):
        """
        将已关闭的对话状态设置为 'open' 并更新话题名称。

        Args:
            user_id: 用户的 Telegram ID.
            topic_id: 话题线程 ID.
        """
        try:
            # 获取 Conversation 记录以获取用户名字
            conv_entry: Conversation = await run_in_threadpool(Conversation.get_or_none, user_id=user_id)
            if not conv_entry:
                 logger.warning(f"重新开启用户 {user_id} 对话 (话题 {topic_id}) 时未找到对话记录。")
                 return # 未找到记录，无法更新状态和话题名称

            # 使用线程池执行同步的数据库操作，更新状态
            new_status = "open"
            updated_count = await run_in_threadpool(Conversation.update(status=new_status).where(Conversation.user_id == user_id).execute)

            if updated_count > 0:
                 logger.info(f"将话题 {topic_id} (用户 {user_id}) 的对话状态设置为 '{new_status}'.")
                 # 通知用户对话已重新开启 (用户发送消息触发，handler 会回复，这里不再重复通知用户)
                 # 决定是否在这里通知用户，还是让 handler 在处理完消息转发后统一通知。
                 # 为了避免双重通知，让 handler 通知更合理。此处仅日志记录和状态更新。

                 # 更新话题名称在群组聊天中 (例如移除 ✅ closed 标记)
                 try:
                      # 使用保存的用户名字和新的状态构建话题名称
                      topic_name = self._build_topic_name(conv_entry.user_first_name, user_id, new_status)
                      await self.tg("editForumTopic",
                                    {"chat_id": self.group_id,
                                     "message_thread_id": topic_id,
                                     "name": topic_name}) # 设置回开放状态的话题名称
                      await self.tg("sendMessage", {"chat_id": user_id,
                                                    "text": "您的对话已重新开启，请发送您的问题或信息。"})  # <--- 应该发送这条消息
                      logger.debug(f"更新话题 {topic_id} 名称为 '{topic_name}'")

                 except Exception as e:
                      logger.warning(f"更新话题 {topic_id} 名称为 'open' 的话题名称失败: {e}")

            else:
                 logger.warning(f"尝试重新开启用户 {user_id} 对话失败，未能找到匹配的对话记录.")
                 # 不通知用户

        except PeeweeException as e:
            logger.error(f"数据库错误：重新开启用户 {user_id} 对话失败: {e}", exc_info=True)
            raise # 重新抛出


    # --- 消息历史记录 ---

    async def record_incoming_message(self, conv_id: int, tg_mid: int, body: str | None = None):
        """
        将用户发来的消息记录到数据库。

        Args:
            conv_id: 对话 ID (即用户的 Telegram ID).
            tg_mid: 消息在用户私聊中的 Telegram Message ID.
            body: 消息文本或 caption.
        """
        try:
            # 使用线程池执行同步的数据库写操作
            await run_in_threadpool(
                Messages.create,
                conv_id=conv_id, # 对话 ID 就是 user_id
                dir='in', # 方向为 'in' (用户发给 bot)
                tg_mid=tg_mid,
                body=body,
                created_at=datetime.now(timezone.utc) # 记录 UTC 时间
            )
            logger.debug(f"记录了用户 {conv_id} 的入站消息 (TG MID: {tg_mid})")
        except PeeweeException as e:
            logger.error(f"数据库错误：记录用户 {conv_id} 的入站消息 (TG MID: {tg_mid}) 失败: {e}", exc_info=True)
            # 记录失败是一个非关键错误，通常不影响核心转发功能，只需日志记录


    async def record_outgoing_message(self, conv_id: int, tg_mid: int, body: str | None = None):
        """
        将 bot (管理员) 发给用户的消息记录到数据库。

        Args:
            conv_id: 对话 ID (即用户的 Telegram ID).
            tg_mid: 消息在群组话题中的 Telegram Message ID (用于关联).
            body: 消息文本或 caption.
        """
        try:
            # 使用线程池执行同步的数据库写操作
            await run_in_threadpool(
                Messages.create,
                conv_id=conv_id, # 对话 ID 就是 user_id
                dir='out', # 方向为 'out' (bot 发给用户)
                tg_mid=tg_mid, # 记录原始消息的 ID (在话题中)
                body=body,
                created_at=datetime.now(timezone.utc) # 记录 UTC 时间
            )
            logger.debug(f"记录了用户 {conv_id} 的出站消息 (TG MID: {tg_mid})")
        except PeeweeException as e:
            logger.error(f"数据库错误：记录用户 {conv_id} 的出站消息 (TG MID: {tg_mid}) 失败: {e}", exc_info=True)
            # 记录失败通常不影响核心转发功能，只需日志记录