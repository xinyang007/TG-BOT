# conversation_service.py

import logging
from datetime import datetime, timezone
from peewee import DoesNotExist, PeeweeException, fn
from starlette.concurrency import run_in_threadpool

from ..store import Conversation, Messages, BlackList, BindingID, get_current_utc_time
from ..tg_utils import tg
from ..settings import settings

logger = logging.getLogger(__name__)

# --- 定义对话状态对应的标记 (Emoji) ---
STATUS_EMOJIS = {
    "open": "🟢",  # 开启状态
    "pending": "🟡",  # 待处理/中间状态
    "closed": "❌",  # 关闭状态 (已修改)
    "resolved": "☑️",  # 已解决 (可选)
}

# --- 定义绑定验证状态对应的标记 ---
VERIFY_EMOJIS = {
    "pending": "🔒",  # 未验证
    "verified": "🔗",  # 已验证
}

MESSAGE_LIMIT_BEFORE_BIND = 10  # 绑定前消息数量限制


class ConversationService:
    def __init__(self, support_group_id: str, external_group_ids: list[str], tg_func):
        self.support_group_id = support_group_id
        self.configured_external_group_ids = set(str(id) for id in external_group_ids)
        logger.info(f"配置的外部群组 ID 列表 (用于参考或其他功能): {self.configured_external_group_ids}")
        self.tg = tg_func

    def _build_topic_name(self, entity_name: str | None, entity_id: int | str, status: str,
                          is_verified: str = "pending") -> str:
        """根据实体名字、ID、状态和验证状态构建话题名称。状态标记在前，验证标记在后。"""
        status_emoji = STATUS_EMOJIS.get(status, "")
        verify_emoji = VERIFY_EMOJIS.get(is_verified, "")

        name_part = entity_name or f"实体 {entity_id}"

        emoji_parts = []
        if status_emoji:
            emoji_parts.append(status_emoji)
        if verify_emoji:
            emoji_parts.append(verify_emoji)

        emoji_prefix_str = "".join(emoji_parts)

        if emoji_prefix_str:
            return f"{emoji_prefix_str} {name_part} ({entity_id})".strip()
        else:
            return f"{name_part} ({entity_id})".strip()

    # ----------------------------------------------------------------------------------
    # ↓↓↓↓↓ 这里应该是您 ConversationService 类中的所有其他方法 ↓↓↓↓↓
    # is_support_group, is_external_group, is_user_banned,
    # get_conversation_by_entity, create_initial_conversation_with_topic,
    # get_conversation_by_topic, close_conversation, ban_user, unban_user,
    # set_user_language, reopen_conversation, increment_message_count_and_check_limit,
    # bind_entity, record_incoming_message, record_outgoing_message
    # (这些方法的具体实现请参考之前我们讨论和修改过的版本)
    # ----------------------------------------------------------------------------------

    def is_support_group(self, chat_id: int | str) -> bool:
        return str(chat_id) == self.support_group_id

    def is_external_group(self, chat_id: int | str) -> bool:
        """检查给定的聊天 ID 是否为配置中列出的需要监听的外部群组."""
        return str(chat_id) in self.configured_external_group_ids

    async def is_user_banned(self, user_id: int | str) -> bool:
        try:
            ban_entry: BlackList = await run_in_threadpool(BlackList.get_or_none,
                                                           str(BlackList.user_id) == str(user_id))
            if ban_entry:
                is_permanent = ban_entry.until is None
                is_expired = ban_entry.until is not None and ban_entry.until <= get_current_utc_time()
                if is_permanent or not is_expired:
                    logger.info(f"用户 {user_id} 当前被拉黑 (永久: {is_permanent}, 到期时间: {ban_entry.until})")
                    return True
                else:
                    logger.info(f"用户 {user_id} 的拉黑已过期.")
            return False
        except PeeweeException as e:
            logger.error(f"数据库错误：检查用户 {user_id} 拉黑状态失败: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"意外错误：检查用户 {user_id} 拉黑状态失败: {e}", exc_info=True)
            raise

    async def get_conversation_by_entity(self, entity_id: int | str, entity_type: str) -> Conversation | None:
        try:
            conv: Conversation = await run_in_threadpool(Conversation.get_or_none,
                                                         entity_id=int(entity_id),
                                                         entity_type=entity_type)
            if not conv:
                logger.debug(f"未找到实体 {entity_type} ID {entity_id} 的对话记录")
            else:
                logger.debug(
                    f"找到实体 {entity_type} ID {entity_id} 的对话记录:话题 {conv.topic_id}, 状态 {conv.status}, 验证状态 {conv.is_verified}")
            return conv
        except DoesNotExist:
            logger.debug(f"数据库查询未找到实体 {entity_type} ID {entity_id} 的对话记录")
            return None
        except Exception as e:
            logger.error(f"数据库错误或数据转换错误：获取实体 {entity_type} ID {entity_id} 对话失败: {e}", exc_info=True)
            raise

    async def create_initial_conversation_with_topic(self, entity_id: int | str, entity_type: str,
                                                     entity_name: str | None) -> Conversation | None:
        entity_id_int = int(entity_id)
        logger.info(f"尝试为实体 {entity_type} ID {entity_id_int} ({entity_name}) 创建带话题的初始对话")
        conv = await self.get_conversation_by_entity(entity_id_int, entity_type)
        topic_id_to_use = None

        if conv and conv.topic_id and conv.is_verified == 'pending':
            logger.info(f"实体 {entity_type} ID {entity_id_int} 已存在带话题 {conv.topic_id} 的待验证对话。")
            topic_id_to_use = conv.topic_id
        elif conv and conv.topic_id and conv.is_verified == 'verified':
            logger.warning(f"实体 {entity_type} ID {entity_id_int} 已通过话题 {conv.topic_id} 验证。此函数可能被误用。")
            return conv
        else:
            topic_name = self._build_topic_name(entity_name, entity_id_int, "open", "pending")
            logger.info(f"为实体 {entity_type} ID {entity_id_int} 创建新话题，名称: '{topic_name}'")
            try:
                topic_response = await self.tg("createForumTopic", {
                    "chat_id": self.support_group_id,
                    "name": topic_name,
                })
                topic_id_to_use = topic_response.get("message_thread_id")
                if not topic_id_to_use:
                    logger.error(f"为实体 {entity_type} ID {entity_id_int} 创建话题失败。响应: {topic_response}")
                    return None
                logger.info(f"成功为实体 {entity_type} ID {entity_id_int} 创建话题 ID: {topic_id_to_use}")
                await self.tg("sendMessage", {
                    "chat_id": self.support_group_id,
                    "message_thread_id": topic_id_to_use,
                    "text": (
                        f"新的未验证对话已开始。\n"
                        f"实体类型: {entity_type}\n"
                        f"实体 ID: {entity_id_int}\n"
                        f"名称: {entity_name or 'N/A'}\n"
                        f"等待实体使用 /bind <自定义ID> 进行绑定"
                    )
                })
            except Exception as e:
                logger.error(f"为实体 {entity_type} ID {entity_id_int} 创建话题时发生异常: {e}", exc_info=True)
                return None

        if conv:
            await run_in_threadpool(Conversation.update(
                topic_id=topic_id_to_use,
                entity_name=entity_name or conv.entity_name,
                status="open",
                is_verified="pending",
                custom_id=None,
                message_count_before_bind=0
            ).where(
                (Conversation.entity_id == entity_id_int) &
                (Conversation.entity_type == entity_type)
            ).execute)
            conv = await self.get_conversation_by_entity(entity_id_int, entity_type)
            logger.info(f"已更新实体 {entity_type} ID {entity_id_int} 的对话记录，话题为 {topic_id_to_use}。")
        else:
            conv = await run_in_threadpool(Conversation.create,
                                           entity_id=entity_id_int,
                                           entity_type=entity_type,
                                           topic_id=topic_id_to_use,
                                           entity_name=entity_name,
                                           status="open",
                                           is_verified="pending",
                                           custom_id=None,
                                           message_count_before_bind=0
                                           )
            logger.info(f"已为实体 {entity_type} ID {entity_id_int} 创建新对话记录，话题为 {topic_id_to_use}。")
        return conv

    async def get_conversation_by_topic(self, topic_id: int):
        try:
            conv: Conversation = await run_in_threadpool(Conversation.get_or_none, topic_id=topic_id)
            if not conv:
                logger.debug(f"未找到话题 ID {topic_id} 对应的对话")
            else:
                logger.debug(
                    f"找到话题 {topic_id} 对应的对话: 实体 {conv.entity_type} ID {conv.entity_id}, 状态 {conv.status}, 验证状态 {conv.is_verified}")
            return conv
        except PeeweeException as e:
            logger.error(f"数据库错误：获取话题 {topic_id} 对话失败: {e}", exc_info=True)
            raise

    async def close_conversation(self, topic_id: int | None, entity_id: int | str, entity_type: str):
        """将对话状态设置为 'closed' 并更新话题名称（如果 topic_id 存在）。"""
        try:
            # 获取 Conversation 记录以获取实体名字和当前验证状态
            conv_entry: Conversation = await run_in_threadpool(Conversation.get_or_none,
                                                               (Conversation.entity_id == int(entity_id)) &
                                                               (Conversation.entity_type == entity_type))
            if not conv_entry:
                logger.warning(f"CLOSE_CONV: 关闭实体 {entity_type} ID {entity_id} 的对话时未找到对话记录。")
                return

            new_status = "closed"  # 新的状态是 "closed"

            # 更新数据库中的状态
            updated_count = await run_in_threadpool(Conversation.update(status=new_status).where(
                (Conversation.entity_id == int(entity_id)) &
                (Conversation.entity_type == entity_type)
            ).execute)

            if updated_count > 0:
                logger.info(f"CLOSE_CONV: 将实体 {entity_type} ID {entity_id} 的对话状态设置为 '{new_status}'.")
                # --- 通知实体的逻辑在这里 ---
                try:
                    message_text = ""
                    if entity_type == 'user':
                        message_text = "您的客服对话已结束。如需新帮助，请发送新消息。"
                    elif entity_type == 'group':
                        message_text = "此群组的客服对话已结束。"

                    if message_text:  # 确保有消息文本才发送
                        logger.info(
                            f"CLOSE_CONV: 准备向实体 {entity_type} ID {entity_id} 发送关闭通知: '{message_text}'")
                        await self.tg("sendMessage", {"chat_id": entity_id, "text": message_text})
                        logger.info(f"CLOSE_CONV: 已向实体 {entity_type} ID {entity_id} 发送关闭通知。")
                    else:
                        logger.info(
                            f"CLOSE_CONV: 无需向实体 {entity_type} ID {entity_id} 发送关闭通知 (未知实体类型或无消息文本)。")

                except Exception as e:
                    logger.warning(f"CLOSE_CONV: 发送'对话已结束'消息给实体 {entity_type} ID {entity_id} 失败: {e}",
                                   exc_info=True)  # 添加 exc_info=True

                # 更新话题名称
                # current_topic_id 应该是 conv_entry.topic_id，因为 topic_id 参数可能来自命令执行的话题，
                # 但我们应该用数据库里该实体关联的 topic_id。
                # 或者，如果命令总是从正确的客服话题发出，那么传入的 topic_id 就是我们要更新的。
                # 为了保险，我们优先使用 conv_entry.topic_id，如果它存在。

                topic_to_update = conv_entry.topic_id  # 优先使用数据库中记录的 topic_id
                if not topic_to_update and topic_id:  # 如果数据库没有，但参数传了 (不太可能发生在此流程)
                    topic_to_update = topic_id

                if topic_to_update:
                    # 获取关闭前的验证状态 conv_entry.is_verified
                    # 获取实体名称 conv_entry.entity_name
                    # 新状态是 new_status ("closed")

                    # 关键点：确保 conv_entry.is_verified 是正确的，并且被传递
                    actual_is_verified_status = conv_entry.is_verified
                    topic_name = self._build_topic_name(
                        conv_entry.entity_name,
                        entity_id,
                        new_status,  # "closed" -> ❌
                        actual_is_verified_status  # 例如 "verified" -> ✅
                    )
                    logger.info(
                        f"CLOSE_CONV: 准备更新话题 {topic_to_update}。名称: '{topic_name}'。参数: status='{new_status}', is_verified='{actual_is_verified_status}'")
                    try:
                        await self.tg("editForumTopic",
                                      {"chat_id": self.support_group_id,
                                       "message_thread_id": topic_to_update,
                                       "name": topic_name})
                        logger.debug(f"CLOSE_CONV: 更新话题 {topic_to_update} 名称为 '{topic_name}'")
                    except Exception as e:
                        logger.warning(f"CLOSE_CONV: 更新话题 {topic_to_update} 名称为 '{new_status}' 状态失败: {e}")
                else:
                    logger.warning(
                        f"CLOSE_CONV: 实体 {entity_type} ID {entity_id} 没有关联的 topic_id，无法更新话题名称。")
            else:
                logger.warning(f"CLOSE_CONV: 关闭实体 {entity_type} ID {entity_id} 的对话时未能更新数据库状态。")

        except PeeweeException as e:
            logger.error(f"CLOSE_CONV: 数据库错误：为实体 {entity_type} ID {entity_id} 设置状态为 'closed' 失败: {e}",
                         exc_info=True)
            raise
        except Exception as e:  # 捕获其他意外错误
            logger.error(f"CLOSE_CONV: 意外错误：为实体 {entity_type} ID {entity_id} 设置状态为 'closed' 失败: {e}",
                         exc_info=True)
            raise

    async def ban_user(self, user_id: int | str):
        try:
            existing_ban = await run_in_threadpool(BlackList.get_or_none, BlackList.user_id == str(user_id))
            if existing_ban:
                logger.info(f"用户 {user_id} 已经被拉黑。")
                return
            await run_in_threadpool(BlackList.create, user_id=str(user_id), until=None)
            logger.info(f"用户 {user_id} 已被拉黑。")
            try:
                await self.tg("sendMessage", {"chat_id": user_id, "text": "您已被禁止发起新的对话。"})
            except Exception as e:
                logger.warning(f"发送拉黑通知给用户 {user_id} 失败: {e}")
        except PeeweeException as e:
            logger.error(f"数据库错误：拉黑用户 {user_id} 失败: {e}", exc_info=True)
            raise

    async def unban_user(self, user_id_to_unban: int | str):
        try:
            deleted_count = await run_in_threadpool(
                BlackList.delete().where(str(BlackList.user_id) == str(user_id_to_unban)).execute)
            if deleted_count > 0:
                logger.info(f"用户 {user_id_to_unban} 已从拉黑列表中移除.")
                try:
                    await self.tg("sendMessage", {"chat_id": user_id_to_unban, "text": "您的账号已被解除拉黑。"})
                except Exception as e:
                    logger.warning(f"发送解除拉黑通知给用户 {user_id_to_unban} 失败: {e}")
                return True
            else:
                logger.info(f"尝试解除拉黑用户 {user_id_to_unban}，但在拉黑列表中未找到.")
                return False
        except PeeweeException as e:
            logger.error(f"数据库错误：解除拉黑用户 {user_id_to_unban} 失败: {e}", exc_info=True)
            raise

    async def set_user_language(self, topic_id: int, user_id: int | str, lang_code: str):
        try:
            conv_entry: Conversation = await run_in_threadpool(Conversation.get_or_none,
                                                               (Conversation.entity_id == int(user_id)) &
                                                               (Conversation.entity_type == 'user'))
            if not conv_entry or conv_entry.topic_id != topic_id:
                logger.warning(f"为话题 {topic_id} (用户 {user_id}) 设置语言时，未找到匹配的用户对话记录。")
                return

            updated_count = await run_in_threadpool(Conversation.update(lang=lang_code).where(
                (Conversation.entity_id == int(user_id)) &
                (Conversation.entity_type == 'user') &
                (Conversation.topic_id == topic_id)
            ).execute)
            if updated_count > 0:
                logger.info(f"用户 {user_id} 的目标语言设置为 '{lang_code}' (话题 {topic_id}).")
                try:
                    await self.tg("sendMessage",
                                  {"chat_id": conv_entry.entity_id, "text": f"您的客服对话语言已设置为: {lang_code}。"})
                except Exception as e:
                    logger.warning(f"发送'语言已设置'消息给实体 {conv_entry.entity_id} 失败: {e}")
            else:
                logger.warning(f"在话题 {topic_id} 中的 /setlang 命令未能找到匹配的对话记录来更新语言.")
        except PeeweeException as e:
            logger.error(f"数据库错误：为话题 {topic_id} (用户 {user_id}) 设置语言失败: {e}", exc_info=True)
            raise

    async def reopen_conversation(self, entity_id: int | str, entity_type: str, topic_id: int):
        """
        将已关闭的对话状态设置为 'open' 并更新话题名称。
        这里的 topic_id 是该实体之前关联的客服话题 ID。
        """
        try:
            # 获取 Conversation 记录以获取实体名字和当前验证状态
            conv_entry: Conversation = await run_in_threadpool(Conversation.get_or_none,
                                                               (Conversation.entity_id == int(entity_id)) &
                                                               (Conversation.entity_type == entity_type))
            if not conv_entry:
                logger.warning(
                    f"REOPEN_CONV: 重新开启实体 {entity_type} ID {entity_id} 对话 (话题 {topic_id}) 时未找到匹配对话记录。")
                return

                # 确保我们操作的是记录中的 topic_id，它应该与传入的 topic_id 一致
            if conv_entry.topic_id != topic_id:
                logger.warning(
                    f"REOPEN_CONV: 实体 {entity_type} ID {entity_id} 记录中的 topic_id ({conv_entry.topic_id}) 与传入的 topic_id ({topic_id}) 不匹配。将使用记录中的 topic_id ({conv_entry.topic_id})。")
                # 通常这意味着调用逻辑可能有点问题，但我们以数据库为准
                # topic_id = conv_entry.topic_id # 如果决定用数据库里的
                # 或者，如果坚持用传入的 topic_id，那后续更新数据库时也要用它
                # 目前的逻辑是，传入的 topic_id 就是我们要操作的那个

            new_status = "open"  # 新的状态是 "open"

            # 更新数据库中的状态 (以及确保 topic_id 是正确的，以防万一)
            # 如果 reopen_conversation 总是被正确调用（即 topic_id 就是该实体当前绑定的 topic_id），
            # 那么更新 topic_id 可能不是必须的，但无害。
            updated_count = await run_in_threadpool(Conversation.update(
                status=new_status,
                topic_id=topic_id  # 确保与此话题关联
            ).where(
                (Conversation.entity_id == int(entity_id)) &
                (Conversation.entity_type == entity_type)
            ).execute)

            if updated_count > 0:
                logger.info(
                    f"REOPEN_CONV: 将话题 {topic_id} (实体 {entity_type} ID {entity_id}) 的对话状态设置为 '{new_status}'.")
                # --- 通知实体的逻辑在这里 ---
                try:
                    message_text = ""
                    if entity_type == 'user':
                        message_text = "您的对话已重新开启，请发送您的问题或信息。"
                    elif entity_type == 'group':
                        message_text = "此群组的客服对话已重新开启。"

                    if message_text:
                        logger.info(
                            f"REOPEN_CONV: 准备向实体 {entity_type} ID {entity_id} 发送重开通知: '{message_text}'")
                        await self.tg("sendMessage", {"chat_id": entity_id, "text": message_text})
                        logger.info(f"REOPEN_CONV: 已向实体 {entity_type} ID {entity_id} 发送重开通知。")
                    else:
                        logger.info(f"REOPEN_CONV: 无需向实体 {entity_type} ID {entity_id} 发送重开通知。")
                except Exception as e:
                    logger.warning(f"REOPEN_CONV: 发送'重新开启'消息给实体 {entity_type} ID {entity_id} 失败: {e}",
                                   exc_info=True)  # 添加 exc_info=True

                # 更新话题名称
                # 获取重新开启前的验证状态 conv_entry.is_verified
                # 获取实体名称 conv_entry.entity_name
                # 新状态是 new_status ("open")

                # 关键点：确保 conv_entry.is_verified 是正确的，并且被传递
                actual_is_verified_status = conv_entry.is_verified
                topic_name = self._build_topic_name(
                    conv_entry.entity_name,
                    entity_id,
                    new_status,  # "open" -> 🟢
                    actual_is_verified_status  # 例如 "verified" -> ✅
                )
                logger.info(
                    f"REOPEN_CONV: 准备更新话题 {topic_id}。名称: '{topic_name}'。参数: status='{new_status}', is_verified='{actual_is_verified_status}'")
                try:
                    await self.tg("editForumTopic",
                                  {"chat_id": self.support_group_id,
                                   "message_thread_id": topic_id,  # 使用传入的（也是刚更新到DB的）topic_id
                                   "name": topic_name})
                    logger.debug(f"REOPEN_CONV: 更新话题 {topic_id} 名称为 '{topic_name}'")
                except Exception as e:
                    logger.warning(f"REOPEN_CONV: 更新话题 {topic_id} 名称为 'open' 状态失败: {e}")
            else:
                logger.warning(
                    f"REOPEN_CONV: 尝试重新开启实体 {entity_type} ID {entity_id} 对话失败，未能更新数据库状态。")

        except PeeweeException as e:
            logger.error(f"REOPEN_CONV: 数据库错误：重新开启实体 {entity_type} ID {entity_id} 失败: {e}", exc_info=True)
            raise
        except Exception as e:  # 捕获其他意外错误
            logger.error(f"REOPEN_CONV: 意外错误：重新开启实体 {entity_type} ID {entity_id} 失败: {e}", exc_info=True)
            raise

    async def increment_message_count_and_check_limit(self, entity_id: int | str, entity_type: str) -> tuple[int, bool]:
        try:
            conv: Conversation = await run_in_threadpool(Conversation.get_or_none,
                                                         (Conversation.entity_id == int(entity_id)) &
                                                         (Conversation.entity_type == entity_type))
            if not conv:
                logger.warning(f"尝试增加消息计数，但未找到实体 {entity_type} ID {entity_id} 的对话记录。")
                return 0, False

            if conv.is_verified == 'verified':
                logger.debug(f"实体 {entity_type} ID {entity_id} 对话已验证，不增加绑定前消息计数。")
                return conv.message_count_before_bind, False

            new_count = conv.message_count_before_bind + 1
            await run_in_threadpool(Conversation.update(message_count_before_bind=new_count).where(
                (Conversation.entity_id == int(entity_id)) &
                (Conversation.entity_type == entity_type)
            ).execute)

            limit_reached = new_count >= MESSAGE_LIMIT_BEFORE_BIND
            logger.debug(
                f"实体 {entity_type} ID {entity_id} 未验证对话消息计数更新为 {new_count}. 限制达到: {limit_reached}")
            return new_count, limit_reached
        except PeeweeException as e:
            logger.error(f"数据库错误：增加实体 {entity_type} ID {entity_id} 消息计数失败: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"意外错误：增加实体 {entity_type} ID {entity_id} 消息计数失败: {e}", exc_info=True)
            raise

    async def bind_entity(self, entity_id: int | str, entity_type: str, entity_name: str | None,
                          custom_id: str) -> bool:
        entity_id_int = int(entity_id)
        try:
            # 1. 验证自定义 ID (这部分逻辑不变)
            binding_id_entry: BindingID = await run_in_threadpool(BindingID.get_or_none,
                                                                  BindingID.custom_id == custom_id)
            if not binding_id_entry:  # 无效ID
                logger.warning(f"BIND_ENTITY: 自定义 ID '{custom_id}' 不存在。实体: {entity_type} ID {entity_id_int}")
                await self.tg("sendMessage",
                              {"chat_id": entity_id_int, "text": f"绑定失败：自定义 ID '{custom_id}' 无效或未被授权。"})
                return False
            if binding_id_entry.is_used == 'used':  # ID 已被使用
                # ... (检查是否被当前实体使用，如果是则返回 True，否则返回 False - 这部分逻辑不变)
                existing_conv_for_custom_id: Conversation = await run_in_threadpool(
                    Conversation.get_or_none,
                    (Conversation.custom_id == custom_id) & (Conversation.is_verified == 'verified')
                )
                if existing_conv_for_custom_id and \
                        existing_conv_for_custom_id.entity_id == entity_id_int and \
                        existing_conv_for_custom_id.entity_type == entity_type:
                    logger.info(f"BIND_ENTITY: 实体 {entity_type} ID {entity_id_int} 已绑定到 '{custom_id}'。")
                    await self.tg("sendMessage",
                                  {"chat_id": entity_id_int, "text": f"您已成功绑定到自定义 ID '{custom_id}'。"})
                    return True
                else:
                    logger.warning(
                        f"BIND_ENTITY: 自定义 ID '{custom_id}' 已被其他实体使用。实体: {entity_type} ID {entity_id_int}")
                    await self.tg("sendMessage", {"chat_id": entity_id_int,
                                                  "text": f"绑定失败：自定义 ID '{custom_id}' 已被其他用户绑定。"})
                    return False

            # 2. 获取对话记录
            conv: Conversation = await self.get_conversation_by_entity(entity_id_int, entity_type)

            # 如果已验证但尝试绑定不同 ID (这部分逻辑不变)
            if conv and conv.is_verified == 'verified' and conv.custom_id != custom_id and conv.custom_id is not None:
                logger.warning(
                    f"BIND_ENTITY: 实体 {entity_type} ID {entity_id_int} 已验证并绑定到其他 ID ({conv.custom_id})。绑定到 '{custom_id}' 被拒绝。")
                await self.tg("sendMessage",
                              {"chat_id": entity_id_int, "text": "您已绑定到另一个自定义 ID。如需更改，请联系管理员。"})
                return False

            # 3. 确定话题名称的参数和数据库状态
            # **关键修改：绑定成功后，对话状态应为 "open"**
            actual_status_for_db_and_topic = "open"
            actual_is_verified_for_topic = "verified"  # 绑定成功，所以是 verified

            entity_name_for_topic = entity_name  # 优先用传入的
            if not entity_name_for_topic and conv and conv.entity_name:  # 其次用DB里的
                entity_name_for_topic = conv.entity_name
            # 如果都没有，_build_topic_name 会用 "实体 ID"

            topic_id_to_use = conv.topic_id if conv and conv.topic_id else None

            # 4. 创建或编辑话题名称
            topic_name = self._build_topic_name(
                entity_name_for_topic,
                entity_id_int,
                actual_status_for_db_and_topic,  # 应该是 "open" (🟢)
                actual_is_verified_for_topic  # 应该是 "verified" (✅)
            )

            if not topic_id_to_use:
                logger.info(
                    f"BIND_ENTITY: 创建新话题。名称: '{topic_name}'。参数: status='{actual_status_for_db_and_topic}', is_verified='{actual_is_verified_for_topic}'")
                topic_response = await self.tg("createForumTopic",
                                               {"chat_id": self.support_group_id, "name": topic_name})
                topic_id_to_use = topic_response.get("message_thread_id")
                if not topic_id_to_use:
                    logger.error(
                        f"BIND_ENTITY: 为实体 {entity_type} ID {entity_id_int} 创建客服话题失败。响应: {topic_response}")
                    await self.tg("sendMessage", {"chat_id": entity_id_int, "text": "绑定失败：无法创建客服通道。"})
                    return False
                logger.info(
                    f"BIND_ENTITY: 成功为实体 {entity_type} ID {entity_id_int} 创建客服话题 ID: {topic_id_to_use}")
            else:
                logger.info(
                    f"BIND_ENTITY: 编辑现有话题 {topic_id_to_use}。名称: '{topic_name}'。参数: status='{actual_status_for_db_and_topic}', is_verified='{actual_is_verified_for_topic}'")
                try:
                    await self.tg("editForumTopic",
                                  {"chat_id": self.support_group_id, "message_thread_id": topic_id_to_use,
                                   "name": topic_name})
                    logger.info(f"BIND_ENTITY: 成功更新话题 {topic_id_to_use} 名称为 '{topic_name}'")
                except Exception as e_topic_edit:
                    logger.warning(f"BIND_ENTITY: 更新话题 {topic_id_to_use} 名称失败: {e_topic_edit}")

            # 5. 更新或创建 Conversation 记录
            if conv:
                await run_in_threadpool(Conversation.update(
                    topic_id=topic_id_to_use,
                    custom_id=custom_id,
                    is_verified=actual_is_verified_for_topic,  # "verified"
                    entity_name=entity_name_for_topic,  # 使用上面确定的 entity_name
                    status=actual_status_for_db_and_topic,  # **确保这里是 "open"**
                    message_count_before_bind=0
                ).where(
                    (Conversation.entity_id == entity_id_int) &
                    (Conversation.entity_type == entity_type)
                ).execute)
                logger.info(
                    f"BIND_ENTITY: 成功更新实体 {entity_type} ID {entity_id_int} 的对话记录。新状态: {actual_status_for_db_and_topic}, 验证: {actual_is_verified_for_topic}")
            else:
                # 理论上 conv 应该存在，因为 private/group handler 会先调用 create_initial_conversation_with_topic
                logger.warning(
                    f"BIND_ENTITY: 尝试绑定时实体 {entity_type} ID {entity_id_int} 的对话记录不存在，将创建新的。")
                conv = await run_in_threadpool(Conversation.create,
                                               entity_id=entity_id_int, entity_type=entity_type,
                                               topic_id=topic_id_to_use,
                                               custom_id=custom_id, is_verified=actual_is_verified_for_topic,
                                               # "verified"
                                               entity_name=entity_name_for_topic, status=actual_status_for_db_and_topic,
                                               # **确保这里是 "open"**
                                               message_count_before_bind=0)
                logger.info(
                    f"BIND_ENTITY: 成功创建实体 {entity_type} ID {entity_id_int} 的对话记录。状态: {actual_status_for_db_and_topic}, 验证: {actual_is_verified_for_topic}")

            # 6. 更新 BindingID 状态 (不变)
            await run_in_threadpool(BindingID.update(is_used='used').where(BindingID.custom_id == custom_id).execute)
            logger.info(f"BIND_ENTITY: 自定义 ID '{custom_id}' 状态更新为 'used'.")

            # 7. 通知实体和客服话题 (不变)
            await self.tg("sendMessage", {"chat_id": entity_id_int,
                                          "text": f"恭喜！您已成功绑定到自定义 ID '{custom_id}'。现在您可以发送消息与客服沟通了。"})
            try:
                await self.tg("sendMessage", {
                    "chat_id": self.support_group_id, "message_thread_id": topic_id_to_use,
                    "text": (f"对话已成功验证并绑定。\n实体类型: {entity_type}\n实体ID: {entity_id_int}\n"
                             f"实体名称: {entity_name_for_topic or 'N/A'}\n自定义ID: {custom_id}")})
            except Exception as e_topic_msg:
                logger.warning(f"BIND_ENTITY: 在客服话题 {topic_id_to_use} 中发送绑定成功消息失败: {e_topic_msg}")

            return True
        except PeeweeException as e:
            # ... (异常处理不变) ...
            logger.error(
                f"BIND_ENTITY: 数据库错误：实体 {entity_type} ID {entity_id_int} 绑定到自定义 ID '{custom_id}' 失败: {e}",
                exc_info=True)
            await self.tg("sendMessage", {"chat_id": entity_id_int, "text": "绑定过程中发生数据库错误，请稍后重试。"})
            return False
        except Exception as e:
            # ... (异常处理不变) ...
            logger.error(
                f"BIND_ENTITY: 意外错误：实体 {entity_type} ID {entity_id_int} 绑定到自定义 ID '{custom_id}' 失败: {e}",
                exc_info=True)
            await self.tg("sendMessage", {"chat_id": entity_id_int, "text": "绑定过程中发生意外错误，请联系管理员。"})
            return False

    async def record_incoming_message(self, conv_id: int | str, conv_entity_type: str, sender_id: int | str | None,
                                      sender_name: str | None, tg_mid: int, body: str | None = None):
        try:
            conv_id_int = int(conv_id) if conv_id is not None else None
            sender_id_int = int(sender_id) if sender_id is not None else None
            await run_in_threadpool(
                Messages.create, conv_entity_id=conv_id_int, conv_entity_type=conv_entity_type, dir='in',
                sender_id=sender_id_int, sender_name=sender_name, tg_mid=tg_mid, body=body,
                created_at=get_current_utc_time()
            )
            logger.debug(
                f"记录了入站消息 for entity {conv_entity_type} ID {conv_id} (sender {sender_id}, TG MID: {tg_mid})")
        except PeeweeException as e:
            logger.error(
                f"Database error: Failed to record incoming message for conv {conv_id} (TG MID: {tg_mid}): {e}",
                exc_info=True)
        except Exception as e:
            logger.error(
                f"Unexpected error while recording incoming message for conv {conv_id} (TG MID: {tg_mid}): {e}",
                exc_info=True)

    async def record_outgoing_message(self, conv_id: int | str, conv_entity_type: str, sender_id: int | str | None,
                                      sender_name: str | None, tg_mid: int, body: str | None = None):
        try:
            conv_id_int = int(conv_id) if conv_id is not None else None
            sender_id_int = int(sender_id) if sender_id is not None else None
            await run_in_threadpool(
                Messages.create, conv_entity_id=conv_id_int, conv_entity_type=conv_entity_type, dir='out',
                sender_id=sender_id_int, sender_name=sender_name, tg_mid=tg_mid, body=body,
                created_at=get_current_utc_time()
            )
            logger.debug(
                f"记录了出站消息 for entity {conv_entity_type} ID {conv_id} (sender {sender_id}, TG MID: {tg_mid})")
        except PeeweeException as e:
            logger.error(
                f"Database error: Failed to record outgoing message for conv {conv_id} (TG MID: {tg_mid}): {e}",
                exc_info=True)
        except Exception as e:
            logger.error(
                f"Unexpected error while recording outgoing message for conv {conv_id} (TG MID: {tg_mid}): {e}",
                exc_info=True)