import logging
from datetime import datetime, timezone
from peewee import DoesNotExist, PeeweeException, fn
from starlette.concurrency import run_in_threadpool
from typing import Optional, Dict, Any

from ..store import Conversation, Messages, BlackList, BindingID, get_current_utc_time
from ..tg_utils import tg
from ..settings import settings
from ..logging_config import get_logger
from ..monitoring import (
    monitor_performance, record_database_operation, record_telegram_api_call,
    update_active_conversations, update_cached_items, MetricsCollector
)
from ..cache import CacheManager

logger = get_logger("app.services.conversation")

# 定义对话状态对应的标记 (Emoji)
STATUS_EMOJIS = {
    "open": "🟢",
    "pending": "🟡",
    "closed": "❌",
    "resolved": "☑️",
}

# 定义绑定验证状态对应的标记
VERIFY_EMOJIS = {
    "pending": "🔒",
    "verified": "🔗",
}

MESSAGE_LIMIT_BEFORE_BIND = 10  # 绑定前消息数量限制


class ConversationService:
    def __init__(self, support_group_id: str, external_group_ids: list[str], tg_func,
                 cache_manager: Optional[CacheManager] = None,
                 metrics_collector: Optional[MetricsCollector] = None):
        self.support_group_id = support_group_id
        self.configured_external_group_ids = set(str(id) for id in external_group_ids)
        self.tg = tg_func
        self.cache = cache_manager
        self.metrics = metrics_collector
        self.logger = get_logger("app.services.conversation")

        self.logger.info(
            "ConversationService initialized",
            extra={
                "support_group_id": support_group_id,
                "external_groups_count": len(external_group_ids),
                "cache_enabled": cache_manager is not None,
                "metrics_enabled": metrics_collector is not None
            }
        )

    def _build_topic_name(self, entity_name: str | None, entity_id: int | str, status: str,
                          is_verified: str = "pending") -> str:
        """根据实体名字、ID、状态和验证状态构建话题名称"""
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

    def is_support_group(self, chat_id: int | str) -> bool:
        return str(chat_id) == self.support_group_id

    def is_external_group(self, chat_id: int | str) -> bool:
        """检查给定的聊天 ID 是否为配置中列出的需要监听的外部群组"""
        return str(chat_id) in self.configured_external_group_ids

    @monitor_performance("is_user_banned")
    async def is_user_banned(self, user_id: int | str) -> bool:
        """检查用户当前是否被拉黑（带缓存）"""
        user_id_int = 0
        try:
            user_id_int = int(user_id)
        except ValueError:
            self.logger.error(f"IS_BANNED: 无效的用户ID格式 '{user_id}'")
            return False

        # 尝试从缓存获取
        if self.cache:
            cached_result = await self.cache.conversation_cache.get_user_ban_status(user_id_int)
            if cached_result is not None:
                self.logger.debug(f"IS_BANNED: 从缓存获取用户 {user_id_int} 拉黑状态: {cached_result}")
                return cached_result

        try:
            self.logger.debug(f"IS_BANNED: 查询用户 {user_id_int} 的拉黑记录...")

            def _check_ban_status():
                return BlackList.get_or_none(BlackList.user_id == user_id_int)

            ban_entry: BlackList = await run_in_threadpool(_check_ban_status)

            if ban_entry:
                is_permanent = ban_entry.until is None
                is_expired = False
                if not is_permanent:
                    if ban_entry.until.tzinfo is None:
                        self.logger.warning(
                            f"IS_BANNED: 用户 {user_id_int} 的拉黑到期时间 {ban_entry.until} 是 naive datetime"
                        )
                        is_expired = ban_entry.until <= datetime.utcnow().replace(tzinfo=None)
                    else:
                        is_expired = ban_entry.until <= get_current_utc_time()

                if is_permanent or not is_expired:
                    result = True
                    self.logger.info(
                        f"IS_BANNED: 用户 {user_id_int} 当前被拉黑。永久: {is_permanent}, 到期: {ban_entry.until}"
                    )
                else:
                    self.logger.info(f"IS_BANNED: 用户 {user_id_int} 的拉黑记录已过期")
                    try:
                        await run_in_threadpool(ban_entry.delete_instance)
                        self.logger.info(f"IS_BANNED: 已自动移除用户 {user_id_int} 的过期拉黑记录")
                    except Exception as e_del:
                        self.logger.error(f"IS_BANNED: 自动移除过期拉黑记录失败: {e_del}", exc_info=True)
                    result = False
            else:
                self.logger.debug(f"IS_BANNED: 未找到用户 {user_id_int} 的拉黑记录")
                result = False

            # 缓存结果
            if self.cache:
                cache_ttl = 300 if result else 60  # 被拉黑的用户缓存更长时间
                await self.cache.conversation_cache.set_user_ban_status(user_id_int, result, cache_ttl)

            record_database_operation("check_user_banned", 0, True)
            return result

        except PeeweeException as e:
            self.logger.error(f"IS_BANNED: 数据库错误：检查用户 {user_id_int} 拉黑状态失败: {e}", exc_info=True)
            record_database_operation("check_user_banned", 0, False)
            return False
        except Exception as e:
            self.logger.error(f"IS_BANNED: 意外错误：检查用户 {user_id_int} 拉黑状态失败: {e}", exc_info=True)
            return False

    @monitor_performance("get_conversation_by_entity")
    async def get_conversation_by_entity(self, entity_id: int | str, entity_type: str) -> Optional[Conversation]:
        """获取实体对话（带缓存）"""
        entity_id_int = int(entity_id)

        # 尝试从缓存获取
        if self.cache:
            cached_conv = await self.cache.conversation_cache.get_conversation_by_entity(entity_id_int, entity_type)
            if cached_conv:
                self.logger.debug(f"从缓存获取实体 {entity_type} ID {entity_id_int} 的对话记录")
                return await self._dict_to_conversation(cached_conv)

        try:
            def _get_conversation():
                return Conversation.get_or_none(
                    entity_id=entity_id_int,
                    entity_type=entity_type
                )

            conv: Conversation = await run_in_threadpool(_get_conversation)

            if conv:
                self.logger.debug(
                    f"找到实体 {entity_type} ID {entity_id_int} 的对话记录: 话题 {conv.topic_id}, 状态 {conv.status}"
                )

                # 缓存结果
                if self.cache:
                    conv_dict = await self._conversation_to_dict(conv)
                    await self.cache.conversation_cache.set_conversation_by_entity(
                        entity_id_int, entity_type, conv_dict
                    )
            else:
                self.logger.debug(f"未找到实体 {entity_type} ID {entity_id_int} 的对话记录")

            record_database_operation("get_conversation_by_entity", 0, True)
            return conv

        except DoesNotExist:
            self.logger.debug(f"数据库查询未找到实体 {entity_type} ID {entity_id_int} 的对话记录")
            record_database_operation("get_conversation_by_entity", 0, True)
            return None
        except Exception as e:
            self.logger.error(f"获取实体 {entity_type} ID {entity_id_int} 对话失败: {e}", exc_info=True)
            record_database_operation("get_conversation_by_entity", 0, False)
            raise

    @monitor_performance("get_conversation_by_topic")
    async def get_conversation_by_topic(self, topic_id: int) -> Optional[Conversation]:
        """获取话题对话（带缓存）"""
        # 尝试从缓存获取
        if self.cache:
            cached_conv = await self.cache.conversation_cache.get_conversation_by_topic(topic_id)
            if cached_conv:
                self.logger.debug(f"从缓存获取话题 {topic_id} 的对话记录")
                return await self._dict_to_conversation(cached_conv)

        try:
            def _get_conversation():
                return Conversation.get_or_none(topic_id=topic_id)

            conv: Conversation = await run_in_threadpool(_get_conversation)

            if conv:
                self.logger.debug(f"找到话题 {topic_id} 对应的对话: 实体 {conv.entity_type} ID {conv.entity_id}")

                # 缓存结果
                if self.cache:
                    conv_dict = await self._conversation_to_dict(conv)
                    await self.cache.conversation_cache.set_conversation_by_topic(topic_id, conv_dict)
            else:
                self.logger.debug(f"未找到话题 ID {topic_id} 对应的对话")

            record_database_operation("get_conversation_by_topic", 0, True)
            return conv

        except PeeweeException as e:
            self.logger.error(f"数据库错误：获取话题 {topic_id} 对话失败: {e}", exc_info=True)
            record_database_operation("get_conversation_by_topic", 0, False)
            raise

    async def _conversation_to_dict(self, conv: Conversation) -> Dict[str, Any]:
        """将 Conversation 对象转换为字典"""
        return {
            "entity_id": conv.entity_id,
            "entity_type": conv.entity_type,
            "topic_id": conv.topic_id,
            "status": conv.status,
            "lang": conv.lang,
            "entity_name": conv.entity_name,
            "custom_id": conv.custom_id,
            "is_verified": conv.is_verified,
            "message_count_before_bind": conv.message_count_before_bind,
            "first_seen": conv.first_seen.isoformat() if conv.first_seen else None
        }

    async def _dict_to_conversation(self, conv_dict: Dict[str, Any]) -> Conversation:
        """将字典转换为 Conversation 对象"""
        conv = Conversation()
        conv.entity_id = conv_dict["entity_id"]
        conv.entity_type = conv_dict["entity_type"]
        conv.topic_id = conv_dict["topic_id"]
        conv.status = conv_dict["status"]
        conv.lang = conv_dict["lang"]
        conv.entity_name = conv_dict["entity_name"]
        conv.custom_id = conv_dict["custom_id"]
        conv.is_verified = conv_dict["is_verified"]
        conv.message_count_before_bind = conv_dict["message_count_before_bind"]
        return conv

    @monitor_performance("create_initial_conversation_with_topic")
    async def create_initial_conversation_with_topic(self, entity_id: int | str, entity_type: str,
                                                     entity_name: str | None) -> Optional[Conversation]:
        """创建初始对话和话题"""
        entity_id_int = int(entity_id)
        self.logger.info(f"尝试为实体 {entity_type} ID {entity_id_int} ({entity_name}) 创建带话题的初始对话")

        conv = await self.get_conversation_by_entity(entity_id_int, entity_type)
        topic_id_to_use = None

        if conv and conv.topic_id and conv.is_verified == 'pending':
            self.logger.info(f"实体 {entity_type} ID {entity_id_int} 已存在带话题 {conv.topic_id} 的待验证对话")
            topic_id_to_use = conv.topic_id
        elif conv and conv.topic_id and conv.is_verified == 'verified':
            self.logger.warning(f"实体 {entity_type} ID {entity_id_int} 已通过话题 {conv.topic_id} 验证")
            return conv
        else:
            topic_name = self._build_topic_name(entity_name, entity_id_int, "open", "pending")
            self.logger.info(f"为实体 {entity_type} ID {entity_id_int} 创建新话题，名称: '{topic_name}'")
            try:
                topic_response = await self.tg("createForumTopic", {
                    "chat_id": self.support_group_id,
                    "name": topic_name,
                })
                topic_id_to_use = topic_response.get("message_thread_id")
                if not topic_id_to_use:
                    self.logger.error(f"创建话题失败。响应: {topic_response}")
                    record_telegram_api_call("createForumTopic", 0, False)
                    return None

                self.logger.info(f"成功创建话题 ID: {topic_id_to_use}")
                record_telegram_api_call("createForumTopic", 0, True)

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
                record_telegram_api_call("sendMessage", 0, True)

            except Exception as e:
                self.logger.error(f"创建话题时发生异常: {e}", exc_info=True)
                record_telegram_api_call("createForumTopic", 0, False)
                return None

        # 更新或创建对话记录
        try:
            if conv:
                def _update_conversation():
                    return Conversation.update(
                        topic_id=topic_id_to_use,
                        entity_name=entity_name or conv.entity_name,
                        status="open",
                        is_verified="pending",
                        custom_id=None,
                        message_count_before_bind=0
                    ).where(
                        (Conversation.entity_id == entity_id_int) &
                        (Conversation.entity_type == entity_type)
                    ).execute()

                await run_in_threadpool(_update_conversation)
                conv = await self.get_conversation_by_entity(entity_id_int, entity_type)
                self.logger.info(f"已更新实体 {entity_type} ID {entity_id_int} 的对话记录")
            else:
                def _create_conversation():
                    return Conversation.create(
                        entity_id=entity_id_int,
                        entity_type=entity_type,
                        topic_id=topic_id_to_use,
                        entity_name=entity_name,
                        status="open",
                        is_verified="pending",
                        custom_id=None,
                        message_count_before_bind=0
                    )

                conv = await run_in_threadpool(_create_conversation)
                self.logger.info(f"已创建实体 {entity_type} ID {entity_id_int} 的新对话记录")

            # 使缓存失效
            if self.cache:
                await self.cache.conversation_cache.invalidate_conversation(
                    entity_id_int, entity_type, topic_id_to_use
                )

            record_database_operation("create_conversation", 0, True)
            return conv

        except Exception as e:
            self.logger.error(f"数据库操作失败: {e}", exc_info=True)
            record_database_operation("create_conversation", 0, False)
            return None

    @monitor_performance("close_conversation")
    async def close_conversation(self, topic_id: int | None, entity_id: int | str, entity_type: str):
        """关闭对话"""
        try:
            def _get_conversation():
                return Conversation.get_or_none(
                    (Conversation.entity_id == int(entity_id)) &
                    (Conversation.entity_type == entity_type)
                )

            conv_entry: Conversation = await run_in_threadpool(_get_conversation)

            if not conv_entry:
                self.logger.warning(f"CLOSE_CONV: 关闭对话时未找到对话记录")
                return

            new_status = "closed"

            def _update_status():
                return Conversation.update(status=new_status).where(
                    (Conversation.entity_id == int(entity_id)) &
                    (Conversation.entity_type == entity_type)
                ).execute()

            updated_count = await run_in_threadpool(_update_status)

            if updated_count > 0:
                self.logger.info(f"CLOSE_CONV: 对话状态设置为 '{new_status}'")

                # 使缓存失效
                if self.cache:
                    await self.cache.conversation_cache.invalidate_conversation(
                        int(entity_id), entity_type, conv_entry.topic_id
                    )

                # 通知实体
                try:
                    message_text = ""
                    if entity_type == 'user':
                        message_text = "您的客服对话已结束。如需新帮助，请发送新消息。"
                    elif entity_type == 'group':
                        message_text = "此群组的客服对话已结束。"

                    if message_text:
                        await self.tg("sendMessage", {"chat_id": entity_id, "text": message_text})
                        record_telegram_api_call("sendMessage", 0, True)
                        self.logger.info(f"CLOSE_CONV: 已向实体发送关闭通知")

                except Exception as e:
                    self.logger.warning(f"CLOSE_CONV: 发送关闭通知失败: {e}", exc_info=True)
                    record_telegram_api_call("sendMessage", 0, False)

                # 更新话题名称
                topic_to_update = conv_entry.topic_id
                if topic_to_update:
                    topic_name = self._build_topic_name(
                        conv_entry.entity_name, entity_id, new_status, conv_entry.is_verified
                    )
                    try:
                        await self.tg("editForumTopic", {
                            "chat_id": self.support_group_id,
                            "message_thread_id": topic_to_update,
                            "name": topic_name
                        })
                        record_telegram_api_call("editForumTopic", 0, True)
                        self.logger.debug(f"CLOSE_CONV: 更新话题名称为 '{topic_name}'")
                    except Exception as e:
                        self.logger.warning(f"CLOSE_CONV: 更新话题名称失败: {e}")
                        record_telegram_api_call("editForumTopic", 0, False)

            record_database_operation("close_conversation", 0, True)

        except PeeweeException as e:
            self.logger.error(f"CLOSE_CONV: 数据库错误: {e}", exc_info=True)
            record_database_operation("close_conversation", 0, False)
            raise
        except Exception as e:
            self.logger.error(f"CLOSE_CONV: 意外错误: {e}", exc_info=True)
            raise

    @monitor_performance("ban_user")
    async def ban_user(self, user_id: int | str):
        """拉黑用户"""
        user_id_int = 0
        try:
            user_id_int = int(user_id)
        except ValueError:
            self.logger.error(f"BAN_USER: 无效的用户ID格式 '{user_id}'")
            return

        try:
            def _check_existing_ban():
                return BlackList.get_or_none(BlackList.user_id == user_id_int)

            existing_ban = await run_in_threadpool(_check_existing_ban)

            if existing_ban:
                self.logger.info(f"BAN_USER: 用户 {user_id_int} 已经被拉黑")
                try:
                    await self.tg("sendMessage", {"chat_id": user_id_int, "text": "您已被禁止发起新的对话。"})
                    record_telegram_api_call("sendMessage", 0, True)
                except Exception as e:
                    self.logger.warning(f"BAN_USER: 发送重复拉黑通知失败: {e}", exc_info=True)
                    record_telegram_api_call("sendMessage", 0, False)
                return

            def _create_ban():
                return BlackList.create(user_id=user_id_int, until=None)

            created_ban_entry = await run_in_threadpool(_create_ban)

            if created_ban_entry and created_ban_entry.user_id == user_id_int:
                self.logger.info(f"BAN_USER: 成功为用户 {user_id_int} 创建拉黑记录")

                # 使缓存失效
                if self.cache:
                    await self.cache.conversation_cache.set_user_ban_status(user_id_int, True, 300)

                try:
                    await self.tg("sendMessage", {"chat_id": user_id_int, "text": "您已被禁止发起新的对话。"})
                    record_telegram_api_call("sendMessage", 0, True)
                    self.logger.info(f"BAN_USER: 已成功向用户 {user_id_int} 发送拉黑通知")
                except Exception as e:
                    self.logger.warning(f"BAN_USER: 发送拉黑通知失败: {e}", exc_info=True)
                    record_telegram_api_call("sendMessage", 0, False)
            else:
                self.logger.error(f"BAN_USER: 创建拉黑记录失败")

            record_database_operation("ban_user", 0, True)

        except PeeweeException as e:
            self.logger.error(f"BAN_USER: 数据库错误：拉黑用户 {user_id_int} 失败: {e}", exc_info=True)
            record_database_operation("ban_user", 0, False)
            raise
        except Exception as e:
            self.logger.error(f"BAN_USER: 意外错误：拉黑用户 {user_id_int} 失败: {e}", exc_info=True)
            raise

    @monitor_performance("unban_user")
    async def unban_user(self, user_id_to_unban: int | str) -> bool:
        """解除用户拉黑"""
        user_id_int = 0
        try:
            user_id_int = int(user_id_to_unban)
        except ValueError:
            self.logger.error(f"UNBAN_USER: 无效的用户ID格式 '{user_id_to_unban}'")
            return False

        try:
            def _delete_ban():
                return BlackList.delete().where(BlackList.user_id == user_id_int).execute()

            deleted_count = await run_in_threadpool(_delete_ban)

            if deleted_count > 0:
                self.logger.info(f"UNBAN_USER: 用户 {user_id_int} 已从拉黑列表中移除")

                # 更新缓存
                if self.cache:
                    await self.cache.conversation_cache.set_user_ban_status(user_id_int, False, 300)

                message_text = "您的账号已被解除拉黑。现在可以继续发起新的对话了。"
                try:
                    await self.tg("sendMessage", {"chat_id": user_id_int, "text": message_text})
                    record_telegram_api_call("sendMessage", 0, True)
                    self.logger.info(f"UNBAN_USER: 已成功向用户 {user_id_int} 发送解除拉黑通知")
                except Exception as e:
                    self.logger.warning(f"UNBAN_USER: 发送解除拉黑通知失败: {e}", exc_info=True)
                    record_telegram_api_call("sendMessage", 0, False)

                record_database_operation("unban_user", 0, True)
                return True
            else:
                self.logger.info(f"UNBAN_USER: 用户 {user_id_int} 不在拉黑列表中")
                record_database_operation("unban_user", 0, True)
                return False

        except PeeweeException as e:
            self.logger.error(f"UNBAN_USER: 数据库错误：解除拉黑用户 {user_id_int} 失败: {e}", exc_info=True)
            record_database_operation("unban_user", 0, False)
            return False
        except Exception as e:
            self.logger.error(f"UNBAN_USER: 意外错误：解除拉黑用户 {user_id_int} 失败: {e}", exc_info=True)
            return False

    @monitor_performance("reopen_conversation")
    async def reopen_conversation(self, entity_id: int | str, entity_type: str, topic_id: int):
        """重新开启对话"""
        try:
            def _get_conversation():
                return Conversation.get_or_none(
                    (Conversation.entity_id == int(entity_id)) &
                    (Conversation.entity_type == entity_type)
                )

            conv_entry: Conversation = await run_in_threadpool(_get_conversation)

            if not conv_entry:
                self.logger.warning(f"REOPEN_CONV: 重新开启对话时未找到匹配对话记录")
                return

            if conv_entry.topic_id != topic_id:
                self.logger.warning(
                    f"REOPEN_CONV: 记录中的 topic_id ({conv_entry.topic_id}) 与传入的 topic_id ({topic_id}) 不匹配"
                )

            new_status = "open"

            def _update_status():
                return Conversation.update(
                    status=new_status,
                    topic_id=topic_id
                ).where(
                    (Conversation.entity_id == int(entity_id)) &
                    (Conversation.entity_type == entity_type)
                ).execute()

            updated_count = await run_in_threadpool(_update_status)

            if updated_count > 0:
                self.logger.info(f"REOPEN_CONV: 对话状态设置为 '{new_status}'")

                # 使缓存失效
                if self.cache:
                    await self.cache.conversation_cache.invalidate_conversation(
                        int(entity_id), entity_type, topic_id
                    )

                # 通知实体
                try:
                    message_text = ""
                    if entity_type == 'user':
                        message_text = "您的对话已重新开启，请发送您的问题或信息。"
                    elif entity_type == 'group':
                        message_text = "此群组的客服对话已重新开启。"

                    if message_text:
                        await self.tg("sendMessage", {"chat_id": entity_id, "text": message_text})
                        record_telegram_api_call("sendMessage", 0, True)
                        self.logger.info(f"REOPEN_CONV: 已向实体发送重开通知")

                except Exception as e:
                    self.logger.warning(f"REOPEN_CONV: 发送'重新开启'消息失败: {e}", exc_info=True)
                    record_telegram_api_call("sendMessage", 0, False)

                # 更新话题名称
                topic_name = self._build_topic_name(
                    conv_entry.entity_name, entity_id, new_status, conv_entry.is_verified
                )
                try:
                    await self.tg("editForumTopic", {
                        "chat_id": self.support_group_id,
                        "message_thread_id": topic_id,
                        "name": topic_name
                    })
                    record_telegram_api_call("editForumTopic", 0, True)
                    self.logger.debug(f"REOPEN_CONV: 更新话题名称为 '{topic_name}'")
                except Exception as e:
                    self.logger.warning(f"REOPEN_CONV: 更新话题名称失败: {e}")
                    record_telegram_api_call("editForumTopic", 0, False)
            else:
                self.logger.warning(f"REOPEN_CONV: 重新开启对话失败，未能更新数据库状态")

            record_database_operation("reopen_conversation", 0, True)

        except PeeweeException as e:
            self.logger.error(f"REOPEN_CONV: 数据库错误：重新开启对话失败: {e}", exc_info=True)
            record_database_operation("reopen_conversation", 0, False)
            raise
        except Exception as e:
            self.logger.error(f"REOPEN_CONV: 意外错误：重新开启对话失败: {e}", exc_info=True)
            raise

    @monitor_performance("increment_message_count_and_check_limit")
    async def increment_message_count_and_check_limit(self, entity_id: int | str, entity_type: str) -> tuple[int, bool]:
        """增加消息计数并检查限制"""
        try:
            def _get_conversation():
                return Conversation.get_or_none(
                    (Conversation.entity_id == int(entity_id)) &
                    (Conversation.entity_type == entity_type)
                )

            conv: Conversation = await run_in_threadpool(_get_conversation)

            if not conv:
                self.logger.warning(f"尝试增加消息计数，但未找到实体 {entity_type} ID {entity_id} 的对话记录")
                return 0, False

            if conv.is_verified == 'verified':
                self.logger.debug(f"实体 {entity_type} ID {entity_id} 对话已验证，不增加绑定前消息计数")
                return conv.message_count_before_bind, False

            new_count = conv.message_count_before_bind + 1

            def _update_count():
                return Conversation.update(message_count_before_bind=new_count).where(
                    (Conversation.entity_id == int(entity_id)) &
                    (Conversation.entity_type == entity_type)
                ).execute()

            await run_in_threadpool(_update_count)

            # 使缓存失效
            if self.cache:
                await self.cache.conversation_cache.invalidate_conversation(
                    int(entity_id), entity_type, conv.topic_id
                )

            limit_reached = new_count >= MESSAGE_LIMIT_BEFORE_BIND
            self.logger.debug(
                f"实体 {entity_type} ID {entity_id} 未验证对话消息计数更新为 {new_count}. 限制达到: {limit_reached}"
            )

            record_database_operation("increment_message_count", 0, True)
            return new_count, limit_reached

        except PeeweeException as e:
            self.logger.error(f"数据库错误：增加消息计数失败: {e}", exc_info=True)
            record_database_operation("increment_message_count", 0, False)
            raise
        except Exception as e:
            self.logger.error(f"意外错误：增加消息计数失败: {e}", exc_info=True)
            raise

    @monitor_performance("bind_entity")
    async def bind_entity(self, entity_id: int | str, entity_type: str, entity_name: str | None,
                          custom_id: str, password: str | None = None) -> bool:
        """绑定实体"""
        entity_id_int = int(entity_id)
        try:
            # 检查实体是否已经绑定
            conv: Conversation = await self.get_conversation_by_entity(entity_id_int, entity_type)
            if conv and conv.is_verified == 'verified':
                self.logger.info(f"BIND_ENTITY: 实体 {entity_type} ID {entity_id_int} 已经绑定")
                await self.tg("sendMessage", {
                    "chat_id": entity_id_int,
                    "text": "您已经完成绑定，无需重复绑定。"
                })
                record_telegram_api_call("sendMessage", 0, True)
                return True

            # 验证自定义 ID 和密码
            def _get_binding_id():
                return BindingID.get_or_none(BindingID.custom_id == custom_id)

            binding_id_entry: BindingID | None = await run_in_threadpool(_get_binding_id)

            if not binding_id_entry:
                self.logger.warning(f"BIND_ENTITY: 自定义 ID '{custom_id}' 不存在")
                await self.tg("sendMessage", {
                    "chat_id": entity_id_int,
                    "text": f"绑定失败：自定义 ID '{custom_id}' 无效或未被授权。"
                })
                record_telegram_api_call("sendMessage", 0, True)
                return False

            # 密码校验
            if binding_id_entry.password_hash:
                if not password:
                    self.logger.warning(f"BIND_ENTITY: ID '{custom_id}' 需要密码，但用户未提供")
                    await self.tg("sendMessage", {
                        "chat_id": entity_id_int,
                        "text": f"绑定失败：此自定义 ID 需要密码。请使用 `/bind {custom_id} <密码>`"
                    })
                    record_telegram_api_call("sendMessage", 0, True)
                    return False
                if not binding_id_entry.check_password(password):
                    self.logger.warning(f"BIND_ENTITY: ID '{custom_id}' 密码错误")
                    await self.tg("sendMessage", {
                        "chat_id": entity_id_int,
                        "text": f"绑定失败：密码错误。"
                    })
                    record_telegram_api_call("sendMessage", 0, True)
                    return False
                self.logger.info(f"BIND_ENTITY: ID '{custom_id}' 密码校验通过")

            if binding_id_entry.is_used == 'used':
                def _check_existing_conv():
                    return Conversation.get_or_none(
                        (Conversation.custom_id == custom_id) &
                        (Conversation.is_verified == 'verified')
                    )

                existing_conv_for_custom_id: Conversation = await run_in_threadpool(_check_existing_conv)

                if (existing_conv_for_custom_id and
                        existing_conv_for_custom_id.entity_id == entity_id_int and
                        existing_conv_for_custom_id.entity_type == entity_type):
                    self.logger.info(f"BIND_ENTITY: 实体 {entity_type} ID {entity_id_int} 已绑定到 '{custom_id}'")
                    await self.tg("sendMessage", {
                        "chat_id": entity_id_int,
                        "text": f"您已成功绑定到自定义 ID '{custom_id}'。"
                    })
                    record_telegram_api_call("sendMessage", 0, True)
                    return True
                else:
                    self.logger.warning(f"BIND_ENTITY: 自定义 ID '{custom_id}' 已被其他实体使用")
                    await self.tg("sendMessage", {
                        "chat_id": entity_id_int,
                        "text": f"绑定失败：自定义 ID '{custom_id}' 已被其他用户绑定。"
                    })
                    record_telegram_api_call("sendMessage", 0, True)
                    return False

            # 获取对话记录
            conv: Conversation = await self.get_conversation_by_entity(entity_id_int, entity_type)

            if (conv and conv.is_verified == 'verified' and
                    conv.custom_id != custom_id and conv.custom_id is not None):
                self.logger.warning(f"BIND_ENTITY: 实体已验证并绑定到其他 ID ({conv.custom_id})")
                await self.tg("sendMessage", {
                    "chat_id": entity_id_int,
                    "text": "您已绑定到另一个自定义 ID。如需更改，请联系管理员。"
                })
                record_telegram_api_call("sendMessage", 0, True)
                return False

            # 确定话题名称和状态
            actual_status_for_db_and_topic = "open"
            actual_is_verified_for_topic = "verified"

            entity_name_for_topic = entity_name
            if not entity_name_for_topic and conv and conv.entity_name:
                entity_name_for_topic = conv.entity_name

            topic_id_to_use = conv.topic_id if conv and conv.topic_id else None

            # 创建或编辑话题名称
            topic_name = self._build_topic_name(
                entity_name_for_topic, entity_id_int,
                actual_status_for_db_and_topic, actual_is_verified_for_topic
            )

            if not topic_id_to_use:
                self.logger.info(f"BIND_ENTITY: 创建新话题")
                topic_response = await self.tg("createForumTopic", {
                    "chat_id": self.support_group_id,
                    "name": topic_name
                })
                topic_id_to_use = topic_response.get("message_thread_id")
                if not topic_id_to_use:
                    self.logger.error(f"BIND_ENTITY: 创建客服话题失败。响应: {topic_response}")
                    await self.tg("sendMessage", {
                        "chat_id": entity_id_int,
                        "text": "绑定失败：无法创建客服通道。"
                    })
                    record_telegram_api_call("createForumTopic", 0, False)
                    record_telegram_api_call("sendMessage", 0, True)
                    return False

                record_telegram_api_call("createForumTopic", 0, True)
                self.logger.info(f"BIND_ENTITY: 成功创建客服话题 ID: {topic_id_to_use}")
            else:
                self.logger.info(f"BIND_ENTITY: 编辑现有话题 {topic_id_to_use}")
                try:
                    await self.tg("editForumTopic", {
                        "chat_id": self.support_group_id,
                        "message_thread_id": topic_id_to_use,
                        "name": topic_name
                    })
                    record_telegram_api_call("editForumTopic", 0, True)
                    self.logger.info(f"BIND_ENTITY: 成功更新话题名称为 '{topic_name}'")
                except Exception as e_topic_edit:
                    self.logger.warning(f"BIND_ENTITY: 更新话题名称失败: {e_topic_edit}")
                    record_telegram_api_call("editForumTopic", 0, False)

            # 更新或创建 Conversation 记录
            if conv:
                def _update_conversation():
                    return Conversation.update(
                        topic_id=topic_id_to_use,
                        custom_id=custom_id,
                        is_verified=actual_is_verified_for_topic,
                        entity_name=entity_name_for_topic,
                        status=actual_status_for_db_and_topic,
                        message_count_before_bind=0
                    ).where(
                        (Conversation.entity_id == entity_id_int) &
                        (Conversation.entity_type == entity_type)
                    ).execute()

                await run_in_threadpool(_update_conversation)
                self.logger.info(f"BIND_ENTITY: 成功更新对话记录")
            else:
                self.logger.warning(f"BIND_ENTITY: 对话记录不存在，将创建新的")

                def _create_conversation():
                    return Conversation.create(
                        entity_id=entity_id_int,
                        entity_type=entity_type,
                        topic_id=topic_id_to_use,
                        custom_id=custom_id,
                        is_verified=actual_is_verified_for_topic,
                        entity_name=entity_name_for_topic,
                        status=actual_status_for_db_and_topic,
                        message_count_before_bind=0
                    )

                conv = await run_in_threadpool(_create_conversation)
                self.logger.info(f"BIND_ENTITY: 成功创建对话记录")

            # 使缓存失效
            if self.cache:
                await self.cache.conversation_cache.invalidate_conversation(
                    entity_id_int, entity_type, topic_id_to_use
                )

            # 更新 BindingID 状态
            def _update_binding_id():
                return BindingID.update(is_used='used').where(
                    BindingID.custom_id == custom_id
                ).execute()

            await run_in_threadpool(_update_binding_id)
            self.logger.info(f"BIND_ENTITY: 自定义 ID '{custom_id}' 状态更新为 'used'")

            # 通知实体和客服话题
            await self.tg("sendMessage", {
                "chat_id": entity_id_int,
                "text": f"恭喜！您已成功绑定到自定义 ID '{custom_id}'。现在您可以发送消息与客服沟通了。"
            })
            record_telegram_api_call("sendMessage", 0, True)

            try:
                await self.tg("sendMessage", {
                    "chat_id": self.support_group_id,
                    "message_thread_id": topic_id_to_use,
                    "text": (
                        f"对话已成功验证并绑定。\n实体类型: {entity_type}\n实体ID: {entity_id_int}\n"
                        f"实体名称: {entity_name_for_topic or 'N/A'}\n自定义ID: {custom_id}"
                    )
                })
                record_telegram_api_call("sendMessage", 0, True)
            except Exception as e_topic_msg:
                self.logger.warning(f"BIND_ENTITY: 在客服话题中发送绑定成功消息失败: {e_topic_msg}")
                record_telegram_api_call("sendMessage", 0, False)

            record_database_operation("bind_entity", 0, True)
            return True

        except PeeweeException as e:
            self.logger.error(f"BIND_ENTITY: 数据库错误：绑定失败: {e}", exc_info=True)
            await self.tg("sendMessage", {
                "chat_id": entity_id_int,
                "text": "绑定过程中发生数据库错误，请稍后重试。"
            })
            record_database_operation("bind_entity", 0, False)
            record_telegram_api_call("sendMessage", 0, True)
            return False
        except Exception as e:
            self.logger.error(f"BIND_ENTITY: 意外错误：绑定失败: {e}", exc_info=True)
            await self.tg("sendMessage", {
                "chat_id": entity_id_int,
                "text": "绑定过程中发生意外错误，请联系管理员。"
            })
            record_telegram_api_call("sendMessage", 0, True)
            return False

    @monitor_performance("record_incoming_message")
    async def record_incoming_message(self, conv_id: int | str, conv_entity_type: str,
                                      sender_id: int | str | None, sender_name: str | None,
                                      tg_mid: int, body: str | None = None):
        """记录入站消息"""
        try:
            conv_id_int = int(conv_id) if conv_id is not None else None
            sender_id_int = int(sender_id) if sender_id is not None else None

            def _create_message():
                return Messages.create(
                    conv_entity_id=conv_id_int,
                    conv_entity_type=conv_entity_type,
                    dir='in',
                    sender_id=sender_id_int,
                    sender_name=sender_name,
                    tg_mid=tg_mid,
                    body=body,
                    created_at=get_current_utc_time()
                )

            await run_in_threadpool(_create_message)
            self.logger.debug(f"记录了入站消息 for entity {conv_entity_type} ID {conv_id}")
            record_database_operation("record_incoming_message", 0, True)

        except PeeweeException as e:
            self.logger.error(f"Database error: Failed to record incoming message: {e}", exc_info=True)
            record_database_operation("record_incoming_message", 0, False)
        except Exception as e:
            self.logger.error(f"Unexpected error while recording incoming message: {e}", exc_info=True)

    @monitor_performance("record_outgoing_message")
    async def record_outgoing_message(self, conv_id: int | str, conv_entity_type: str,
                                      sender_id: int | str | None, sender_name: str | None,
                                      tg_mid: int, body: str | None = None):
        """记录出站消息"""
        try:
            conv_id_int = int(conv_id) if conv_id is not None else None
            sender_id_int = int(sender_id) if sender_id is not None else None

            def _create_message():
                return Messages.create(
                    conv_entity_id=conv_id_int,
                    conv_entity_type=conv_entity_type,
                    dir='out',
                    sender_id=sender_id_int,
                    sender_name=sender_name,
                    tg_mid=tg_mid,
                    body=body,
                    created_at=get_current_utc_time()
                )

            await run_in_threadpool(_create_message)
            self.logger.debug(f"记录了出站消息 for entity {conv_entity_type} ID {conv_id}")
            record_database_operation("record_outgoing_message", 0, True)

        except PeeweeException as e:
            self.logger.error(f"Database error: Failed to record outgoing message: {e}", exc_info=True)
            record_database_operation("record_outgoing_message", 0, False)
        except Exception as e:
            self.logger.error(f"Unexpected error while recording outgoing message: {e}", exc_info=True)

    @monitor_performance("create_binding_id")
    async def create_binding_id(self, custom_id: str, password: str | None = None) -> tuple[bool, str]:
        """创建新的绑定ID"""
        self.logger.info(f"CREATE_BIND_ID: 尝试创建自定义ID '{custom_id}'")

        if not custom_id:
            return False, "自定义ID不能为空。"

        def _create_binding_id_in_db():
            from ..store import db as service_db
            with service_db.atomic():
                # 检查ID是否已存在
                existing_entry = BindingID.get_or_none(BindingID.custom_id == custom_id)
                if existing_entry:
                    return False, f"自定义ID '{custom_id}' 已存在。"

                # 创建新的绑定ID
                new_binding_id = BindingID.create(
                    custom_id=custom_id,
                    is_used='unused'
                )

                # 设置密码（如果提供）
                if password and password.strip():
                    new_binding_id.set_password(password.strip())
                    new_binding_id.save()
                    self.logger.info(f"CREATE_BIND_ID: 已为自定义ID '{custom_id}' 设置密码")
                    return True, f"已创建自定义ID '{custom_id}' 并设置密码。"
                else:
                    self.logger.info(f"CREATE_BIND_ID: 已创建自定义ID '{custom_id}' 无密码")
                    return True, f"已创建自定义ID '{custom_id}' 无密码要求。"

        try:
            success, message = await run_in_threadpool(_create_binding_id_in_db)

            # 使缓存失效
            if self.cache:
                await self.cache.conversation_cache.invalidate_binding_id(custom_id)

            record_database_operation("create_binding_id", 0, True)
            return success, message

        except PeeweeException as e:
            self.logger.error(f"CREATE_BIND_ID: 创建绑定ID时发生数据库错误: {e}", exc_info=True)
            record_database_operation("create_binding_id", 0, False)
            return False, "创建绑定ID时发生数据库错误。"
        except Exception as e:
            self.logger.error(f"CREATE_BIND_ID: 创建绑定ID时发生意外错误: {e}", exc_info=True)
            return False, "创建绑定ID时发生意外错误。"

    @monitor_performance("set_binding_id_password")
    async def set_binding_id_password(self, custom_id: str, new_password: str | None) -> tuple[bool, str]:
        """修改指定自定义ID的密码（会替换之前的密码）"""
        self.logger.info(f"SET_BIND_PASS: 尝试修改自定义ID '{custom_id}' 的密码")

        if not custom_id:
            return False, "自定义ID不能为空。"

        def _update_password_in_db():
            from ..store import db as service_db
            with service_db.atomic():
                binding_entry: BindingID | None = BindingID.get_or_none(BindingID.custom_id == custom_id)
                if not binding_entry:
                    return False, f"自定义ID '{custom_id}' 不存在。"

                if new_password and new_password.strip():
                    # 设置新密码（会替换之前的密码）
                    binding_entry.set_password(new_password.strip())
                    binding_entry.save()
                    self.logger.info(f"SET_BIND_PASS: 已为自定义ID '{custom_id}' 更新密码")
                    return True, f"已为自定义ID '{custom_id}' 更新密码。"
                else:
                    # 清除密码
                    binding_entry.password_hash = None
                    binding_entry.save()
                    self.logger.info(f"SET_BIND_PASS: 已清除自定义ID '{custom_id}' 的密码")
                    return True, f"已清除自定义ID '{custom_id}' 的密码。现在绑定时无需提供密码。"

        try:
            success, message = await run_in_threadpool(_update_password_in_db)

            # 使缓存失效
            if self.cache:
                await self.cache.conversation_cache.invalidate_binding_id(custom_id)

            record_database_operation("set_binding_id_password", 0, True)
            return success, message

        except PeeweeException as e:
            self.logger.error(f"SET_BIND_PASS: 修改密码时发生数据库错误: {e}", exc_info=True)
            record_database_operation("set_binding_id_password", 0, False)
            return False, "修改密码时发生数据库错误。"
        except Exception as e:
            self.logger.error(f"SET_BIND_PASS: 修改密码时发生意外错误: {e}", exc_info=True)
            return False, "修改密码时发生意外错误。"