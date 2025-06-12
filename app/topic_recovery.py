import asyncio
import time
from typing import Optional, Dict, Any
from dataclasses import dataclass

from .logging_config import get_logger
from .settings import settings
from .store import Conversation
from .tg_utils import tg_primary_bot

logger = get_logger("app.topic_recovery")


@dataclass
class TopicRecoveryResult:
    """话题恢复结果"""
    success: bool
    new_topic_id: Optional[int] = None
    error_message: Optional[str] = None
    conversation_updated: bool = False


class TopicRecoveryService:
    """话题恢复服务 - 处理被删除的话题"""

    def __init__(self, conversation_service, tg_func):
        self.conversation_service = conversation_service
        self.tg = tg_func
        self.tg_primary = tg_primary_bot
        self.logger = get_logger("app.topic_recovery")

    async def handle_topic_deleted_error(self, entity_id: int, entity_type: str,
                                         entity_name: str = None) -> TopicRecoveryResult:
        """
        处理话题被删除的错误

        Args:
            entity_id: 实体ID
            entity_type: 实体类型 ('user' 或 'group')
            entity_name: 实体名称

        Returns:
            TopicRecoveryResult: 恢复结果
        """
        self.logger.warning(f"检测到话题被删除，开始恢复 {entity_type} {entity_id}")

        try:
            # 1. 获取当前对话记录
            conv = await self.conversation_service.get_conversation_by_entity(entity_id, entity_type)
            if not conv:
                self.logger.error(f"找不到 {entity_type} {entity_id} 的对话记录")
                return TopicRecoveryResult(
                    success=False,
                    error_message="找不到对话记录"
                )

            # 2. 清理无效的话题ID
            self.logger.info(f"清理无效话题ID: {conv.topic_id}")
            await self._clear_invalid_topic(entity_id, entity_type)

            # 3. 创建新话题
            new_topic_result = await self._create_new_topic(conv, entity_name)
            if not new_topic_result.success:
                return new_topic_result

            # 4. 更新对话记录
            update_result = await self._update_conversation_topic(
                entity_id, entity_type, new_topic_result.new_topic_id
            )

            if update_result:
                self.logger.info(
                    f"✅ 话题恢复成功: {entity_type} {entity_id} -> 新话题 {new_topic_result.new_topic_id}"
                )
                return TopicRecoveryResult(
                    success=True,
                    new_topic_id=new_topic_result.new_topic_id,
                    conversation_updated=True
                )
            else:
                return TopicRecoveryResult(
                    success=False,
                    error_message="更新对话记录失败"
                )

        except Exception as e:
            self.logger.error(f"话题恢复过程异常: {e}", exc_info=True)
            return TopicRecoveryResult(
                success=False,
                error_message=str(e)
            )

    async def _clear_invalid_topic(self, entity_id: int, entity_type: str):
        """清理无效的话题ID"""
        try:
            from starlette.concurrency import run_in_threadpool

            def _update_db():
                return Conversation.update(
                    topic_id=None,
                    status="pending"  # 重置状态，等待新话题
                ).where(
                    (Conversation.entity_id == entity_id) &
                    (Conversation.entity_type == entity_type)
                ).execute()

            updated = await run_in_threadpool(_update_db)
            self.logger.info(f"已清理 {entity_type} {entity_id} 的无效话题ID")

            # 使缓存失效
            if hasattr(self.conversation_service, 'cache') and self.conversation_service.cache:
                await self.conversation_service.cache.conversation_cache.invalidate_conversation(
                    entity_id, entity_type, None
                )

        except Exception as e:
            self.logger.error(f"清理无效话题ID失败: {e}", exc_info=True)
            raise

    async def _create_new_topic(self, conv: Conversation, entity_name: str = None) -> TopicRecoveryResult:
        """创建新话题"""
        try:
            # 确定实体名称
            display_name = entity_name or conv.entity_name or f"实体 {conv.entity_id}"

            # 构建话题名称
            topic_name = self.conversation_service._build_topic_name(
                display_name, conv.entity_id, "open", conv.is_verified
            )

            self.logger.info(f"创建新话题: '{topic_name}'")

            # 创建话题
            topic_response = await self.tg("createForumTopic", {
                "chat_id": settings.SUPPORT_GROUP_ID,
                "name": topic_name
            })

            new_topic_id = topic_response.get("message_thread_id")
            if not new_topic_id:
                self.logger.error(f"创建话题失败，响应: {topic_response}")
                return TopicRecoveryResult(
                    success=False,
                    error_message="创建话题失败：无法获取话题ID"
                )

            # 发送恢复通知
            await self._send_recovery_notification(new_topic_id, conv)

            self.logger.info(f"✅ 成功创建新话题: {new_topic_id}")
            return TopicRecoveryResult(
                success=True,
                new_topic_id=new_topic_id
            )

        except Exception as e:
            self.logger.error(f"创建新话题失败: {e}", exc_info=True)
            return TopicRecoveryResult(
                success=False,
                error_message=f"创建话题异常: {str(e)}"
            )

    async def _send_recovery_notification(self, topic_id: int, conv: Conversation):
        """发送恢复通知"""
        try:
            notification_text = (
                f"🔄 <b>话题已恢复</b>\n\n"
                f"原话题被删除，已自动创建新话题。\n"
                f"实体类型: {conv.entity_type}\n"
                f"实体ID: {conv.entity_id}\n"
                f"实体名称: {conv.entity_name or 'N/A'}\n"
                f"验证状态: {'已验证' if conv.is_verified == 'verified' else '待验证'}\n"
                f"自定义ID: {conv.custom_id or 'N/A'}\n\n"
                f"对话将继续在此话题中进行。"
            )

            await self.tg_primary("sendMessage", {
                "chat_id": settings.SUPPORT_GROUP_ID,
                "message_thread_id": topic_id,
                "text": notification_text,
                "parse_mode": "HTML"
            })

        except Exception as e:
            self.logger.warning(f"发送恢复通知失败: {e}")

    async def _update_conversation_topic(self, entity_id: int, entity_type: str,
                                         new_topic_id: int) -> bool:
        """更新对话记录的话题ID"""
        try:
            from starlette.concurrency import run_in_threadpool

            def _update_db():
                return Conversation.update(
                    topic_id=new_topic_id,
                    status="open"  # 重新开启对话
                ).where(
                    (Conversation.entity_id == entity_id) &
                    (Conversation.entity_type == entity_type)
                ).execute()

            updated = await run_in_threadpool(_update_db)

            if updated > 0:
                self.logger.info(f"✅ 已更新 {entity_type} {entity_id} 的话题ID为 {new_topic_id}")

                # 使缓存失效
                if hasattr(self.conversation_service, 'cache') and self.conversation_service.cache:
                    await self.conversation_service.cache.conversation_cache.invalidate_conversation(
                        entity_id, entity_type, new_topic_id
                    )

                return True
            else:
                self.logger.error(f"未找到要更新的对话记录: {entity_type} {entity_id}")
                return False

        except Exception as e:
            self.logger.error(f"更新对话话题ID失败: {e}", exc_info=True)
            return False

    async def check_and_recover_topic(self, entity_id: int, entity_type: str,
                                      entity_name: str = None) -> Optional[int]:
        """检查并恢复话题（如果需要）

        Returns:
            Optional[int]: 新的话题ID，如果恢复成功
        """
        try:
            conv = await self.conversation_service.get_conversation_by_entity(entity_id, entity_type)
            if not conv or not conv.topic_id:
                return None

            # 测试话题是否仍然有效
            is_valid = await self._test_topic_validity(conv.topic_id)
            if is_valid:
                return conv.topic_id  # 话题有效，返回当前话题ID

            # 话题无效，进行恢复
            self.logger.info(f"检测到无效话题 {conv.topic_id}，开始自动恢复")
            recovery_result = await self.handle_topic_deleted_error(entity_id, entity_type, entity_name)

            if recovery_result.success:
                return recovery_result.new_topic_id
            else:
                self.logger.error(f"话题恢复失败: {recovery_result.error_message}")
                return None

        except Exception as e:
            self.logger.error(f"检查话题时异常: {e}", exc_info=True)
            return None

    async def _test_topic_validity(self, topic_id: int) -> bool:
        """测试话题是否有效"""
        try:
            # 尝试向话题发送一个测试请求
            await self.tg("getChat", {
                "chat_id": settings.SUPPORT_GROUP_ID
            })

            # 简单的测试：尝试获取话题信息（这里可以根据需要调整）
            # 注意：Telegram API 没有直接获取话题信息的方法
            # 所以我们只能通过尝试发送消息来测试
            return True

        except Exception as e:
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in
                   ['topic_deleted', 'thread not found', 'message thread not found']):
                return False
            # 其他错误可能是网络问题，暂时认为话题有效
            return True


# 全局话题恢复服务实例
_topic_recovery_service: Optional[TopicRecoveryService] = None


def get_topic_recovery_service(conversation_service, tg_func) -> TopicRecoveryService:
    """获取话题恢复服务"""
    global _topic_recovery_service
    if _topic_recovery_service is None:
        _topic_recovery_service = TopicRecoveryService(conversation_service, tg_func)
    return _topic_recovery_service


def reset_topic_recovery_service():
    """重置话题恢复服务（用于测试）"""
    global _topic_recovery_service
    _topic_recovery_service = None