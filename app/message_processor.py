# app/message_processor.py

import asyncio
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass

from .logging_config import get_logger, get_message_logger
from .validation import validate_webhook_update, validate_telegram_message, ValidationError
from .monitoring import record_message_processing
from .handlers import private, group
from .settings import settings
from .message_coordinator import CoordinationResult

# from .tg_utils import tg_with_specific_bot # 这个导入现在不需要了，因为我们直接用 tg_utils.tg


logger = get_logger("app.message_processor")


@dataclass
class ProcessingResult:
    """消息处理结果"""
    success: bool
    error_message: Optional[str] = None
    processing_time: float = 0.0
    bot_id: Optional[str] = None
    retry_recommended: bool = False


class MessageProcessor:
    """消息处理器 - 负责实际执行消息处理逻辑"""

    def __init__(self, conversation_service):
        self.conversation_service = conversation_service
        self.logger = get_logger("app.message_processor")

    async def process_message(self, queued_msg, bot_instance) -> ProcessingResult:
        """
        处理单个消息

        Args:
            queued_msg: 队列中的消息对象
            bot_instance: 分配的机器人实例

        Returns:
            ProcessingResult: 处理结果
        """
        start_time = time.time()
        bot_id = bot_instance.bot_id if bot_instance else None
        specific_bot_token = bot_instance.config.token if bot_instance else None  # 获取具体的bot token

        if not specific_bot_token:
            self.logger.error(f"消息 {queued_msg.message_id} 没有有效的机器人Token，无法处理。")
            return ProcessingResult(
                success=False,
                error_message="No valid bot token",
                processing_time=time.time() - start_time,
                bot_id=bot_id,
                retry_recommended=False
            )

        try:
            # 验证消息格式
            raw_update = queued_msg.payload

            try:
                validated_update = validate_webhook_update(raw_update)
            except ValidationError as e:
                self.logger.warning(
                    f"消息 {queued_msg.message_id} 验证失败: {e.message}"
                )
                return ProcessingResult(
                    success=False,
                    error_message=f"消息验证失败: {e.message}",
                    processing_time=time.time() - start_time,
                    bot_id=bot_id
                )

            # 获取消息数据
            msg_data = validated_update.get_message()
            if not msg_data:
                return ProcessingResult(
                    success=True,
                    error_message="非消息类型更新",
                    processing_time=time.time() - start_time,
                    bot_id=bot_id
                )

            # 验证消息内容
            try:
                validated_message = validate_telegram_message(msg_data)
            except ValidationError as e:
                self.logger.warning(
                    f"消息内容验证失败: {e.message}"
                )
                return ProcessingResult(
                    success=False,
                    error_message=f"消息内容验证失败: {e.message}",
                    processing_time=time.time() - start_time,
                    bot_id=bot_id
                )

            # 获取基本信息
            chat_type = validated_message.chat.get("type")
            chat_id = validated_message.get_chat_id()
            msg_id = validated_message.message_id
            user_id = validated_message.get_user_id()
            user_name = validated_message.get_user_name()

            # 创建消息日志器
            msg_logger = get_message_logger(
                message_id=msg_id,
                chat_id=chat_id,
                operation="coordinated_processing"
            )

            msg_logger.info(
                f"开始处理消息 (机器人: {bot_id})",
                extra={
                    "message_id": queued_msg.message_id,
                    "chat_type": chat_type,
                    "user_id": user_id,
                    "user_name": user_name,
                    "bot_id": bot_id,
                    "specific_bot_token_used": specific_bot_token  # 添加此日志
                }
            )

            # **核心修改点：将 specific_bot_token 传递给 handle_private/handle_group**
            # 这要求 private.py 和 group.py 中的 handle_private/handle_group 签名修改
            # 它们再将此 token 传递给 conversation_service 的相关方法
            # conversation_service 的方法再传递给 tg_utils.tg

            try:
                if chat_type == "private":
                    await private.handle_private(msg_data, self.conversation_service,
                                                 specific_bot_token=specific_bot_token)
                    record_message_processing("private", time.time() - start_time, True)
                    msg_logger.info("私聊消息处理完成")

                elif chat_type in ("group", "supergroup"):
                    if str(chat_id) == settings.SUPPORT_GROUP_ID:
                        await group.handle_group(msg_data, self.conversation_service,
                                                 specific_bot_token=specific_bot_token)
                        record_message_processing("support_group", time.time() - start_time, True)
                        msg_logger.info("客服群组消息处理完成")
                    else:
                        await group.handle_group(msg_data, self.conversation_service,
                                                 specific_bot_token=specific_bot_token)
                        record_message_processing("external_group", time.time() - start_time, True)
                        msg_logger.info("外部群组消息处理完成")
                else:
                    msg_logger.debug(f"忽略未处理的聊天类型: {chat_type}")
                    return ProcessingResult(
                        success=True,
                        error_message=f"未支持的聊天类型: {chat_type}",
                        processing_time=time.time() - start_time,
                        bot_id=bot_id
                    )

                return ProcessingResult(
                    success=True,
                    processing_time=time.time() - start_time,
                    bot_id=bot_id
                )

            finally:
                # 之前在这里恢复 tg_func 的代码块，现在可以完全移除
                pass

        except Exception as processing_error:
            self.logger.error(
                f"消息 {queued_msg.message_id} 处理异常",
                extra={"processing_error": str(processing_error)},
                exc_info=True
            )

            retry_recommended = self._should_retry_error(processing_error)

            record_message_processing(
                queued_msg.chat_type or "unknown",
                time.time() - start_time,
                False
            )

            return ProcessingResult(
                success=False,
                error_message=str(processing_error),
                processing_time=time.time() - start_time,
                bot_id=bot_id,
                retry_recommended=retry_recommended
            )

    def _should_retry_error(self, error: Exception) -> bool:
        error_str = str(error).lower()

        if any(keyword in error_str for keyword in ['timeout', 'connection', 'network', 'httpx']):
            return True

        if '429' in error_str or 'too many requests' in error_str:
            return True

        if any(keyword in error_str for keyword in ['502', '503', '504', 'service unavailable']):
            return True

        if any(keyword in error_str for keyword in
               ['topic_deleted', 'thread not found', 'message thread not found', 'topic not found',
                'forum topic not found']):
            return False

        return False


class CoordinatedMessageHandler:
    """协调式消息处理器 - 集成消息协调器和处理器"""

    def __init__(self, message_coordinator, conversation_service):
        self.message_coordinator = message_coordinator
        self.message_processor = MessageProcessor(conversation_service)
        self.logger = get_logger("app.coordinated_handler")

    async def handle_webhook_message(self, raw_update: Dict[str, Any]) -> str:
        try:
            result = await self.message_coordinator.coordinate_message(raw_update)

            if result == CoordinationResult.QUEUED:
                self.logger.debug("消息已成功提交到协调器")
                return "queued"
            if result == CoordinationResult.DUPLICATE:
                self.logger.debug("检测到重复消息，已跳过")
                return "duplicate"

            self.logger.error("消息协调失败")
            return "coordination_failed"

        except Exception as e:
            self.logger.error(f"消息协调异常: {e}", exc_info=True)
            return "coordination_error"

    async def process_queued_message(self, queued_msg, bot_instance) -> ProcessingResult:
        result = await self.message_processor.process_message(queued_msg, bot_instance)
        _processing_stats.record_processing(result)
        return result


async def create_coordinated_handler(conversation_service):
    from .message_coordinator import get_message_coordinator

    coordinator = await get_message_coordinator()
    handler = CoordinatedMessageHandler(coordinator, conversation_service)
    coordinator._message_processor_callback = handler.process_queued_message
    return handler


class MessageProcessingStats:
    """消息处理统计"""

    def __init__(self):
        self.processed_count = 0
        self.failed_count = 0
        self.total_processing_time = 0.0
        self.last_processed = None
        self.bot_usage = {}

    def record_processing(self, result: ProcessingResult):
        if result.success:
            self.processed_count += 1
        else:
            self.failed_count += 1

        self.total_processing_time += result.processing_time
        self.last_processed = time.time()

        if result.bot_id:
            if result.bot_id not in self.bot_usage:
                self.bot_usage[result.bot_id] = {"success": 0, "failed": 0, "total_time": 0.0}

            if result.success:
                self.bot_usage[result.bot_id]["success"] += 1
            else:
                self.bot_usage[result.bot_id]["failed"] += 1

            self.bot_usage[result.bot_id]["total_time"] += result.processing_time

    def get_stats(self) -> Dict[str, Any]:
        total_count = self.processed_count + self.failed_count
        success_rate = (self.processed_count / total_count * 100) if total_count > 0 else 0
        avg_processing_time = (self.total_processing_time / total_count) if total_count > 0 else 0

        return {
            "total_processed": total_count,
            "success_count": self.processed_count,
            "failed_count": self.failed_count,
            "success_rate": round(success_rate, 2),
            "average_processing_time": round(avg_processing_time, 3),
            "total_processing_time": round(self.total_processing_time, 3),
            "last_processed": self.last_processed,
            "bot_usage": self.bot_usage
        }


_processing_stats = MessageProcessingStats()


def get_processing_stats() -> MessageProcessingStats:
    return _processing_stats