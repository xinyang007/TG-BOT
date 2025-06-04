# app/tg_utils.py

import time

import httpx
import json
import logging
import asyncio
from typing import Optional, Dict, Any
from .settings import settings
from .logging_config import get_logger
from .failover_manager import get_failover_manager, FailoverReason
from .circuit_breaker import get_circuit_breaker, CircuitBreakerConfig, CircuitBreakerError
from .failover_events import get_failover_event_store, FailoverEventRecord, EventSeverity
import uuid

logger = get_logger("app.tg_utils")

client = httpx.AsyncClient(timeout=30)

_bot_manager = None
_failover_manager = None
_circuit_breaker_registry = None


class TelegramAPIError(Exception):
    """Telegram API 特定错误，保留详细的错误信息"""

    def __init__(self, description: str, error_code: int = None,
                 http_status: int = None, response_text: str = None):
        self.description = description
        self.error_code = error_code
        self.http_status = http_status
        self.response_text = response_text

        message = f"Telegram API Error: {description}"
        if error_code:
            message += f" (Code: {error_code})"
        if http_status:
            message += f" (HTTP: {http_status})"

        super().__init__(message)

    def __str__(self):
        return self.description

    def is_topic_related(self) -> bool:
        """检查是否是话题相关的错误"""
        if not self.description:
            return False

        topic_keywords = [
            'topic_deleted',
            'thread not found',
            'message thread not found',
            'topic not found',
            'forum topic not found'
        ]

        description_lower = self.description.lower()
        return any(keyword in description_lower for keyword in topic_keywords)


async def get_bot_manager():
    """获取机器人管理器实例"""
    global _bot_manager
    if _bot_manager is None and getattr(settings, 'MULTI_BOT_ENABLED', False):
        try:
            from .bot_manager import get_bot_manager
            _bot_manager = await get_bot_manager()
        except Exception as e:
            logger.warning(f"无法获取机器人管理器: {e}")
    return _bot_manager


def get_base_url(token: str) -> str:
    """根据token构建API基础URL"""
    return f"https://api.telegram.org/bot{token}"


async def tg_with_bot_selection(method: str, data: dict, max_retries: int = 5, initial_delay: int = 1):
    """
    使用机器人选择策略发送请求到 Telegram Bot API（增强故障转移版本）
    """
    bot_manager = await get_bot_manager()
    failover_manager = await get_failover_manager_instance()

    # 如果没有启用多机器人模式或无法获取管理器，使用原始逻辑
    if not bot_manager:
        return await tg_single_bot(method, data, max_retries, initial_delay)

    # 尝试使用健康的机器人
    # get_available_bots 返回的是按负载排序的可用机器人
    available_bots = bot_manager.get_available_bots()
    if not available_bots:
        logger.error("没有可用的机器人")
        # 记录严重故障事件
        if failover_manager:
            await _record_system_failure_event("no_available_bots", "所有机器人不可用")
        # 此时只能尝试 tg_single_bot，但它也可能失败
        return await tg_single_bot(method, data, max_retries, initial_delay)

    # 遍历可用机器人，尝试找到一个没有被熔断的
    for bot in available_bots:
        circuit_breaker = await get_circuit_breaker(f"bot_{bot.bot_id}",
                                                    CircuitBreakerConfig(**settings.get_circuit_breaker_config()))

        try:
            # 通过熔断器检查机器人状态，如果熔断器打开，会抛出 CircuitBreakerError
            await circuit_breaker.call(lambda: _check_bot_readiness(bot))

            logger.debug(f"选择机器人 {bot.bot_id} ({bot.config.name}) 执行 {method}")

            # 使用选中的机器人发送请求
            result = await _execute_bot_request(bot, method, data, max_retries, initial_delay)

            # 记录成功请求
            await bot_manager.record_bot_request(bot.bot_id)
            return result

        except CircuitBreakerError as e:
            logger.warning(f"机器人 {bot.bot_id} 熔断器开启，跳过该机器人: {e}")
            # CircuitBreakerError 不会触发故障转移，因为熔断器已经处理了状态
            continue  # 尝试下一个机器人

        except Exception as e:
            # 其他错误，可能是连接问题、API错误等
            logger.warning(f"机器人 {bot.bot_id} 执行请求失败: {e}. 尝试下一个机器人。", exc_info=True)
            # 在这里触发故障转移，并继续尝试下一个机器人
            await _handle_bot_api_exception(bot, e, failover_manager)
            continue

    # 如果所有机器人尝试都失败了 (都被熔断或都失败了)
    logger.error(f"所有可用机器人尝试 {method} 失败")
    # 此时，作为最后的手段，尝试使用 tg_single_bot （如果它仍然存在）
    # 但这通常意味着系统已处于非常不健康的状态
    return await tg_single_bot(method, data, max_retries, initial_delay)


async def _execute_with_failover(bot, method: str, data: dict, max_retries: int, initial_delay: int):
    """带故障转移的请求执行 (此函数逻辑已并入tg_with_bot_selection，不再需要单独调用)"""
    # 此函数实际上已经不再被直接调用，其逻辑已整合到 tg_with_bot_selection 中
    # 保留它只是为了避免删除可能存在的外部引用，但在当前设计中它是冗余的
    logger.warning("_execute_with_failover 函数被调用，但这可能是不必要的。请检查调用栈。")
    return await _execute_bot_request(bot, method, data, max_retries, initial_delay)


async def _execute_bot_request(bot_instance, method: str, data: dict, max_retries: int, initial_delay: int):
    """执行机器人请求的核心逻辑，会抛出TelegramAPIError"""
    # 这里的 `bot_instance` 是一个 BotInstance 对象，包含 token 和 bot_id
    # CircuitBreaker 和 FailoverManager 已经由调用者处理，这里只管执行请求和抛出异常
    return await tg_with_specific_bot(
        bot_instance.config.token, method, data, max_retries, initial_delay,
        bot_id_for_logging=bot_instance.bot_id  # 传入bot_id用于tg_with_specific_bot的日志
    )


def _check_bot_readiness(bot) -> bool:
    """检查机器人就绪状态（同步函数，供熔断器使用）"""
    # 简单的就绪检查
    if not bot.is_available():
        raise Exception(f"机器人 {bot.bot_id} 不可用")
    return True


async def _record_system_failure_event(reason: str, description: str):
    """记录系统级故障事件"""
    try:
        event_store = await get_failover_event_store()
        event = FailoverEventRecord(
            event_id=str(uuid.uuid4())[:8],
            bot_id="system",
            event_type="system_failure",
            severity=EventSeverity.CRITICAL,
            timestamp=time.time(),
            description=description,
            metadata={"reason": reason}
        )
        await event_store.store_event(event)
    except Exception as e:
        logger.error(f"记录系统故障事件失败: {e}")


# Helper to handle exceptions from _execute_bot_request and notify failover manager
async def _handle_bot_api_exception(bot_instance, exception: Exception, failover_manager):
    """处理机器人API调用异常并通知故障转移管理器"""
    from .failover_manager import FailoverReason  # Ensure import
    error_str = str(exception).lower()
    reason = FailoverReason.API_ERROR  # Default reason

    if "429" in error_str or "too many requests" in error_str:
        reason = FailoverReason.RATE_LIMITED
        logger.warning(f"机器人 {bot_instance.bot_id} 遇到429限速错误")
    elif "connection" in error_str or "timeout" in error_str or isinstance(exception, httpx.RequestError):
        reason = FailoverReason.CONNECTION_ERROR

    if failover_manager:
        await failover_manager.handle_bot_failure(
            bot_instance.bot_id, reason, str(exception)
        )
    else:
        logger.warning(f"未能通知故障转移管理器关于机器人 {bot_instance.bot_id} 的故障 ({reason.value})：{exception}")


async def tg_with_specific_bot(token: str, method: str, data: dict,
                               max_retries: int = 5, initial_delay: int = 1,
                               bot_id_for_logging: Optional[str] = None):
    """
    使用指定token的机器人发送请求到 Telegram Bot API
    """
    url = f"{get_base_url(token)}/{method}"
    retries = 0
    delay = initial_delay
    log_prefix = f"Bot({bot_id_for_logging or 'N/A'}) API Call({method}):"

    while retries <= max_retries:
        try:
            r = await client.post(url, json=data)

            try:
                result = r.json()
            except json.JSONDecodeError:
                # If not JSON, but status is bad, raise specific error
                if r.status_code >= 400:
                    raise TelegramAPIError(
                        description=f"HTTP {r.status_code}: Non-JSON response for {method}. Text: {r.text[:200]}",
                        http_status=r.status_code,
                        response_text=r.text
                    )
                # If not JSON but status is good, something else is wrong
                raise Exception(f"{log_prefix} Unexpected non-JSON response with status {r.status_code}") from None

            if r.status_code >= 400:
                error_code = result.get("error_code", r.status_code)
                description = result.get("description", f"HTTP {r.status_code}")

                detailed_error = TelegramAPIError(
                    description=description,
                    error_code=error_code,
                    http_status=r.status_code,
                    response_text=r.text
                )

                if error_code == 429 and retries < max_retries:
                    retry_after = result.get("parameters", {}).get("retry_after", delay)
                    logger.warning(
                        f"{log_prefix} 429 Rate limited, retry after {retry_after}s. Attempt {retries + 1}/{max_retries + 1}")
                    await asyncio.sleep(retry_after)
                    retries += 1
                    delay *= 2
                    continue
                else:
                    logger.error(f"{log_prefix} Telegram API failed: code={error_code}, description='{description}'")
                    raise detailed_error

            if not result.get("ok"):
                error_code = result.get("error_code", "N/A")
                description = result.get("description", "No description")

                if error_code == 429 and retries < max_retries:
                    retry_after = result.get("parameters", {}).get("retry_after", delay)
                    logger.warning(
                        f"{log_prefix} API returned 429, retry after {retry_after}s. Attempt {retries + 1}/{max_retries + 1}")
                    await asyncio.sleep(retry_after)
                    retries += 1
                    delay *= 2
                    continue
                else:
                    logger.error(
                        f"{log_prefix} Telegram API reported failure: code={error_code}, description='{description}'")
                    raise TelegramAPIError(
                        description=description,
                        error_code=error_code,
                        http_status=r.status_code,
                        response_text=r.text
                    )

            return result.get("result")

        except TelegramAPIError:
            raise

        except httpx.RequestError as e:
            logger.error(f"{log_prefix} Request error: {e}", exc_info=True)
            if retries < max_retries:
                logger.warning(
                    f"{log_prefix} Request error, retrying in {delay}s. Attempt {retries + 1}/{max_retries + 1}")
                await asyncio.sleep(delay)
                retries += 1
                delay *= 2
                continue
            else:
                raise

        except Exception as e:
            logger.error(f"{log_prefix} Unexpected error: {e}", exc_info=True)
            raise

    logger.error(f"{log_prefix} Failed after {max_retries + 1} attempts.")
    raise Exception(f"{log_prefix} Failed after multiple retries.")


async def tg_single_bot(method: str, data: dict, max_retries: int = 5, initial_delay: int = 1):
    """
    使用单机器人模式（原始逻辑），支持向后兼容
    """
    token = getattr(settings, 'BOT_TOKEN', '') or getattr(settings, 'PRIMARY_BOT_TOKEN', '')
    if not token:
        raise ValueError("未设置机器人Token")

    return await tg_with_specific_bot(token, method, data, max_retries, initial_delay, bot_id_for_logging="SINGLE_BOT")


async def tg(method: str, data: dict, specific_bot_token: Optional[str] = None, max_retries: int = 5,
             initial_delay: int = 1):
    """
    主要的API调用函数，自动选择单机器人或多机器人模式，并支持指定优先机器人。

    Args:
        method: API 方法名
        data: API 方法的参数字典
        specific_bot_token: 可选。如果提供，将优先尝试使用此token对应的机器人。
                            如果该机器人失败，会回退到自动选择其他机器人。
        max_retries: 最大重试次数
        initial_delay: 初始重试等待秒数

    Returns:
        Telegram API 响应中的 'result' 部分的 JSON 数据
    """
    bot_manager = await get_bot_manager()
    failover_manager = await get_failover_manager_instance()

    # 如果没有启用多机器人模式，或者没有机器人管理器，回退到单机器人模式
    if not getattr(settings, 'MULTI_BOT_ENABLED', False) or not bot_manager:
        return await tg_single_bot(method, data, max_retries, initial_delay)

    # 优先尝试使用 specific_bot_token
    if specific_bot_token:
        # 查找对应的 BotInstance
        preferred_bot = None
        for bot_id, bot_instance in bot_manager.bots.items():
            if bot_instance.config.token == specific_bot_token:
                preferred_bot = bot_instance
                break

        if preferred_bot and preferred_bot.is_available():
            circuit_breaker = await get_circuit_breaker(f"bot_{preferred_bot.bot_id}",
                                                        CircuitBreakerConfig(**settings.get_circuit_breaker_config()))
            try:
                # 通过熔断器尝试使用此特定机器人
                await circuit_breaker.call(lambda: _check_bot_readiness(preferred_bot))
                logger.debug(f"优先使用指定机器人 {preferred_bot.bot_id} ({preferred_bot.config.name}) 执行 {method}")
                result = await _execute_bot_request(preferred_bot, method, data, max_retries, initial_delay)
                await bot_manager.record_bot_request(preferred_bot.bot_id)
                return result
            except CircuitBreakerError as e:
                logger.warning(f"指定机器人 {preferred_bot.bot_id} 熔断器开启，回退到自动选择: {e}")
                # Fall through to auto-selection below
            except Exception as e:
                logger.warning(f"指定机器人 {preferred_bot.bot_id} 执行请求失败，回退到自动选择: {e}", exc_info=True)
                await _handle_bot_api_exception(preferred_bot, e, failover_manager)
                # Fall through to auto-selection below
        else:
            if specific_bot_token:  # Log only if a specific token was actually passed
                logger.warning(
                    f"指定机器人 (token ending with {specific_bot_token[-5:]}) 不可用或未找到，回退到自动选择。")

    # 如果没有指定机器人，或者指定机器人不可用/失败，则进行自动选择
    return await tg_with_bot_selection(method, data, max_retries, initial_delay)


async def copy_any(src_chat_id, dst_chat_id, message_id: int, extra_params: dict | None = None,
                   specific_bot_token: Optional[str] = None):
    """
    复制消息的辅助函数，支持指定机器人token
    """
    payload = {
        "chat_id": dst_chat_id,
        "from_chat_id": src_chat_id,
        "message_id": message_id,
        "allow_sending_without_reply": True
    }
    if extra_params:
        payload.update(extra_params)

    logger.debug(f"复制消息 {message_id} 从 {src_chat_id} 到 {dst_chat_id}")
    return await tg("copyMessage", payload, specific_bot_token=specific_bot_token)


# 在 send_with_prefix 函数中增强话题恢复的故障转移逻辑
async def send_with_prefix(source_chat_id, dest_chat_id, message_thread_id, sender_name, msg,
                           conversation_service=None, entity_id=None, entity_type=None, entity_name=None,
                           specific_bot_token: Optional[str] = None):  # 新增 specific_bot_token
    """发送带前缀的消息（增强故障转移版本）"""
    prefix = f"👤 {sender_name or '未知发送者'}:\n"
    msg_to_send = msg.copy()

    original_body = msg_to_send.get("text") or msg_to_send.get("caption")

    if original_body is not None:
        if "text" in msg_to_send and msg_to_send.get("text") is not None:
            msg_to_send["text"] = prefix + msg_to_send.get("text", "")
        elif "caption" in msg_to_send and msg_to_send.get("caption") is not None:
            msg_to_send["caption"] = prefix + msg_to_send.get("caption", "")

    # 增强的话题恢复处理函数（带故障转移）
    async def handle_topic_recovery_with_failover(error_str: str, auto_tg=None):
        """处理话题恢复（增强故障转移版本）"""
        if not conversation_service or not entity_id or not entity_type:
            logger.warning("话题恢复需要 conversation_service, entity_id 和 entity_type 参数")
            return None

        # 检测话题相关错误
        topic_keywords = ['topic_deleted', 'thread not found', 'message thread not found', 'topic not found',
                          'forum topic not found']
        if any(keyword in error_str.lower() for keyword in topic_keywords):
            logger.warning(f"检测到话题错误: {error_str}，开始话题恢复")

            try:
                from .topic_recovery import get_topic_recovery_service
                recovery_service = get_topic_recovery_service(conversation_service, auto_tg)  # 话题恢复使用auto_tg

                # 使用故障转移管理器记录话题故障
                failover_manager = await get_failover_manager_instance()
                if failover_manager:
                    await _record_topic_failure_event(entity_id, entity_type, error_str)

                recovery_result = await recovery_service.handle_topic_deleted_error(
                    entity_id, entity_type, entity_name
                )

                if recovery_result.success:
                    logger.info(f"✅ 话题恢复成功，新话题ID: {recovery_result.new_topic_id}")

                    # 记录恢复成功事件
                    if failover_manager:
                        await _record_topic_recovery_event(entity_id, entity_type, recovery_result.new_topic_id)

                    return recovery_result.new_topic_id
                else:
                    logger.error(f"❌ 话题恢复失败: {recovery_result.error_message}")

            except Exception as recovery_error:
                logger.error(f"话题恢复过程异常: {recovery_error}", exc_info=True)

        return None

    # 增强的错误处理和重试逻辑
    async def send_message_with_recovery_and_failover(payload, current_message_thread_id):
        """发送消息，包含话题恢复和故障转移功能"""
        try:
            # 尝试发送消息
            return await tg(payload["method"], payload["data"], specific_bot_token=specific_bot_token)

        except Exception as e:
            # 获取详细的错误信息
            error_description = ""
            is_topic_error = False

            if isinstance(e, TelegramAPIError):
                error_description = e.description
                is_topic_error = e.is_topic_related()
                logger.warning(f"Telegram API 错误: {error_description}")
            else:
                error_description = str(e).lower()
                topic_keywords = ['topic_deleted', 'thread not found', 'message thread not found', 'topic not found',
                                  'forum topic not found']
                is_topic_error = any(keyword in error_description for keyword in topic_keywords)
                logger.warning(f"发送消息失败: {error_description}")

            # 尝试话题恢复
            if is_topic_error:
                logger.warning(f"检测到话题相关错误: {error_description}，尝试话题恢复")
                new_topic_id = await handle_topic_recovery_with_failover(error_description)

                if new_topic_id and new_topic_id != current_message_thread_id:
                    # 使用新话题ID重试
                    logger.info(f"使用新话题ID {new_topic_id} 重试发送消息")
                    recovery_data = payload["data"].copy()
                    recovery_data["message_thread_id"] = new_topic_id

                    try:
                        result = await tg(payload["method"], recovery_data, specific_bot_token=specific_bot_token)
                        logger.info("✅ 使用恢复的话题成功发送消息")
                        return result
                    except Exception as recovery_send_error:
                        logger.error(f"使用恢复话题发送仍然失败: {recovery_send_error}")

                # 话题恢复失败或新话题ID与原ID相同（可能意味着没有实际恢复），尝试移除话题ID
                logger.warning("话题恢复失败或未改变话题ID，尝试移除话题ID重新发送")
                fallback_data = payload["data"].copy()
                fallback_data.pop("message_thread_id", None)

                try:
                    result = await tg(payload["method"], fallback_data, specific_bot_token=specific_bot_token)
                    logger.info(f"✅ 成功通过移除话题ID发送消息")
                    return result
                except Exception as fallback_error:
                    logger.error(f"移除话题ID后仍然失败: {fallback_error}")
                    raise fallback_error
            else:
                # 非话题相关错误直接抛出
                raise e

    # 使用增强的发送函数
    try:
        if "photo" in msg_to_send:
            photo = sorted(msg_to_send.get("photo"), key=lambda x: x.get("width", 0), reverse=True)[
                0] if msg_to_send.get("photo") else None
            if photo:
                logger.debug(f"发送图片消息到话题 {message_thread_id}")
                return await send_message_with_recovery_and_failover({
                    "method": "sendPhoto",
                    "data": {
                        "chat_id": dest_chat_id,
                        "message_thread_id": message_thread_id,
                        "photo": photo.get("file_id"),
                        "caption": msg_to_send.get("caption"),
                        "parse_mode": "HTML"
                    }
                }, message_thread_id)  # 传入当前message_thread_id
        elif "video" in msg_to_send:
            logger.debug(f"发送视频消息到话题 {message_thread_id}")
            return await send_message_with_recovery_and_failover({
                "method": "sendVideo",
                "data": {
                    "chat_id": dest_chat_id,
                    "message_thread_id": message_thread_id,
                    "video": msg_to_send.get("video", {}).get("file_id"),
                    "caption": msg_to_send.get("caption"),
                    "parse_mode": "HTML"
                }
            }, message_thread_id)
        elif "document" in msg_to_send:
            logger.debug(f"发送文档消息到话题 {message_thread_id}")
            return await send_message_with_recovery_and_failover({
                "method": "sendDocument",
                "data": {
                    "chat_id": dest_chat_id,
                    "message_thread_id": message_thread_id,
                    "document": msg_to_send.get("document", {}).get("file_id"),
                    "caption": msg_to_send.get("caption"),
                    "parse_mode": "HTML"
                }
            }, message_thread_id)
        elif "text" in msg_to_send and msg_to_send.get("text") is not None:
            logger.debug(f"发送文本消息到话题 {message_thread_id}")
            return await send_message_with_recovery_and_failover({
                "method": "sendMessage",
                "data": {
                    "chat_id": dest_chat_id,
                    "message_thread_id": message_thread_id,
                    "text": msg_to_send.get("text"),
                    "parse_mode": "HTML"
                }
            }, message_thread_id)
        else:
            # 回退到 copyMessage
            logger.debug(f"回退到复制消息模式")
            try:
                # copy_any 也需要 specific_bot_token
                return await copy_any(source_chat_id, dest_chat_id, msg_to_send.get("message_id"),
                                      {"message_thread_id": message_thread_id},
                                      specific_bot_token=specific_bot_token)
            except Exception as copy_error:
                error_str = str(copy_error).lower()

                # 尝试话题恢复
                new_topic_id = await handle_topic_recovery_with_failover(error_str)
                if new_topic_id and new_topic_id != message_thread_id:
                    logger.info(f"使用恢复的话题ID {new_topic_id} 重试复制消息")
                    try:
                        return await copy_any(source_chat_id, dest_chat_id, msg_to_send.get("message_id"),
                                              {"message_thread_id": new_topic_id},
                                              specific_bot_token=specific_bot_token)
                    except Exception as recovery_copy_error:
                        logger.error(f"使用恢复话题复制消息仍然失败: {recovery_copy_error}")

                # 最后回退：不使用话题复制
                if any(keyword in error_str for keyword in
                       ['topic_deleted', 'thread not found', 'message thread not found', 'topic not found',
                        'forum topic not found']):
                    logger.warning("话题无效，使用无话题的复制")
                    # 无话题复制也需要 specific_bot_token
                    return await copy_any(source_chat_id, dest_chat_id, msg_to_send.get("message_id"), {},
                                          specific_bot_token=specific_bot_token)
                else:
                    raise copy_error

    except Exception as e:
        logger.error(f"发送带前缀消息失败: {e}", exc_info=True)

        # 最后的回退：直接发送到群组（不使用话题）
        try:
            logger.warning("尝试最后的回退方案：直接发送到群组")
            simple_text = f"{prefix}{original_body or '无法转发的消息内容'}"

            await tg("sendMessage", {
                "chat_id": dest_chat_id,
                "text": simple_text[:4096],  # 限制长度
                "parse_mode": "HTML"
            }, specific_bot_token=specific_bot_token)  # 传入 specific_bot_token
            logger.info("成功通过回退方案发送消息")

        except Exception as final_error:
            logger.error(f"所有发送方案都失败: {final_error}")
            raise final_error


async def _record_topic_failure_event(entity_id, entity_type, error_description):
    """记录话题故障事件"""
    try:
        event_store = await get_failover_event_store()
        event = FailoverEventRecord(
            event_id=str(uuid.uuid4())[:8],
            bot_id=f"{entity_type}_{entity_id}",  # 使用 entity_type_entity_id 作为 bot_id
            event_type="topic_failure",
            severity=EventSeverity.MEDIUM,
            timestamp=time.time(),
            description=f"话题故障: {error_description}",
            metadata={
                "entity_id": entity_id,
                "entity_type": entity_type,
                "error": error_description
            }
        )
        await event_store.store_event(event)
    except Exception as e:
        logger.error(f"记录话题故障事件失败: {e}")


async def _record_topic_recovery_event(entity_id, entity_type, new_topic_id):
    """记录话题恢复事件"""
    try:
        event_store = await get_failover_event_store()
        event = FailoverEventRecord(
            event_id=str(uuid.uuid4())[:8],
            bot_id=f"{entity_type}_{entity_id}",  # 使用 entity_type_entity_id 作为 bot_id
            event_type="topic_recovery",
            severity=EventSeverity.LOW,
            timestamp=time.time(),
            description=f"话题恢复成功，新话题ID: {new_topic_id}",
            metadata={
                "entity_id": entity_id,
                "entity_type": entity_type,
                "new_topic_id": new_topic_id
            },
            resolved=True,
            resolution_time=time.time()
        )
        await event_store.store_event(event)
    except Exception as e:
        logger.error(f"记录话题恢复事件失败: {e}")


# 为了向后兼容，保留原函数签名的包装器
# NOTE: 移除这个包装器，直接修改send_with_prefix的调用处，或者在send_with_prefix内部处理
# async def send_with_prefix_legacy(source_chat_id, dest_chat_id, message_thread_id, sender_name, msg):
#     """向后兼容的包装器"""
#     return await send_with_prefix(
#         source_chat_id, dest_chat_id, message_thread_id, sender_name, msg
#     )


async def close_http_client():
    """关闭HTTP客户端"""
    logger.info("关闭HTTP客户端...")
    try:
        await client.aclose()
        logger.info("HTTP客户端已关闭")
    except Exception as e:
        logger.warning(f"关闭HTTP客户端时出错: {e}")


# 多机器人管理的便利函数
async def get_bot_status():
    """获取所有机器人状态"""
    bot_manager = await get_bot_manager()
    if bot_manager:
        return bot_manager.get_all_bots_status()
    return {"error": "多机器人模式未启用"}


async def switch_to_bot(bot_id: str):
    """切换到指定机器人（用于测试）"""
    bot_manager = await get_bot_manager()
    if bot_manager:
        bot = bot_manager.get_bot_by_id(bot_id)
        if bot:
            return f"找到机器人: {bot.config.name}"
        return f"未找到机器人: {bot_id}"
    return "多机器人模式未启用"


async def get_failover_manager_instance():
    """获取故障转移管理器实例"""
    global _failover_manager
    if _failover_manager is None and getattr(settings, 'MULTI_BOT_ENABLED', False):
        try:
            _failover_manager = await get_failover_manager()
        except Exception as e:
            logger.warning(f"无法获取故障转移管理器: {e}")
    return _failover_manager