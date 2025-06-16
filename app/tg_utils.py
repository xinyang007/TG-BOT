import html

import httpx
import json
import logging
import asyncio  # 导入 asyncio 用于 sleep
from typing import Optional, Dict, Any
from .settings import settings  # 使用加载的设置
from .logging_config import get_logger

logger = get_logger("app.tg_utils")

# 使用一个 httpx 客户端实例，可以在应用生命周期内重用
client = httpx.AsyncClient(timeout=30)  # 增加超时时间，特别是对于可能需要等待的 API

# 全局机器人管理器引用
_bot_manager = None


class TelegramAPIError(Exception):
    """Telegram API 特定错误，保留详细的错误信息"""

    def __init__(
            self,
            description: str,
            error_code: int = None,
            http_status: int = None,
            response_text: str = None,
    ):
        self.description = description
        self.error_code = error_code
        self.http_status = http_status
        self.response_text = response_text

        # 构造错误消息
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
            "topic_deleted",
            "thread not found",
            "message thread not found",
            "topic not found",
            "forum topic not found",
        ]

        description_lower = self.description.lower()
        return any(keyword in description_lower for keyword in topic_keywords)


async def get_bot_manager():
    """获取机器人管理器实例"""
    global _bot_manager
    if _bot_manager is None and getattr(settings, "MULTI_BOT_ENABLED", False):
        try:
            from .bot_manager import get_bot_manager
            _bot_manager = await get_bot_manager()
        except Exception as e:
            logger.warning(f"无法获取机器人管理器: {e}")
    return _bot_manager


def get_base_url(token: str) -> str:
    """根据token构建API基础URL"""
    return f"https://api.telegram.org/bot{token}"


async def tg_with_bot_selection(
        method: str, data: dict, max_retries: int = 5, initial_delay: int = 1
):
    """
    使用机器人选择策略发送请求到 Telegram Bot API

    Args:
        method: API 方法名
        data: API 方法的参数字典
        max_retries: 最大重试次数
        initial_delay: 初始重试等待秒数

    Returns:
        Telegram API 响应中的 'result' 部分的 JSON 数据
    """
    bot_manager = await get_bot_manager()

    # 如果没有启用多机器人模式或无法获取管理器，使用原始逻辑
    if not bot_manager:
        return await tg_single_bot(method, data, max_retries, initial_delay)

    # 尝试使用健康的机器人
    healthy_bots = bot_manager.get_healthy_bots()
    if not healthy_bots:
        logger.error("没有可用的健康机器人，尝试使用主机器人")
        return await tg_single_bot(method, data, max_retries, initial_delay)

    # 按优先级和负载选择最佳机器人
    best_bot = bot_manager.get_best_bot()
    if not best_bot:
        logger.error("无法选择最佳机器人，回退到主机器人")
        return await tg_single_bot(method, data, max_retries, initial_delay)

    logger.debug(f"选择机器人 {best_bot.bot_id} ({best_bot.config.name}) 执行 {method}")

    try:
        # 使用选中的机器人发送请求
        result = await tg_with_specific_bot(
            best_bot.config.token, method, data, max_retries, initial_delay
        )

        # 记录成功请求
        await bot_manager.record_bot_request(best_bot.bot_id)
        return result

    except Exception as e:
        logger.warning(f"机器人 {best_bot.bot_id} 请求失败: {e}")

        # 检查是否是429错误
        if "429" in str(e) or "Too Many Requests" in str(e):
            await bot_manager.mark_bot_rate_limited(best_bot.bot_id, 60)

            # 尝试使用其他健康机器人
            other_bots = [bot for bot in healthy_bots if bot.bot_id != best_bot.bot_id]
            for fallback_bot in other_bots:
                try:
                    logger.info(f"尝试使用备用机器人 {fallback_bot.bot_id}")
                    result = await tg_with_specific_bot(
                        fallback_bot.config.token,
                        method,
                        data,
                        max_retries,
                        initial_delay,
                    )
                    await bot_manager.record_bot_request(fallback_bot.bot_id)
                    return result
                except Exception as fallback_e:
                    logger.warning(
                        f"备用机器人 {fallback_bot.bot_id} 也失败: {fallback_e}"
                    )
                    continue

        # 如果所有机器人都失败，抛出最后的异常
        raise


async def tg_with_specific_bot(
        token: str, method: str, data: dict, max_retries: int = 5, initial_delay: int = 1
):
    """
    使用指定token的机器人发送请求到 Telegram Bot API
    """
    url = f"{get_base_url(token)}/{method}"
    retries = 0
    delay = initial_delay

    while retries <= max_retries:
        try:
            r = await client.post(url, json=data)

            # 先获取响应内容（无论状态码如何）
            try:
                result = r.json()
            except:
                # 如果不能解析 JSON，创建基本错误信息
                if r.status_code >= 400:
                    raise TelegramAPIError(
                        description=f"HTTP {r.status_code}: {r.text[:200]}",
                        http_status=r.status_code,
                        response_text=r.text,
                    )
                raise

            # 检查 HTTP 状态码
            if r.status_code >= 400:
                # 从 Telegram API 响应中提取详细错误信息
                error_code = result.get("error_code", r.status_code)
                description = result.get("description", f"HTTP {r.status_code}")

                # 创建包含详细信息的异常
                detailed_error = TelegramAPIError(
                    description=description,
                    error_code=error_code,
                    http_status=r.status_code,
                    response_text=r.text,
                )

                # 如果是 429 错误，进行重试逻辑
                if error_code == 429 and retries < max_retries:
                    retry_after = result.get("parameters", {}).get("retry_after", delay)
                    logger.warning(
                        f"机器人被限速，{retry_after} 秒后重试。尝试 {retries + 1}/{max_retries + 1}"
                    )
                    await asyncio.sleep(retry_after)
                    retries += 1
                    delay *= 2
                    continue
                else:
                    logger.error(
                        f"Telegram API 失败: method={method}, code={error_code}, description='{description}'"
                    )
                    raise detailed_error

            # 检查 Telegram specific 'ok' field
            if not result.get("ok"):
                error_code = result.get("error_code", "N/A")
                description = result.get("description", "No description")

                if error_code == 429 and retries < max_retries:
                    retry_after = result.get("parameters", {}).get("retry_after", delay)
                    logger.warning(
                        f"Telegram API 返回 429，{retry_after} 秒后重试。尝试 {retries + 1}/{max_retries + 1}"
                    )
                    await asyncio.sleep(retry_after)
                    retries += 1
                    delay *= 2
                    continue
                else:
                    logger.error(
                        f"Telegram API 报告失败: method={method}, code={error_code}, description='{description}'"
                    )
                    raise TelegramAPIError(
                        description=description,
                        error_code=error_code,
                        http_status=r.status_code,
                        response_text=r.text,
                    )

            return result.get("result")  # 成功返回结果

        except TelegramAPIError:
            # 重新抛出我们的自定义异常
            raise

        except httpx.HTTPStatusError as e:
            # 处理其他 HTTP 错误（理论上不应该到这里，因为上面已经处理了）
            error_code = e.response.status_code
            if error_code == 429 and retries < max_retries:
                retry_after_header = e.response.headers.get("Retry-After")
                try:
                    retry_after = (
                        int(retry_after_header) if retry_after_header else delay
                    )
                except ValueError:
                    retry_after = delay

                logger.warning(
                    f"HTTP 429 限速，{retry_after} 秒后重试。尝试 {retries + 1}/{max_retries + 1}"
                )
                await asyncio.sleep(retry_after)
                retries += 1
                delay *= 2
                continue
            else:
                logger.error(
                    f"HTTP 错误 {method}: {e.response.status_code} - {e.response.text}"
                )
                # 尝试从响应中提取 Telegram 错误
                try:
                    response_data = e.response.json()
                    description = response_data.get("description", str(e))
                    raise TelegramAPIError(
                        description=description,
                        error_code=response_data.get("error_code", error_code),
                        http_status=error_code,
                        response_text=e.response.text,
                    )
                except:
                    raise TelegramAPIError(
                        description=str(e),
                        http_status=error_code,
                        response_text=(
                            e.response.text if hasattr(e, "response") else str(e)
                        ),
                    )

        except httpx.RequestError as e:
            logger.error(f"请求错误 {method}: {e}")
            if retries < max_retries:
                logger.warning(
                    f"请求错误重试，{delay} 秒后重试。尝试 {retries + 1}/{max_retries + 1}"
                )
                await asyncio.sleep(delay)
                retries += 1
                delay *= 2
                continue
            else:
                raise

        except Exception as e:
            logger.error(f"调用 {method} 时发生意外错误: {e}")
            raise

    logger.error(f"方法 {method} 在 {max_retries + 1} 次尝试后仍然失败")
    raise Exception(f"方法 {method} 在多次重试后仍然失败")


async def tg_single_bot(
        method: str, data: dict, max_retries: int = 5, initial_delay: int = 1
):
    """
    使用单机器人模式（原始逻辑），支持向后兼容
    """
    # 获取主要token
    token = getattr(settings, "BOT_TOKEN", "") or getattr(
        settings, "PRIMARY_BOT_TOKEN", ""
    )
    if not token:
        raise ValueError("未设置机器人Token")

    return await tg_with_specific_bot(token, method, data, max_retries, initial_delay)


async def tg_primary_bot(
        method: str, data: dict, max_retries: int = 5, initial_delay: int = 1
):
    """使用主机器人发送请求"""
    token = settings.get_primary_bot_token()
    if not token:
        raise ValueError("未设置主机器人Token")

    return await tg_with_specific_bot(token, method, data, max_retries, initial_delay)


async def tg(method: str, data: dict, max_retries: int = 5, initial_delay: int = 1):
    """
    主要的API调用函数，自动选择单机器人或多机器人模式

    Args:
        method: API 方法名
        data: API 方法的参数字典
        max_retries: 最大重试次数
        initial_delay: 初始重试等待秒数

    Returns:
        Telegram API 响应中的 'result' 部分的 JSON 数据
    """
    if getattr(settings, "MULTI_BOT_ENABLED", False):
        return await tg_with_bot_selection(method, data, max_retries, initial_delay)
    else:
        return await tg_single_bot(method, data, max_retries, initial_delay)


async def copy_any(
        src_chat_id,
        dst_chat_id,
        message_id: int,
        extra_params: dict | None = None,
        use_primary_bot: bool = False,
):
    """
    复制消息的辅助函数
    """
    payload = {
        "chat_id": dst_chat_id,
        "from_chat_id": src_chat_id,
        "message_id": message_id,
        "allow_sending_without_reply": True,
    }
    if extra_params:
        payload.update(extra_params)

    logger.debug(f"复制消息 {message_id} 从 {src_chat_id} 到 {dst_chat_id}")
    if use_primary_bot:
        return await tg_primary_bot("copyMessage", payload)
    return await tg("copyMessage", payload)


async def send_with_prefix(
        source_chat_id,
        dest_chat_id,
        message_thread_id,
        sender_name,
        msg,
        conversation_service=None,
        entity_id=None,
        entity_type=None,
        entity_name=None,
        use_primary_bot: bool = False,
):
    """发送带前缀的消息，根据消息类型选择不同的发送方法，包含话题恢复功能"""

    # 构建基础前缀
    prefix = f"👤 {sender_name or '未知发送者'}:\n"

    # 处理引用消息 - 新增功能
    reply_context = ""
    if msg.get("reply_to_message"):
        reply_msg = msg["reply_to_message"]
        reply_context = await _build_reply_context(reply_msg)
        if reply_context:
            prefix = f"📝 引用消息:\n{reply_context}\n\n{prefix}"

    # 创建消息副本进行修改
    msg_to_send = msg.copy()

    # 在消息文本或 caption 前添加前缀
    original_body = msg_to_send.get("text") or msg_to_send.get("caption")

    if original_body is not None:
        if "text" in msg_to_send and msg_to_send.get("text") is not None:
            msg_to_send["text"] = prefix + msg_to_send.get("text", "")
        elif "caption" in msg_to_send and msg_to_send.get("caption") is not None:
            msg_to_send["caption"] = prefix + msg_to_send.get("caption", "")

    # 话题恢复处理函数
    async def handle_topic_recovery(error_str: str):
        """处理话题恢复"""
        if not conversation_service or not entity_id or not entity_type:
            logger.warning(
                "话题恢复需要 conversation_service, entity_id 和 entity_type 参数"
            )
            return None

        # 检测话题相关错误
        topic_errors = ["topic_deleted", "thread not found", "message thread not found"]
        if any(keyword in error_str.lower() for keyword in topic_errors):
            logger.warning(f"检测到话题错误: {error_str}，开始话题恢复")

            try:
                from .topic_recovery import get_topic_recovery_service
                recovery_service = get_topic_recovery_service(conversation_service, tg)

                recovery_result = await recovery_service.handle_topic_deleted_error(
                    entity_id, entity_type, entity_name
                )

                if recovery_result.success:
                    logger.info(
                        f"✅ 话题恢复成功，新话题ID: {recovery_result.new_topic_id}"
                    )
                    return recovery_result.new_topic_id
                else:
                    logger.error(f"❌ 话题恢复失败: {recovery_result.error_message}")

            except Exception as recovery_error:
                logger.error(f"话题恢复过程异常: {recovery_error}", exc_info=True)

        return None

    # 增强的错误处理和重试逻辑
    async def send_message_with_recovery(payload):
        """发送消息，包含话题恢复功能"""
        tg_func = tg_primary_bot if use_primary_bot else tg
        try:
            return await tg_func(payload["method"], payload["data"])

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
                # 传统的关键词检测作为后备
                topic_keywords = [
                    "topic_deleted",
                    "thread not found",
                    "message thread not found",
                ]
                is_topic_error = any(
                    keyword in error_description for keyword in topic_keywords
                )
                logger.warning(f"发送消息失败: {error_description}")

            # 尝试话题恢复
            if is_topic_error:
                logger.warning(f"检测到话题相关错误: {error_description}，尝试话题恢复")
                new_topic_id = await handle_topic_recovery(error_description)

                if new_topic_id:
                    # 使用新话题ID重试
                    logger.info(f"使用新话题ID {new_topic_id} 重试发送消息")
                    recovery_data = payload["data"].copy()
                    recovery_data["message_thread_id"] = new_topic_id

                    try:
                        result = await tg_func(payload["method"], recovery_data)
                        logger.info("✅ 使用恢复的话题成功发送消息")
                        return result
                    except Exception as recovery_send_error:
                        logger.error(f"使用恢复话题发送仍然失败: {recovery_send_error}")

                # 话题恢复失败，尝试移除话题ID
                logger.warning("话题恢复失败，尝试移除话题ID重新发送")
                fallback_data = payload["data"].copy()
                fallback_data.pop("message_thread_id", None)

                try:
                    result = await tg_func(payload["method"], fallback_data)
                    logger.info(f"✅ 成功通过移除话题ID发送消息")
                    return result
                except Exception as fallback_error:
                    logger.error(f"移除话题ID后仍然失败: {fallback_error}")
                    raise fallback_error
            else:
                # 非话题相关错误直接抛出
                raise e

    # 根据消息类型选择不同的发送方法
    try:
        if "photo" in msg_to_send:
            photo = (
                sorted(
                    msg_to_send.get("photo"),
                    key=lambda x: x.get("width", 0),
                    reverse=True,
                )[0]
                if msg_to_send.get("photo")
                else None
            )
            if photo:
                logger.debug(f"发送图片消息到话题 {message_thread_id}")
                return await send_message_with_recovery(
                    {
                        "method": "sendPhoto",
                        "data": {
                            "chat_id": dest_chat_id,
                            "message_thread_id": message_thread_id,
                            "photo": photo.get("file_id"),
                            "caption": msg_to_send.get("caption"),
                            "parse_mode": "HTML",
                        },
                    }
                )
        elif "video" in msg_to_send:
            logger.debug(f"发送视频消息到话题 {message_thread_id}")
            return await send_message_with_recovery(
                {
                    "method": "sendVideo",
                    "data": {
                        "chat_id": dest_chat_id,
                        "message_thread_id": message_thread_id,
                        "video": msg_to_send.get("video", {}).get("file_id"),
                        "caption": msg_to_send.get("caption"),
                        "parse_mode": "HTML"
                    },
                }
            )
        elif "document" in msg_to_send:
            logger.debug(f"发送文档消息到话题 {message_thread_id}")
            return await send_message_with_recovery(
                {
                    "method": "sendDocument",
                    "data": {
                        "chat_id": dest_chat_id,
                        "message_thread_id": message_thread_id,
                        "document": msg_to_send.get("document", {}).get("file_id"),
                        "caption": msg_to_send.get("caption"),
                        "parse_mode": "HTML",
                    },
                }
            )
        elif "text" in msg_to_send and msg_to_send.get("text") is not None:
            logger.debug(f"发送文本消息到话题 {message_thread_id}")
            return await send_message_with_recovery(
                {
                    "method": "sendMessage",
                    "data": {
                        "chat_id": dest_chat_id,
                        "message_thread_id": message_thread_id,
                        "text": msg_to_send.get("text"),
                        "parse_mode": "HTML",
                    },
                }
            )
        else:
            # 回退到 copyMessage - 修复消息ID获取逻辑
            logger.debug(f"回退到复制消息模式")

            # 修复：确保消息ID正确获取
            source_message_id = msg.get("message_id")  # 使用原始消息的message_id
            if not source_message_id:
                logger.error("无法获取源消息ID，跳过复制")
                raise Exception("无法获取源消息ID")

            # 验证消息ID是否为有效整数
            try:
                source_message_id = int(source_message_id)
            except (ValueError, TypeError):
                logger.error(f"消息ID格式无效: {source_message_id}")
                raise Exception(f"消息ID格式无效: {source_message_id}")

            logger.debug(f"尝试复制消息 {source_message_id} 从 {source_chat_id} 到 {dest_chat_id}")

            try:
                return await copy_any(
                    source_chat_id,
                    dest_chat_id,
                    source_message_id,  # 使用修复后的消息ID
                    {"message_thread_id": message_thread_id},
                    use_primary_bot=use_primary_bot,
                )
            except Exception as copy_error:
                error_str = str(copy_error).lower()

                # 检查是否是"消息未找到"错误
                if "message to copy not found" in error_str or "message not found" in error_str:
                    logger.warning(f"源消息 {source_message_id} 不存在或已被删除，使用文本回退方案")

                    # 回退方案：发送纯文本消息
                    fallback_text = f"{prefix}{original_body or '消息内容无法复制（原消息可能已被删除）'}"

                    try:
                        tg_func = tg_primary_bot if use_primary_bot else tg
                        return await tg_func(
                            "sendMessage",
                            {
                                "chat_id": dest_chat_id,
                                "message_thread_id": message_thread_id,
                                "text": fallback_text[:4096],  # 限制长度
                                "parse_mode": "HTML",
                            },
                        )
                    except Exception as fallback_error:
                        logger.error(f"文本回退方案也失败: {fallback_error}")
                        raise fallback_error

                # 尝试话题恢复
                new_topic_id = await handle_topic_recovery(error_str)
                if new_topic_id:
                    logger.info(f"使用恢复的话题ID {new_topic_id} 重试复制消息")
                    try:
                        return await copy_any(
                            source_chat_id,
                            dest_chat_id,
                            source_message_id,
                            {"message_thread_id": new_topic_id},
                            use_primary_bot=use_primary_bot,
                        )
                    except Exception as recovery_copy_error:
                        logger.error(
                            f"使用恢复话题复制消息仍然失败: {recovery_copy_error}"
                        )

                # 最后回退：不使用话题复制
                if "thread not found" in error_str or "topic_deleted" in error_str:
                    logger.warning("话题无效，使用无话题的复制")
                    try:
                        return await copy_any(
                            source_chat_id,
                            dest_chat_id,
                            source_message_id,
                            {},
                            use_primary_bot=use_primary_bot,
                        )
                    except Exception as no_topic_error:
                        logger.error(f"无话题复制也失败: {no_topic_error}")
                        # 最终回退到文本消息
                        fallback_text = f"{prefix}{original_body or '消息内容无法复制'}"

                        try:
                            tg_func = tg_primary_bot if use_primary_bot else tg
                            return await tg_func(
                                "sendMessage",
                                {
                                    "chat_id": dest_chat_id,
                                    "text": fallback_text[:4096],
                                    "parse_mode": "HTML",
                                },
                            )
                        except Exception as final_fallback_error:
                            logger.error(f"最终回退方案失败: {final_fallback_error}")
                            raise final_fallback_error
                else:
                    raise copy_error

    except Exception as e:
        logger.error(f"发送带前缀消息失败: {e}", exc_info=True)

        # 最后的回退：直接发送到群组（不使用话题）
        try:
            logger.warning("尝试最后的回退方案：直接发送到群组")
            simple_text = f"{prefix}{original_body or '无法转发的消息内容'}"

            tg_func = tg_primary_bot if use_primary_bot else tg
            await tg_func(
                "sendMessage",
                {
                    "chat_id": dest_chat_id,
                    "text": simple_text[:4096],  # 限制长度
                    "parse_mode": "HTML",
                },
            )
            logger.info("成功通过回退方案发送消息")

        except Exception as final_error:
            logger.error(f"所有发送方案都失败: {final_error}")
            raise final_error


async def _build_reply_context(reply_msg: dict) -> str:
    """构建引用消息的上下文文本"""
    try:
        # 获取引用消息的发送者信息
        reply_sender = reply_msg.get("from", {})
        reply_sender_name = "未知发送者"
        sender_type = "user"  # user, bot, admin

        if reply_sender:
            is_bot = reply_sender.get("is_bot", False)
            sender_id = reply_sender.get("id")

            if is_bot:
                # 机器人消息 - 可能是主机器人或备用机器人发送的客服回复
                bot_first_name = reply_sender.get("first_name", "客服机器人")
                bot_username = reply_sender.get("username", "")

                # 判断是否是已知的机器人
                known_bot_tokens = []
                try:
                    # 获取所有机器人token的bot_id部分用于比较
                    if hasattr(settings, 'BOT_CONFIGS') and settings.BOT_CONFIGS:
                        for bot_config in settings.BOT_CONFIGS:
                            if hasattr(bot_config, 'token') and bot_config.token:
                                bot_id = bot_config.token.split(':')[0]
                                known_bot_tokens.append(int(bot_id))
                    elif settings.BOT_TOKEN:
                        bot_id = settings.BOT_TOKEN.split(':')[0]
                        known_bot_tokens.append(int(bot_id))
                except Exception as e:
                    logger.debug(f"解析机器人token失败: {e}")

                if sender_id in known_bot_tokens:
                    # 这是我们的客服机器人（主机器人或备用机器人）
                    reply_sender_name = f"客服·{bot_first_name}"
                    sender_type = "admin"  # 统一标记为admin类型
                else:
                    # 其他未知机器人
                    reply_sender_name = f"机器人·{bot_first_name}"
                    if bot_username:
                        reply_sender_name += f"@{bot_username}"
                    sender_type = "unknown_bot"
            else:
                # 普通用户消息
                first_name = reply_sender.get("first_name", "")
                last_name = reply_sender.get("last_name", "")
                username = reply_sender.get("username", "")

                if first_name or last_name:
                    reply_sender_name = f"{first_name} {last_name}".strip()
                elif username:
                    reply_sender_name = f"@{username}"
                else:
                    reply_sender_name = f"用户{sender_id}" if sender_id else "匿名用户"

                sender_type = "user"

        # 构建引用消息的时间戳（北京时间 UTC+8）
        time_info = ""
        if reply_msg.get("date"):
            import datetime
            try:
                # Telegram的date是UTC时间戳
                utc_time = datetime.datetime.fromtimestamp(reply_msg["date"], tz=datetime.timezone.utc)
                # 转换为北京时间 (UTC+8)
                beijing_time = utc_time + datetime.timedelta(hours=8)

                # 获取当前北京时间用于判断是否同一天
                current_utc = datetime.datetime.now(tz=datetime.timezone.utc)
                current_beijing = current_utc + datetime.timedelta(hours=8)

                # 如果是今天，只显示时间；如果不是今天，显示日期+时间
                if beijing_time.date() == current_beijing.date():
                    time_info = beijing_time.strftime("%H:%M")
                else:
                    time_info = beijing_time.strftime("%m-%d %H:%M")

            except Exception as e:
                logger.debug(f"时间解析失败: {e}")
                time_info = "时间未知"

        # 提取引用消息的内容
        reply_content = _extract_message_content(reply_msg)

        # 智能清理转发内容 - 修改逻辑
        if sender_type == "admin" and reply_content:
            # 对于我们自己的机器人消息，进行智能清理
            cleaned_content = _smart_clean_forwarded_content(reply_content)
            # 只有清理后的内容明显更好时才使用清理后的版本
            if cleaned_content and len(cleaned_content.strip()) >= len(reply_content.strip()) * 0.3:
                reply_content = cleaned_content

        # 限制引用内容长度
        max_length = 120
        if len(reply_content) > max_length:
            reply_content = reply_content[:max_length - 3] + "..."

        # 根据发送者类型选择不同的图标和格式
        if sender_type == "admin":
            icon = "💬"
            type_label = "客服"
        elif sender_type == "unknown_bot":
            icon = "🤖"
            type_label = "机器人"
        else:
            icon = "👤"
            type_label = "用户"

        # 构建更简洁美观的引用格式
        if time_info:
            header = f"{icon} 引用 {type_label} {reply_sender_name} ({time_info}):"
        else:
            header = f"{icon} 引用 {type_label} {reply_sender_name}:"

        # 使用引号包围内容，多行时使用特殊格式
        if '\n' in reply_content:
            # 多行内容
            lines = reply_content.split('\n')
            formatted_content = f"┌ {lines[0]}\n"
            for line in lines[1:-1]:
                formatted_content += f"│ {line}\n"
            if len(lines) > 1:
                formatted_content += f"└ {lines[-1]}"
            context = f"{header}\n{formatted_content}"
        else:
            # 单行内容
            context = f"{header}\n「{reply_content}」"

        return context

    except Exception as e:
        logger.error(f"构建引用消息上下文失败: {e}", exc_info=True)
        return "💬 引用消息解析失败"


def _extract_message_content(msg: dict) -> str:
    """提取消息内容"""
    if msg.get("text"):
        return msg["text"]
    elif msg.get("caption"):
        return msg["caption"]
    elif msg.get("photo"):
        content = "📸 图片"
        if msg.get("caption"):
            content += f" | {msg['caption']}"
        return content
    elif msg.get("video"):
        content = "🎥 视频"
        if msg.get("caption"):
            content += f" | {msg['caption']}"
        return content
    elif msg.get("document"):
        doc = msg.get("document", {})
        doc_name = doc.get("file_name", "")
        file_size = doc.get("file_size", 0)

        content = "📄 文档"
        if doc_name:
            content += f": {doc_name}"
        if file_size and file_size > 0:
            # 转换文件大小为可读格式
            if file_size < 1024:
                size_str = f"{file_size}B"
            elif file_size < 1024 * 1024:
                size_str = f"{file_size / 1024:.1f}KB"
            else:
                size_str = f"{file_size / (1024 * 1024):.1f}MB"
            content += f" ({size_str})"

        if msg.get("caption"):
            content += f" | {msg['caption']}"
        return content
    elif msg.get("audio"):
        audio = msg.get("audio", {})
        title = audio.get("title", "")
        duration = audio.get("duration", 0)

        content = "🎵 音频"
        if title:
            content += f": {title}"
        if duration:
            minutes, seconds = divmod(duration, 60)
            content += f" ({minutes:02d}:{seconds:02d})"
        return content
    elif msg.get("voice"):
        duration = msg.get("voice", {}).get("duration", 0)
        if duration:
            minutes, seconds = divmod(duration, 60)
            return f"🎤 语音消息 ({minutes:02d}:{seconds:02d})"
        return "🎤 语音消息"
    elif msg.get("sticker"):
        sticker = msg.get("sticker", {})
        emoji = sticker.get("emoji", "")
        set_name = sticker.get("set_name", "")
        content = f"😀 贴纸{emoji}"
        if set_name:
            content += f" ({set_name})"
        return content
    elif msg.get("animation"):
        content = "🎬 GIF动图"
        if msg.get("caption"):
            content += f" | {msg['caption']}"
        return content
    elif msg.get("contact"):
        contact = msg["contact"]
        first_name = contact.get("first_name", "")
        last_name = contact.get("last_name", "")
        phone = contact.get("phone_number", "")

        name = f"{first_name} {last_name}".strip() or "联系人"
        content = f"👤 联系人: {name}"
        if phone:
            content += f" ({phone})"
        return content
    elif msg.get("location"):
        location = msg.get("location", {})
        latitude = location.get("latitude")
        longitude = location.get("longitude")
        if latitude and longitude:
            return f"📍 位置: {latitude:.4f}, {longitude:.4f}"
        return "📍 位置信息"
    elif msg.get("venue"):
        venue = msg["venue"]
        title = venue.get("title", "地点")
        address = venue.get("address", "")
        content = f"🏢 地点: {title}"
        if address:
            content += f" ({address})"
        return content
    elif msg.get("poll"):
        poll = msg["poll"]
        question = poll.get("question", "")
        poll_type = poll.get("type", "regular")
        content = f"🗳️ {'匿名投票' if poll_type == 'quiz' else '投票'}"
        if question:
            content += f": {question}"
        return content
    elif msg.get("game"):
        game = msg["game"]
        title = game.get("title", "游戏")
        return f"🎮 游戏: {title}"
    elif msg.get("invoice"):
        invoice = msg["invoice"]
        title = invoice.get("title", "发票")
        return f"🧾 发票: {title}"
    else:
        return "💬 其他类型消息"


def _smart_clean_forwarded_content(content: str) -> str:
    """智能清理可能包含转发前缀的内容"""
    try:
        import re

        # 保存原始内容用于比较
        original_content = content

        # 模式1: "👤 用户名:\n内容" - 只清理明确的用户前缀
        pattern1 = r'^👤\s+[^:\n]+:\s*\n'
        content = re.sub(pattern1, '', content, flags=re.MULTILINE)

        # 模式2: "🏠群组名 | 👤用户名:\n内容" - 清理群组转发前缀
        pattern2 = r'^🏠[^|]+\|\s*👤[^:\n]+:\s*\n'
        content = re.sub(pattern2, '', content, flags=re.MULTILINE)

        # 模式3: "-- 发送者: xxx" (在行尾) - 清理发送者后缀
        pattern3 = r'\n-- 发送者:\s*[^\n]+$'
        content = re.sub(pattern3, '', content)

        # 模式4: "📝 引用消息:\n...内容..." - 清理引用前缀（避免嵌套）
        pattern4 = r'^📝\s*引用消息:\s*\n.*?\n\n'
        content = re.sub(pattern4, '', content, flags=re.DOTALL)

        # 只在有明显改善时才清理
        # 避免清理掉所有内容或清理得过于激进
        content = content.strip()

        # 如果清理后内容为空或过短，返回原始内容
        if not content or len(content.strip()) < len(original_content.strip()) * 0.3:
            logger.debug("清理结果过短，返回原始内容")
            return original_content

        # 移除连续的空行
        content = re.sub(r'\n\s*\n', '\n', content)

        return content if content else original_content

    except Exception as e:
        logger.debug(f"智能清理转发内容失败: {e}")
        return content


def _clean_forwarded_content(content: str) -> str:
    """清理可能包含转发前缀的内容（保持向后兼容）"""
    # 这个函数现在只做基础清理，避免过于激进
    return _smart_clean_forwarded_content(content)


# 为了向后兼容，保留原函数签名的包装器
async def send_with_prefix_legacy(
        source_chat_id, dest_chat_id, message_thread_id, sender_name, msg
):
    """向后兼容的包装器"""
    return await send_with_prefix(
        source_chat_id, dest_chat_id, message_thread_id, sender_name, msg
    )


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