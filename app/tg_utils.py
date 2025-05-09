import httpx
import json
import logging
from .settings import settings # 使用加载的设置

logger = logging.getLogger(__name__)

# Telegram Bot API 的基础 URL，使用设置中的 Token 构建
BASE_URL = f"https://api.telegram.org/bot{settings.BOT_TOKEN}"

# 使用一个 httpx 客户端实例，可以在应用生命周期内重用
client = httpx.AsyncClient(timeout=20) # 稍微增加超时时间

async def tg(method: str, data: dict):
    """
    发送请求到 Telegram Bot API.

    Args:
        method: API 方法名 (例如: 'sendMessage', 'createForumTopic').
        data: API 方法的参数字典.

    Returns:
        Telegram API 响应中的 'result' 部分的 JSON 数据.

    Raises:
        httpx.HTTPStatusError: 对于 4xx/5xx 的 HTTP 状态码抛出.
        httpx.RequestError: 对于连接或超时错误抛出.
        Exception: 对于其他意外错误 (例如: JSON 解析错误, Telegram API 特定错误) 抛出.
    """
    url = f"{BASE_URL}/{method}"
    try:
        # logger.debug(f"调用 Telegram API 方法: {method} 参数: {data}") # 谨慎日志敏感数据
        r = await client.post(url, json=data)
        r.raise_for_status() # 对于不好的状态码 (4xx 或 5xx) 抛出异常
        result = r.json()
        # logger.debug(f"Telegram API 方法 {method} 成功响应: {result}")
        # 检查 Telegram 响应中的 'ok' 字段
        if not result.get("ok"):
             # 如果 Telegram 返回失败，记录其特定的错误描述
             error_code = result.get("error_code", "N/A")
             description = result.get("description", "无描述")
             logger.error(f"Telegram API 报告失败: 方法={method}, 错误码={error_code}, 描述='{description}', 参数={data}")
             # 考虑在此处抛出更特定的自定义异常
             raise Exception(f"Telegram API 报告失败: {description} (错误码: {error_code})")
        return result.get("result") # 返回实际的结果部分

    except httpx.HTTPStatusError as e:
        logger.error(f"调用 {method} 时发生 Telegram API HTTP 错误: {e.response.status_code} - {e.response.text}", exc_info=True)
        raise # 重新抛出原始异常
    except httpx.RequestError as e:
        logger.error(f"调用 {method} 时发生 Telegram API 请求错误: {e}", exc_info=True)
        raise # 重新抛出原始异常
    except Exception as e:
        logger.error(f"调用 Telegram API 方法 {method} 时发生意外错误: {e}", exc_info=True)
        raise # 重新抛出原始异常


async def copy_any(src_chat_id, dst_chat_id, message_id: int, extra_params: dict | None = None):
    """
    将一条消息从一个聊天复制到另一个聊天.

    Args:
        src_chat_id: 源聊天 ID.
        dst_chat_id: 目标聊天 ID.
        message_id: 要复制的消息在源聊天中的 ID.
        extra_params: 可选的额外参数字典，例如 message_thread_id, text, caption, reply_markup 等.
                      注意: 此处传递的 'text' 和 'caption' 将覆盖原始消息的文本/caption.

    Returns:
        copyMessage API 调用的 JSON 结果.
    """
    payload = {
        "chat_id": dst_chat_id,
        "from_chat_id": src_chat_id,
        "message_id": message_id,
        "allow_sending_without_reply": True # 常见做法，避免原始消息被删除时出错
    }
    if extra_params:
        payload.update(extra_params)

    # 注意: 如果在 extra_params 中提供了 text/caption (例如来自翻译)，Telegram 的 copyMessage
    # 会使用这些新内容而不是原始消息的 text/caption。
    # 我们的翻译逻辑修改了消息字典的副本，并将修改后的 text/caption 放在那里，
    # 调用此函数的 handler 应该将修改后的 text/caption 放入 extra_params。
    # 这个函数只是合并 extra_params。

    logger.debug(f"复制消息 {message_id} 从 {src_chat_id} 到 {dst_chat_id} 参数: {payload}")
    return await tg("copyMessage", payload)

# 可选: 添加一个函数在应用关闭时关闭 httpx 客户端
# 在这个简单的示例结构中，我们依赖进程退出，但在大型应用中明确管理生命周期更好。
async def close_http_client():
    """尝试异步关闭全局 httpx 客户端."""
    logger.info("尝试关闭 HTTP 客户端...")
    # 在实际的 FastAPI 应用中管理客户端生命周期，你可以在异步关闭事件处理器中调用 client.aclose()。
    # 对于这个简单的全局客户端，此处处理不够优雅。
    # await client.aclose() # 如果客户端是异步创建的，需要 await
    # 如果 client 是在模块级别同步创建的，可能只需要同步关闭 client.close()
    # 或者依赖框架管理或垃圾回收。此处简单起见，仅作日志提示。
    logger.info("HTTP 客户端关闭尝试完成。")