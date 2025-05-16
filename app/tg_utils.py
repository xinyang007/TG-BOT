import httpx
import json
import logging
import asyncio # 导入 asyncio 用于 sleep
from .settings import settings # 使用加载的设置

logger = logging.getLogger(__name__)

# Telegram Bot API 的基础 URL，使用设置中的 Token 构建
BASE_URL = f"https://api.telegram.org/bot{settings.BOT_TOKEN}"

# 使用一个 httpx 客户端实例，可以在应用生命周期内重用
client = httpx.AsyncClient(timeout=30) # 增加超时时间，特别是对于可能需要等待的 API

async def tg(method: str, data: dict, max_retries: int = 5, initial_delay: int = 1):
    """
    发送请求到 Telegram Bot API，包含重试逻辑（特别是针对 429 错误）.

    Args:
        method: API 方法名 (例如: 'sendMessage', 'createForumTopic').
        data: API 方法的参数字典.
        max_retries: 最大重试次数.
        initial_delay: 初始重试等待秒数.

    Returns:
        Telegram API 响应中的 'result' 部分的 JSON 数据.

    Raises:
        httpx.HTTPStatusError: 对于 4xx/5xx 的 HTTP 状态码抛出 (在重试次数耗尽后).
        httpx.RequestError: 对于连接或超时错误抛出 (在重试次数耗尽后).
        Exception: 对于其他意外错误 (例如: JSON 解析错误, Telegram API 特定错误) 抛出.
    """
    url = f"{BASE_URL}/{method}"
    retries = 0
    delay = initial_delay

    while retries <= max_retries:
        try:
            # logger.debug(f"调用 Telegram API 方法: {method} (尝试 {retries + 1}/{max_retries + 1}) 参数: {data}")
            r = await client.post(url, json=data)
            r.raise_for_status() # 对于不好的状态码 (4xx 或 5xx) 抛出异常

            result = r.json()
            # logger.debug(f"Telegram API 方法 {method} 成功响应: {result}")

            # 检查 Telegram specific 'ok' field
            if not result.get("ok"):
                 error_code = result.get("error_code", "N/A")
                 description = result.get("description", "No description")
                 # 如果是 429 错误，并且还在重试次数内，尝试重试
                 if error_code == 429 and retries < max_retries:
                      retry_after = result.get("parameters", {}).get("retry_after", delay) # 使用 Telegram 建议的等待时间
                      logger.warning(f"Telegram API 返回 429 Too Many Requests for method {method}. Retrying after {retry_after} seconds. Attempt {retries + 1}/{max_retries + 1}")
                      await asyncio.sleep(retry_after)
                      retries += 1
                      delay *= 2 # 指数退避增加等待时间 (如果 Telegram 没有提供 retry_after)
                      continue # 跳过异常处理，进入下一次循环尝试
                 else:
                      # 非 429 错误，或重试次数已耗尽，记录并抛出
                      logger.error(f"Telegram API 报告失败: method={method}, code={error_code}, description='{description}', data={data} (重试 {retries}/{max_retries})")
                      raise Exception(f"Telegram API 报告失败: {description} (Code: {error_code})")

            return result.get("result") # 成功返回结果

        except httpx.HTTPStatusError as e:
            # 捕获 HTTP 状态码错误
            error_code = e.response.status_code
            # 如果是 429 错误，并且还在重试次数内，尝试重试
            if error_code == 429 and retries < max_retries:
                 # 尝试从响应头中获取 Retry-After，否则使用默认或指数退避
                 retry_after_header = e.response.headers.get("Retry-After")
                 try:
                     retry_after = int(retry_after_header) if retry_after_header else delay
                 except ValueError:
                     retry_after = delay # 如果 Retry-After 头无效，使用当前计算的延迟

                 logger.warning(f"Telegram API 返回 HTTP 429 Too Many Requests for method {method}. Retrying after {retry_after} seconds. Attempt {retries + 1}/{max_retries + 1}")
                 await asyncio.sleep(retry_after)
                 retries += 1
                 delay *= 2
                 continue # 进入下一次循环尝试
            else:
                 # 非 429 错误，或重试次数已耗尽，记录并抛出
                 logger.error(f"Telegram API HTTP error calling {method}: {e.response.status_code} - {e.response.text} (重试 {retries}/{max_retries})", exc_info=True)
                 raise # 重新抛出原始异常

        except httpx.RequestError as e:
            # 捕获连接或超时错误
            logger.error(f"Telegram API request error calling {method}: {e} (重试 {retries}/{max_retries})", exc_info=True)
            # 对于请求错误，也可以选择重试几次
            if retries < max_retries:
                 logger.warning(f"Telegram API 请求错误 for method {method}. Retrying in {delay} seconds. Attempt {retries + 1}/{max_retries + 1}")
                 await asyncio.sleep(delay)
                 retries += 1
                 delay *= 2
                 continue
            else:
                 raise # 重新抛出原始异常

        except Exception as e:
            # 捕获其他意外错误 (如 JSON 解析)
            logger.error(f"调用 Telegram API 方法 {method} 时发生意外错误: {e} (重试 {retries}/{max_retries})", exc_info=True)
            # 对于非预期的错误，不建议立即重试，直接抛出以便更高层级处理
            raise # 重新抛出原始异常

    # 如果重试次数耗尽仍然失败
    logger.error(f"Telegram API 方法 {method} 在 {max_retries + 1} 次尝试后仍然失败.")
    # 上面的异常处理应该已经抛出了最后一个错误，代码不会实际到达这里，
    # 但作为防御性编程，可以再次抛出或返回一个特定的失败指示。
    raise Exception(f"Telegram API 方法 {method} 在多次重试后仍然失败.")


async def copy_any(src_chat_id, dst_chat_id, message_id: int, extra_params: dict | None = None):
    # copy_any 函数内部调用 tg 函数，重试逻辑在 tg 函数中处理
    # ... (copy_any 函数代码保持不变) ...
    payload = {
        "chat_id": dst_chat_id,
        "from_chat_id": src_chat_id,
        "message_id": message_id,
        "allow_sending_without_reply": True # Common practice, avoid errors if original msg deleted
    }
    if extra_params:
        payload.update(extra_params)

    logger.debug(f"复制消息 {message_id} 从 {src_chat_id} 到 {dst_chat_id} 参数: {payload}")
    # 调用 tg 函数，它现在包含了重试逻辑
    return await tg("copyMessage", payload)


# --- 新增: 发送带前缀的消息方法 ---
async def send_with_prefix(source_chat_id, dest_chat_id, message_thread_id, sender_name, msg):
    """发送带前缀的消息，根据消息类型选择不同的发送方法."""
    # 修正: 格式化前缀，确保 sender_name 不为 None
    prefix = f"👤 {sender_name or '未知发送者'}:\n"

    # 创建消息副本进行修改
    msg_to_send = msg.copy()

    # 在消息文本或 caption 前添加前缀
    original_body = msg_to_send.get("text") or msg_to_send.get("caption")

    if original_body is not None:
         if "text" in msg_to_send and msg_to_send.get("text") is not None:
              msg_to_send["text"] = prefix + msg_to_send.get("text", "")
         elif "caption" in msg_to_send and msg_to_send.get("caption") is not None:
              msg_to_send["caption"] = prefix + msg_to_send.get("caption", "")


    # 根据消息类型选择不同的发送方法
    # Check common media types first, then text, then fallback to copyMessage
    if "photo" in msg_to_send:
         # 获取最大尺寸的图片
         photo = sorted(msg_to_send.get("photo"), key=lambda x: x.get("width", 0), reverse=True)[0] if msg_to_send.get("photo") else None
         if photo:
              logger.debug(f"Sending photo with prefix to chat {dest_chat_id} topic {message_thread_id}")
              # sendPhoto 参数不同于 copyMessage
              return await tg("sendPhoto", {
                  "chat_id": dest_chat_id,
                  "message_thread_id": message_thread_id,
                  "photo": photo.get("file_id"),
                  "caption": msg_to_send.get("caption"), # 使用添加了前缀的 caption
                  "parse_mode": "HTML"  # 可选
              })
    elif "video" in msg_to_send:
         logger.debug(f"Sending video with prefix to chat {dest_chat_id} topic {message_thread_id}")
         return await tg("sendVideo", {
             "chat_id": dest_chat_id,
             "message_thread_id": message_thread_id,
             "video": msg_to_send.get("video", {}).get("file_id"),
             "caption": msg_to_send.get("caption"), # 使用添加了前缀的 caption
             "parse_mode": "HTML"  # 可选
         })
    elif "document" in msg_to_send:
         logger.debug(f"Sending document with prefix to chat {dest_chat_id} topic {message_thread_id}")
         return await tg("sendDocument", {
             "chat_id": dest_chat_id,
             "message_thread_id": message_thread_id,
             "document": msg_to_send.get("document", {}).get("file_id"),
             "caption": msg_to_send.get("caption"), # 使用添加了前缀的 caption
             "parse_mode": "HTML"  # 可选
         })
    # ... Add other media types (audio, voice, sticker, animation) if needed ...

    elif "text" in msg_to_send and msg_to_send.get("text") is not None: # 如果是纯文本消息 (已在前面添加前缀)
         logger.debug(f"Sending text message with prefix to chat {dest_chat_id} topic {message_thread_id}")
         return await tg("sendMessage", {
             "chat_id": dest_chat_id,
             "message_thread_id": message_thread_id,
             "text": msg_to_send.get("text"), # 使用添加了前缀的 text
             "parse_mode": "HTML"  # 可选
         })
    # 如果不是以上特殊处理的类型 (如贴纸、服务消息、其他未知类型)，仍然使用 copyMessage
    else:
        logger.debug(f"Falling back to copyMessage for message {msg_to_send.get('message_id')} (type: {', '.join(msg_to_send.keys()) if msg_to_send else 'unknown'})")
        # 在 copyMessage 回退时，我们无法在接收方那边添加前缀，这是 send_* 方法的局限性
        # copy_any 只需要原始消息ID，源chat ID，目标chat ID 和话题ID
        return await copy_any(source_chat_id, dest_chat_id, msg_to_send.get("message_id"),
                              {"message_thread_id": message_thread_id})

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