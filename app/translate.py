import httpx
import json
import logging
from .settings import settings # 使用加载的设置

logger = logging.getLogger(__name__)

# 翻译 API 的 URL 和 Headers
XAI_URL = "https://api.x.ai/v1/chat/completions"
HEADERS = {
    "Authorization": f"Bearer {settings.XAI_API_KEY}",
    "Content-Type": "application/json"
}

async def translate(txt: str, target_lang: str) -> str:
    """
    使用 XAI API (或类似服务) 翻译文本.

    Args:
        txt: 要翻译的文本.
        target_lang: 目标语言代码 (例如: "zh-CN", "en", "fr").

    Returns:
        翻译后的文本.

    Raises:
        Exception: 如果翻译失败 (API 错误, 意外响应).
    """
    if not txt or not target_lang:
        logger.debug("跳过翻译: 文本或目标语言为空")
        return "" # 没有内容或目标语言，无需翻译

    body = {
        "model": "xai-chat-1", # 或您选择的模型
        "messages": [
            {"role": "system",
             # 明确翻译任务和输出格式要求
             "content": f"将用户文本 *只* 翻译成 {target_lang}。仅提供翻译后的文本，不要任何额外评论或格式。"},
            {"role": "user", "content": txt}
        ],
        "max_tokens": 1024 # 根据需要调整
        # 可选: 添加 temperature=0 以获得更字面的翻译
    }

    # 使用临时的 httpx 客户端实例，保证其生命周期仅限于此函数
    async with httpx.AsyncClient(timeout=30) as c: # 为 LLM 调用增加超时时间
        try:
            logger.debug(f"正在翻译文本 (前50字符: '{txt[:50]}...') 到 {target_lang}")
            r = await c.post(XAI_URL, headers=HEADERS, json=body)
            r.raise_for_status() # 对于 4xx/5xx 响应抛出异常
            j = r.json()
            logger.debug(f"翻译 API 响应: {j}")

            # 验证响应结构
            if not (j and "choices" in j and j["choices"] and j["choices"][0] and "message" in j["choices"][0] and "content" in j["choices"][0]["message"]):
                 logger.error(f"翻译 API 返回意外结构，文本: '{txt[:50]}...'. 响应: {j}")
                 raise ValueError("翻译 API 返回意外响应格式")

            translated_text = j["choices"][0]["message"]["content"].strip()
            logger.debug(f"翻译成功: '{translated_text[:50]}...'")
            return translated_text

        except httpx.HTTPStatusError as e:
            logger.error(f"翻译 API HTTP 错误，文本 '{txt[:50]}...': {e.response.status_code} - {e.response.text}", exc_info=True)
            raise # 重新抛出
        except httpx.RequestError as e:
            logger.error(f"翻译 API 请求错误，文本 '{txt[:50]}...': {e}", exc_info=True)
            raise # 重新抛出
        except Exception as e:
            logger.error(f"翻译时发生意外错误，文本 '{txt[:50]}...': {e}", exc_info=True)
            raise # 重新抛出