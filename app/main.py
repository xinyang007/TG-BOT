import logging
import sys # 用于潜在的关键错误退出
from fastapi import FastAPI, Request, Depends
from starlette.responses import PlainTextResponse
from starlette.concurrency import run_in_threadpool # 服务层内部使用，这里导入仅供参考

# 导入应用组件
from .settings import settings # 加载设置
from .store import connect_db, close_db, create_all_tables # DB 管理函数
from .tg_utils import tg, close_http_client # Telegram API 工具及客户端管理 (虽然目前关闭处理不够优雅)
from .services.conversation_service import ConversationService # 导入服务层
from .handlers import private, group # 导入 handlers

# --- 配置日志 ---
# 基本配置，将日志输出到控制台。生产环境请配置文件日志、日志轮转等。
logging.basicConfig(level=logging.INFO, # 设置基础日志级别 (INFO, DEBUG, WARNING, ERROR)
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)]) # 将日志输出到标准输出

logger = logging.getLogger(__name__)

# --- 初始化 FastAPI 应用 ---
app = FastAPI(
    title="Telegram Customer Support Bot",
    description="通过群组话题处理私聊作为支持请求。",
    version="1.0.0",
)

# --- 依赖注入 / 服务初始化 ---
# 在设置加载后，初始化服务。
# 在大型应用中，你可能使用 FastAPI 的 Depends 或依赖注入容器。
# 简单起见，我们在启动事件中创建实例，并将其传递给 handlers。

# 声明服务变量，将在启动事件中初始化
conversation_service: ConversationService = None

# --- 您的服务器公共 HTTPS URL 前缀 ---
# 务必将其替换为您实际部署服务器的公共 HTTPS URL 前缀 (例如: https://your.domain.com)
# 这个值应该与你在宝塔中设置的反向代理的监听地址和端口一致 (通常是 443 端口)
# 如果你没有域名，只有 IP 地址，且宝塔将 80/443 端口映射到你的应用端口，这里就是 https://你的服务器IP
# 如果你没有域名，且没有反向代理，你的应用直接监听某个端口 (如 8000)，但 Telegram 只支持 80, 88, 443, 8443 端口作为 Webhook，
# 且必须是 HTTPS，所以直接使用 IP:端口 的形式做 Webhook 几乎不可行。
# 强烈建议使用域名+反向代理+SSL。
# 这个值也可以从 settings 中读取，而不是硬编码在这里，这样更灵活。
# 如果 PUBLIC_BASE_URL 从 settings 中读取，那么 settings.py 需要添加这个字段并从 .env 中加载。
# 让我们修改为从 settings 中读取，这样更好。

# 从 settings 中读取 PUBLIC_BASE_URL
# PUBLIC_BASE_URL = settings.PUBLIC_BASE_URL # <-- 这个字段需要添加到 settings.py 中

@app.on_event("startup")
async def startup_event():
    """FastAPI 应用启动时运行."""
    logger.info("应用启动中...")
    # 1. 连接数据库
    connect_db()
    # 2. 创建表 (包括 Conversation, Messages, BlackList) 如果它们不存在的话
    create_all_tables()
    logger.info("数据库连接并检查/创建了表。")
    # 3. 初始化服务，在依赖项 (如 DB 连接) 准备好之后
    global conversation_service
    # 将必要的依赖项传递给服务构造函数
    conversation_service = ConversationService(group_id=settings.GROUP_ID, tg_func=tg)
    logger.info("ConversationService 已初始化。")

    # 4. 自动设置 Webhook URL
    # 从 settings 中读取 PUBLIC_BASE_URL
    public_base_url = str(settings.PUBLIC_BASE_URL).rstrip('/') # 确保是字符串并移除末尾斜杠

    if public_base_url: # 检查 PUBLIC_BASE_URL 是否已配置
        # 构建完整的 Webhook URL： 公共基地址 + Webhook Path
        webhook_url = f"{public_base_url}/{settings.WEBHOOK_PATH}"
        print(webhook_url)

        try:
            logger.info(f"正在检查或设置 Webhook 为 {webhook_url}")
            # 检查当前 Webhook 信息
            webhook_info = await tg("getWebhookInfo", {})
            # 如果当前 Webhook URL 与期望的不符，或者没有设置 Webhook
            if webhook_info.get("url") != webhook_url:
                logger.info(f"当前 Webhook URL ('{webhook_info.get('url')}') 与期望不符，正在设置新的 Webhook。")
                # 调用 setWebhook API
                await tg("setWebhook", {"url": webhook_url})
                logger.info("Webhook 设置成功。")
            else:
                logger.info("Webhook 已正确设置，无需更新。")

            # 可选：检查并处理可能的待处理更新 (pending updates)
            # 如果在 Webhook 不工作期间有消息，它们会累积。设置新 Webhook 后，Telegram 可能不会立即发送这些旧更新。
            # 如果需要处理旧更新，可能需要先 deleteWebhook，然后再次 setWebhook。
            # 或者在设置 Webhook 之后，通过 getUpdates 拉取一次旧消息（不推荐，会让逻辑复杂）。
            # 对于大多数情况，让 Telegram 自己处理旧更新即可。

        except Exception as e:
            logger.error(f"自动检查或设置 Webhook 失败: {e}", exc_info=True)
    else:
        logger.warning("settings.PUBLIC_BASE_URL 未设置，跳过自动设置 Webhook。请在 .env 中配置 PUBLIC_BASE_URL。")


@app.on_event("shutdown")
async def shutdown_event():
    """FastAPI 应用关闭时运行."""
    logger.info("应用关闭中...")
    # 1. 关闭数据库连接
    close_db()
    # 2. 关闭用于 Telegram API 调用的 HTTP 客户端
    # 注意: 这需要异步关闭。如果在 tg_utils 中使用了特定的客户端实例，需要妥善处理其生命周期。
    # 目前 tg_utils 使用全局客户端，在此处干净地关闭比较棘手。
    # await tg_utils.client.aclose() # 如果 tg_utils 暴露了客户端。
    # 对于此示例，依赖于进程退出或垃圾回收处理。
    # await close_http_client() # 如果 close_http_client 是异步的

# --- 根路径端点 (健康检查) ---
@app.get("/")
async def root():
    """基础健康检查端点."""
    logger.info("访问根路径端点。")
    return PlainTextResponse("ok")


# --- Webhook 端点 ---
# 使用设置中的随机 Webhook 路径进行基础安全防范
@app.post(f"/{settings.WEBHOOK_PATH}")
async def webhook(req: Request):
    """接收 Telegram 更新的 Webhook 端点."""
    update_id = None
    try:
        upd = await req.json()
        update_id = upd.get("update_id", "N/A")
        logger.debug(f"收到更新: {update_id}")

        # 提取消息或相关的更新类型
        msg = (upd.get("message") or
               upd.get("edited_message") or
               upd.get("callback_query", {}).get("message") or
               upd.get("channel_post") or # 考虑处理频道消息吗？
               upd.get("edited_channel_post") or
               upd.get("my_chat_member") or # 处理 bot 被添加/移除吗？
               upd.get("chat_member") or # 处理用户加入/离开群组吗？
               upd.get("chat_join_request") # 处理加入请求吗？
              ) # 根据需要添加其他更新类型

        if not msg:
            # 记录并跳过不包含消息或相关触发器的更新
            logger.debug(f"更新 {update_id} 不包含可处理的消息类型。跳过。")
            return PlainTextResponse("skip")

        chat_type = msg.get("chat", {}).get("type")
        chat_id = msg.get("chat", {}).get("id")
        msg_id = msg.get("message_id", "N/A")
        from_user_id = msg.get("from", {}).get("id", "N/A") # 消息发送者

        logger.info(f"处理消息 {msg_id} (更新 {update_id}) 来自用户 {from_user_id} 在聊天 {chat_id} (类型: {chat_type})")

        # 将已初始化的服务实例传递给 handlers
        if chat_type == "private":
            # private handler 不需要知道具体的 chat_id 或 msg_id，只需要 msg 字典和 service
            await private.handle_private(msg, conversation_service)
        elif chat_type in ("group", "supergroup"):
            # 确保是正确的支持群组
            if str(chat_id) == settings.GROUP_ID: # 将 chat_id 转换为字符串进行比较 (Telegram API 可能返回 int 或 string)
                await group.handle_group(msg, conversation_service)
            else:
                logger.debug(f"忽略在未配置聊天 {chat_id} 中的群组消息 {msg_id}")
                # 可选: 自动离开意外的群组
                # try: await tg("leaveChat", {"chat_id": chat_id}) except Exception: pass
        else:
             logger.debug(f"忽略未处理的聊天类型: {chat_type}")

        logger.info(f"成功处理更新 {update_id}")
        # Telegram 期望快速返回 200 OK 响应
        return PlainTextResponse("ok")

    except Exception as e:
        # 捕获所有从 handlers 逃逸的异常
        logger.error(f"处理更新 {update_id} 时发生未处理错误: {e}", exc_info=True)
        # 返回 200 OK 给 Telegram，以防止其对暂时性错误进行过多的重试，
        # 但同时记录错误的严重性。
        # 根据你的重试策略，你可能希望返回 500 让 Telegram 重试 (需谨慎!)
        return PlainTextResponse("error", status_code=200) # 或 status_code=500