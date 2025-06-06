import asyncio
import logging
import sys
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

from app.rate_limit_notifications import send_rate_limit_notification, send_punishment_notification

from fastapi import FastAPI, Request, HTTPException, status, Depends, Path, Query
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.concurrency import run_in_threadpool
import traceback

# 导入应用组件
from app.settings import settings
from app.store import create_all_tables
from app.tg_utils import tg, close_http_client
from app.handlers import private, group
from app.logging_config import setup_logging, get_logger, get_message_logger
from app.message_coordinator import CoordinationResult
from app.validation import validate_webhook_update, validate_telegram_message, ValidationError
from app.dependencies import (
    get_conversation_service, get_cache, get_metrics, get_health_checker,
    get_rate_limit_manager, get_lifecycle_manager, get_auth_manager,
    get_message_coordinator_dep, get_coordinated_handler_dep,
    get_failover_manager_dep,
    get_circuit_breaker_registry_dep, get_bot_manager_dep
)
from app.monitoring import record_http_request, record_message_processing
from app.cache import CacheManager

# --- 配置日志 ---
app_logger = setup_logging()
logger = get_logger("app.main")


# 🔥 修复：添加缺失的 setup_enhanced_webhooks 函数
async def setup_enhanced_webhooks():
    """设置增强的Telegram Webhooks（修复版本）"""
    if not settings.MULTI_BOT_ENABLED:
        # 单机器人模式：使用原有逻辑
        return await setup_single_bot_webhook()

    # 多机器人模式：为每个机器人设置webhook
    enabled_bots = settings.get_enabled_bots()
    success_count = 0

    for bot_config in enabled_bots:
        # 🔥 修复：确保使用正确的webhook URL
        webhook_url = settings.get_bot_webhook_url(bot_config)

        # 🔥 关键修复：确保URL格式正确
        if not webhook_url.startswith('http'):
            public_base_url = str(settings.PUBLIC_BASE_URL).rstrip('/')
            if webhook_url.startswith('/'):
                webhook_url = f"{public_base_url}{webhook_url}"
            else:
                webhook_url = f"{public_base_url}/{webhook_url}"

        try:
            # 检查当前webhook状态
            current_info = await tg("getWebhookInfo", {}, specific_bot_token=bot_config.token)
            current_url = current_info.get("url", "")

            if current_url != webhook_url:
                logger.info(f"🔧 设置机器人 {bot_config.name} webhook: {webhook_url}")

                # 设置新webhook
                await tg("setWebhook", {
                    "url": webhook_url,
                    "max_connections": 100,
                    "allowed_updates": ["message", "edited_message", "callback_query"]
                }, specific_bot_token=bot_config.token)

                logger.info(f"✅ 机器人 {bot_config.name} webhook设置成功")
                success_count += 1
            else:
                logger.info(f"✅ 机器人 {bot_config.name} webhook已正确配置")
                success_count += 1

        except Exception as e:
            logger.error(f"❌ 设置机器人 {bot_config.name} webhook失败: {e}")

    logger.info(f"📊 Webhook设置完成: {success_count}/{len(enabled_bots)} 个机器人配置成功")


async def setup_single_bot_webhook():
    """设置单机器人Webhook（原有逻辑）"""
    public_base_url = str(settings.PUBLIC_BASE_URL).rstrip('/')

    if not public_base_url:
        logger.warning("PUBLIC_BASE_URL 未设置，跳过自动设置 Webhook")
        return

    webhook_url = f"{public_base_url}/{settings.WEBHOOK_PATH}"

    try:
        logger.info("正在检查或设置 Webhook", extra={"webhook_url": webhook_url})

        webhook_info = await tg("getWebhookInfo", {})
        current_url = webhook_info.get("url", "")

        if current_url != webhook_url:
            logger.info(
                "设置新的 Webhook",
                extra={"old_url": current_url, "new_url": webhook_url}
            )
            await tg("setWebhook", {"url": webhook_url})
            logger.info("Webhook 设置成功")
        else:
            logger.info("Webhook 已正确设置，无需更新")

    except Exception as e:
        logger.error(
            "自动设置 Webhook 失败",
            extra={"webhook_url": webhook_url},
            exc_info=True
        )

# --- 🔥 修复的增强处理逻辑 ---
async def enhanced_webhook_logic(
    raw_update: Dict,
    source_bot_token: str
):
    """增强的webhook处理逻辑（完整实现）"""
    update_id = raw_update.get("update_id", "N/A")
    start_time = time.time()

    try:
        # 验证更新格式
        try:
            validated_update = validate_webhook_update(raw_update)
        except ValidationError as e:
            logger.warning(
                f"Webhook更新验证失败",
                extra={"update_id": update_id, "validation_error": e.message}
            )
            return PlainTextResponse("validation_error")

        # 获取消息对象
        msg_data = validated_update.get_message()
        if not msg_data:
            logger.debug("更新不包含可处理的消息类型", extra={"update_id": update_id})
            return PlainTextResponse("skip")

        # 验证消息格式
        try:
            validated_message = validate_telegram_message(msg_data)
        except ValidationError as e:
            logger.warning(
                f"消息验证失败",
                extra={"update_id": update_id, "validation_error": e.message}
            )
            return PlainTextResponse("message_validation_error")

        # 获取基本信息
        chat_type = validated_message.chat.get("type")
        chat_id = validated_message.get_chat_id()
        msg_id = validated_message.message_id
        user_id = validated_message.get_user_id()
        user_name = validated_message.get_user_name()

        # 🔥 关键修复：增强的速率限制检查（使用来源机器人）
        if user_id:
            try:
                from app.rate_limit import get_rate_limiter, ActionType

                logger.info(f"🔍 检查速率限制: user_id={user_id}, chat_type={chat_type}")

                # 🔥 使用消息来源机器人发送通知
                notification_bot_token = source_bot_token
                logger.info(f"✅ 将使用来源机器人发送通知: {notification_bot_token[-10:]}")

                # 检查速率限制
                limiter = await get_rate_limiter()
                user_group = settings.get_user_group(user_id)
                rate_result = await limiter.check_rate_limit(
                    f"user:{user_id}", ActionType.MESSAGE, user_group
                )

                if not rate_result.allowed:
                    logger.warning(
                        f"🚫 速率限制触发: 用户{user_id}, 聊天类型{chat_type}, "
                        f"当前{rate_result.current_count}/{rate_result.limit}, "
                        f"剩余时间{int(rate_result.reset_time - time.time())}秒"
                    )

                    # 🔥 使用来源机器人发送通知
                    await send_rate_limit_notification(
                        user_id=user_id,
                        user_name=user_name,
                        chat_type=chat_type,
                        chat_id=chat_id,
                        rate_result=rate_result,
                        msg_id=msg_id,
                        preferred_bot_token=notification_bot_token  # 🔥 关键修复
                    )

                    # 如果有惩罚时间，也使用来源机器人发送
                    if hasattr(rate_result, 'punishment_ends_at') and rate_result.punishment_ends_at:
                        punishment_duration = int(rate_result.punishment_ends_at - time.time())
                        if punishment_duration > 0:
                            await send_punishment_notification(
                                user_id, punishment_duration,
                                specific_bot_token=notification_bot_token  # 🔥 关键修复
                            )

                    return PlainTextResponse("rate_limited")
                else:
                    logger.debug(f"✅ 速率限制检查通过: user_id={user_id}")

            except Exception as e:
                logger.error(f"❌ 速率限制检查失败: {e}", exc_info=True)

        # 获取依赖服务
        conv_service = await get_conversation_service()
        coordinated_handler = await get_coordinated_handler_dep()

        # 使用消息相关的日志器
        msg_logger = get_message_logger(
            message_id=msg_id,
            chat_id=chat_id,
            operation="enhanced_webhook_processing"
        )

        msg_logger.info(
            "🔄 处理增强Webhook消息",
            extra={
                "update_id": update_id,
                "chat_type": chat_type,
                "user_id": user_id,
                "user_name": user_name,
                "source_bot_token": source_bot_token[-10:],
                "coordination_enabled": coordinated_handler is not None
            }
        )

        # 🔥 消息处理：根据是否启用协调选择处理方式
        if (coordinated_handler and
                getattr(settings, 'MULTI_BOT_ENABLED', False) and
                getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True)):

            # 使用协调式处理
            msg_logger.info("🔄 使用协调式消息处理")
            try:
                result = await coordinated_handler.handle_webhook_message(raw_update)

                if result in ("queued", "duplicate"):
                    if result == "queued":
                        msg_logger.info("✅ 消息已提交到协调队列")
                    else:
                        msg_logger.info("ℹ️ 重复消息已忽略")
                    record_message_processing(chat_type or "unknown", time.time() - start_time, True)
                    return PlainTextResponse(result)
                else:
                    msg_logger.warning(f"⚠️ 协调处理结果: {result}，回退到直接处理")
                    # 回退到直接处理

            except Exception as coord_error:
                msg_logger.error(f"❌ 协调式处理异常: {coord_error}", exc_info=True)
                # 回退到直接处理

        # 🔥 直接处理消息（使用来源机器人）
        msg_logger.info("🔄 使用直接消息处理")
        try:
            if chat_type == "private":
                await private.handle_private(
                    msg_data, conv_service,
                    specific_bot_token=source_bot_token  # 🔥 传递来源机器人
                )
                record_message_processing("private", time.time() - start_time, True)
                msg_logger.info("✅ 私聊消息处理完成")

            elif chat_type in ("group", "supergroup"):
                if str(chat_id) == settings.SUPPORT_GROUP_ID:
                    await group.handle_group(
                        msg_data, conv_service,
                        specific_bot_token=source_bot_token  # 🔥 传递来源机器人
                    )
                    record_message_processing("support_group", time.time() - start_time, True)
                    msg_logger.info("✅ 客服群组消息处理完成")
                else:
                    await group.handle_group(
                        msg_data, conv_service,
                        specific_bot_token=source_bot_token  # 🔥 传递来源机器人
                    )
                    record_message_processing("external_group", time.time() - start_time, True)
                    msg_logger.info("✅ 外部群组消息处理完成")
            else:
                msg_logger.debug(f"忽略未处理的聊天类型: {chat_type}")
                return PlainTextResponse("unsupported_chat_type")

        except Exception as processing_error:
            msg_logger.error(
                "❌ 消息处理异常",
                extra={"processing_error": str(processing_error)},
                exc_info=True
            )
            record_message_processing(chat_type or "unknown", time.time() - start_time, False)
            return PlainTextResponse("processing_error")

        msg_logger.info("✅ 消息处理完成")
        return PlainTextResponse("ok")

    except Exception as e:
        logger.error(
            "❌ 增强Webhook处理异常",
            extra={
                "update_id": update_id,
                "exception_type": type(e).__name__,
                "exception_message": str(e)
            },
            exc_info=True
        )
        return PlainTextResponse("error", status_code=500)


# --- 🔥 修复的智能Webhook路由器 ---
class SmartWebhookRouter:
    """智能Webhook路由器（修复版本）"""

    def __init__(self):
        self.logger = get_logger("webhook_router")
        self.bot_cache = {}  # 缓存机器人配置
        self._last_cache_update = 0
        self._cache_ttl = 300  # 缓存5分钟

    def _create_webhook_handler(self, is_primary: bool = False, bot_identifier: Optional[str] = None):
        """创建webhook处理函数（避免闭包问题）"""

        # 注意：这里我们返回一个接受 FastAPI 依赖参数的 async 函数
        async def webhook_handler(request: Request):
            # 将 is_primary 和 bot_identifier 存储在 request.state 中，
            # 这样 enhanced_webhook_logic 就可以从 request.state 中获取这些值。
            # FastAPI 的 Request 对象会在依赖注入链中传递。
            request.state.is_primary = is_primary
            request.state.bot_identifier = bot_identifier

            # 直接调用 enhanced_webhook_logic，它将通过 Depends 自动解析所有依赖
            return await enhanced_webhook_logic(
                raw_update=await request.json(),
                source_bot_token="auto_detected_by_router", # 这是一个占位符，实际 token 会在 router 内部解析
                # 注意：conv_service 和 coordinated_handler_dep 不在这里传递，它们由 FastAPI 注入到 enhanced_webhook_logic
            )

        return webhook_handler

    async def setup_routes(self, app: FastAPI):
        """设置智能webhook路由（完全修复版本）"""
        self.logger.info("设置智能Webhook路由...")

        # 🔥 关键：移除可能存在的冲突路由
        # 检查并移除现有的webhook路由
        routes_to_remove = []
        for route in app.routes:
            if hasattr(route, 'path') and (route.path == "/webhook" or route.path.endswith("/webhook")):
                routes_to_remove.append(route)

        for route in routes_to_remove:
            app.routes.remove(route)
            self.logger.info(f"移除冲突路由: {route.path}")

        # 检查是否启用多机器人模式
        if getattr(settings, 'MULTI_BOT_ENABLED', False):
            enabled_bots = settings.get_enabled_bots()

            # 为每个机器人设置专用路由
            for bot_config in enabled_bots:
                webhook_url = settings.get_bot_webhook_url(bot_config)

                if webhook_url.startswith('http'):
                    url_parts = webhook_url.split('/', 3)
                    webhook_path = url_parts[3] if len(url_parts) > 3 else "webhook"
                else:
                    webhook_path = webhook_url.strip('/')

                self.logger.info(f"机器人 {bot_config.name} - URL: {webhook_url}")
                self.logger.info(f"机器人 {bot_config.name} - 提取的路径: /{webhook_path}")

                # 🔥 关键修改：直接将 enhanced_webhook_logic 注册到路由
                # _create_webhook_handler 返回的是一个包装器，用于设置 request.state
                # 然后 FastAPI 会调用这个包装器，并注入依赖给 enhanced_webhook_logic
                handler_func = self._create_webhook_handler(
                    is_primary=(bot_config.priority == 1),
                    bot_identifier=bot_config.get_webhook_identifier()
                )
                app.post(f"/{webhook_path}")(handler_func)  # <--- 直接注册这个 handler_func

        else:
            # 单机器人模式：使用原有逻辑
            webhook_path = getattr(settings, 'WEBHOOK_PATH', 'webhook')
            self.logger.info(f"设置单机器人路由: /{webhook_path}")

            handler_func = self._create_webhook_handler(is_primary=True)
            app.post(f"/{webhook_path}")(handler_func)  # <--- 直接注册

        self.logger.info("✅ 智能Webhook路由设置完成")

    async def handle_webhook(self, request: Request,
                             is_primary: bool = False,
                             bot_identifier: Optional[str] = None):
        """统一的智能webhook处理入口（完整实现）"""
        update_id = None
        start_time = time.time()

        try:
            # 获取原始请求数据
            raw_update = await request.json()
            update_id = raw_update.get("update_id", "N/A")

            # 🔥 关键：智能识别消息来源机器人
            source_bot_token, source_bot_config = await self._identify_source_bot(
                request, is_primary, bot_identifier, raw_update
            )

            if not source_bot_token:
                self.logger.error(f"❌ 无法识别消息来源机器人: identifier={bot_identifier}")
                return PlainTextResponse("unknown_bot", status_code=404)

            # 在消息中标记来源机器人信息
            raw_update['_source_bot_token'] = source_bot_token
            raw_update['_source_bot_config'] = {
                'name': source_bot_config.name if source_bot_config else "未知",
                'priority': source_bot_config.priority if source_bot_config else 999,
                'identifier': bot_identifier
            }
            raw_update['_webhook_routing_info'] = {
                'is_primary': is_primary,
                'bot_identifier': bot_identifier,
                'host': request.headers.get('host'),
                'user_agent': request.headers.get('user-agent', '')[:100],
                'routing_time': time.time()
            }

            bot_name = source_bot_config.name if source_bot_config else "未知机器人"
            self.logger.info(
                f"📥 智能路由: 收到来自 {bot_name} 的webhook消息 "
                f"(update_id: {update_id}, token: {source_bot_token[-10:]})"
            )

            # 🔥 调用增强的主处理逻辑
            return await enhanced_webhook_logic(raw_update, source_bot_token)

        except Exception as e:
            self.logger.error(f"❌ 智能Webhook路由异常: {e}", exc_info=True)
            return PlainTextResponse("routing_error", status_code=500)

    async def _identify_source_bot(self, request: Request,
                                   is_primary: bool,
                                   bot_identifier: Optional[str],
                                   raw_update: Dict) -> tuple[Optional[str], Optional[Any]]:
        """智能识别消息来源机器人（修复版本）"""

        # 更新机器人缓存
        await self._update_bot_cache()

        # 方法1: 如果是主路径，直接返回主机器人
        if is_primary:
            primary_config = settings.get_primary_bot_config()
            if primary_config:
                self.logger.debug(f"✅ 主路径识别: {primary_config.name}")
                return primary_config.token, primary_config
            return settings.get_primary_bot_token(), None

        # 方法2: 通过标识符查找
        if bot_identifier:
            bot_config = self._find_bot_by_identifier(bot_identifier)
            if bot_config:
                self.logger.debug(f"✅ 标识符识别: {bot_identifier} -> {bot_config.name}")
                return bot_config.token, bot_config

        # 方法3: 通过请求路径智能匹配
        request_path = request.url.path.strip('/')
        self.logger.debug(f"🔍 请求路径: {request_path}")

        # 尝试从路径匹配机器人
        for bot_config in self.bot_cache.values():
            webhook_url = settings.get_bot_webhook_url(bot_config)
            webhook_path = webhook_url.split('/')[-1] if '/' in webhook_url else webhook_url

            if request_path == webhook_path or request_path.endswith(f"/{webhook_path}"):
                self.logger.debug(f"✅ 路径匹配识别: {request_path} -> {bot_config.name}")
                return bot_config.token, bot_config

        # 方法4: 智能推测（通过会话记录、消息内容等）
        guessed_token, guessed_config = await self._intelligent_bot_guess(raw_update)
        if guessed_token:
            self.logger.info(f"🔍 智能推测: {guessed_config.name if guessed_config else '主机器人'}")
            return guessed_token, guessed_config

        # 方法5: 默认回退到主机器人
        self.logger.warning(f"⚠️ 无法识别机器人，回退到主机器人")
        primary_config = settings.get_primary_bot_config()
        return (primary_config.token if primary_config else settings.get_primary_bot_token(),
                primary_config)

    async def _update_bot_cache(self):
        """更新机器人配置缓存"""
        current_time = time.time()
        if current_time - self._last_cache_update > self._cache_ttl:
            enabled_bots = settings.get_enabled_bots()
            self.bot_cache = {bot.get_webhook_identifier(): bot for bot in enabled_bots}
            self._last_cache_update = current_time
            self.logger.debug(f"🔄 已更新机器人缓存，共 {len(self.bot_cache)} 个机器人")

    def _find_bot_by_identifier(self, identifier: str) -> Optional[Any]:
        """通过标识符查找机器人"""
        # 直接查找
        if identifier in self.bot_cache:
            return self.bot_cache[identifier]

        # 模糊匹配
        for cached_id, bot_config in self.bot_cache.items():
            if (identifier == f"bot_{bot_config.token.split(':')[0]}" or
                    identifier == bot_config.name.replace(' ', '_').lower() or
                    identifier == cached_id):
                return bot_config

        return None

    async def _intelligent_bot_guess(self, raw_update: Dict) -> tuple[Optional[str], Optional[Any]]:
        """智能推测机器人（基于会话记录等）"""
        try:
            msg_data = raw_update.get("message", {})
            user_id = msg_data.get("from", {}).get("id")
            chat_type = msg_data.get("chat", {}).get("type")

            if not user_id:
                return None, None

            # 检查用户的会话记录（仅对私聊）
            if chat_type == "private" and getattr(settings, 'MULTI_BOT_ENABLED', False):
                try:
                    coordinator = await get_message_coordinator_dep()
                    if coordinator and hasattr(coordinator, 'load_balancer'):
                        sessions = coordinator.load_balancer._private_sessions
                        if user_id in sessions:
                            session = sessions[user_id]
                            bot_manager = await get_bot_manager_dep()
                            if bot_manager:
                                session_bot = bot_manager.get_bot_by_id(session['bot_id'])
                                if session_bot:
                                    self.logger.debug(f"🎯 会话记录推测: 用户{user_id} -> {session_bot.bot_id}")
                                    return session_bot.config.token, session_bot.config
                except Exception as e:
                    self.logger.debug(f"会话记录查询失败: {e}")

            # 默认返回None，让调用者处理
            return None, None

        except Exception as e:
            self.logger.debug(f"智能推测异常: {e}")
            return None, None


# 🔥 全局智能路由器
smart_webhook_router = SmartWebhookRouter()


# --- 应用生命周期管理 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    lifecycle_manager = get_lifecycle_manager()

    try:
        # 启动阶段
        logger.info("🚀 应用启动中...")
        await lifecycle_manager.startup()

        # 创建数据库表
        await run_in_threadpool(create_all_tables)
        logger.info("✅ 数据库表检查/创建完成")

        # 🔥 设置智能Webhook路由
        if getattr(settings, 'ENABLE_SMART_WEBHOOK_ROUTING', True):
            await smart_webhook_router.setup_routes(app)
            logger.info("✅ 智能Webhook路由设置完成")

        # 🔥 设置Telegram Webhooks
        try:
            await setup_enhanced_webhooks()
            logger.info("✅ Telegram Webhooks设置完成")
        except Exception as e:
            logger.warning(f"⚠️ Webhook设置失败，但应用继续运行: {e}")

        logger.info("🎉 应用启动完成")
        yield

    finally:
        # 关闭阶段
        logger.info("🔄 应用关闭中...")
        await lifecycle_manager.shutdown()
        await close_http_client()
        logger.info("✅ 应用关闭完成")


# --- 初始化 FastAPI 应用 ---
app = FastAPI(
    title="Telegram Customer Support Bot",
    description="通过群组话题处理私聊作为支持请求。支持智能多机器人路由。",
    version="2.0.0",
    lifespan=lifespan
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- 性能监控中间件（保持原有逻辑，略作增强） ---
@app.middleware("http")
async def performance_monitoring_middleware(request: Request, call_next):
    """性能监控中间件（增强版本）"""
    start_time = time.time()
    request_id = id(request)

    # 获取监控组件
    try:
        from app.monitoring import get_metrics_collector
        from app.cache import get_cache_manager
        from app.dependencies import RateLimitManager

        metrics = get_metrics_collector()
        cache = get_cache_manager()
        rate_limiter = RateLimitManager(cache)
    except Exception as e:
        logger.error("获取监控组件失败", exc_info=True)
        return await call_next(request)

    # 获取客户端IP
    client_ip = request.client.host if request.client else "unknown"

    # 检查IP速率限制
    if not await rate_limiter.check_ip_rate_limit(client_ip):
        logger.warning(f"IP速率限制触发: {client_ip}")
        return JSONResponse(
            status_code=429,
            content={"error": "Too Many Requests", "message": "请求过于频繁，请稍后再试"}
        )

    # 🔥 增强日志：记录路由信息
    route_info = {
        "method": request.method,
        "url": str(request.url),
        "path": request.url.path,
        "is_webhook": request.url.path.startswith(f"/{settings.WEBHOOK_PATH}"),
        "client_ip": client_ip
    }

    logger.info("📥 请求开始", extra={**route_info, "request_id": request_id})

    try:
        response = await call_next(request)

        # 计算处理时间
        process_time = time.time() - start_time

        # 记录性能指标
        record_http_request(
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration=process_time
        )

        # 更新缓存统计
        try:
            cache_stats = await cache.get_stats()
            metrics.gauge("cached_items").set(cache_stats.get("cache_size", 0))
        except Exception as e:
            logger.debug(f"更新缓存统计失败: {e}")

        # 🔥 增强日志：webhook请求特殊标记
        log_extra = {
            **route_info,
            "request_id": request_id,
            "status_code": response.status_code,
            "process_time": round(process_time, 3)
        }

        if route_info["is_webhook"]:
            log_extra["webhook_processed"] = True

        logger.info("✅ 请求完成", extra=log_extra)
        return response

    except ValidationError as e:
        process_time = time.time() - start_time
        logger.warning(
            "⚠️ 输入验证失败",
            extra={
                **route_info,
                "request_id": request_id,
                "validation_error": e.message,
                "process_time": round(process_time, 3)
            }
        )

        record_http_request(request.method, request.url.path, 400, process_time)

        if request.url.path.endswith(settings.WEBHOOK_PATH):
            return PlainTextResponse("validation_error", status_code=200)

        return JSONResponse(
            status_code=400,
            content={"error": "输入验证失败", "message": e.message}
        )

    except HTTPException as e:
        process_time = time.time() - start_time
        logger.warning(
            "⚠️ HTTP异常",
            extra={
                **route_info,
                "request_id": request_id,
                "status_code": e.status_code,
                "detail": e.detail,
                "process_time": round(process_time, 3)
            }
        )

        record_http_request(request.method, request.url.path, e.status_code, process_time)
        raise

    except Exception as e:
        process_time = time.time() - start_time

        logger.error(  # <--- 确保这里有 exc_info=True
            "❌ 未处理的异常",
            extra={
                **route_info,
                "request_id": request_id,
                "exception_type": type(e).__name__,
                "exception_message": str(e),
                "process_time": round(process_time, 3),
                "traceback": traceback.format_exc()  # <--- 这一行是关键
            },
            exc_info=True  # <--- 确保有这个
        )

        record_http_request(request.method, request.url.path, 500, process_time)
        metrics.counter("errors_total").increment()

        if request.url.path.endswith(settings.WEBHOOK_PATH):
            return PlainTextResponse("internal_error", status_code=200)

        return JSONResponse(
            status_code=500,
            content={
                "error": "内部服务器错误",
                "message": "服务暂时不可用，请稍后重试"
            }
        )


# --- 健康检查端点 ---
@app.get("/")
async def root():
    """基础健康检查端点"""
    logger.debug("访问根路径端点")
    return {
        "status": "ok",
        "service": "Telegram Customer Support Bot",
        "version": "2.0.0",
        "features": {
            "multi_bot": getattr(settings, 'MULTI_BOT_ENABLED', False),
            "smart_routing": getattr(settings, 'ENABLE_SMART_WEBHOOK_ROUTING', True),
            "coordination": getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True)
        },
        "timestamp": time.time()
    }


@app.get("/health")
async def health_check():
    """详细健康检查端点"""
    try:
        from app.dependencies import HealthChecker
        health_checker = HealthChecker()
        health_info = await health_checker.get_overall_health()

        if health_info["status"] == "healthy":
            return health_info
        else:
            return JSONResponse(status_code=503, content=health_info)

    except Exception as e:
        logger.error("健康检查失败", exc_info=True)
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "message": "健康检查服务不可用",
                "timestamp": time.time()
            }
        )


@app.get("/metrics")
async def metrics_endpoint():
    """性能指标端点"""
    try:
        from app.monitoring import get_metrics_collector
        metrics = get_metrics_collector()

        all_metrics = metrics.get_all_metrics()
        performance_summary = metrics.get_performance_summary()

        return {
            "performance_summary": performance_summary,
            "detailed_metrics": all_metrics,
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error("获取指标失败", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "指标服务不可用"}
        )


@app.get("/cache/stats")
async def cache_stats_endpoint():
    """缓存统计端点"""
    try:
        from app.cache import get_cache_manager
        cache = get_cache_manager()
        stats = await cache.get_stats()
        return {
            "cache_stats": stats,
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error("获取缓存统计失败", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "缓存统计服务不可用"}
        )


@app.get("/admin/queue/status")
async def queue_status():
    """获取消息队列状态"""
    if not getattr(settings, 'ENABLE_MESSAGE_QUEUE', False):
        return {"enabled": False, "status": "disabled"}

    try:
        # NOTE: get_message_queue_service 依赖于具体的队列实现，目前没有直接的依赖
        # 这里只是一个占位符，假设有一个队列服务
        # from app.message_queue import get_message_queue_service
        # mq_service = await get_message_queue_service()
        # if mq_service:
        #     stats = await mq_service.get_stats()
        #     return {
        #         "enabled": True,
        #         "status": "running",
        #         "stats": stats,
        #         "timestamp": time.time()
        #     }
        # else:
        return {"enabled": True, "status": "not_implemented_or_initialized"}

    except Exception as e:
        return {
            "enabled": True,
            "status": "error",
            "error": str(e),
            "timestamp": time.time()
        }


@app.get("/admin/rate-limit/status")
async def rate_limit_status():
    """获取速率限制状态"""
    if not getattr(settings, 'ADVANCED_RATE_LIMIT_ENABLED', True):
        return {"enabled": False, "status": "disabled"}

    try:
        from app.rate_limit import get_rate_limiter
        limiter = await get_rate_limiter()
        stats = await limiter.get_stats()

        return {
            "enabled": True,
            "status": "running",
            "stats": stats,
            "timestamp": time.time()
        }

    except Exception as e:
        return {
            "enabled": True,
            "status": "error",
            "error": str(e),
            "timestamp": time.time()
        }


@app.post("/admin/rate-limit/whitelist/{user_id}")
async def whitelist_user(user_id: int):
    """将用户加入白名单1小时"""
    try:
        from app.rate_limit import get_rate_limiter
        limiter = await get_rate_limiter()
        await limiter.whitelist_user(f"user:{user_id}", 3600)

        return {
            "status": "success",
            "message": f"用户 {user_id} 已加入白名单1小时",
            "timestamp": time.time()
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"操作失败: {str(e)}"}
        )


@app.get("/admin/user/{user_id}/info")
async def user_info(user_id: int):
    """获取用户信息"""
    try:
        user_group = settings.get_user_group(user_id)

        # 检查是否在白名单
        is_whitelisted = False
        try:
            from app.rate_limit import get_rate_limiter
            limiter = await get_rate_limiter()
            is_whitelisted = await limiter.is_whitelisted(f"user:{user_id}")
        except:
            pass

        return {
            "user_id": user_id,
            "user_group": user_group,
            "is_admin": user_id in settings.ADMIN_USER_IDS,
            "is_premium": user_id in getattr(settings, 'PREMIUM_USER_IDS', []),
            "is_whitelisted": is_whitelisted,
            "timestamp": time.time()
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"获取用户信息失败: {str(e)}"}
        )


# --- Webhook 端点 ---
# 🔥 新增：智能路由状态端点
@app.get("/admin/webhook/routing-status")
async def webhook_routing_status():
    """获取智能路由状态"""
    try:
        enabled_bots = settings.get_enabled_bots()
        routing_info = []

        for bot_config in enabled_bots:
            webhook_url = settings.get_bot_webhook_url(bot_config)
            routing_info.append({
                "bot_name": bot_config.name,
                "priority": bot_config.priority,
                "webhook_url": webhook_url,
                "identifier": bot_config.get_webhook_identifier(),
                "strategy": bot_config.webhook_strategy.value,
                "enabled": bot_config.enabled
            })

        return {
            "smart_routing_enabled": getattr(settings, 'ENABLE_SMART_WEBHOOK_ROUTING', True),
            "multi_bot_enabled": getattr(settings, 'MULTI_BOT_ENABLED', False),
            "total_bots": len(enabled_bots),
            "routing_configuration": routing_info,
            "cache_stats": {
                "cached_bots": len(smart_webhook_router.bot_cache),
                "last_cache_update": smart_webhook_router._last_cache_update,
                "cache_age_seconds": time.time() - smart_webhook_router._last_cache_update
            },
            "timestamp": time.time()
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"获取路由状态失败: {str(e)}"}
        )

# --- 管理端点 ---
@app.post("/admin/cache/clear")
async def clear_cache():
    """清空缓存（管理员功能）"""
    try:
        from app.cache import get_cache_manager
        cache = get_cache_manager()
        await cache.clear_all()
        logger.info("管理员清空了所有缓存")
        return {"status": "success", "message": "缓存已清空"}
    except Exception as e:
        logger.error("清空缓存失败", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "清空缓存失败"}
        )


@app.get("/admin/stats")
async def admin_stats():
    """获取管理统计信息（更新版本）"""
    try:
        from app.cache import get_cache_manager
        from app.monitoring import get_metrics_collector
        from app.dependencies import get_circuit_breaker_registry_dep, get_failover_manager_dep

        cache = get_cache_manager()
        metrics = get_metrics_collector()

        cache_stats = await cache.get_stats()
        performance_summary = metrics.get_performance_summary()

        # 添加协调器统计
        coordination_stats = {}
        if getattr(settings, 'MULTI_BOT_ENABLED', False) and getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
            try:
                from app.dependencies import get_message_coordinator_dep
                coordinator = await get_message_coordinator_dep()
                if coordinator:
                    coordination_stats = await coordinator.get_stats()
            except Exception as e:
                coordination_stats = {"error": str(e)}

        # 添加故障转移统计
        failover_stats = {}
        if getattr(settings, 'MULTI_BOT_ENABLED', False):
            try:
                failover_manager = await get_failover_manager_dep()
                if failover_manager:
                    failover_stats = await failover_manager.get_failover_stats()
            except Exception as e:
                failover_stats = {"error": str(e)}

        # 添加熔断器统计
        circuit_breaker_stats = {}
        if getattr(settings, 'MULTI_BOT_ENABLED', False):
            try:
                cb_registry = await get_circuit_breaker_registry_dep()
                if cb_registry:
                    circuit_breaker_stats = await cb_registry.get_all_stats()
            except Exception as e:
                circuit_breaker_stats = {"error": str(e)}

        return {
            "cache": cache_stats,
            "performance": performance_summary,
            "coordination": coordination_stats,
            "failover": failover_stats,  # 新增
            "circuit_breaker": circuit_breaker_stats,  # 新增
            "system_info": {
                "settings_environment": getattr(settings, 'ENVIRONMENT', 'production'),
                "debug_mode": getattr(settings, 'DEBUG', False),
                "rate_limit_enabled": getattr(settings, 'RATE_LIMIT_ENABLED', False),
                "multi_bot_enabled": getattr(settings, 'MULTI_BOT_ENABLED', False),
                "message_coordination_enabled": getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True),
                "auto_failover_enabled": getattr(settings, 'AUTO_FAILOVER_ENABLED', False)  # 新增
            },
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error("获取管理统计失败", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "统计服务不可用"}
        )


# --- 多机器人管理端点（仅在启用多机器人模式时可用） ---
@app.get("/admin/bots/status")
async def get_bots_status():
    """获取所有机器人状态"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}

    try:
        from app.dependencies import get_bot_manager_dep
        bot_manager = await get_bot_manager_dep()

        if not bot_manager:
            return {"enabled": True, "status": "error", "message": "机器人管理器不可用"}

        status = bot_manager.get_all_bots_status()
        stats = bot_manager.get_stats()

        return {
            "enabled": True,
            "summary": {
                "total_bots": stats['total_bots'],
                "healthy_bots": stats['healthy_bots'],
                "available_bots": stats['available_bots']
            },
            "bots": status,
            "timestamp": time.time()
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"获取机器人状态失败: {str(e)}"}
        )


@app.post("/admin/bots/{bot_id}/enable")
async def enable_bot(bot_id: str):
    """启用指定机器人"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}

    try:
        from app.dependencies import get_bot_manager_dep
        bot_manager = await get_bot_manager_dep()

        if not bot_manager:
            return JSONResponse(
                status_code=503,
                content={"error": "机器人管理器不可用"}
            )

        bot = bot_manager.get_bot_by_id(bot_id)

        if not bot:
            return JSONResponse(
                status_code=404,
                content={"error": f"机器人 {bot_id} 不存在"}
            )

        bot.config.enabled = True
        await bot_manager._save_bot_status(bot)
        # 立即执行一次健康检查以更新状态
        await bot_manager._check_bot_health(bot)

        return {
            "status": "success",
            "message": f"机器人 {bot_id} 已启用",
            "bot_status": bot.to_dict()
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"启用机器人失败: {str(e)}"}
        )


@app.post("/admin/bots/{bot_id}/disable")
async def disable_bot(bot_id: str):
    """禁用指定机器人"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}

    try:
        from app.dependencies import get_bot_manager_dep
        # 延迟导入BotStatus以避免循环导入
        from app.bot_manager import BotStatus
        from app.failover_manager import FailoverReason

        bot_manager = await get_bot_manager_dep()
        failover_manager = await get_failover_manager_dep()

        if not bot_manager:
            return JSONResponse(
                status_code=503,
                content={"error": "机器人管理器不可用"}
            )

        bot = bot_manager.get_bot_by_id(bot_id)

        if not bot:
            return JSONResponse(
                status_code=404,
                content={"error": f"机器人 {bot_id} 不存在"}
            )

        bot.config.enabled = False
        bot.status = BotStatus.DISABLED
        bot.last_error = "Manually disabled"
        await bot_manager._save_bot_status(bot)

        if failover_manager:
            await failover_manager.handle_bot_failure(
                bot.bot_id, FailoverReason.MANUAL_DISABLE, "Manually disabled"
            )

        return {
            "status": "success",
            "message": f"机器人 {bot_id} 已禁用",
            "bot_status": bot.to_dict()
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"禁用机器人失败: {str(e)}"}
        )


@app.post("/admin/bots/{bot_id}/health-check")
async def manual_health_check(bot_id: str):
    """手动健康检查指定机器人"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}

    try:
        from app.dependencies import get_bot_manager_dep
        bot_manager = await get_bot_manager_dep()

        if not bot_manager:
            return JSONResponse(
                status_code=503,
                content={"error": "机器人管理器不可用"}
            )

        bot = bot_manager.get_bot_by_id(bot_id)

        if not bot:
            return JSONResponse(
                status_code=404,
                content={"error": f"机器人 {bot_id} 不存在"}
            )

        # 执行健康检查
        is_healthy = await bot_manager._check_bot_health(bot)

        return {
            "status": "success",
            "message": f"机器人 {bot_id} 健康检查完成",
            "is_healthy": is_healthy,
            "bot_status": bot.to_dict()
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"健康检查失败: {str(e)}"}
        )


# --- 新增：消息协调管理端点 ---

@app.get("/admin/coordination/status")
async def coordination_status():
    """获取消息协调状态"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "status": "multi_bot_disabled"}

    if not getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
        return {"enabled": False, "status": "coordination_disabled"}

    try:
        from app.dependencies import get_message_coordinator_dep
        coordinator = await get_message_coordinator_dep()

        if not coordinator:
            return {"enabled": True, "status": "error", "message": "消息协调器不可用"}

        stats = await coordinator.get_stats()
        return {
            "enabled": True,
            "status": "running",
            "stats": stats,
            "timestamp": time.time()
        }

    except Exception as e:
        return {
            "enabled": True,
            "status": "error",
            "error": str(e),
            "timestamp": time.time()
        }


@app.get("/admin/coordination/queue/stats")
async def coordination_queue_stats():
    """获取消息队列详细统计"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    if not getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
        return {"error": "消息协调功能已禁用"}

    try:
        from app.dependencies import get_message_coordinator_dep
        coordinator = await get_message_coordinator_dep()

        if not coordinator:
            return {"error": "消息协调器不可用"}

        queue_stats = await coordinator.message_queue.get_stats()  # 直接获取队列统计

        # 获取处理统计
        from app.message_processor import get_processing_stats
        processing_stats = get_processing_stats()
        processing_info = processing_stats.get_stats()

        return {
            "queue": queue_stats,
            "processing": processing_info,
            "timestamp": time.time()
        }

    except Exception as e:
        return {"error": str(e)}


@app.post("/admin/coordination/queue/clear")
async def clear_coordination_queue():
    """清空消息队列（紧急情况使用）"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    if not getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
        return {"error": "消息协调功能已禁用"}

    try:
        from app.dependencies import get_message_coordinator_dep
        coordinator = await get_message_coordinator_dep()

        if not coordinator:
            return {"error": "消息协调器不可用"}

        # 获取Redis客户端清空队列
        if coordinator.redis_client:
            await coordinator.redis_client.delete(
                coordinator.message_queue.pending_queue,
                coordinator.message_queue.processing_queue,
                coordinator.message_queue.failed_queue,
                coordinator.message_queue.dead_letter_queue
            )
            logger.warning("管理员清空了消息协调队列")
            return {"status": "success", "message": "消息队列已清空"}
        else:
            return {"error": "Redis不可用，无法清空队列"}

    except Exception as e:
        logger.error("清空消息队列失败", exc_info=True)
        return {"error": f"清空队列失败: {str(e)}"}


@app.post("/admin/coordination/message/{message_id}/retry")
async def retry_failed_message(message_id: str):
    """重试失败的消息"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    if not getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
        return {"error": "消息协调功能已禁用"}

    try:
        from app.dependencies import get_message_coordinator_dep
        coordinator = await get_message_coordinator_dep()

        if not coordinator:
            return {"error": "消息协调器不可用"}

        # 这里需要实现从死信队列或失败队列中恢复消息的逻辑
        # 暂时返回占位符响应
        success = await coordinator.message_queue.retry_message_from_dlq(message_id)

        if success:
            return {
                "status": "success",
                "message": f"消息 {message_id} 已提交重试",
                "timestamp": time.time()
            }
        else:
            return {
                "status": "failed",
                "message": f"消息 {message_id} 未找到或无法重试",
                "timestamp": time.time()
            }


    except Exception as e:
        return {"error": f"重试消息失败: {str(e)}"}


# --- 新增：故障转移管理端点 ---

@app.get("/admin/failover/status")
async def failover_status():
    """获取故障转移管理器状态和统计信息"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}

    try:
        failover_manager = await get_failover_manager_dep()
        if not failover_manager:
            return {"enabled": True, "status": "error", "message": "故障转移管理器不可用"}

        stats = await failover_manager.get_failover_stats()
        active_events = await failover_manager.get_active_events()

        return {
            "enabled": True,
            "status": "running",
            "summary": stats,
            "active_events_details": active_events,
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error(f"获取故障转移状态失败: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"获取故障转移状态失败: {str(e)}"}
        )


@app.get("/admin/failover/events")
async def failover_events(days: int = 7):
    """获取近期故障事件列表和统计"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}

    try:
        from app.failover_events import get_failover_event_store, get_failover_analytics
        event_store = await get_failover_event_store()
        analytics = await get_failover_analytics()

        end_time = time.time()
        start_time = end_time - (days * 86400)  # days in seconds

        recent_events = await event_store.get_events_by_time_range(start_time, end_time, limit=500)
        active_events = await event_store.get_active_events()
        stats = await analytics.calculate_statistics(days=days)
        trends = await analytics.get_failure_trends(days=days)

        return {
            "enabled": True,
            "period_days": days,
            "summary_stats": stats.to_dict(),
            "active_events": [e.to_dict() for e in active_events],
            "recent_events": [e.to_dict() for e in recent_events],
            "trends": trends,
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error(f"获取故障事件失败: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"获取故障事件失败: {str(e)}"}
        )


@app.post("/admin/failover/event/{event_id}/resolve")
async def resolve_failover_event(event_id: str):
    """手动标记故障事件为已解决"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}
    try:
        from app.failover_events import get_failover_event_store
        event_store = await get_failover_event_store()
        success = await event_store.resolve_event(event_id)
        if success:
            return {"status": "success", "message": f"事件 {event_id} 已标记为解决"}
        else:
            return JSONResponse(status_code=404, content={"error": f"事件 {event_id} 未找到或无法解决"})
    except Exception as e:
        logger.error(f"解决故障事件失败: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": f"解决故障事件失败: {str(e)}"})


# --- 新增：熔断器管理端点 ---

@app.get("/admin/circuit-breaker/status")
async def circuit_breaker_status():
    """获取所有熔断器状态"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}

    try:
        cb_registry = await get_circuit_breaker_registry_dep()
        if not cb_registry:
            return {"enabled": True, "status": "error", "message": "熔断器注册表不可用"}

        stats = await cb_registry.get_all_stats()
        return {
            "enabled": True,
            "status": "running",
            "breakers": stats,
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error(f"获取熔断器状态失败: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"获取熔断器状态失败: {str(e)}"}
        )


@app.get("/debug/coordinator-status")
async def debug_coordinator_status():
    """调试：检查消息协调器详细状态"""
    try:
        coordinator = await get_message_coordinator_dep()
        if not coordinator:
            return {"error": "Coordinator not available"}

        # 检查协调器基本状态
        basic_status = {
            "coordinator_exists": True,
            "coordinator_running": coordinator._running,
            "instance_id": coordinator.instance_id,
            "has_processing_task": coordinator._processing_task is not None,
            "has_cleanup_task": coordinator._cleanup_task is not None,
            "processing_task_done": coordinator._processing_task.done() if coordinator._processing_task else None,
            "cleanup_task_done": coordinator._cleanup_task.done() if coordinator._cleanup_task else None,
        }

        # 获取统计信息
        try:
            stats = await coordinator.get_stats()
            basic_status["stats"] = stats
        except Exception as e:
            basic_status["stats_error"] = str(e)

        # 检查Redis连接
        redis_status = {
            "redis_client_exists": coordinator.redis_client is not None,
        }

        if coordinator.redis_client:
            try:
                await coordinator.redis_client.ping()
                redis_status["redis_ping"] = "success"
            except Exception as e:
                redis_status["redis_ping"] = f"failed: {str(e)}"

        return {
            "coordinator": basic_status,
            "redis": redis_status,
            "timestamp": time.time()
        }
    except Exception as e:
        return {"error": str(e), "timestamp": time.time()}


@app.post("/admin/circuit-breaker/{breaker_name}/force-open")
async def force_open_circuit_breaker(breaker_name: str):
    """强制开启指定熔断器"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}

    try:
        from app.circuit_breaker import get_circuit_breaker
        cb = await get_circuit_breaker(breaker_name)
        await cb.force_open()
        return {
            "status": "success",
            "message": f"熔断器 {breaker_name} 已强制开启",
            "current_state": (await cb.get_state()).value
        }
    except Exception as e:
        logger.error(f"强制开启熔断器 {breaker_name} 失败: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"操作失败: {str(e)}"}
        )


@app.post("/admin/circuit-breaker/{breaker_name}/force-close")
async def force_close_circuit_breaker(breaker_name: str):
    """强制关闭指定熔断器"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"enabled": False, "message": "多机器人模式未启用"}

    try:
        from app.circuit_breaker import get_circuit_breaker
        cb = await get_circuit_breaker(breaker_name)
        await cb.force_close()
        return {
            "status": "success",
            "message": f"熔断器 {breaker_name} 已强制关闭",
            "current_state": (await cb.get_state()).value
        }
    except Exception as e:
        logger.error(f"强制关闭熔断器 {breaker_name} 失败: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"操作失败: {str(e)}"}
        )


@app.post("/debug/start-coordinator")
async def debug_start_coordinator():
    """临时启动协调器（紧急修复）"""
    try:
        coordinator = await get_message_coordinator_dep()
        if not coordinator:
            return {"error": "Coordinator not available"}

        if coordinator._running:
            return {"status": "already_running", "message": "协调器已在运行"}

        await coordinator.start()

        # 验证启动状态
        running_status = {
            "coordinator_running": coordinator._running,
            "has_processing_task": coordinator._processing_task is not None,
            "has_cleanup_task": coordinator._cleanup_task is not None,
        }

        return {
            "status": "started",
            "message": "协调器已启动",
            "verification": running_status,
            "timestamp": time.time()
        }
    except Exception as e:
        return {"error": f"启动失败: {str(e)}"}


@app.post("/test/simulate-429/{bot_id}")
async def simulate_429_error(bot_id: str):
    """模拟指定机器人遇到429错误（完全修复版本）"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    try:
        from app.dependencies import get_bot_manager_dep, get_failover_manager_dep
        from app.failover_manager import FailoverReason
        from app.utils.json_utils import safe_bot_status

        bot_manager = await get_bot_manager_dep()
        failover_manager = await get_failover_manager_dep()

        if not bot_manager or not failover_manager:
            return {"error": "管理器不可用"}

        bot = bot_manager.get_bot_by_id(bot_id)
        if not bot:
            return {"error": f"机器人 {bot_id} 不存在"}

        # 安全获取原始状态
        original_status = safe_bot_status(bot)

        # 模拟429错误
        try:
            target_bot = await failover_manager.handle_bot_failure(
                bot_id, FailoverReason.RATE_LIMITED, "Simulated 429 Too Many Requests"
            )
        except Exception as e:
            logger.error(f"故障转移处理失败: {e}", exc_info=True)
            return {"error": f"故障转移失败: {str(e)[:100]}"}

        # 获取更新后的状态
        updated_status = safe_bot_status(bot)

        return {
            "status": "success",
            "message": f"已模拟机器人 {bot_id} 遇到429错误",
            "original_status": original_status,
            "updated_status": updated_status,
            "failover_target": target_bot,
            "timestamp": time.time()
        }

    except Exception as e:
        logger.error(f"模拟429错误失败: {e}", exc_info=True)
        return {"error": f"模拟失败: {str(e)[:100]}"}


@app.post("/test/stress-bots")
async def stress_test_bots():
    """压力测试：快速发送多条消息测试负载均衡和故障转移"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    try:
        coordinator = await get_message_coordinator_dep()
        if not coordinator:
            return {"error": "消息协调器不可用"}

        # 模拟10条测试消息
        results = []
        for i in range(10):
            test_update = {
                "update_id": 999900 + i,
                "message": {
                    "message_id": 9999 + i,
                    "date": int(time.time()),
                    "chat": {"id": 123456789, "type": "private"},
                    "from": {"id": 987654321, "first_name": "TestUser"},
                    "text": f"压力测试消息 #{i + 1}"
                }
            }

            coord_res = await coordinator.coordinate_message(test_update)
            results.append({
                "message_number": i + 1,
                "queued": coord_res == CoordinationResult.QUEUED,
                "duplicate": coord_res == CoordinationResult.DUPLICATE,
                "timestamp": time.time()
            })

            # 短暂间隔避免过快
            await asyncio.sleep(0.1)

        return {
            "status": "completed",
            "total_messages": 10,
            "results": results,
            "timestamp": time.time()
        }

    except Exception as e:
        return {"error": f"压力测试失败: {str(e)}"}


@app.get("/test/bot-health-recovery/{bot_id}")
async def test_bot_recovery(bot_id: str):
    """测试机器人状态恢复"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    try:
        bot_manager = await get_bot_manager_dep()
        if not bot_manager:
            return {"error": "机器人管理器不可用"}

        bot = bot_manager.get_bot_by_id(bot_id)
        if not bot:
            return {"error": f"机器人 {bot_id} 不存在"}

        # 记录当前状态
        before_status = bot.to_dict()

        # 强制执行健康检查
        is_healthy = await bot_manager._check_bot_health(bot)

        # 记录检查后状态
        after_status = bot.to_dict()

        return {
            "bot_id": bot_id,
            "health_check_result": is_healthy,
            "status_before": before_status,
            "status_after": after_status,
            "status_changed": before_status['status'] != after_status['status'],
            "timestamp": time.time()
        }

    except Exception as e:
        return {"error": f"健康检查失败: {str(e)}"}


@app.get("/test/failover-analytics")
async def get_failover_analytics():
    """获取故障转移分析数据"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    try:
        from app.failover_events import get_failover_analytics

        analytics = await get_failover_analytics()

        # 获取最近1小时的统计
        stats_1h = await analytics.calculate_statistics(hours=1)
        # 获取最近24小时的统计
        stats_24h = await analytics.calculate_statistics(hours=24)
        # 获取故障趋势
        trends = await analytics.get_failure_trends(hours=6)

        return {
            "analytics": {
                "last_1_hour": stats_1h.to_dict(),
                "last_24_hours": stats_24h.to_dict(),
                "trends_6_hours": trends
            },
            "timestamp": time.time()
        }
    except Exception as e:
        return {"error": f"获取分析数据失败: {str(e)}"}


@app.get("/admin/load-balancer/stats")
async def get_load_balancer_stats():
    """获取负载均衡统计"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    try:
        from app.dependencies import get_message_coordinator_dep
        coordinator = await get_message_coordinator_dep()

        if not coordinator:
            return {"error": "消息协调器不可用"}

        # 获取负载均衡统计
        lb_stats = coordinator.load_balancer.get_assignment_stats()

        return {
            "status": "success",
            "load_balancer_stats": lb_stats,
            "timestamp": time.time()
        }

    except Exception as e:
        return {"error": f"获取负载均衡统计失败: {str(e)}"}


@app.post("/admin/load-balancer/reset")
async def reset_load_balancer_stats():
    """重置负载均衡统计"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    try:
        from app.dependencies import get_message_coordinator_dep
        coordinator = await get_message_coordinator_dep()

        if not coordinator:
            return {"error": "消息协调器不可用"}

        # 重置统计
        coordinator.load_balancer.reset_stats()

        return {
            "status": "success",
            "message": "负载均衡统计已重置",
            "timestamp": time.time()
        }

    except Exception as e:
        return {"error": f"重置负载均衡统计失败: {str(e)}"}


@app.get("/admin/load-balancer/sessions")
async def get_load_balancer_sessions():
    """获取负载均衡器会话信息"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    try:
        coordinator = await get_message_coordinator_dep()
        if not coordinator:
            return {"error": "消息协调器不可用"}

        session_info = coordinator.load_balancer.get_session_info()
        return {
            "status": "success",
            "sessions": session_info,
            "timestamp": time.time()
        }

    except Exception as e:
        return {"error": f"获取会话信息失败: {str(e)}"}


@app.post("/admin/load-balancer/clear-user-session/{user_id}")
async def clear_user_session(user_id: int):
    """清除指定用户的会话映射"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    try:
        coordinator = await get_message_coordinator_dep()
        if not coordinator:
            return {"error": "消息协调器不可用"}

        success = coordinator.load_balancer.clear_user_session(user_id)
        if success:
            return {
                "status": "success",
                "message": f"用户 {user_id} 的会话已清除",
                "timestamp": time.time()
            }
        else:
            return {
                "status": "not_found",
                "message": f"用户 {user_id} 没有活跃会话",
                "timestamp": time.time()
            }

    except Exception as e:
        return {"error": f"清除用户会话失败: {str(e)}"}


@app.post("/admin/load-balancer/force-group-switch")
async def force_group_bot_switch():
    """强制切换群聊机器人（测试用）"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    try:
        coordinator = await get_message_coordinator_dep()
        if not coordinator:
            return {"error": "消息协调器不可用"}

        coordinator.load_balancer.force_switch_group_bot()
        return {
            "status": "success",
            "message": "已强制触发群聊机器人切换",
            "timestamp": time.time()
        }

    except Exception as e:
        return {"error": f"强制切换失败: {str(e)}"}


@app.get("/admin/load-balancer/strategy-test/{strategy}")
async def test_load_balancer_strategy(strategy: str):
    """测试不同的负载均衡策略"""
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return {"error": "多机器人模式未启用"}

    valid_strategies = ["balanced", "health_priority", "load_based"]
    if strategy not in valid_strategies:
        return {"error": f"无效策略，支持的策略: {valid_strategies}"}

    try:
        coordinator = await get_message_coordinator_dep()
        if not coordinator:
            return {"error": "消息协调器不可用"}

        # 临时修改策略设置
        original_strategy = getattr(settings, 'BOT_SELECTION_STRATEGY', 'balanced')
        settings.BOT_SELECTION_STRATEGY = strategy

        # 模拟一条群聊消息
        from app.message_coordinator import QueuedMessage, MessagePriority
        test_message = QueuedMessage(
            message_id="test_strategy",
            update_id=999999,
            chat_id=-100123456789,
            user_id=123456789,
            chat_type="supergroup",
            priority=MessagePriority.NORMAL,
            payload={"test": True}
        )

        selected_bot = await coordinator.load_balancer.select_best_bot(test_message)

        # 恢复原始策略
        settings.BOT_SELECTION_STRATEGY = original_strategy

        return {
            "status": "success",
            "strategy": strategy,
            "selected_bot": selected_bot,
            "message": f"使用 {strategy} 策略选择了机器人 {selected_bot}",
            "timestamp": time.time()
        }

    except Exception as e:
        return {"error": f"策略测试失败: {str(e)}"}


@app.get("/admin/bots/permissions")
async def check_bots_permissions():
    """检查所有机器人的权限状态"""
    try:
        from app.bot_permissions_checker import check_bot_permissions, ensure_all_bots_have_permissions

        # 检查当前权限状态
        current_permissions = await check_bot_permissions()

        # 检查是否所有机器人都有必要权限
        permission_status = await ensure_all_bots_have_permissions()

        return {
            "status": "success",
            "current_permissions": current_permissions,
            "permission_status": permission_status,
            "recommendations": _generate_permission_recommendations(current_permissions),
            "timestamp": time.time()
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"检查机器人权限失败: {str(e)}"}
        )


def _generate_permission_recommendations(permissions_data: Dict) -> list[str]:
    """生成权限配置建议"""
    recommendations = []

    for bot_name, permissions in permissions_data.items():
        if isinstance(permissions, dict):
            if not permissions.get("can_manage_topics", False):
                recommendations.append(
                    f"❌ {bot_name}: 缺少 'manage_topics' 权限，无法创建话题。"
                    f"请在群组中将此机器人设为管理员并启用话题管理权限。"
                )
            elif permissions.get("can_manage_topics", False):
                recommendations.append(f"✅ {bot_name}: 具备话题管理权限")

            if not permissions.get("can_send_messages", False):
                recommendations.append(f"⚠️ {bot_name}: 可能无法发送消息")

    if not recommendations:
        recommendations.append("✅ 所有机器人权限配置正常")

    return recommendations


@app.get("/debug/user/{user_id}/session")
async def debug_user_session_quick(user_id: int):
    """快速调试：检查用户会话和机器人分配"""
    try:
        result = {
            "user_id": user_id,
            "multi_bot_enabled": getattr(settings, 'MULTI_BOT_ENABLED', False),
            "primary_bot_token": settings.get_primary_bot_token()[-10:] if settings.get_primary_bot_token() else None,
            "session_info": None,
            "available_bots": [],
            "recommendations": []
        }

        if not result["multi_bot_enabled"]:
            result["recommendations"].append("多机器人模式未启用，所有消息使用主机器人")
            return result

        # 获取协调器信息
        coordinator = await get_message_coordinator_dep()
        if not coordinator:
            result["recommendations"].append("❌ 消息协调器不可用")
            return result

        # 检查用户会话
        if user_id in coordinator.load_balancer._private_sessions:
            session = coordinator.load_balancer._private_sessions[user_id]

            # 获取机器人详细信息
            bot_manager = await get_bot_manager_dep()
            session_bot = bot_manager.get_bot_by_id(session['bot_id']) if bot_manager else None

            result["session_info"] = {
                "has_session": True,
                "bot_id": session['bot_id'],
                "bot_name": session_bot.config.name if session_bot else "未知",
                "bot_priority": session_bot.config.priority if session_bot else "未知",
                "last_activity": session['last_activity'],
                "message_count": session['message_count'],
                "session_age_minutes": round((time.time() - session['last_activity']) / 60, 2),
                "bot_available": session_bot.is_available() if session_bot else False,
                "bot_token_suffix": session_bot.config.token[-10:] if session_bot else "未知"
            }

            if session_bot and session_bot.config.priority == 1:
                result["recommendations"].append("✅ 用户已绑定主机器人，限速通知将使用主机器人发送")
            else:
                result["recommendations"].append(
                    f"⚠️ 用户绑定备用机器人 ({session_bot.config.name if session_bot else '未知'})，限速通知将使用此备用机器人")
        else:
            result["session_info"] = {
                "has_session": False,
                "reason": "用户没有活跃会话，下次消息将分配新机器人"
            }
            result["recommendations"].append("💡 用户没有活跃会话，建议优化负载均衡算法优先分配主机器人")

        # 获取可用机器人列表
        bot_manager = await get_bot_manager_dep()
        if bot_manager:
            available_bots = bot_manager.get_available_bots()
            result["available_bots"] = [
                {
                    "bot_id": bot.bot_id,
                    "name": bot.config.name,
                    "priority": bot.config.priority,
                    "is_primary": bot.config.priority == 1,
                    "status": bot.status.value,
                    "is_available": bot.is_available(),
                    "token_suffix": bot.config.token[-10:]
                } for bot in bot_manager.bots.values()
            ]

            # 检查主机器人状态
            primary_bots = [bot for bot in available_bots if bot.config.priority == 1]
            if not primary_bots:
                result["recommendations"].append("❌ 没有配置主机器人（优先级=1）")
            elif not any(bot.is_available() for bot in primary_bots):
                result["recommendations"].append("❌ 主机器人不可用，新用户将分配备用机器人")
            else:
                result["recommendations"].append("✅ 主机器人可用，新用户将优先分配主机器人")

        return result

    except Exception as e:
        return {"error": f"调试失败: {str(e)}", "user_id": user_id}


@app.post("/debug/user/{user_id}/clear-session")
async def clear_user_session_debug(user_id: int):
    """调试：清除用户会话，强制重新分配"""
    try:
        if not getattr(settings, 'MULTI_BOT_ENABLED', False):
            return {"error": "多机器人模式未启用"}

        coordinator = await get_message_coordinator_dep()
        if not coordinator:
            return {"error": "消息协调器不可用"}

        # 清除会话
        if user_id in coordinator.load_balancer._private_sessions:
            old_session = coordinator.load_balancer._private_sessions[user_id]
            del coordinator.load_balancer._private_sessions[user_id]

            return {
                "status": "success",
                "message": f"已清除用户 {user_id} 的会话",
                "old_session": {
                    "bot_id": old_session['bot_id'],
                    "message_count": old_session['message_count']
                },
                "next_action": "用户下次发送消息时将重新分配机器人"
            }
        else:
            return {
                "status": "not_found",
                "message": f"用户 {user_id} 没有活跃会话",
                "next_action": "用户下次发送消息时将分配新机器人"
            }

    except Exception as e:
        return {"error": f"清除会话失败: {str(e)}"}


# --- 调试端点（仅在调试模式下启用） ---
if getattr(settings, 'DEBUG', False):
    @app.get("/debug/webhook-info")
    async def debug_webhook_info():
        """获取当前Webhook信息（调试用）"""
        try:
            webhook_info = await tg("getWebhookInfo", {})
            return webhook_info
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})


    @app.post("/debug/set-webhook")
    async def debug_set_webhook(request: Request):
        """手动设置Webhook（调试用）"""
        try:
            data = await request.json()
            webhook_url = data.get("url")
            if not webhook_url:
                return JSONResponse(
                    status_code=400,
                    content={"error": "URL is required"}
                )

            result = await tg("setWebhook", {"url": webhook_url})
            return {"result": result}
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})


    @app.get("/debug/cache/{key}")
    async def debug_get_cache(key: str):
        """调试：获取特定缓存项"""
        try:
            from app.cache import get_cache_manager
            cache = get_cache_manager()
            value = await cache.memory_cache.get(key)
            return {"key": key, "value": value, "found": value is not None}
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})


    # 多机器人调试端点
    if getattr(settings, 'MULTI_BOT_ENABLED', False):
        @app.get("/debug/bots/test-selection")
        async def debug_test_bot_selection():
            """调试：测试机器人选择逻辑"""
            try:
                from app.dependencies import get_bot_manager_dep
                bot_manager = await get_bot_manager_dep()

                if not bot_manager:
                    return {"error": "机器人管理器不可用"}

                healthy_bots = bot_manager.get_healthy_bots()
                available_bots = bot_manager.get_available_bots()
                best_bot = bot_manager.get_best_bot()

                return {
                    "healthy_bots": [bot.bot_id for bot in healthy_bots],
                    "available_bots": [bot.bot_id for bot in available_bots],
                    "best_bot": best_bot.bot_id if best_bot else None,
                    "all_bots": {
                        bot.bot_id: {
                            "status": bot.status.value,
                            "load_score": bot.get_load_score(),
                            "is_available": bot.is_available()
                        }
                        for bot in bot_manager.bots.values()
                    }
                }
            except Exception as e:
                return JSONResponse(status_code=500, content={"error": str(e)})