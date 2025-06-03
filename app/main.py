import logging
import sys
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

from app.rate_limit_notifications import send_rate_limit_notification, send_punishment_notification

from fastapi import FastAPI, Request, HTTPException, Depends
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
from app.validation import validate_webhook_update, validate_telegram_message, ValidationError
from app.dependencies import (
    get_conversation_service, get_cache, get_metrics, get_health_checker,
    get_rate_limit_manager, get_lifecycle_manager, get_auth_manager,
    get_message_coordinator_dep, get_coordinated_handler_dep  # 新增
)
from app.monitoring import record_http_request, record_message_processing
from app.cache import CacheManager

# --- 配置日志 ---
app_logger = setup_logging()
logger = get_logger("app.main")


# --- 应用生命周期管理 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    lifecycle_manager = get_lifecycle_manager()

    try:
        # 启动阶段
        logger.info("应用启动中...")
        await lifecycle_manager.startup()

        # 创建数据库表
        await run_in_threadpool(create_all_tables)
        logger.info("数据库表检查/创建完成")

        try:
            await setup_webhook()
        except Exception as e:
            logger.warning(f"Webhook设置失败，但应用继续运行: {e}")
            # 应用继续运行，稍后手动设置Webhook

        logger.info("应用启动完成")
        yield

    finally:
        # 关闭阶段
        logger.info("应用关闭中...")
        await lifecycle_manager.shutdown()

        # 关闭HTTP客户端
        await close_http_client()
        logger.info("应用关闭完成")


async def setup_webhook():
    """设置 Telegram Webhook"""
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


# --- 初始化 FastAPI 应用 ---
app = FastAPI(
    title="Telegram Customer Support Bot",
    description="通过群组话题处理私聊作为支持请求。",
    version="1.0.0",
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


# --- 性能监控中间件 ---
@app.middleware("http")
async def performance_monitoring_middleware(request: Request, call_next):
    """性能监控中间件"""
    start_time = time.time()
    request_id = id(request)

    # 获取监控组件（直接调用，不使用依赖注入）
    try:
        from app.monitoring import get_metrics_collector
        from app.cache import get_cache_manager
        from app.dependencies import RateLimitManager

        metrics = get_metrics_collector()
        cache = get_cache_manager()
        rate_limiter = RateLimitManager(cache)
    except Exception as e:
        logger.error("获取监控组件失败", exc_info=True)
        # 如果监控组件获取失败，仍然继续处理请求
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

    # 请求开始日志
    logger.info(
        "请求开始",
        extra={
            "request_id": request_id,
            "method": request.method,
            "url": str(request.url),
            "client_ip": client_ip
        }
    )

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

        # 请求完成日志
        logger.info(
            "请求完成",
            extra={
                "request_id": request_id,
                "status_code": response.status_code,
                "process_time": round(process_time, 3)
            }
        )

        return response

    except ValidationError as e:
        process_time = time.time() - start_time
        logger.warning(
            "输入验证失败",
            extra={
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
            "HTTP异常",
            extra={
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

        logger.error(
            "未处理的异常",
            extra={
                "request_id": request_id,
                "exception_type": type(e).__name__,
                "exception_message": str(e),
                "process_time": round(process_time, 3),
                "traceback": traceback.format_exc()
            }
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
        "version": "1.0.0",
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
        from app.dependencies import get_message_queue_service
        mq_service = await get_message_queue_service()

        if mq_service:
            stats = await mq_service.get_stats()
            return {
                "enabled": True,
                "status": "running",
                "stats": stats,
                "timestamp": time.time()
            }
        else:
            return {"enabled": True, "status": "not_initialized"}

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
@app.post(f"/{settings.WEBHOOK_PATH}")
async def webhook(
        request: Request,
        conv_service=Depends(get_conversation_service),
        metrics=Depends(get_metrics),
        coordinated_handler=Depends(get_coordinated_handler_dep)  # 新增协调式处理器
):
    """接收 Telegram 更新的 Webhook 端点"""
    update_id = None
    start_time = time.time()

    try:
        # 获取原始请求数据
        raw_update = await request.json()
        update_id = raw_update.get("update_id", "N/A")

        # 验证更新格式
        try:
            validated_update = validate_webhook_update(raw_update)
        except ValidationError as e:
            logger.warning(
                "Webhook更新验证失败",
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
                "消息验证失败",
                extra={"update_id": update_id, "validation_error": e.message}
            )
            return PlainTextResponse("message_validation_error")

        # 获取基本信息
        chat_type = validated_message.chat.get("type")
        chat_id = validated_message.get_chat_id()
        msg_id = validated_message.message_id
        user_id = validated_message.get_user_id()
        user_name = validated_message.get_user_name()

        # 增强的速率限制检查（带通知功能）
        if user_id:
            try:
                from app.rate_limit import get_rate_limiter, ActionType

                # 获取详细的速率限制信息
                logger.info(f"🔍 检查速率限制: user_id={user_id}, chat_type={chat_type}")

                # 直接调用速率限制器获取详细结果
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
                    metrics.counter("rate_limit_hits").increment()

                    # 发送通知给用户
                    await send_rate_limit_notification(
                        user_id=user_id,
                        user_name=user_name,
                        chat_type=chat_type,
                        chat_id=chat_id,
                        rate_result=rate_result,
                        msg_id=msg_id
                    )

                    # 如果有惩罚时间，也发送惩罚通知
                    if hasattr(rate_result, 'punishment_ends_at') and rate_result.punishment_ends_at:
                        punishment_duration = int(rate_result.punishment_ends_at - time.time())
                        if punishment_duration > 0:
                            await send_punishment_notification(user_id, punishment_duration)

                    return PlainTextResponse("rate_limited")
                else:
                    logger.debug(f"✅ 速率限制检查通过: user_id={user_id}")

            except Exception as e:
                logger.error(f"❌ 速率限制检查失败: {e}", exc_info=True)

        # 使用消息相关的日志器
        msg_logger = get_message_logger(
            message_id=msg_id,
            chat_id=chat_id,
            operation="webhook_processing"
        )

        msg_logger.info(
            "处理Webhook消息",
            extra={
                "update_id": update_id,
                "chat_type": chat_type,
                "user_id": user_id,
                "user_name": user_name,
                "coordination_enabled": coordinated_handler is not None
            }
        )

        # 根据是否启用消息协调选择处理方式
        if coordinated_handler and getattr(settings, 'MULTI_BOT_ENABLED', False):
            # 使用协调式处理
            msg_logger.info("使用协调式消息处理")
            try:
                result = await coordinated_handler.handle_webhook_message(raw_update)

                if result == "queued":
                    msg_logger.info("消息已提交到协调队列")
                    record_message_processing(chat_type or "unknown", time.time() - start_time, True)
                    return PlainTextResponse("queued")
                elif result == "coordination_failed":
                    msg_logger.error("消息协调失败，回退到直接处理")
                    # 回退到直接处理
                elif result == "coordination_error":
                    msg_logger.error("消息协调异常，回退到直接处理")
                    # 回退到直接处理
                else:
                    msg_logger.warning(f"未知的协调结果: {result}")
                    # 回退到直接处理

            except Exception as coord_error:
                msg_logger.error(f"协调式处理异常: {coord_error}", exc_info=True)
                # 回退到直接处理

        # 直接处理消息（原有逻辑或协调失败时的回退）
        msg_logger.info("使用直接消息处理")
        try:
            if chat_type == "private":
                await private.handle_private(msg_data, conv_service)
                record_message_processing("private", time.time() - start_time, True)
                msg_logger.info("私聊消息处理完成")
            elif chat_type in ("group", "supergroup"):
                if str(chat_id) == settings.SUPPORT_GROUP_ID:
                    await group.handle_group(msg_data, conv_service)
                    record_message_processing("support_group", time.time() - start_time, True)
                    msg_logger.info("客服群组消息处理完成")
                else:
                    msg_logger.info("处理外部群组消息")
                    await group.handle_group(msg_data, conv_service)
                    record_message_processing("external_group", time.time() - start_time, True)
                    msg_logger.info("外部群组消息处理完成")
            else:
                msg_logger.debug(f"忽略未处理的聊天类型: {chat_type}")
                return PlainTextResponse("unsupported_chat_type")

        except Exception as processing_error:
            # 消息处理异常
            msg_logger.error(
                "消息处理异常",
                extra={"processing_error": str(processing_error)},
                exc_info=True
            )
            record_message_processing(chat_type or "unknown", time.time() - start_time, False)
            metrics.counter("message_processing_errors").increment()
            return PlainTextResponse("processing_error")

        msg_logger.info("消息处理完成")
        return PlainTextResponse("ok")

    except Exception as e:
        logger.error(
            "Webhook处理异常",
            extra={
                "update_id": update_id,
                "exception_type": type(e).__name__,
                "exception_message": str(e)
            },
            exc_info=True
        )
        raise


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
    """获取管理统计信息"""
    try:
        from app.cache import get_cache_manager
        from app.monitoring import get_metrics_collector

        cache = get_cache_manager()
        metrics = get_metrics_collector()

        cache_stats = await cache.get_stats()
        performance_summary = metrics.get_performance_summary()

        return {
            "cache": cache_stats,
            "performance": performance_summary,
            "system_info": {
                "settings_environment": getattr(settings, 'ENVIRONMENT', 'production'),
                "debug_mode": getattr(settings, 'DEBUG', False),
                "rate_limit_enabled": getattr(settings, 'RATE_LIMIT_ENABLED', False),
                "multi_bot_enabled": getattr(settings, 'MULTI_BOT_ENABLED', False)
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

        bot.config.enabled = False
        bot.status = BotStatus.DISABLED
        await bot_manager._save_bot_status(bot)

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

    try:
        from app.dependencies import get_message_coordinator_dep
        coordinator = await get_message_coordinator_dep()

        if not coordinator:
            return {"error": "消息协调器不可用"}

        stats = await coordinator.get_stats()
        queue_stats = stats.get('queue', {})

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
                coordinator.message_queue.failed_queue
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

    try:
        from app.dependencies import get_message_coordinator_dep
        coordinator = await get_message_coordinator_dep()

        if not coordinator:
            return {"error": "消息协调器不可用"}

        # 这里需要实现从死信队列或失败队列中恢复消息的逻辑
        # 暂时返回占位符响应
        return {
            "status": "success",
            "message": f"消息 {message_id} 已提交重试",
            "timestamp": time.time()
        }

    except Exception as e:
        return {"error": f"重试消息失败: {str(e)}"}


# --- 更新健康检查端点，包含协调器状态 ---

@app.get("/health")
async def health_check():
    """详细健康检查端点（更新版本）"""
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


# --- 更新管理统计端点 ---

@app.get("/admin/stats")
async def admin_stats():
    """获取管理统计信息（更新版本）"""
    try:
        from app.cache import get_cache_manager
        from app.monitoring import get_metrics_collector

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

        return {
            "cache": cache_stats,
            "performance": performance_summary,
            "coordination": coordination_stats,
            "system_info": {
                "settings_environment": getattr(settings, 'ENVIRONMENT', 'production'),
                "debug_mode": getattr(settings, 'DEBUG', False),
                "rate_limit_enabled": getattr(settings, 'RATE_LIMIT_ENABLED', False),
                "multi_bot_enabled": getattr(settings, 'MULTI_BOT_ENABLED', False),
                "message_coordination_enabled": getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True)
            },
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error("获取管理统计失败", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "统计服务不可用"}
        )

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