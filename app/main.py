import logging
import sys
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

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
    get_rate_limit_manager, get_lifecycle_manager, get_auth_manager
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

        # 自动设置 Webhook
        await setup_webhook()

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
        from app.monitoring import get_metrics_collector  # 修复：改为绝对导入
        from app.cache import get_cache_manager  # 修复：改为绝对导入
        from app.dependencies import RateLimitManager  # 修复：改为绝对导入

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
        from app.dependencies import HealthChecker  # 修复：改为绝对导入
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
        from app.monitoring import get_metrics_collector  # 修复：改为绝对导入
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
        from app.cache import get_cache_manager  # 修复：改为绝对导入
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


# --- Webhook 端点 ---
@app.post(f"/{settings.WEBHOOK_PATH}")
async def webhook(
        request: Request,
        conv_service=Depends(get_conversation_service),
        metrics=Depends(get_metrics)
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

        # 用户速率限制检查
        if user_id:
            try:
                from app.cache import get_cache_manager  # 修复：改为绝对导入
                from app.dependencies import RateLimitManager  # 修复：改为绝对导入
                cache = get_cache_manager()
                rate_limiter = RateLimitManager(cache)

                if not await rate_limiter.check_user_rate_limit(user_id):
                    logger.warning(f"用户速率限制触发: {user_id}")
                    # 对于Telegram webhook，仍然返回200，但记录限制事件
                    metrics.counter("rate_limit_hits").increment()
                    return PlainTextResponse("rate_limited")
            except Exception as e:
                logger.debug(f"速率限制检查失败: {e}")

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
                "user_name": user_name
            }
        )

        # 根据聊天类型分发处理
        try:
            if chat_type == "private":
                await private.handle_private(msg_data, conv_service)
                record_message_processing("private", time.time() - start_time, True)
            elif chat_type in ("group", "supergroup"):
                if str(chat_id) == settings.SUPPORT_GROUP_ID:
                    await group.handle_group(msg_data, conv_service)
                    record_message_processing("support_group", time.time() - start_time, True)
                else:
                    msg_logger.info("处理外部群组消息")
                    await group.handle_group(msg_data, conv_service)
                    record_message_processing("external_group", time.time() - start_time, True)
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
            # 仍然返回200给Telegram，避免重试
            return PlainTextResponse("processing_error")

        msg_logger.info("消息处理完成")
        return PlainTextResponse("ok")

    except Exception as e:
        # 这个异常会被中间件捕获，但我们在这里也记录一下
        logger.error(
            "Webhook处理异常",
            extra={
                "update_id": update_id,
                "exception_type": type(e).__name__,
                "exception_message": str(e)
            },
            exc_info=True
        )
        # 重新抛出让中间件处理
        raise


# --- 管理端点 ---
@app.post("/admin/cache/clear")
async def clear_cache():
    """清空缓存（管理员功能）"""
    try:
        from app.cache import get_cache_manager  # 修复：改为绝对导入
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
        from app.cache import get_cache_manager  # 修复：改为绝对导入
        from app.monitoring import get_metrics_collector  # 修复：改为绝对导入

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
                "rate_limit_enabled": getattr(settings, 'RATE_LIMIT_ENABLED', False)
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
            from app.cache import get_cache_manager  # 修复：改为绝对导入
            cache = get_cache_manager()
            value = await cache.memory_cache.get(key)
            return {"key": key, "value": value, "found": value is not None}
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})