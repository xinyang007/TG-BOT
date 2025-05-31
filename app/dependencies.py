from functools import lru_cache
from typing import Optional, AsyncGenerator
from contextlib import asynccontextmanager
import asyncio

from fastapi import Depends, HTTPException, status
from starlette.concurrency import run_in_threadpool

from app.settings import settings
from app.store import db, connect_db, close_db, Conversation  # 修复：改为绝对导入
from app.tg_utils import tg  # 修复：改为绝对导入
from app.services.conversation_service import ConversationService  # 修复：改为绝对导入
from app.cache import CacheManager, get_cache_manager  # 修复：改为绝对导入
from app.monitoring import MetricsCollector, get_metrics_collector  # 修复：改为绝对导入
from app.logging_config import get_logger  # 修复：改为绝对导入

logger = get_logger("app.dependencies")


# === 数据库依赖 ===

class DatabaseManager:
    """数据库连接管理器"""

    def __init__(self):
        self._connection_pool_initialized = False
        self.logger = get_logger("app.database")

    async def initialize(self):
        """初始化数据库连接"""
        if not self._connection_pool_initialized:
            try:
                await run_in_threadpool(connect_db)
                self._connection_pool_initialized = True
                self.logger.info("Database connection initialized")
            except Exception as e:
                self.logger.error("Failed to initialize database", exc_info=True)
                raise

    async def close(self):
        """关闭数据库连接"""
        if self._connection_pool_initialized:
            try:
                await run_in_threadpool(close_db)
                self._connection_pool_initialized = False
                self.logger.info("Database connection closed")
            except Exception as e:
                self.logger.error("Error closing database connection", exc_info=True)

    @asynccontextmanager
    async def get_connection(self):
        """获取数据库连接的上下文管理器"""
        if not self._connection_pool_initialized:
            await self.initialize()

        try:
            # 检查连接状态
            if db.is_closed():
                await self.initialize()
            yield db
        except Exception as e:
            self.logger.error("Database connection error", exc_info=True)
            raise


# 全局数据库管理器
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """获取数据库管理器"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


async def get_database() -> DatabaseManager:
    """FastAPI依赖：获取数据库连接"""
    db_manager = get_database_manager()
    await db_manager.initialize()
    return db_manager


# === 缓存依赖 ===

async def get_cache() -> CacheManager:
    """FastAPI依赖：获取缓存管理器"""
    return get_cache_manager()


# === 监控依赖 ===

async def get_metrics() -> MetricsCollector:
    """FastAPI依赖：获取指标收集器"""
    return get_metrics_collector()


# === 服务依赖 ===

class ServiceManager:
    """服务管理器"""

    def __init__(self):
        self._conversation_service: Optional[ConversationService] = None
        self.logger = get_logger("app.services")

    async def get_conversation_service(
            self,
            cache: CacheManager = Depends(get_cache),
            metrics: MetricsCollector = Depends(get_metrics),
            db_manager: DatabaseManager = Depends(get_database)
    ) -> ConversationService:
        """获取对话服务实例"""
        if self._conversation_service is None:
            try:
                # 简化服务初始化，移除可能不存在的参数
                self._conversation_service = ConversationService(
                    support_group_id=settings.SUPPORT_GROUP_ID,
                    external_group_ids=settings.EXTERNAL_GROUP_IDS,
                    tg_func=tg
                )
                self.logger.info("ConversationService initialized")
            except Exception as e:
                self.logger.error("Failed to initialize ConversationService", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Service initialization failed"
                )

        return self._conversation_service

    async def cleanup(self):
        """清理服务资源"""
        self._conversation_service = None
        self.logger.info("Services cleaned up")


# 全局服务管理器
_service_manager: Optional[ServiceManager] = None


def get_service_manager() -> ServiceManager:
    """获取服务管理器"""
    global _service_manager
    if _service_manager is None:
        _service_manager = ServiceManager()
    return _service_manager


async def get_conversation_service(
        service_manager: ServiceManager = Depends(get_service_manager),
        cache: CacheManager = Depends(get_cache),
        metrics: MetricsCollector = Depends(get_metrics),
        db_manager: DatabaseManager = Depends(get_database)
) -> ConversationService:
    """FastAPI依赖：获取对话服务"""
    return await service_manager.get_conversation_service(cache, metrics, db_manager)


# === 认证和权限依赖 ===

class AuthManager:
    """认证管理器"""

    def __init__(self):
        self.logger = get_logger("app.auth")

    async def verify_admin_user(self, user_id: int) -> bool:
        """验证是否为管理员用户"""
        is_admin = user_id in settings.ADMIN_USER_IDS
        self.logger.debug(f"Admin verification for user {user_id}: {is_admin}")
        return is_admin

    async def verify_webhook_request(self, request_path: str) -> bool:
        """验证Webhook请求"""
        expected_path = f"/{settings.WEBHOOK_PATH}"
        is_valid = request_path == expected_path
        self.logger.debug(f"Webhook verification for path {request_path}: {is_valid}")
        return is_valid


async def get_auth_manager() -> AuthManager:
    """FastAPI依赖：获取认证管理器"""
    return AuthManager()


# === 速率限制依赖 ===

class RateLimitManager:
    """速率限制管理器"""

    def __init__(self, cache: CacheManager):
        self.cache = cache
        self.logger = get_logger("app.rate_limit")

    async def check_user_rate_limit(self, user_id: int) -> bool:
        """检查用户速率限制"""
        if not hasattr(settings, 'RATE_LIMIT_ENABLED') or not settings.RATE_LIMIT_ENABLED:
            return True

        try:
            allowed, current_count = await self.cache.rate_limit_cache.check_rate_limit(
                f"user:{user_id}",
                getattr(settings, 'RATE_LIMIT_REQUESTS', 10),
                getattr(settings, 'RATE_LIMIT_WINDOW', 60)
            )

            if not allowed:
                self.logger.warning(
                    f"Rate limit exceeded for user {user_id}: {current_count}/{getattr(settings, 'RATE_LIMIT_REQUESTS', 10)}"
                )

            return allowed
        except Exception as e:
            self.logger.error(f"Rate limit check failed for user {user_id}: {e}")
            return True  # 失败时允许通过

    async def check_ip_rate_limit(self, ip_address: str) -> bool:
        """检查IP速率限制"""
        if not hasattr(settings, 'RATE_LIMIT_ENABLED') or not settings.RATE_LIMIT_ENABLED:
            return True

        try:
            # IP限制通常更宽松一些
            rate_limit_requests = getattr(settings, 'RATE_LIMIT_REQUESTS', 10)
            allowed, current_count = await self.cache.rate_limit_cache.check_rate_limit(
                f"ip:{ip_address}",
                rate_limit_requests * 2,  # IP限制是用户限制的2倍
                getattr(settings, 'RATE_LIMIT_WINDOW', 60)
            )

            if not allowed:
                self.logger.warning(
                    f"Rate limit exceeded for IP {ip_address}: {current_count}/{rate_limit_requests * 2}"
                )

            return allowed
        except Exception as e:
            self.logger.error(f"Rate limit check failed for IP {ip_address}: {e}")
            return True  # 失败时允许通过


async def get_rate_limit_manager(cache: CacheManager = Depends(get_cache)) -> RateLimitManager:
    """FastAPI依赖：获取速率限制管理器"""
    return RateLimitManager(cache)


# === 应用生命周期管理 ===

class ApplicationLifecycleManager:
    """应用生命周期管理器"""

    def __init__(self):
        self.logger = get_logger("app.lifecycle")
        self._initialized = False

    async def startup(self):
        """应用启动时的初始化"""
        if self._initialized:
            return

        try:
            self.logger.info("Starting application initialization...")

            # 1. 初始化数据库
            db_manager = get_database_manager()
            await db_manager.initialize()

            # 2. 初始化缓存
            cache_manager = get_cache_manager()
            if hasattr(cache_manager, 'start_cleanup_task'):
                await cache_manager.start_cleanup_task()

            # 3. 初始化监控
            metrics = get_metrics_collector()
            if hasattr(metrics, 'start_background_tasks'):
                metrics.start_background_tasks()

            # 4. 初始化服务
            service_manager = get_service_manager()
            # 服务将在首次使用时初始化
            # 4. 检查新功能配置
            self.logger.info(f"消息队列启用状态: {getattr(settings, 'ENABLE_MESSAGE_QUEUE', False)}")
            self.logger.info(f"Redis URL: {getattr(settings, 'REDIS_URL', 'Not set')}")
            self.logger.info(f"高级速率限制启用状态: {getattr(settings, 'ADVANCED_RATE_LIMIT_ENABLED', True)}")
            self.logger.info(f"高级用户数量: {len(getattr(settings, 'PREMIUM_USER_IDS', []))}")

            # 5. 测试速率限制器初始化
            try:
                from app.rate_limit import get_rate_limiter
                rate_limiter = await get_rate_limiter()
                self.logger.info("✅ 高级速率限制器初始化成功")
            except Exception as e:
                self.logger.warning(f"⚠️ 高级速率限制器初始化失败: {e}")

            # 6. 测试消息队列初始化（如果启用）
            if getattr(settings, 'ENABLE_MESSAGE_QUEUE', False):
                try:
                    from app.dependencies import get_message_queue_service
                    mq_service = await get_message_queue_service()
                    if mq_service:
                        self.logger.info("✅ 消息队列服务初始化成功")
                    else:
                        self.logger.warning("⚠️ 消息队列服务未初始化")
                except Exception as e:
                    self.logger.warning(f"⚠️ 消息队列服务初始化失败: {e}")

            self._initialized = True
            self.logger.info("Application initialization completed")

        except Exception as e:
            self.logger.error("Application initialization failed", exc_info=True)
            raise


    async def shutdown(self):
        """应用关闭时的清理"""
        if not self._initialized:
            return

        try:
            self.logger.info("Starting application shutdown...")

            # 1. 清理服务
            service_manager = get_service_manager()
            await service_manager.cleanup()

            # 2. 停止监控
            metrics = get_metrics_collector()
            if hasattr(metrics, 'stop_background_tasks'):
                await metrics.stop_background_tasks()

            # 3. 清理缓存
            cache_manager = get_cache_manager()
            if hasattr(cache_manager, 'stop_cleanup_task'):
                await cache_manager.stop_cleanup_task()

            # 4. 关闭数据库
            db_manager = get_database_manager()
            await db_manager.close()

            self._initialized = False
            self.logger.info("Application shutdown completed")

        except Exception as e:
            self.logger.error("Error during application shutdown", exc_info=True)


# 全局生命周期管理器
_lifecycle_manager: Optional[ApplicationLifecycleManager] = None


def get_lifecycle_manager() -> ApplicationLifecycleManager:
    """获取应用生命周期管理器"""
    global _lifecycle_manager
    if _lifecycle_manager is None:
        _lifecycle_manager = ApplicationLifecycleManager()
    return _lifecycle_manager


# === 健康检查依赖 ===

class HealthChecker:
    """健康检查器"""

    def __init__(self):
        self.logger = get_logger("app.health")

    async def check_database_health(self) -> dict:
        """检查数据库健康状态"""
        try:
            # 修复：使用文件顶部已导入的 db 和 Conversation

            # # 检查数据库连接状态
            # if not db or db.is_closed():
            #     return {"status": "unhealthy", "details": "Database connection is closed"}

            # 执行一个简单的查询来验证连接是否真正可用
            def _test_database():
                try:
                    # 首先尝试简单的查询
                    cursor = db.execute_sql("SELECT 1")
                    basic_result = cursor.fetchone()

                    if not basic_result:
                        return False, "Basic query failed"

                    # 然后尝试查询我们的实际表
                    count = Conversation.select().count()
                    return True, f"Database responding, {count} conversations in table"

                except Exception as e:
                    return False, f"Query error: {str(e)}"

            # 在线程池中执行数据库查询
            success, details = await run_in_threadpool(_test_database)

            if success:
                return {"status": "healthy", "details": details}
            else:
                return {"status": "unhealthy", "details": details}

        except Exception as e:
            self.logger.error(f"数据库健康检查异常: {e}", exc_info=True)
            return {"status": "unhealthy", "details": f"Database health check error: {str(e)}"}

    async def check_cache_health(self) -> dict:
        """检查缓存健康状态"""
        try:
            cache = get_cache_manager()
            stats = await cache.get_stats()
            return {
                "status": "healthy",
                "details": f"Cache active with {stats['cache_size']} entries"
            }
        except Exception as e:
            return {"status": "unhealthy", "details": f"Cache error: {str(e)}"}

    async def check_services_health(self) -> dict:
        """检查服务健康状态"""
        try:
            service_manager = get_service_manager()
            if service_manager._conversation_service:
                return {"status": "healthy", "details": "ConversationService active"}
            else:
                return {"status": "healthy", "details": "ConversationService not yet initialized"}
        except Exception as e:
            return {"status": "unhealthy", "details": f"Service error: {str(e)}"}

    async def get_overall_health(self) -> dict:
        """获取整体健康状态"""
        checks = {
            "database": await self.check_database_health(),
            "cache": await self.check_cache_health(),
            "services": await self.check_services_health()
        }

        # 判断整体状态
        all_healthy = all(check["status"] == "healthy" for check in checks.values())
        overall_status = "healthy" if all_healthy else "unhealthy"

        return {
            "status": overall_status,
            "timestamp": asyncio.get_event_loop().time(),
            "checks": checks
        }


async def get_health_checker() -> HealthChecker:
    """FastAPI依赖：获取健康检查器"""
    return HealthChecker()


# === 消息队列依赖 ===
_message_queue_service = None


# async def get_message_queue_service():
#     """获取消息队列服务"""
#     global _message_queue_service
#     if _message_queue_service is None and getattr(settings, 'ENABLE_MESSAGE_QUEUE', False):
#         try:
#             from app.message_handlers import MessageQueueService
#             from app.services.conversation_service import ConversationService

#             # 简化初始化
#             conv_service = ConversationService(
#                 support_group_id=settings.SUPPORT_GROUP_ID,
#                 external_group_ids=settings.EXTERNAL_GROUP_IDS,
#                 tg_func=tg
#             )
#             _message_queue_service = MessageQueueService(conv_service)
#             logger.info("Message queue service initialized")
#         except Exception as e:
#             logger.error(f"Failed to initialize message queue service: {e}")

#     return _message_queue_service


# === 高级速率限制依赖 ===
async def check_advanced_rate_limit(user_id: int, action_type: str = "message") -> bool:
    """检查高级速率限制"""
    if not getattr(settings, 'ADVANCED_RATE_LIMIT_ENABLED', True):
        return True

    try:
        from app.rate_limit import check_user_rate_limit, ActionType

        # 转换操作类型
        action_enum = ActionType.MESSAGE if action_type == "message" else ActionType.API_CALL

        # 获取用户组
        user_group = settings.get_user_group(user_id)

        # 检查速率限制
        result = await check_user_rate_limit(user_id, action_enum, user_group)
        return result.allowed

    except Exception as e:
        logger.error(f"Advanced rate limit check failed: {e}")
        return True  # 失败时允许通过