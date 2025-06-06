# app/dependencies.py

from functools import lru_cache
from typing import Optional, AsyncGenerator
from contextlib import asynccontextmanager
import asyncio

from fastapi import Depends, HTTPException, status
from starlette.concurrency import run_in_threadpool

from app.settings import settings
from app.store import db, connect_db, close_db, Conversation
from app.tg_utils import tg
from app.services.conversation_service import ConversationService
from app.cache import CacheManager, get_cache_manager
from app.monitoring import MetricsCollector, get_metrics_collector
from app.logging_config import get_logger

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


# === 机器人管理依赖 ===

# 全局机器人管理器引用
_bot_manager_instance: Optional = None


async def get_bot_manager_dep():
    """FastAPI依赖：获取机器人管理器"""
    global _bot_manager_instance

    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return None

    if _bot_manager_instance is None:
        try:
            from app.bot_manager import get_bot_manager
            _bot_manager_instance = await get_bot_manager()
        except Exception as e:
            logger.error(f"获取机器人管理器失败: {e}", exc_info=True)
            return None

    return _bot_manager_instance


async def cleanup_bot_manager_dep():
    """清理机器人管理器依赖"""
    global _bot_manager_instance
    if _bot_manager_instance:
        try:
            from app.bot_manager import cleanup_bot_manager
            await cleanup_bot_manager()
            _bot_manager_instance = None
        except Exception as e:
            logger.error(f"清理机器人管理器失败: {e}", exc_info=True)


# === 故障转移管理依赖 ===

_failover_manager_instance: Optional = None

async def get_failover_manager_dep():
    """FastAPI依赖：获取故障转移管理器"""
    global _failover_manager_instance
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return None # 仅在多机器人模式下启用

    if _failover_manager_instance is None:
        try:
            from app.failover_manager import get_failover_manager
            _failover_manager_instance = await get_failover_manager()
        except Exception as e:
            logger.error(f"获取故障转移管理器失败: {e}", exc_info=True)
            return None
    return _failover_manager_instance

async def cleanup_failover_manager_dep():
    """清理故障转移管理器依赖"""
    global _failover_manager_instance
    if _failover_manager_instance:
        try:
            from app.failover_manager import cleanup_failover_manager
            await cleanup_failover_manager()
            _failover_manager_instance = None
        except Exception as e:
            logger.error(f"清理故障转移管理器失败: {e}", exc_info=True)


# === 熔断器注册表依赖 ===

_circuit_breaker_registry_instance: Optional = None

async def get_circuit_breaker_registry_dep():
    """FastAPI依赖：获取熔断器注册表"""
    global _circuit_breaker_registry_instance
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return None # 仅在多机器人模式下启用

    if _circuit_breaker_registry_instance is None:
        try:
            from app.circuit_breaker import get_circuit_breaker_registry
            _circuit_breaker_registry_instance = await get_circuit_breaker_registry()
        except Exception as e:
            logger.error(f"获取熔断器注册表失败: {e}", exc_info=True)
            return None
    return _circuit_breaker_registry_instance

async def cleanup_circuit_breaker_registry_dep():
    """清理熔断器注册表依赖"""
    global _circuit_breaker_registry_instance
    if _circuit_breaker_registry_instance:
        try:
            # 熔断器注册表不需要显式停止，但需要清理引用
            await _circuit_breaker_registry_instance.reset_all() # 重置所有熔断器状态
            _circuit_breaker_registry_instance = None
        except Exception as e:
            logger.error(f"清理熔断器注册表失败: {e}", exc_info=True)


# === 服务依赖 ===

class ServiceManager:
    """服务管理器"""

    def __init__(self):
        self._conversation_service: Optional[ConversationService] = None
        self.logger = get_logger("app.services")

    async def get_conversation_service(
            self,
            cache: 'CacheManager',  # <--- 修改
            metrics: 'MetricsCollector',  # <--- 修改
            db_manager: 'DatabaseManager'  # <--- 修改
    ) -> ConversationService:
        """获取对话服务实例"""
        if self._conversation_service is None:
            try:
                # 简化服务初始化，移除可能不存在的参数
                self._conversation_service = ConversationService(
                    support_group_id=settings.SUPPORT_GROUP_ID,
                    external_group_ids=settings.EXTERNAL_GROUP_IDS,
                    tg_func=tg,
                    cache_manager=cache,
                    metrics_collector=metrics
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

            # 4. 初始化机器人管理器（如果启用）
            if getattr(settings, 'MULTI_BOT_ENABLED', False):
                try:
                    bot_manager = await get_bot_manager_dep()
                    if bot_manager:
                        self.logger.info("✅ 多机器人管理器初始化成功")
                        # 启动机器人管理器内部任务
                        await bot_manager.start()

                        # 记录机器人状态
                        stats = bot_manager.get_stats()
                        self.logger.info(f"机器人统计: {stats['healthy_bots']}/{stats['total_bots']} 健康")

                        # 5. 初始化故障转移管理器
                        failover_manager = await get_failover_manager_dep()
                        if failover_manager:
                            self.logger.info("✅ 故障转移管理器初始化成功")
                            # 故障转移管理器在get_failover_manager()内部已启动

                        # 6. 初始化熔断器注册表
                        circuit_breaker_registry = await get_circuit_breaker_registry_dep()
                        if circuit_breaker_registry:
                            self.logger.info("✅ 熔断器注册表初始化成功")


                        # 7. 初始化消息协调器（如果启用）
                        if getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
                            try:
                                coordinator = await get_message_coordinator_dep()
                                if coordinator:
                                    self.logger.info("✅ 消息协调器初始化成功")

                                    # 🔥 关键修复：启动消息协调器的处理循环
                                    self.logger.info("正在启动消息协调器处理循环...")
                                    await coordinator.start()
                                    self.logger.info("✅ 消息协调器处理循环已启动")

                                    # # 等待一小段时间确保任务启动
                                    # await asyncio.sleep(0.1)

                                    # 验证协调器是否真正运行
                                    if coordinator._running:
                                        self.logger.info("✅ 协调器运行状态验证成功")
                                    else:
                                        self.logger.error("❌ 协调器运行状态验证失败")

                                    # 获取协调器统计（处理可能的Redis错误）
                                    try:
                                        coord_stats = await coordinator.get_stats()
                                        coordinator_info = coord_stats.get('coordinator', {})
                                        self.logger.info(
                                            f"协调器实例: {coordinator_info.get('instance_id', 'unknown')}")
                                        self.logger.info(f"协调器运行状态: {coordinator_info.get('running', False)}")

                                        queue_info = coord_stats.get('queue', {})
                                        self.logger.info(
                                            f"队列状态 - 待处理: {queue_info.get('pending_count', 0)}, 处理中: {queue_info.get('processing_count', 0)}")
                                    except Exception as stats_error:
                                        self.logger.warning(f"获取协调器统计失败: {stats_error}")
                                else:
                                    self.logger.warning("⚠️ 消息协调器初始化失败")
                            except Exception as e:
                                self.logger.error(f"❌ 消息协调器初始化异常: {e}", exc_info=True)
                        else:
                            self.logger.info("ℹ️ 消息协调功能已禁用")

                    else:
                        self.logger.warning("⚠️ 多机器人管理器初始化失败")
                except Exception as e:
                    self.logger.error(f"❌ 多机器人管理器初始化异常: {e}", exc_info=True)
            else:
                self.logger.info("ℹ️ 单机器人模式，跳过多机器人功能初始化")

            # 8. 初始化服务
            service_manager = get_service_manager()
            # 服务将在首次使用时初始化

            # 9. 检查其他功能配置
            self.logger.info(f"消息队列启用状态: {getattr(settings, 'ENABLE_MESSAGE_QUEUE', False)}")
            self.logger.info(f"消息协调启用状态: {getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True)}")
            self.logger.info(f"Redis URL: {getattr(settings, 'REDIS_URL', 'Not set')}")
            self.logger.info(f"高级速率限制启用状态: {getattr(settings, 'ADVANCED_RATE_LIMIT_ENABLED', True)}")
            self.logger.info(f"高级用户数量: {len(getattr(settings, 'PREMIUM_USER_IDS', []))}")

            # 10. 测试速率限制器初始化
            try:
                from app.rate_limit import get_rate_limiter
                rate_limiter = await get_rate_limiter()
                self.logger.info("✅ 高级速率限制器初始化成功")
            except Exception as e:
                self.logger.warning(f"⚠️ 高级速率限制器初始化失败: {e}")

            # 11. 测试消息队列初始化（如果启用）
            if getattr(settings, 'ENABLE_MESSAGE_QUEUE', False):
                try:
                    # 这里可以添加消息队列服务的初始化
                    self.logger.info("✅ 消息队列服务配置检查完成")
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

            # 1. 清理消息协调相关组件
            await cleanup_message_coordination_deps()

            # 2. 清理故障转移管理器
            await cleanup_failover_manager_dep()

            # 3. 清理熔断器注册表
            await cleanup_circuit_breaker_registry_dep()

            # 4. 清理机器人管理器
            await cleanup_bot_manager_dep()

            # 5. 清理服务
            service_manager = get_service_manager()
            await service_manager.cleanup()

            # 6. 停止监控
            metrics = get_metrics_collector()
            if hasattr(metrics, 'stop_background_tasks'):
                await metrics.stop_background_tasks()

            # 7. 清理缓存
            cache_manager = get_cache_manager()
            if hasattr(cache_manager, 'stop_cleanup_task'):
                await cache_manager.stop_cleanup_task()

            # 8. 关闭数据库
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

    async def check_bots_health(self) -> dict:
        """检查机器人健康状态"""
        try:
            if not getattr(settings, 'MULTI_BOT_ENABLED', False):
                return {"status": "healthy", "details": "Single bot mode"}

            bot_manager = await get_bot_manager_dep()
            if not bot_manager:
                return {"status": "unhealthy", "details": "Bot manager not available"}

            stats = bot_manager.get_stats()
            if stats['healthy_bots'] > 0:
                return {
                    "status": "healthy",
                    "details": f"{stats['healthy_bots']}/{stats['total_bots']} bots healthy"
                }
            else:
                return {
                    "status": "unhealthy",
                    "details": f"No healthy bots (0/{stats['total_bots']})"
                }
        except Exception as e:
            return {"status": "unhealthy", "details": f"Bot health check error: {str(e)}"}

    async def check_coordination_health(self) -> dict:
        """检查消息协调器健康状态"""
        try:
            if not getattr(settings, 'MULTI_BOT_ENABLED', False):
                return {"status": "healthy", "details": "Single bot mode, coordination not needed"}

            if not getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
                return {"status": "healthy", "details": "Message coordination disabled"}

            coordinator = await get_message_coordinator_dep()
            if not coordinator:
                return {"status": "unhealthy", "details": "Message coordinator not available"}

            stats = await coordinator.get_stats()
            coordinator_running = stats.get('coordinator', {}).get('running', False)

            if coordinator_running:
                queue_stats = stats.get('queue', {})
                pending_count = queue_stats.get('pending_count', 0)
                processing_count = queue_stats.get('processing_count', 0)

                return {
                    "status": "healthy",
                    "details": f"Coordinator running, {pending_count} pending, {processing_count} processing"
                }
            else:
                return {"status": "unhealthy", "details": "Message coordinator not running"}

        except Exception as e:
            return {"status": "unhealthy", "details": f"Coordination health check error: {str(e)}"}

    async def check_failover_health(self) -> dict:
        """检查故障转移管理器健康状态"""
        try:
            if not getattr(settings, 'MULTI_BOT_ENABLED', False):
                return {"status": "healthy", "details": "Single bot mode, failover not needed"}

            failover_manager = await get_failover_manager_dep()
            if not failover_manager:
                return {"status": "unhealthy", "details": "Failover manager not available"}

            stats = await failover_manager.get_failover_stats()
            active_events = stats.get("active_events", 0)

            if active_events == 0:
                return {"status": "healthy", "details": f"Failover manager active, 0 active events"}
            else:
                return {"status": "unhealthy", "details": f"Failover manager active, {active_events} active events"}

        except Exception as e:
            return {"status": "unhealthy", "details": f"Failover health check error: {str(e)}"}

    async def check_circuit_breaker_health(self) -> dict:
        """检查熔断器注册表健康状态"""
        try:
            if not getattr(settings, 'MULTI_BOT_ENABLED', False):
                return {"status": "healthy", "details": "Single bot mode, circuit breaker not needed"}

            registry = await get_circuit_breaker_registry_dep()
            if not registry:
                return {"status": "unhealthy", "details": "Circuit breaker registry not available"}

            all_cb_stats = await registry.get_all_stats()
            open_breakers = [name for name, stats in all_cb_stats.items() if stats.get("state") == "open"]
            half_open_breakers = [name for name, stats in all_cb_stats.items() if stats.get("state") == "half_open"]

            if not open_breakers and not half_open_breakers:
                return {"status": "healthy", "details": "All circuit breakers are closed"}
            else:
                details = f"Circuit breakers: {len(open_breakers)} open, {len(half_open_breakers)} half-open"
                return {"status": "unhealthy", "details": details, "open": open_breakers, "half_open": half_open_breakers}

        except Exception as e:
            return {"status": "unhealthy", "details": f"Circuit breaker health check error: {str(e)}"}


    async def get_overall_health(self) -> dict:
        """获取整体健康状态"""
        checks = {
            "database": await self.check_database_health(),
            "cache": await self.check_cache_health(),
            "services": await self.check_services_health(),
            "bots": await self.check_bots_health(),
            "coordination": await self.check_coordination_health(),
            "failover": await self.check_failover_health(), # 新增
            "circuit_breaker": await self.check_circuit_breaker_health() # 新增
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


# === 消息协调器依赖 ===

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.message_coordinator import MessageCoordinator
    from app.message_processor import CoordinatedMessageHandler, create_coordinated_handler

# 全局消息协调器和处理器引用
_message_coordinator_instance: Optional['MessageCoordinator'] = None
_coordinated_handler_instance: Optional['CoordinatedMessageHandler'] = None


async def get_message_coordinator_dep():
    """FastAPI依赖：获取消息协调器"""
    global _message_coordinator_instance

    # 检查是否启用了多机器人模式和Redis
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return None

    if not getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
        return None

    if _message_coordinator_instance is None:
        try:
            from app.message_coordinator import get_message_coordinator
            _message_coordinator_instance = await get_message_coordinator()
            logger.info("消息协调器依赖初始化成功")
        except Exception as e:
            logger.error(f"获取消息协调器失败: {e}", exc_info=True)
            return None

    return _message_coordinator_instance


async def get_coordinated_handler_dep(
        conv_service=Depends(get_conversation_service)
):
    """FastAPI依赖：获取协调式消息处理器"""
    global _coordinated_handler_instance

    # 检查是否启用了消息协调
    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        return None

    if not getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
        return None

    if _coordinated_handler_instance is None:
        try:
            from app.message_processor import create_coordinated_handler
            _coordinated_handler_instance = await create_coordinated_handler(conv_service)
            logger.info("协调式消息处理器依赖初始化成功")
        except Exception as e:
            logger.error(f"获取协调式消息处理器失败: {e}", exc_info=True)
            return None

    return _coordinated_handler_instance


async def cleanup_message_coordination_deps():
    """清理消息协调相关依赖"""
    global _message_coordinator_instance, _coordinated_handler_instance

    if _message_coordinator_instance:
        try:
            from app.message_coordinator import cleanup_message_coordinator
            await cleanup_message_coordinator()
            _message_coordinator_instance = None
        except Exception as e:
            logger.error(f"清理消息协调器失败: {e}", exc_info=True)

    _coordinated_handler_instance = None


# 更新 ApplicationLifecycleManager 类

class ApplicationLifecycleManager:
    """应用生命周期管理器（更新版本）"""

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

            # 4. 初始化机器人管理器（如果启用）
            if getattr(settings, 'MULTI_BOT_ENABLED', False):
                try:
                    bot_manager = await get_bot_manager_dep()
                    if bot_manager:
                        self.logger.info("✅ 多机器人管理器初始化成功")
                        # 启动机器人管理器内部任务
                        await bot_manager.start()

                        # 记录机器人状态
                        stats = bot_manager.get_stats()
                        self.logger.info(f"机器人统计: {stats['healthy_bots']}/{stats['total_bots']} 健康")

                        # 5. 初始化故障转移管理器
                        failover_manager = await get_failover_manager_dep()
                        if failover_manager:
                            self.logger.info("✅ 故障转移管理器初始化成功")
                            # 故障转移管理器在get_failover_manager()内部已启动

                        # 6. 初始化熔断器注册表
                        circuit_breaker_registry = await get_circuit_breaker_registry_dep()
                        if circuit_breaker_registry:
                            self.logger.info("✅ 熔断器注册表初始化成功")

                        # 7. 初始化消息协调器（如果启用）
                        if getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
                            try:
                                # 先获取协调器实例
                                coordinator = await get_message_coordinator_dep()
                                if coordinator:
                                    self.logger.info("✅ 消息协调器实例获取成功")

                                    # <<< 关键修改开始 >>>
                                    # 确保 coordinated_handler_instance 被创建，从而设置回调
                                    db_manager_instance = get_database_manager()
                                    await db_manager_instance.initialize()  # 确保数据库连接已初始化

                                    conversation_service_instance = ConversationService(
                                        support_group_id=settings.SUPPORT_GROUP_ID,
                                        external_group_ids=settings.EXTERNAL_GROUP_IDS,
                                        tg_func=tg,  # 这里的tg是app.tg_utils.tg
                                        cache_manager=get_cache_manager(),
                                        metrics_collector=get_metrics_collector()
                                    )
                                    self.logger.info("✅ ConversationService 实例已为启动过程创建")

                                    # 确保 coordinated_handler_instance 被创建，从而设置回调
                                    coordinated_handler_instance = await create_coordinated_handler(
                                        conversation_service=conversation_service_instance  # 传递真实的实例
                                    )
                                    if coordinated_handler_instance:
                                        self.logger.info("✅ 协调式消息处理器初始化成功并设置回调。")
                                    else:
                                        self.logger.warning("⚠️ 协调式消息处理器初始化失败。")

                                    # 在回调设置之后，再启动协调器循环
                                    await coordinator.start()  # 启动消息协调器内部任务
                                    self.logger.info("✅ 消息协调器循环已启动。")
                                    # <<< 关键修改结束 >>>

                                    # 获取协调器统计
                                    coord_stats = await coordinator.get_stats()
                                    self.logger.info(f"协调器实例: {coord_stats['coordinator']['instance_id']}")
                                else:
                                    self.logger.warning("⚠️ 消息协调器实例获取失败")
                            except Exception as e:
                                self.logger.error(f"❌ 消息协调器初始化异常: {e}", exc_info=True)
                        else:
                            self.logger.info("ℹ️ 消息协调功能已禁用")

                    else:
                        self.logger.warning("⚠️ 多机器人管理器初始化失败")
                except Exception as e:
                    self.logger.error(f"❌ 多机器人管理器初始化异常: {e}", exc_info=True)
            else:
                self.logger.info("ℹ️ 单机器人模式，跳过多机器人功能初始化")

            # 8. 初始化服务 (保持不变)
            service_manager = get_service_manager()
            # 服务将在首次使用时初始化

            # 9. 检查其他功能配置
            self.logger.info(f"消息队列启用状态: {getattr(settings, 'ENABLE_MESSAGE_QUEUE', False)}")
            self.logger.info(f"消息协调启用状态: {getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True)}")
            self.logger.info(f"Redis URL: {getattr(settings, 'REDIS_URL', 'Not set')}")
            self.logger.info(f"高级速率限制启用状态: {getattr(settings, 'ADVANCED_RATE_LIMIT_ENABLED', True)}")
            self.logger.info(f"高级用户数量: {len(getattr(settings, 'PREMIUM_USER_IDS', []))}")

            # 10. 测试速率限制器初始化
            try:
                from app.rate_limit import get_rate_limiter
                rate_limiter = await get_rate_limiter()
                self.logger.info("✅ 高级速率限制器初始化成功")
            except Exception as e:
                self.logger.warning(f"⚠️ 高级速率限制器初始化失败: {e}")

            # 11. 测试消息队列初始化（如果启用）
            if getattr(settings, 'ENABLE_MESSAGE_QUEUE', False):
                try:
                    # 这里可以添加消息队列服务的初始化
                    self.logger.info("✅ 消息队列服务配置检查完成")
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

            # 1. 清理消息协调相关组件
            await cleanup_message_coordination_deps()

            # 2. 清理故障转移管理器
            await cleanup_failover_manager_dep()

            # 3. 清理熔断器注册表
            await cleanup_circuit_breaker_registry_dep()

            # 4. 清理机器人管理器
            await cleanup_bot_manager_dep()

            # 5. 清理服务
            service_manager = get_service_manager()
            await service_manager.cleanup()

            # 6. 停止监控
            metrics = get_metrics_collector()
            if hasattr(metrics, 'stop_background_tasks'):
                await metrics.stop_background_tasks()

            # 7. 清理缓存
            cache_manager = get_cache_manager()
            if hasattr(cache_manager, 'stop_cleanup_task'):
                await cache_manager.stop_cleanup_task()

            # 8. 关闭数据库
            db_manager = get_database_manager()
            await db_manager.close()

            self._initialized = False
            self.logger.info("Application shutdown completed")

        except Exception as e:
            self.logger.error("Error during application shutdown", exc_info=True)


# 更新健康检查器

class HealthChecker:
    """健康检查器（更新版本）"""

    def __init__(self):
        self.logger = get_logger("app.health")

    async def check_database_health(self) -> dict:
        """检查数据库健康状态"""
        try:
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

    async def check_bots_health(self) -> dict:
        """检查机器人健康状态"""
        try:
            if not getattr(settings, 'MULTI_BOT_ENABLED', False):
                return {"status": "healthy", "details": "Single bot mode"}

            bot_manager = await get_bot_manager_dep()
            if not bot_manager:
                return {"status": "unhealthy", "details": "Bot manager not available"}

            stats = bot_manager.get_stats()
            if stats['healthy_bots'] > 0:
                return {
                    "status": "healthy",
                    "details": f"{stats['healthy_bots']}/{stats['total_bots']} bots healthy"
                }
            else:
                return {
                    "status": "unhealthy",
                    "details": f"No healthy bots (0/{stats['total_bots']})"
                }
        except Exception as e:
            return {"status": "unhealthy", "details": f"Bot health check error: {str(e)}"}

    async def check_coordination_health(self) -> dict:
        """检查消息协调器健康状态"""
        try:
            if not getattr(settings, 'MULTI_BOT_ENABLED', False):
                return {"status": "healthy", "details": "Single bot mode, coordination not needed"}

            if not getattr(settings, 'ENABLE_MESSAGE_COORDINATION', True):
                return {"status": "healthy", "details": "Message coordination disabled"}

            coordinator = await get_message_coordinator_dep()
            if not coordinator:
                return {"status": "unhealthy", "details": "Message coordinator not available"}

            stats = await coordinator.get_stats()
            coordinator_running = stats.get('coordinator', {}).get('running', False)

            if coordinator_running:
                queue_stats = stats.get('queue', {})
                pending_count = queue_stats.get('pending_count', 0)
                processing_count = queue_stats.get('processing_count', 0)

                return {
                    "status": "healthy",
                    "details": f"Coordinator running, {pending_count} pending, {processing_count} processing"
                }
            else:
                return {"status": "unhealthy", "details": "Message coordinator not running"}

        except Exception as e:
            return {"status": "unhealthy", "details": f"Coordination health check error: {str(e)}"}

    async def check_failover_health(self) -> dict:
        """检查故障转移管理器健康状态"""
        try:
            if not getattr(settings, 'MULTI_BOT_ENABLED', False):
                return {"status": "healthy", "details": "Single bot mode, failover not needed"}

            failover_manager = await get_failover_manager_dep()
            if not failover_manager:
                return {"status": "unhealthy", "details": "Failover manager not available"}

            stats = await failover_manager.get_failover_stats()
            active_events = stats.get("active_events", 0)

            if active_events == 0:
                return {"status": "healthy", "details": f"Failover manager active, 0 active events"}
            else:
                # 如果有活跃事件，认为是不健康的，但仍提供详细信息
                return {"status": "unhealthy", "details": f"Failover manager active, {active_events} active events"}

        except Exception as e:
            return {"status": "unhealthy", "details": f"Failover health check error: {str(e)}"}

    async def check_circuit_breaker_health(self) -> dict:
        """检查熔断器注册表健康状态"""
        try:
            if not getattr(settings, 'MULTI_BOT_ENABLED', False):
                return {"status": "healthy", "details": "Single bot mode, circuit breaker not needed"}

            registry = await get_circuit_breaker_registry_dep()
            if not registry:
                return {"status": "unhealthy", "details": "Circuit breaker registry not available"}

            all_cb_stats = await registry.get_all_stats()
            open_breakers = [name for name, stats in all_cb_stats.items() if stats.get("state") == "open"]
            half_open_breakers = [name for name, stats in all_cb_stats.items() if stats.get("state") == "half_open"]

            if not open_breakers and not half_open_breakers:
                return {"status": "healthy", "details": "All circuit breakers are closed"}
            else:
                details = f"Circuit breakers: {len(open_breakers)} open, {len(half_open_breakers)} half-open"
                # 如果有熔断器处于开启或半开状态，将其视为不健康
                return {"status": "unhealthy", "details": details, "open": open_breakers, "half_open": half_open_breakers}

        except Exception as e:
            return {"status": "unhealthy", "details": f"Circuit breaker health check error: {str(e)}"}

    async def get_overall_health(self) -> dict:
        """获取整体健康状态"""
        checks = {
            "database": await self.check_database_health(),
            "cache": await self.check_cache_health(),
            "services": await self.check_services_health(),
            "bots": await self.check_bots_health(),
            "coordination": await self.check_coordination_health(),
            "failover": await self.check_failover_health(),
            "circuit_breaker": await self.check_circuit_breaker_health()
        }

        # 判断整体状态
        all_healthy = all(check["status"] == "healthy" for check in checks.values())
        overall_status = "healthy" if all_healthy else "unhealthy"

        return {
            "status": overall_status,
            "timestamp": asyncio.get_event_loop().time(),
            "checks": checks
        }