# app/bot_manager.py

import time
import asyncio
import uuid
from enum import Enum
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
import json

# 条件导入以避免循环依赖
if TYPE_CHECKING:
    from .settings import BotConfig
    import redis.asyncio as redis
    from .failover_manager import FailoverManager, FailoverReason # 这里的FailoverReason只用于类型检查，运行时不会导入

from .logging_config import get_logger

logger = get_logger("app.bot_manager")


class BotStatus(Enum):
    """机器人状态"""
    HEALTHY = "healthy"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"
    DISABLED = "disabled"
    UNKNOWN = "unknown"


@dataclass
class BotInstance:
    """机器人实例信息"""
    bot_id: str
    config: 'BotConfig'
    status: BotStatus = BotStatus.UNKNOWN
    last_heartbeat: float = field(default_factory=time.time)
    last_error: Optional[str] = None
    rate_limit_reset_time: Optional[float] = None
    request_count: int = 0
    last_request_time: float = field(default_factory=time.time)
    health_check_count: int = 0
    consecutive_failures: int = 0
    health_score:int = 100

    def to_dict(self) -> Dict:
        """转换为字典 - 防循环引用版本"""
        try:
            config_info = {
                "name": getattr(self.config, 'name', 'Unknown') if hasattr(self,
                                                                           'config') and self.config else 'Unknown',
                "enabled": getattr(self.config, 'enabled', True) if hasattr(self, 'config') and self.config else True,
                "priority": getattr(self.config, 'priority', 1) if hasattr(self, 'config') and self.config else 1,
            }

            status_value = self.status.value if hasattr(self.status, 'value') else str(self.status)

            return {
                "bot_id": str(self.bot_id),
                "config": config_info,
                "status": status_value,
                "health_score": getattr(self, 'health_score', 100),
                "request_count": getattr(self, 'request_count', 0),
                "consecutive_failures": getattr(self, 'consecutive_failures', 0),
                "last_request_time": getattr(self, 'last_request_time', 0),
                "last_heartbeat": getattr(self, 'last_heartbeat', 0),
                "last_error": getattr(self, 'last_error', None),
            }
        except Exception as e:
            return {
                "bot_id": str(getattr(self, 'bot_id', 'unknown')),
                "status": "serialization_error",
                "error": str(e)[:100],
            }

    def is_available(self) -> bool:
        """检查机器人是否可用"""
        if not self.config.enabled:
            return False

        if self.status == BotStatus.DISABLED:
            return False

        if self.status == BotStatus.RATE_LIMITED:
            if self.rate_limit_reset_time and time.time() < self.rate_limit_reset_time:
                return False
            return False

            # 检查请求频率限制
        if self._is_request_rate_limited():
            self.logger.debug(
                f"Bot {self.bot_id} is internally rate limited. Request count: {self.request_count}, Max: {self.config.max_requests_per_minute}, Time since last request: {time.time() - self.last_request_time:.1f}s")
            return False

        return self.status in [BotStatus.HEALTHY, BotStatus.UNKNOWN]

    def _is_request_rate_limited(self) -> bool:
        """检查是否达到请求频率限制"""
        current_time = time.time()
        time_window = 60  # 1分钟窗口

        # 简单的频率检查：如果在过去1分钟内请求数超过限制
        if (current_time - self.last_request_time < time_window and
                self.request_count >= self.config.max_requests_per_minute):
            return True

        # 重置计数器（简化版本，实际应该使用滑动窗口）
        if current_time - self.last_request_time >= time_window:
            self.request_count = 0

        return False

    def get_load_score(self) -> float:
        """获取负载评分，分数越低越好"""
        base_score = self.config.priority * 1000  # 优先级权重

        # 请求计数权重
        request_weight = self.request_count * 10

        # 连续失败惩罚
        failure_penalty = self.consecutive_failures * 100

        # 状态权重
        status_weight = {
            BotStatus.HEALTHY: 0,
            BotStatus.UNKNOWN: 50,
            BotStatus.RATE_LIMITED: 500,
            BotStatus.ERROR: 1000,
            BotStatus.DISABLED: 10000
        }.get(self.status, 1000)

        return base_score + request_weight + failure_penalty + status_weight


class BotManager:
    """机器人管理器"""

    def __init__(self, redis_client: Optional['redis.Redis'] = None):
        self.redis_client = redis_client
        self.instance_id = str(uuid.uuid4())[:8]  # 当前应用实例ID
        self.bots: Dict[str, BotInstance] = {}
        self.logger = get_logger("app.bot_manager")
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._status_check_task: Optional[asyncio.Task] = None
        self._running = False
        self._failover_manager: Optional['FailoverManager'] = None # 用于持有FailoverManager的引用

        # 延迟初始化机器人实例（避免循环导入）
        self._initialize_bots()

    def _initialize_bots(self):
        """初始化机器人实例"""
        try:
            # 延迟导入以避免循环依赖
            from .settings import settings

            enabled_bots = settings.get_enabled_bots()
            for i, bot_config in enumerate(enabled_bots):
                # 为每个机器人实例生成一个唯一的bot_id
                # 尽量让bot_id保持稳定，但允许根据实例ID区分
                stable_bot_id = f"bot_{i + 1}_{bot_config.name.replace(' ', '_')}"
                self.bots[stable_bot_id] = BotInstance(
                    bot_id=stable_bot_id,
                    config=bot_config,
                    status=BotStatus.UNKNOWN
                )

            self.logger.info(f"初始化了 {len(self.bots)} 个机器人实例")

            # 如果没有机器人配置，记录警告
            if not self.bots:
                self.logger.warning("没有找到任何机器人配置")

        except Exception as e:
            self.logger.error(f"初始化机器人失败: {e}", exc_info=True)

    async def _get_failover_manager(self) -> Optional['FailoverManager']:
        """延迟获取故障转移管理器实例，避免循环依赖"""
        if self._failover_manager is None:
            try:
                # 延迟导入，因为FailoverManager也可能需要BotManager
                # 这里通过get_failover_manager函数获取全局单例
                from .failover_manager import get_failover_manager
                self._failover_manager = await get_failover_manager()
            except Exception as e:
                self.logger.warning(f"无法获取故障转移管理器: {e}")
        return self._failover_manager

    async def start(self):
        """启动机器人管理器"""
        if self._running:
            return

        self.logger.info("启动机器人管理器...")
        self._running = True

        # 初始化所有机器人状态
        for bot in self.bots.values():
            if bot.config.enabled:
                await self._check_bot_health(bot)

        # 启动后台任务
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._status_check_task = asyncio.create_task(self._status_check_loop())

        healthy_count = len(self.get_healthy_bots())
        self.logger.info(f"机器人管理器启动完成，{healthy_count}/{len(self.bots)} 个机器人健康")

    async def stop(self):
        """停止机器人管理器"""
        if not self._running:
            return

        self.logger.info("停止机器人管理器...")
        self._running = False

        # 取消后台任务
        tasks = [self._heartbeat_task, self._status_check_task]
        for task in tasks:
            if task and not task.done():
                task.cancel()

        # 等待任务完成
        for task in tasks:
            if task and not task.done():
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        self.logger.info("机器人管理器已停止")

    async def _check_bot_health(self, bot: BotInstance) -> bool:
        """检查机器人健康状态"""
        from .failover_manager import FailoverReason

        current_status = bot.status
        is_healthy = False  # 🔥 关键修复：在方法开始时初始化
        error_msg = None
        failover_reason = None

        current_status = bot.status
        try:
            # 导入httpx客户端
            from .tg_utils import client as http_client

            # 调用 getMe API 检查机器人状态
            url = f"https://api.telegram.org/bot{bot.config.token}/getMe"

            bot.health_check_count += 1

            response = await http_client.get(url, timeout=10)
            is_healthy = False
            error_msg = None
            failover_reason = None

            if response.status_code == 200:
                result = response.json()
                if result.get("ok"):
                    bot.status = BotStatus.HEALTHY
                    bot.last_error = None
                    bot.last_heartbeat = time.time()
                    bot.consecutive_failures = 0
                    is_healthy = True
                    self.logger.debug(f"机器人 {bot.bot_id} ({bot.config.name}) 健康检查成功")
                else:
                    error_msg = result.get("description", "API返回ok=false")
                    bot.status = BotStatus.ERROR
                    bot.last_error = error_msg
                    bot.consecutive_failures += 1
                    failover_reason = FailoverReason.API_ERROR
                    self.logger.warning(f"机器人 {bot.bot_id} API错误: {error_msg}")

            elif response.status_code == 429:
                # 处理429限速
                retry_after = response.headers.get('Retry-After', '60')
                try:
                    retry_after_int = int(retry_after)
                except ValueError:
                    retry_after_int = 60

                bot.status = BotStatus.RATE_LIMITED
                bot.rate_limit_reset_time = time.time() + retry_after_int
                bot.last_error = f"Rate limited, retry after {retry_after_int}s"
                bot.consecutive_failures += 1
                failover_reason = FailoverReason.RATE_LIMITED
                self.logger.warning(f"机器人 {bot.bot_id} 被限速，{retry_after_int}秒后重试")

            elif response.status_code == 401:
                # Token无效
                bot.status = BotStatus.ERROR
                bot.last_error = "Invalid bot token (401 Unauthorized)"
                bot.consecutive_failures += 1
                failover_reason = FailoverReason.API_ERROR
                self.logger.error(f"机器人 {bot.bot_id} Token无效")

            else:
                # 其他HTTP错误
                error_text = response.text[:100] if response.text else "Unknown error"
                bot.status = BotStatus.ERROR
                bot.last_error = f"HTTP {response.status_code}: {error_text}"
                bot.consecutive_failures += 1
                failover_reason = FailoverReason.API_ERROR
                self.logger.error(f"机器人 {bot.bot_id} HTTP错误 {response.status_code}: {error_text}")

        except asyncio.TimeoutError:
            bot.status = BotStatus.ERROR
            bot.last_error = "Health check timeout"
            bot.consecutive_failures += 1
            failover_reason = FailoverReason.TIMEOUT
            self.logger.warning(f"机器人 {bot.bot_id} 健康检查超时")

        except Exception as e:
            bot.status = BotStatus.ERROR
            bot.last_error = str(e)[:100]
            bot.consecutive_failures += 1
            failover_reason = FailoverReason.CONNECTION_ERROR # 假设是连接问题
            self.logger.error(f"机器人 {bot.bot_id} 健康检查异常: {e}", exc_info=True)
        finally:
            await self._save_bot_status(bot)
            # 通知故障转移管理器
            failover_manager = await self._get_failover_manager()
            if failover_manager:
                if is_healthy:
                    await failover_manager.handle_bot_recovery(bot.bot_id)
                elif bot.status != current_status and failover_reason:
                    # 仅在状态实际变化且有明确故障原因时才通知
                    await failover_manager.handle_bot_failure(
                        bot.bot_id, failover_reason, bot.last_error
                    )
            return is_healthy

    async def _save_bot_status(self, bot: BotInstance):
        """保存机器人状态到Redis"""
        if not self.redis_client:
            return

        try:
            key = f"bot_status:{bot.bot_id}"
            data = bot.to_dict()
            await self.redis_client.setex(key, 300, json.dumps(data))  # 5分钟过期
        except Exception as e:
            self.logger.debug(f"保存机器人状态失败: {e}")

    async def _load_bot_status(self, bot_id: str) -> Optional[Dict]:
        """从Redis加载机器人状态"""
        if not self.redis_client:
            return None

        try:
            key = f"bot_status:{bot_id}"
            data = await self.redis_client.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            self.logger.debug(f"加载机器人状态失败: {e}")

        return None

    async def _heartbeat_loop(self):
        """心跳循环"""
        while self._running:
            try:
                for bot in self.bots.values():
                    if bot.config.enabled and bot.status != BotStatus.DISABLED:
                        bot.last_heartbeat = time.time()
                        await self._save_bot_status(bot)

                await asyncio.sleep(30)  # 每30秒心跳一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"心跳循环异常: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def _status_check_loop(self):
        """状态检查循环"""
        from .settings import settings # 延迟导入
        while self._running:
            try:
                current_time = time.time()

                for bot in self.bots.values():
                    if not bot.config.enabled:
                        if bot.status != BotStatus.DISABLED:
                            bot.status = BotStatus.DISABLED
                            await self._save_bot_status(bot)
                        continue

                    # 检查是否需要恢复被限速的机器人
                    if (bot.status == BotStatus.RATE_LIMITED and
                            bot.rate_limit_reset_time and
                            current_time > bot.rate_limit_reset_time):
                        self.logger.info(f"尝试恢复被限速的机器人 {bot.bot_id}")
                        await self._check_bot_health(bot)

                    # 定期健康检查（根据设置的间隔检查）
                    health_check_interval = settings.BOT_HEALTH_CHECK_INTERVAL
                    if bot.status == BotStatus.HEALTHY:
                        if current_time - bot.last_heartbeat > health_check_interval:
                            await self._check_bot_health(bot)
                    elif bot.status in [BotStatus.UNKNOWN, BotStatus.ERROR]:
                        # 错误状态的机器人更频繁检查，但有退避机制
                        backoff_time = min(health_check_interval * (2 ** min(bot.consecutive_failures, 5)), 3600)  # 最长1小时
                        if current_time - bot.last_heartbeat > backoff_time:
                            await self._check_bot_health(bot)

                await asyncio.sleep(60)  # 每分钟检查一次，但实际检查频率由BOT_HEALTH_CHECK_INTERVAL和退避机制决定
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"状态检查循环异常: {e}", exc_info=True)
                await asyncio.sleep(120)

    def get_healthy_bots(self) -> List[BotInstance]:
        """获取健康的机器人列表"""
        healthy_bots = [
            bot for bot in self.bots.values()
            if bot.is_available() and bot.status == BotStatus.HEALTHY
        ]

        # 按负载评分排序（分数越低越好）
        return sorted(healthy_bots, key=lambda b: b.get_load_score())

    def get_available_bots(self) -> List[BotInstance]:
        """获取所有可用的机器人列表（包括可能恢复的）"""
        available_bots = []

        # 🔍 增加详细诊断（新增）
        for bot_id, bot in self.bots.items():
            is_available = bot.is_available()
            if not is_available:
                self.logger.debug(f"机器人 {bot_id} 不可用: 状态={bot.status.value}, 启用={bot.config.enabled}")
            if is_available:
                available_bots.append(bot)

        # 🚨 关键诊断：如果没有可用机器人，记录详细原因（新增）
        if not available_bots:
            self.logger.error(f"⚠️ 无可用机器人！总计{len(self.bots)}个机器人的详细状态:")
            for bot_id, bot in self.bots.items():
                self.logger.error(f"  - {bot_id}: 状态={bot.status.value}, 启用={bot.config.enabled}, 失败次数={bot.consecutive_failures}")

        # 按负载评分排序（保持原逻辑）
        return sorted(available_bots, key=lambda b: b.get_load_score())

    def get_best_bot(self) -> Optional[BotInstance]:
        """获取最佳机器人（负载最低的健康机器人）"""
        healthy_bots = self.get_healthy_bots()
        if healthy_bots:
            return healthy_bots[0]

        # 如果没有健康的机器人，尝试获取可用的机器人
        available_bots = self.get_available_bots()
        if available_bots:
            self.logger.warning("没有健康的机器人，使用可用的机器人")
            return available_bots[0]

        return None

    async def mark_bot_rate_limited(self, bot_id: str, retry_after: int = 60):
        """标记机器人被限速"""
        from .failover_manager import FailoverReason # 延迟导入
        if bot_id in self.bots:
            bot = self.bots[bot_id]
            bot.status = BotStatus.RATE_LIMITED
            bot.rate_limit_reset_time = time.time() + retry_after
            bot.last_error = f"Rate limited, retry after {retry_after}s"
            bot.consecutive_failures += 1
            await self._save_bot_status(bot)
            self.logger.warning(f"机器人 {bot_id} 被标记为限速状态")

            failover_manager = await self._get_failover_manager()
            if failover_manager:
                await failover_manager.handle_bot_failure(bot.bot_id, FailoverReason.RATE_LIMITED, bot.last_error)

    async def mark_bot_error(self, bot_id: str, error_message: str, reason: str = 'api_error',
                             _from_failover: bool = False):
        """标记机器人错误 - 防循环版本"""
        from .failover_manager import FailoverReason  # 延迟导入
        if bot_id in self.bots:
            bot = self.bots[bot_id]
            bot.status = BotStatus.ERROR
            bot.last_error = error_message[:100]
            bot.consecutive_failures += 1
            await self._save_bot_status(bot)
            self.logger.error(f"机器人 {bot_id} 被标记为错误状态: {error_message}")

            # 🔥 关键修复：只有不是来自故障转移管理器的调用才触发故障转移
            if not _from_failover:
                failover_manager = await self._get_failover_manager()
                if failover_manager:
                    try:
                        failover_reason_enum = FailoverReason(reason)
                    except ValueError:
                        self.logger.warning(f"未知故障原因字符串 '{reason}', 默认为 API_ERROR")
                        failover_reason_enum = FailoverReason.API_ERROR

                    await failover_manager.handle_bot_failure(
                        bot.bot_id, failover_reason_enum, bot.last_error
                    )


    async def record_bot_request(self, bot_id: str):
        """记录机器人请求"""
        if bot_id in self.bots:
            bot = self.bots[bot_id]
            current_time = time.time()

            # 重置计数器（如果时间窗口过了）
            if current_time - bot.last_request_time >= 60:
                bot.request_count = 1
            else:
                bot.request_count += 1

            bot.last_request_time = current_time
            await self._save_bot_status(bot)

    def get_bot_by_id(self, bot_id: str) -> Optional[BotInstance]:
        """根据ID获取机器人"""
        return self.bots.get(bot_id)

    def get_all_bots_status(self) -> Dict[str, Dict]:
        """获取所有机器人状态"""
        return {bot_id: bot.to_dict() for bot_id, bot in self.bots.items()}

    def get_stats(self) -> Dict[str, any]:
        """获取统计信息"""
        total_bots = len(self.bots)
        healthy_bots = len(self.get_healthy_bots())
        available_bots = len(self.get_available_bots())

        status_counts = {}
        for status in BotStatus:
            status_counts[status.value] = sum(1 for bot in self.bots.values() if bot.status == status)

        total_requests = sum(bot.request_count for bot in self.bots.values())

        return {
            "total_bots": total_bots,
            "healthy_bots": healthy_bots,
            "available_bots": available_bots,
            "status_distribution": status_counts,
            "total_requests": total_requests,
            "instance_id": self.instance_id,
            "running": self._running
        }


# 全局机器人管理器实例
_bot_manager: Optional[BotManager] = None


async def get_bot_manager() -> BotManager:
    """获取全局机器人管理器"""
    global _bot_manager
    if _bot_manager is None:
        # 延迟导入以避免循环依赖
        from .settings import settings
        import redis.asyncio as redis # 确保这里导入

        # 尝试连接Redis
        redis_client = None
        if redis:
            try:
                redis_url = getattr(settings, 'REDIS_URL', 'redis://localhost:6379')
                redis_client = redis.from_url(redis_url)
                await redis_client.ping()
                logger.info("Redis连接成功，将使用Redis存储机器人状态")
            except Exception as e:
                logger.info(f"Redis不可用，使用本地状态管理: {e}")
                redis_client = None
        else:
            logger.info("Redis库未安装，使用本地状态管理")

        _bot_manager = BotManager(redis_client)
        # 暂时不在这里调用 _bot_manager.start()，因为它的启动依赖于settings中的BOT_HEALTH_CHECK_INTERVAL，
        # 并且启动逻辑应该由dependencies.py中的ApplicationLifecycleManager负责。
        # _bot_manager.start() 将在 dependencies.py 中被调用。

    return _bot_manager


async def cleanup_bot_manager():
    """清理机器人管理器"""
    global _bot_manager
    if _bot_manager:
        await _bot_manager.stop()
        _bot_manager = None
        logger.info("机器人管理器已清理")