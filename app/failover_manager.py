import asyncio
import time
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from .logging_config import get_logger
from .settings import settings

logger = get_logger("app.failover_manager")


class FailoverReason(Enum):
    """故障转移原因"""
    RATE_LIMITED = "rate_limited"
    CONNECTION_ERROR = "connection_error"
    API_ERROR = "api_error"
    HEALTH_CHECK_FAILED = "health_check_failed"
    MANUAL_DISABLE = "manual_disable"
    TIMEOUT = "timeout"


class FailoverStrategy(Enum):
    """故障转移策略"""
    IMMEDIATE = "immediate"  # 立即转移
    GRADUAL = "gradual"  # 渐进式转移
    PRIORITY_BASED = "priority_based"  # 基于优先级转移


@dataclass
class FailoverEvent:
    """故障转移事件"""
    event_id: str
    failed_bot_id: str
    reason: FailoverReason
    timestamp: float = field(default_factory=time.time)
    target_bot_id: Optional[str] = None
    affected_messages: int = 0
    recovery_time: Optional[float] = None
    metadata: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "event_id": self.event_id,
            "failed_bot_id": self.failed_bot_id,
            "reason": self.reason.value,
            "timestamp": self.timestamp,
            "target_bot_id": self.target_bot_id,
            "affected_messages": self.affected_messages,
            "recovery_time": self.recovery_time,
            "metadata": self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'FailoverEvent':
        return cls(
            event_id=data["event_id"],
            failed_bot_id=data["failed_bot_id"],
            reason=FailoverReason(data["reason"]),
            timestamp=data.get("timestamp", time.time()),
            target_bot_id=data.get("target_bot_id"),
            affected_messages=data.get("affected_messages", 0),
            recovery_time=data.get("recovery_time"),
            metadata=data.get("metadata", {})
        )


class FailoverManager:
    """故障转移管理器"""

    def __init__(self, bot_manager, redis_client: Optional[redis.Redis] = None):
        self.bot_manager = bot_manager
        self.redis_client = redis_client
        self.logger = get_logger("app.failover_manager")

        # 配置
        self.failure_threshold = getattr(settings, 'BOT_FAILURE_THRESHOLD', 3)
        self.recovery_check_interval = getattr(settings, 'BOT_RECOVERY_CHECK_INTERVAL', 300)
        self.auto_failover_enabled = getattr(settings, 'AUTO_FAILOVER_ENABLED', True)

        # 状态跟踪
        self.active_events: Dict[str, FailoverEvent] = {}
        self.bot_failure_counts: Dict[str, int] = {}
        self.last_recovery_check: Dict[str, float] = {}

        # 后台任务
        self._recovery_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """启动故障转移管理器"""
        if self._running:
            return

        self.logger.info("启动故障转移管理器...")
        self._running = True

        # 启动恢复检查任务
        self._recovery_task = asyncio.create_task(self._recovery_check_loop())

        self.logger.info("故障转移管理器已启动")

    async def stop(self):
        """停止故障转移管理器"""
        if not self._running:
            return

        self.logger.info("停止故障转移管理器...")
        self._running = False

        if self._recovery_task and not self._recovery_task.done():
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass

        self.logger.info("故障转移管理器已停止")


    async def handle_bot_failure(self, bot_id: str, reason: FailoverReason,
                           error_details: str = None) -> Optional[str]:
        """处理机器人故障"""
        if not self.auto_failover_enabled:
            self.logger.warning(f"自动故障转移已禁用，忽略机器人 {bot_id} 的故障")
            return None

        # 🔥 新增：防止同一机器人短时间内频繁故障转移
        current_time = time.time()
        if not hasattr(self, '_last_failure_times'):
            self._last_failure_times = {}

        last_failure_time = self._last_failure_times.get(bot_id, 0)

        if current_time - last_failure_time < 60:  # 60秒内不重复处理同一机器人故障
            self.logger.warning(f"机器人 {bot_id} 在60秒内重复故障，跳过处理避免循环")
            return None

        # 记录故障时间
        self._last_failure_times[bot_id] = current_time

        # 增加故障计数
        self.bot_failure_counts[bot_id] = self.bot_failure_counts.get(bot_id, 0) + 1

        self.logger.warning(
            f"机器人 {bot_id} 故障，原因: {reason.value}, "
            f"连续故障次数: {self.bot_failure_counts[bot_id]}"
        )

        # 检查是否达到故障阈值
        if self.bot_failure_counts[bot_id] >= self.failure_threshold:
            return await self._execute_failover(bot_id, reason, error_details)
        else:
            self.logger.info(f"机器人 {bot_id} 故障次数未达到阈值，继续监控")
            return None

    async def _execute_failover(self, failed_bot_id: str, reason: FailoverReason,
                                error_details: str = None) -> Optional[str]:
        """执行故障转移"""
        import uuid
        event_id = str(uuid.uuid4())[:8]

        self.logger.warning(f"执行故障转移：机器人 {failed_bot_id} -> 事件 {event_id}")

        # 🔥 关键修复：使用_from_failover=True避免循环调用
        await self.bot_manager.mark_bot_error(
            failed_bot_id,
            f"Failover: {reason.value}",
            reason.value,
            _from_failover=True  # 新增参数
        )

        # 选择目标机器人
        target_bot_id = await self._select_failover_target(failed_bot_id)

        if not target_bot_id:
            self.logger.error(f"无法找到故障转移目标机器人，故障转移失败")
            return None

        # 创建故障转移事件
        event = FailoverEvent(
            event_id=event_id,
            failed_bot_id=failed_bot_id,
            reason=reason,
            target_bot_id=target_bot_id,
            metadata={
                "error_details": error_details,
                "failure_count": self.bot_failure_counts[failed_bot_id]
            }
        )

        self.active_events[event_id] = event

        # 保存事件到Redis
        await self._save_failover_event(event)

        # 记录成功转移
        self.logger.info(
            f"✅ 故障转移完成: {failed_bot_id} -> {target_bot_id}, 事件ID: {event_id}"
        )

        return target_bot_id

    async def _select_failover_target(self, failed_bot_id: str) -> Optional[str]:
        """选择故障转移目标机器人"""
        # 获取健康的机器人（排除故障机器人）
        healthy_bots = [
            bot for bot in self.bot_manager.get_healthy_bots()
            if bot.bot_id != failed_bot_id
        ]

        if not healthy_bots:
            # 如果没有健康机器人，尝试获取可用机器人
            available_bots = [
                bot for bot in self.bot_manager.get_available_bots()
                if bot.bot_id != failed_bot_id
            ]

            if available_bots:
                self.logger.warning("没有健康机器人，使用可用机器人作为故障转移目标")
                return available_bots[0].bot_id
            else:
                return None

        # 选择负载最低的健康机器人
        return healthy_bots[0].bot_id

    async def handle_bot_recovery(self, bot_id: str) -> bool:
        """
        处理机器人恢复

        Args:
            bot_id: 恢复的机器人ID

        Returns:
            bool: 是否成功处理恢复
        """
        # 重置故障计数
        if bot_id in self.bot_failure_counts:
            del self.bot_failure_counts[bot_id]

        # 标记相关事件为已恢复
        current_time = time.time()
        recovered_events = []

        for event_id, event in self.active_events.items():
            if event.failed_bot_id == bot_id and event.recovery_time is None:
                event.recovery_time = current_time
                recovered_events.append(event_id)

                # 更新Redis中的事件
                await self._save_failover_event(event)

        # 从活跃事件中移除已恢复的事件
        for event_id in recovered_events:
            del self.active_events[event_id]

        if recovered_events:
            self.logger.info(
                f"✅ 机器人 {bot_id} 已恢复，关闭 {len(recovered_events)} 个故障转移事件"
            )

        return len(recovered_events) > 0

    async def _recovery_check_loop(self):
        """恢复检查循环"""
        while self._running:
            try:
                await self._check_failed_bots_recovery()
                await asyncio.sleep(self.recovery_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"恢复检查循环异常: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def _check_failed_bots_recovery(self):
        """检查故障机器人的恢复状态"""
        current_time = time.time()

        # 获取所有有活跃故障事件的机器人
        failed_bot_ids = {event.failed_bot_id for event in self.active_events.values()}

        for bot_id in failed_bot_ids:
            # 检查是否需要进行恢复检查
            last_check = self.last_recovery_check.get(bot_id, 0)
            if current_time - last_check < self.recovery_check_interval:
                continue

            self.last_recovery_check[bot_id] = current_time

            # 获取机器人实例
            bot = self.bot_manager.get_bot_by_id(bot_id)
            if not bot:
                continue

            # 执行健康检查
            self.logger.info(f"检查故障机器人 {bot_id} 的恢复状态")
            is_healthy = await self.bot_manager._check_bot_health(bot)

            if is_healthy:
                await self.handle_bot_recovery(bot_id)

    async def _save_failover_event(self, event: FailoverEvent):
        """保存故障转移事件到Redis - 修复版本"""
        """保存故障转移事件到Redis - 安全版本"""
        if not self.redis_client:
            return

        try:
            from app.utils.json_utils import safe_json_dumps

            # 使用安全序列化
            event_dict = event.to_dict()
            data = safe_json_dumps(event_dict)

            key = f"failover_event:{event.event_id}"
            await self.redis_client.setex(key, 86400, data)  # 保存24小时

            # 添加到事件列表
            list_key = "failover_events_list"
            await self.redis_client.lpush(list_key, event.event_id)
            await self.redis_client.ltrim(list_key, 0, 999)  # 保留最近1000个事件

            self.logger.debug(f"故障转移事件 {event.event_id} 已安全保存")

        except Exception as e:
            self.logger.error(f"保存故障转移事件失败: {e}", exc_info=True)

    async def get_failover_stats(self) -> Dict:
        """获取故障转移统计信息"""
        stats = {
            "active_events": len(self.active_events),
            "bot_failure_counts": self.bot_failure_counts.copy(),
            "auto_failover_enabled": self.auto_failover_enabled,
            "failure_threshold": self.failure_threshold,
            "recovery_check_interval": self.recovery_check_interval,
            "recent_events": []
        }

        # 获取最近的事件
        if self.redis_client:
            try:
                event_ids = await self.redis_client.lrange("failover_events_list", 0, 9)
                for event_id in event_ids:
                    key = f"failover_event:{event_id}"
                    data = await self.redis_client.get(key)
                    if data:
                        event_data = json.loads(data)
                        stats["recent_events"].append(event_data)
            except Exception as e:
                self.logger.error(f"获取故障转移统计失败: {e}")

        return stats

    async def get_active_events(self) -> List[Dict]:
        """获取活跃的故障转移事件"""
        return [event.to_dict() for event in self.active_events.values()]


# 全局故障转移管理器实例
_failover_manager: Optional[FailoverManager] = None


async def get_failover_manager():
    """获取全局故障转移管理器"""
    global _failover_manager
    if _failover_manager is None:
        from .bot_manager import get_bot_manager

        bot_manager = await get_bot_manager()

        # 获取Redis客户端
        redis_client = None
        if redis:
            try:
                redis_url = getattr(settings, 'REDIS_URL', 'redis://localhost:6379')
                redis_client = redis.from_url(redis_url)
                await redis_client.ping()
            except Exception as e:
                logger.warning(f"Redis不可用，故障转移管理器功能受限: {e}")

        _failover_manager = FailoverManager(bot_manager, redis_client)
        await _failover_manager.start()

    return _failover_manager


async def cleanup_failover_manager():
    """清理故障转移管理器"""
    global _failover_manager
    if _failover_manager:
        await _failover_manager.stop()
        _failover_manager = None