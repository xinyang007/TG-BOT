import time
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from .logging_config import get_logger
from .settings import settings

logger = get_logger("app.failover_events")


class EventSeverity(Enum):
    """事件严重程度"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class FailoverEventRecord:
    """故障转移事件记录"""
    event_id: str
    bot_id: str
    event_type: str  # "failure", "recovery", "manual_disable", etc.
    severity: EventSeverity
    timestamp: float
    description: str
    metadata: Dict = field(default_factory=dict)
    resolved: bool = False
    resolution_time: Optional[float] = None
    impact_metrics: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "event_id": self.event_id,
            "bot_id": self.bot_id,
            "event_type": self.event_type,
            "severity": self.severity.value,
            "timestamp": self.timestamp,
            "description": self.description,
            "metadata": self.metadata,
            "resolved": self.resolved,
            "resolution_time": self.resolution_time,
            "impact_metrics": self.impact_metrics,
            "duration": self.get_duration()
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'FailoverEventRecord':
        return cls(
            event_id=data["event_id"],
            bot_id=data["bot_id"],
            event_type=data["event_type"],
            severity=EventSeverity(data["severity"]),
            timestamp=data["timestamp"],
            description=data["description"],
            metadata=data.get("metadata", {}),
            resolved=data.get("resolved", False),
            resolution_time=data.get("resolution_time"),
            impact_metrics=data.get("impact_metrics", {})
        )

    def get_duration(self) -> Optional[float]:
        """获取事件持续时间"""
        if self.resolved and self.resolution_time:
            return self.resolution_time - self.timestamp
        elif not self.resolved:
            return time.time() - self.timestamp
        return None

    def get_age_minutes(self) -> float:
        """获取事件年龄（分钟）"""
        return (time.time() - self.timestamp) / 60


@dataclass
class FailoverStatistics:
    """故障转移统计信息"""
    total_events: int = 0
    active_events: int = 0
    resolved_events: int = 0
    events_by_severity: Dict[str, int] = field(default_factory=dict)
    events_by_bot: Dict[str, int] = field(default_factory=dict)
    events_by_type: Dict[str, int] = field(default_factory=dict)
    average_resolution_time: float = 0.0
    mttr: float = 0.0  # Mean Time To Recovery
    mtbf: float = 0.0  # Mean Time Between Failures
    availability_percentage: float = 100.0

    def to_dict(self) -> Dict:
        return {
            "total_events": self.total_events,
            "active_events": self.active_events,
            "resolved_events": self.resolved_events,
            "events_by_severity": self.events_by_severity,
            "events_by_bot": self.events_by_bot,
            "events_by_type": self.events_by_type,
            "average_resolution_time": self.average_resolution_time,
            "mttr_minutes": self.mttr,
            "mtbf_hours": self.mtbf,
            "availability_percentage": self.availability_percentage
        }


class FailoverEventStore:
    """故障转移事件存储"""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.logger = get_logger("app.failover_events.store")

        # Redis键前缀
        self.event_key_prefix = "failover_event:"
        self.events_list_key = "failover_events_chronological"
        self.events_by_bot_key = "failover_events_by_bot:"
        self.active_events_key = "failover_active_events"

    async def store_event(self, event: FailoverEventRecord) -> bool:
        """存储故障转移事件"""
        if not self.redis_client:
            self.logger.warning("Redis不可用，无法存储事件")
            return False

        try:
            # 存储事件详情
            event_key = f"{self.event_key_prefix}{event.event_id}"
            event_data = json.dumps(event.to_dict())
            await self.redis_client.setex(event_key, 86400 * 7, event_data)  # 保存7天

            # 添加到时间序列列表
            await self.redis_client.zadd(
                self.events_list_key,
                {event.event_id: event.timestamp}
            )

            # 添加到机器人事件列表
            bot_events_key = f"{self.events_by_bot_key}{event.bot_id}"
            await self.redis_client.zadd(
                bot_events_key,
                {event.event_id: event.timestamp}
            )

            # 如果是活跃事件，添加到活跃事件集合
            if not event.resolved:
                await self.redis_client.sadd(self.active_events_key, event.event_id)

            self.logger.debug(f"存储故障转移事件: {event.event_id}")
            return True

        except Exception as e:
            self.logger.error(f"存储事件失败: {e}", exc_info=True)
            return False

    async def resolve_event(self, event_id: str, resolution_time: float = None) -> bool:
        """标记事件为已解决"""
        if not self.redis_client:
            return False

        try:
            # 获取事件
            event = await self.get_event(event_id)
            if not event:
                return False

            # 更新事件状态
            event.resolved = True
            event.resolution_time = resolution_time or time.time()

            # 重新存储
            await self.store_event(event)

            # 从活跃事件中移除
            await self.redis_client.srem(self.active_events_key, event_id)

            self.logger.info(f"事件 {event_id} 已标记为解决")
            return True

        except Exception as e:
            self.logger.error(f"解决事件失败: {e}", exc_info=True)
            return False

    async def get_event(self, event_id: str) -> Optional[FailoverEventRecord]:
        """获取单个事件"""
        if not self.redis_client:
            return None

        try:
            event_key = f"{self.event_key_prefix}{event_id}"
            data = await self.redis_client.get(event_key)

            if data:
                event_dict = json.loads(data)
                return FailoverEventRecord.from_dict(event_dict)

            return None

        except Exception as e:
            self.logger.error(f"获取事件失败: {e}", exc_info=True)
            return None

    async def get_events_by_time_range(self, start_time: float, end_time: float,
                                       limit: int = 100) -> List[FailoverEventRecord]:
        """根据时间范围获取事件"""
        if not self.redis_client:
            return []

        try:
            # 从时间序列中获取事件ID
            event_ids = await self.redis_client.zrangebyscore(
                self.events_list_key, start_time, end_time,
                start=0, num=limit
            )

            # 获取事件详情
            events = []
            for event_id in event_ids:
                event = await self.get_event(event_id)
                if event:
                    events.append(event)

            return events

        except Exception as e:
            self.logger.error(f"获取时间范围事件失败: {e}", exc_info=True)
            return []

    async def get_events_by_bot(self, bot_id: str, limit: int = 50) -> List[FailoverEventRecord]:
        """获取特定机器人的事件"""
        if not self.redis_client:
            return []

        try:
            bot_events_key = f"{self.events_by_bot_key}{bot_id}"

            # 获取最近的事件ID（倒序）
            event_ids = await self.redis_client.zrevrange(bot_events_key, 0, limit - 1)

            # 获取事件详情
            events = []
            for event_id in event_ids:
                event = await self.get_event(event_id)
                if event:
                    events.append(event)

            return events

        except Exception as e:
            self.logger.error(f"获取机器人事件失败: {e}", exc_info=True)
            return []

    async def get_active_events(self) -> List[FailoverEventRecord]:
        """获取所有活跃事件"""
        if not self.redis_client:
            return []

        try:
            # 获取活跃事件ID
            event_ids = await self.redis_client.smembers(self.active_events_key)

            # 获取事件详情
            events = []
            for event_id in event_ids:
                event = await self.get_event(event_id)
                if event and not event.resolved:
                    events.append(event)
                elif event and event.resolved:
                    # 清理已解决但仍在活跃列表中的事件
                    await self.redis_client.srem(self.active_events_key, event_id)

            # 按时间戳排序
            events.sort(key=lambda e: e.timestamp, reverse=True)
            return events

        except Exception as e:
            self.logger.error(f"获取活跃事件失败: {e}", exc_info=True)
            return []

    async def cleanup_old_events(self, days_to_keep: int = 30):
        """清理旧事件"""
        if not self.redis_client:
            return

        try:
            cutoff_time = time.time() - (days_to_keep * 86400)

            # 获取过期的事件ID
            expired_event_ids = await self.redis_client.zrangebyscore(
                self.events_list_key, 0, cutoff_time
            )

            # 删除过期事件
            for event_id in expired_event_ids:
                event_key = f"{self.event_key_prefix}{event_id}"
                await self.redis_client.delete(event_key)

            # 从时间序列中移除
            await self.redis_client.zremrangebyscore(
                self.events_list_key, 0, cutoff_time
            )

            self.logger.info(f"清理了 {len(expired_event_ids)} 个过期事件")

        except Exception as e:
            self.logger.error(f"清理旧事件失败: {e}", exc_info=True)


class FailoverAnalytics:
    """故障转移分析"""

    def __init__(self, event_store: FailoverEventStore):
        self.event_store = event_store
        self.logger = get_logger("app.failover_events.analytics")

    async def calculate_statistics(self, days: int = 7) -> FailoverStatistics:
        """计算故障转移统计信息"""
        try:
            end_time = time.time()
            start_time = end_time - (days * 86400)

            # 获取时间范围内的所有事件
            events = await self.event_store.get_events_by_time_range(start_time, end_time, 1000)
            active_events = await self.event_store.get_active_events()

            stats = FailoverStatistics()
            stats.total_events = len(events)
            stats.active_events = len(active_events)
            stats.resolved_events = len([e for e in events if e.resolved])

            # 按严重程度统计
            for event in events:
                severity = event.severity.value
                stats.events_by_severity[severity] = stats.events_by_severity.get(severity, 0) + 1

            # 按机器人统计
            for event in events:
                bot_id = event.bot_id
                stats.events_by_bot[bot_id] = stats.events_by_bot.get(bot_id, 0) + 1

            # 按事件类型统计
            for event in events:
                event_type = event.event_type
                stats.events_by_type[event_type] = stats.events_by_type.get(event_type, 0) + 1

            # 计算平均解决时间
            resolved_events = [e for e in events if e.resolved and e.resolution_time]
            if resolved_events:
                total_resolution_time = sum(e.get_duration() for e in resolved_events)
                stats.average_resolution_time = total_resolution_time / len(resolved_events)
                stats.mttr = stats.average_resolution_time / 60  # 转换为分钟

            # 计算可用性
            stats.availability_percentage = await self._calculate_availability(events, days)

            # 计算MTBF
            stats.mtbf = await self._calculate_mtbf(events, days)

            return stats

        except Exception as e:
            self.logger.error(f"计算统计信息失败: {e}", exc_info=True)
            return FailoverStatistics()

    async def _calculate_availability(self, events: List[FailoverEventRecord], days: int) -> float:
        """计算可用性百分比"""
        try:
            total_time = days * 86400  # 总时间（秒）
            downtime = 0

            failure_events = [e for e in events if e.event_type == "failure"]

            for event in failure_events:
                if event.resolved and event.resolution_time:
                    downtime += event.get_duration()
                else:
                    # 如果未解决，计算到当前时间
                    downtime += time.time() - event.timestamp

            if total_time > 0:
                availability = ((total_time - downtime) / total_time) * 100
                return max(0, min(100, availability))

            return 100.0

        except Exception as e:
            self.logger.error(f"计算可用性失败: {e}")
            return 100.0

    async def _calculate_mtbf(self, events: List[FailoverEventRecord], days: int) -> float:
        """计算平均故障间隔时间（小时）"""
        try:
            failure_events = [e for e in events if e.event_type == "failure"]

            if len(failure_events) <= 1:
                return float('inf')

            total_time_hours = days * 24
            failure_count = len(failure_events)

            # MTBF = 总运行时间 / 故障次数
            return total_time_hours / failure_count

        except Exception as e:
            self.logger.error(f"计算MTBF失败: {e}")
            return 0.0

    async def get_failure_trends(self, days: int = 30) -> Dict:
        """获取故障趋势分析"""
        try:
            end_time = time.time()
            start_time = end_time - (days * 86400)

            events = await self.event_store.get_events_by_time_range(start_time, end_time, 1000)
            failure_events = [e for e in events if e.event_type == "failure"]

            # 按天分组
            daily_failures = {}
            for event in failure_events:
                day = datetime.fromtimestamp(event.timestamp).strftime('%Y-%m-%d')
                daily_failures[day] = daily_failures.get(day, 0) + 1

            # 按机器人分组
            bot_failures = {}
            for event in failure_events:
                bot_id = event.bot_id
                bot_failures[bot_id] = bot_failures.get(bot_id, 0) + 1

            return {
                "daily_failures": daily_failures,
                "bot_failures": bot_failures,
                "total_failures": len(failure_events),
                "period_days": days
            }

        except Exception as e:
            self.logger.error(f"获取故障趋势失败: {e}", exc_info=True)
            return {}


# 全局事件存储实例
_event_store: Optional[FailoverEventStore] = None
_analytics: Optional[FailoverAnalytics] = None


async def get_failover_event_store() -> FailoverEventStore:
    """获取全局事件存储"""
    global _event_store
    if _event_store is None:
        # 获取Redis客户端
        redis_client = None
        if redis:
            try:
                redis_url = getattr(settings, 'REDIS_URL', 'redis://localhost:6379')
                redis_client = redis.from_url(redis_url)
                await redis_client.ping()
            except Exception as e:
                logger.warning(f"Redis不可用，事件存储功能受限: {e}")

        _event_store = FailoverEventStore(redis_client)

    return _event_store


async def get_failover_analytics() -> FailoverAnalytics:
    """获取故障转移分析器"""
    global _analytics
    if _analytics is None:
        event_store = await get_failover_event_store()
        _analytics = FailoverAnalytics(event_store)

    return _analytics