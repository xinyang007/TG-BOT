import time
import asyncio
from typing import Dict, Optional, Callable, Any, List
from dataclasses import dataclass, field
from enum import Enum
import json

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from .logging_config import get_logger
from .settings import settings

logger = get_logger("app.circuit_breaker")


class CircuitBreakerState(Enum):
    """熔断器状态"""
    CLOSED = "closed"  # 正常状态，允许请求通过
    OPEN = "open"  # 熔断状态，拒绝所有请求
    HALF_OPEN = "half_open"  # 半开状态，允许少量请求测试


@dataclass
class CircuitBreakerConfig:
    """熔断器配置"""
    failure_threshold: int = 5  # 故障阈值
    recovery_timeout: int = 60  # 恢复超时（秒）
    success_threshold: int = 3  # 恢复成功阈值
    timeout: float = 30.0  # 请求超时
    max_failures_in_window: int = 10  # 时间窗口内最大失败次数
    time_window: int = 300  # 时间窗口（秒）


@dataclass
class CircuitBreakerStats:
    """熔断器统计信息"""
    state: CircuitBreakerState
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rejected_requests: int = 0
    state_change_time: float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time,
            "last_success_time": self.last_success_time,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "rejected_requests": self.rejected_requests,
            "state_change_time": self.state_change_time,
            "success_rate": (self.successful_requests / self.total_requests * 100) if self.total_requests > 0 else 0
        }


class CircuitBreakerError(Exception):
    """熔断器错误"""
    pass


class CircuitBreaker:
    """熔断器实现"""

    def __init__(self, name: str, config: CircuitBreakerConfig = None,
                 redis_client: Optional[redis.Redis] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.redis_client = redis_client
        self.logger = get_logger(f"app.circuit_breaker.{name}")

        # 本地状态（Redis不可用时的后备）
        self._local_stats = CircuitBreakerStats(state=CircuitBreakerState.CLOSED)
        self._failure_times: List[float] = []

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        通过熔断器调用函数

        Args:
            func: 要调用的函数
            *args, **kwargs: 函数参数

        Returns:
            函数返回值

        Raises:
            CircuitBreakerError: 熔断器拒绝请求
            其他异常: 函数执行异常
        """
        # 检查熔断器状态
        stats = await self._get_stats()

        if stats.state == CircuitBreakerState.OPEN:
            # 检查是否可以进入半开状态
            if self._should_attempt_reset(stats):
                await self._set_state(CircuitBreakerState.HALF_OPEN)
                self.logger.info(f"熔断器 {self.name} 进入半开状态")
                stats.state = CircuitBreakerState.HALF_OPEN
            else:
                await self._record_rejected()
                raise CircuitBreakerError(f"熔断器 {self.name} 处于开启状态，请求被拒绝")

        # 执行请求
        start_time = time.time()
        try:
            # 设置超时
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(func(*args, **kwargs), timeout=self.config.timeout)
            else:
                result = func(*args, **kwargs)

            # 记录成功
            await self._record_success()

            # 如果在半开状态，检查是否可以关闭熔断器
            if stats.state == CircuitBreakerState.HALF_OPEN:
                current_stats = await self._get_stats()
                if current_stats.success_count >= self.config.success_threshold:
                    await self._set_state(CircuitBreakerState.CLOSED)
                    await self._reset_counters()
                    self.logger.info(f"熔断器 {self.name} 恢复正常，关闭熔断器")

            return result

        except asyncio.TimeoutError:
            await self._record_failure()
            self.logger.warning(f"熔断器 {self.name} 调用超时: {self.config.timeout}s")
            raise CircuitBreakerError(f"调用超时: {self.config.timeout}s")

        except Exception as e:
            await self._record_failure()

            # 检查是否需要开启熔断器
            await self._check_failure_threshold()

            raise e

    async def _get_stats(self) -> CircuitBreakerStats:
        """获取熔断器统计信息"""
        if not self.redis_client:
            return self._local_stats

        try:
            key = f"circuit_breaker:{self.name}:stats"
            data = await self.redis_client.get(key)

            if data:
                stats_dict = json.loads(data)
                stats = CircuitBreakerStats(
                    state=CircuitBreakerState(stats_dict["state"]),
                    failure_count=stats_dict.get("failure_count", 0),
                    success_count=stats_dict.get("success_count", 0),
                    last_failure_time=stats_dict.get("last_failure_time"),
                    last_success_time=stats_dict.get("last_success_time"),
                    total_requests=stats_dict.get("total_requests", 0),
                    successful_requests=stats_dict.get("successful_requests", 0),
                    failed_requests=stats_dict.get("failed_requests", 0),
                    rejected_requests=stats_dict.get("rejected_requests", 0),
                    state_change_time=stats_dict.get("state_change_time", time.time())
                )
                return stats
            else:
                # 初始化Redis中的统计信息
                await self._save_stats(self._local_stats)
                return self._local_stats

        except Exception as e:
            self.logger.warning(f"获取熔断器统计失败，使用本地状态: {e}")
            return self._local_stats

    async def _save_stats(self, stats: CircuitBreakerStats):
        """保存统计信息"""
        if self.redis_client:
            try:
                key = f"circuit_breaker:{self.name}:stats"
                data = json.dumps(stats.to_dict())
                await self.redis_client.setex(key, 3600, data)  # 1小时过期
            except Exception as e:
                self.logger.warning(f"保存熔断器统计失败: {e}")

        # 同时更新本地状态
        self._local_stats = stats

    async def _record_success(self):
        """记录成功请求"""
        stats = await self._get_stats()
        stats.success_count += 1
        stats.total_requests += 1
        stats.successful_requests += 1
        stats.last_success_time = time.time()

        await self._save_stats(stats)

    async def _record_failure(self):
        """记录失败请求"""
        stats = await self._get_stats()
        stats.failure_count += 1
        stats.total_requests += 1
        stats.failed_requests += 1
        stats.last_failure_time = time.time()

        # 记录失败时间（用于时间窗口检查）
        current_time = time.time()
        self._failure_times.append(current_time)

        # 清理过期的失败记录
        cutoff_time = current_time - self.config.time_window
        self._failure_times = [t for t in self._failure_times if t > cutoff_time]

        await self._save_stats(stats)

    async def _record_rejected(self):
        """记录被拒绝的请求"""
        stats = await self._get_stats()
        stats.rejected_requests += 1
        stats.total_requests += 1

        await self._save_stats(stats)

    async def _check_failure_threshold(self):
        """检查是否达到故障阈值"""
        stats = await self._get_stats()

        # 检查连续失败次数
        if stats.failure_count >= self.config.failure_threshold:
            await self._set_state(CircuitBreakerState.OPEN)
            self.logger.warning(
                f"熔断器 {self.name} 达到故障阈值 {self.config.failure_threshold}，开启熔断器"
            )
            return

        # 检查时间窗口内的失败次数
        if len(self._failure_times) >= self.config.max_failures_in_window:
            await self._set_state(CircuitBreakerState.OPEN)
            self.logger.warning(
                f"熔断器 {self.name} 在 {self.config.time_window} 秒内失败 "
                f"{len(self._failure_times)} 次，开启熔断器"
            )

    async def _set_state(self, new_state: CircuitBreakerState):
        """设置熔断器状态"""
        stats = await self._get_stats()
        old_state = stats.state

        stats.state = new_state
        stats.state_change_time = time.time()

        # 状态转换时重置计数器
        if new_state != old_state:
            if new_state == CircuitBreakerState.CLOSED:
                stats.failure_count = 0
                stats.success_count = 0
            elif new_state == CircuitBreakerState.HALF_OPEN:
                stats.success_count = 0

        await self._save_stats(stats)

        if old_state != new_state:
            self.logger.info(f"熔断器 {self.name} 状态变更: {old_state.value} -> {new_state.value}")

    def _should_attempt_reset(self, stats: CircuitBreakerStats) -> bool:
        """检查是否应该尝试重置（进入半开状态）"""
        if stats.state != CircuitBreakerState.OPEN:
            return False

        # 检查恢复超时
        if stats.last_failure_time:
            time_since_last_failure = time.time() - stats.last_failure_time
            return time_since_last_failure >= self.config.recovery_timeout

        return False

    async def _reset_counters(self):
        """重置计数器"""
        stats = await self._get_stats()
        stats.failure_count = 0
        stats.success_count = 0
        self._failure_times.clear()

        await self._save_stats(stats)

    async def get_state(self) -> CircuitBreakerState:
        """获取当前状态"""
        stats = await self._get_stats()
        return stats.state

    async def get_stats_dict(self) -> Dict:
        """获取统计信息字典"""
        stats = await self._get_stats()
        return stats.to_dict()

    async def force_open(self):
        """强制开启熔断器"""
        await self._set_state(CircuitBreakerState.OPEN)
        self.logger.warning(f"熔断器 {self.name} 被强制开启")

    async def force_close(self):
        """强制关闭熔断器"""
        await self._set_state(CircuitBreakerState.CLOSED)
        await self._reset_counters()
        self.logger.info(f"熔断器 {self.name} 被强制关闭")


class CircuitBreakerRegistry:
    """熔断器注册表"""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.logger = get_logger("app.circuit_breaker.registry")

    def get_circuit_breaker(self, name: str, config: CircuitBreakerConfig = None) -> CircuitBreaker:
        """获取或创建熔断器"""
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreaker(name, config, self.redis_client)
            self.logger.info(f"创建熔断器: {name}")

        return self.circuit_breakers[name]

    async def get_all_stats(self) -> Dict[str, Dict]:
        """获取所有熔断器的统计信息"""
        stats = {}
        for name, cb in self.circuit_breakers.items():
            stats[name] = await cb.get_stats_dict()
        return stats

    async def reset_all(self):
        """重置所有熔断器"""
        for cb in self.circuit_breakers.values():
            await cb.force_close()
        self.logger.info("重置所有熔断器")


# 全局熔断器注册表
_circuit_breaker_registry: Optional[CircuitBreakerRegistry] = None


async def get_circuit_breaker_registry() -> CircuitBreakerRegistry:
    """获取全局熔断器注册表"""
    global _circuit_breaker_registry
    if _circuit_breaker_registry is None:
        # 获取Redis客户端
        redis_client = None
        if redis:
            try:
                redis_url = getattr(settings, 'REDIS_URL', 'redis://localhost:6379')
                redis_client = redis.from_url(redis_url)
                await redis_client.ping()
            except Exception as e:
                logger.warning(f"Redis不可用，熔断器使用本地状态: {e}")

        _circuit_breaker_registry = CircuitBreakerRegistry(redis_client)

    return _circuit_breaker_registry


async def get_circuit_breaker(name: str, config: CircuitBreakerConfig = None) -> CircuitBreaker:
    """获取熔断器的便利函数"""
    registry = await get_circuit_breaker_registry()
    return registry.get_circuit_breaker(name, config)