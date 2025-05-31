import time
import asyncio
from typing import Dict, Any, Optional, List, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import redis.asyncio as redis
import json
import hashlib

from .logging_config import get_logger
from .settings import settings

logger = get_logger("app.rate_limit")


class LimitType(Enum):
    """限制类型"""
    PER_SECOND = "per_second"
    PER_MINUTE = "per_minute"
    PER_HOUR = "per_hour"
    PER_DAY = "per_day"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"


class ActionType(Enum):
    """操作类型"""
    MESSAGE = "message"
    API_CALL = "api_call"
    LOGIN = "login"
    BIND = "bind"
    COMMAND = "command"
    FILE_UPLOAD = "file_upload"


@dataclass
class RateLimitRule:
    """速率限制规则"""
    name: str
    limit_type: LimitType
    max_requests: int
    window_seconds: int
    action_types: List[ActionType] = field(default_factory=list)
    user_groups: List[str] = field(default_factory=list)  # ["admin", "premium", "normal"]
    burst_allowance: int = 0  # 突发允许量
    punishment_duration: int = 0  # 惩罚时长（秒）
    enabled: bool = True

    def get_key_prefix(self) -> str:
        """获取键前缀"""
        return f"rate_limit:{self.name}"


@dataclass
class RateLimitResult:
    """速率限制结果"""
    allowed: bool
    current_count: int
    limit: int
    remaining: int
    reset_time: float
    retry_after: Optional[int] = None
    punishment_ends_at: Optional[float] = None


class AdvancedRateLimiter:
    """高级速率限制器"""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.rules: Dict[str, RateLimitRule] = {}
        self.local_cache: Dict[str, Any] = {}
        self.cache_ttl = 60  # 本地缓存TTL
        self.logger = get_logger("app.rate_limit.advanced")

        # 默认规则
        self._setup_default_rules()

    def _setup_default_rules(self):
        """设置默认速率限制规则 - 30秒内5次"""
        # 普通用户消息限制 - 30秒5次
        self.add_rule(RateLimitRule(
            name="user_message_per_30s",
            limit_type=LimitType.SLIDING_WINDOW,  # 使用滑动窗口更精确
            max_requests=5,
            window_seconds=30,
            action_types=[ActionType.MESSAGE],
            user_groups=["normal"],
            burst_allowance=2,  # 突发允许2次额外
            punishment_duration=60  # 1分钟惩罚
        ))

        # 高级用户限制 - 30秒5次（与普通用户相同）
        self.add_rule(RateLimitRule(
            name="premium_message_per_30s",
            limit_type=LimitType.SLIDING_WINDOW,
            max_requests=5,
            window_seconds=30,
            action_types=[ActionType.MESSAGE],
            user_groups=["premium"],
            burst_allowance=2,  # 突发允许2次额外
            punishment_duration=60  # 1分钟惩罚
        ))

        # 管理员限制 - 30秒5次（统一限制）
        self.add_rule(RateLimitRule(
            name="admin_message_per_30s",
            limit_type=LimitType.SLIDING_WINDOW,
            max_requests=5,
            window_seconds=30,
            action_types=[ActionType.MESSAGE],
            user_groups=["admin"],
            burst_allowance=2,  # 突发允许2次额外
            punishment_duration=60  # 1分钟惩罚
        ))

        # API调用限制 - 30秒5次
        self.add_rule(RateLimitRule(
            name="api_calls_per_30s",
            limit_type=LimitType.SLIDING_WINDOW,
            max_requests=5,
            window_seconds=30,
            action_types=[ActionType.API_CALL],
            burst_allowance=1,  # API调用突发允许1次
            punishment_duration=60  # 1分钟惩罚
        ))

        # 绑定操作限制 - 30秒5次
        self.add_rule(RateLimitRule(
            name="bind_attempts_per_30s",
            limit_type=LimitType.SLIDING_WINDOW,
            max_requests=5,
            window_seconds=30,
            action_types=[ActionType.BIND],
            burst_allowance=1,  # 绑定操作突发允许1次
            punishment_duration=300  # 绑定失败惩罚5分钟
        ))

        # 文件上传限制 - 30秒5次
        self.add_rule(RateLimitRule(
            name="file_upload_per_30s",
            limit_type=LimitType.SLIDING_WINDOW,
            max_requests=5,
            window_seconds=30,
            action_types=[ActionType.FILE_UPLOAD],
            burst_allowance=1,  # 文件上传突发允许1次
            punishment_duration=120  # 2分钟惩罚
        ))

    def add_rule(self, rule: RateLimitRule):
        """添加限制规则"""
        self.rules[rule.name] = rule
        self.logger.info(f"Added rate limit rule: {rule.name}")

    def remove_rule(self, rule_name: str):
        """移除限制规则"""
        if rule_name in self.rules:
            del self.rules[rule_name]
            self.logger.info(f"Removed rate limit rule: {rule_name}")

    def get_applicable_rules(self, action_type: ActionType,
                             user_group: str = "normal") -> List[RateLimitRule]:
        """获取适用的规则"""
        applicable = []
        for rule in self.rules.values():
            if not rule.enabled:
                continue

            # 检查操作类型
            if rule.action_types and action_type not in rule.action_types:
                continue

            # 检查用户组
            if rule.user_groups and user_group not in rule.user_groups:
                continue

            applicable.append(rule)

        return applicable

    async def check_rate_limit(self, identifier: str, action_type: ActionType,
                               user_group: str = "normal",
                               weight: int = 1) -> RateLimitResult:
        """检查速率限制"""
        applicable_rules = self.get_applicable_rules(action_type, user_group)

        if not applicable_rules:
            # 没有适用规则，允许通过
            return RateLimitResult(
                allowed=True,
                current_count=0,
                limit=float('inf'),
                remaining=float('inf'),
                reset_time=time.time()
            )

        # 检查所有适用规则
        for rule in applicable_rules:
            result = await self._check_single_rule(identifier, rule, weight)
            if not result.allowed:
                return result

        # 所有规则都通过
        return RateLimitResult(
            allowed=True,
            current_count=0,
            limit=applicable_rules[0].max_requests,
            remaining=applicable_rules[0].max_requests,
            reset_time=time.time()
        )

    async def _check_single_rule(self, identifier: str, rule: RateLimitRule,
                                 weight: int) -> RateLimitResult:
        """检查单个规则"""
        # 先检查是否在惩罚期
        punishment_key = f"{rule.get_key_prefix()}:punishment:{identifier}"
        if self.redis_client:
            punishment_end = await self.redis_client.get(punishment_key)
            if punishment_end:
                punishment_end_time = float(punishment_end)
                if time.time() < punishment_end_time:
                    return RateLimitResult(
                        allowed=False,
                        current_count=rule.max_requests + 1,
                        limit=rule.max_requests,
                        remaining=0,
                        reset_time=punishment_end_time,
                        retry_after=int(punishment_end_time - time.time()),
                        punishment_ends_at=punishment_end_time
                    )

        # 根据限制类型检查
        if rule.limit_type == LimitType.SLIDING_WINDOW:
            return await self._check_sliding_window(identifier, rule, weight)
        elif rule.limit_type == LimitType.TOKEN_BUCKET:
            return await self._check_token_bucket(identifier, rule, weight)
        else:
            return await self._check_fixed_window(identifier, rule, weight)

    async def _check_sliding_window(self, identifier: str, rule: RateLimitRule,
                                    weight: int) -> RateLimitResult:
        """滑动窗口算法"""
        if not self.redis_client:
            return await self._check_local_cache(identifier, rule, weight)

        key = f"{rule.get_key_prefix()}:sliding:{identifier}"
        current_time = time.time()
        window_start = current_time - rule.window_seconds

        pipe = self.redis_client.pipeline()

        # 清理过期记录
        pipe.zremrangebyscore(key, 0, window_start)

        # 获取当前窗口内的请求数
        pipe.zcard(key)

        # 添加当前请求
        pipe.zadd(key, {str(current_time): current_time})

        # 设置过期时间
        pipe.expire(key, rule.window_seconds + 1)

        results = await pipe.execute()
        current_count = results[1]

        # 检查是否超限
        effective_limit = rule.max_requests + rule.burst_allowance
        allowed = current_count + weight <= effective_limit

        if not allowed and rule.punishment_duration > 0:
            # 应用惩罚
            await self._apply_punishment(identifier, rule)

        return RateLimitResult(
            allowed=allowed,
            current_count=current_count,
            limit=rule.max_requests,
            remaining=max(0, effective_limit - current_count),
            reset_time=current_time + rule.window_seconds
        )

    async def _check_token_bucket(self, identifier: str, rule: RateLimitRule,
                                  weight: int) -> RateLimitResult:
        """令牌桶算法"""
        if not self.redis_client:
            return await self._check_local_cache(identifier, rule, weight)

        key = f"{rule.get_key_prefix()}:bucket:{identifier}"
        current_time = time.time()

        # Lua脚本实现原子性令牌桶操作
        lua_script = """
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local refill_rate = tonumber(ARGV[2])
        local requested = tonumber(ARGV[3])
        local current_time = tonumber(ARGV[4])

        local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
        local tokens = tonumber(bucket[1]) or capacity
        local last_refill = tonumber(bucket[2]) or current_time

        -- 计算需要补充的令牌
        local time_passed = current_time - last_refill
        local new_tokens = math.min(capacity, tokens + (time_passed * refill_rate))

        local allowed = 0
        if new_tokens >= requested then
            new_tokens = new_tokens - requested
            allowed = 1
        end

        -- 更新桶状态
        redis.call('HMSET', key, 'tokens', new_tokens, 'last_refill', current_time)
        redis.call('EXPIRE', key, 3600)

        return {allowed, new_tokens, capacity}
        """

        capacity = rule.max_requests + rule.burst_allowance
        refill_rate = rule.max_requests / rule.window_seconds

        result = await self.redis_client.eval(
            lua_script, 1, key, capacity, refill_rate, weight, current_time
        )

        allowed = bool(result[0])
        current_tokens = int(result[1])

        if not allowed and rule.punishment_duration > 0:
            await self._apply_punishment(identifier, rule)

        return RateLimitResult(
            allowed=allowed,
            current_count=capacity - current_tokens,
            limit=rule.max_requests,
            remaining=current_tokens,
            reset_time=current_time + (capacity - current_tokens) / refill_rate
        )

    async def _check_fixed_window(self, identifier: str, rule: RateLimitRule,
                                  weight: int) -> RateLimitResult:
        """固定窗口算法"""
        if not self.redis_client:
            return await self._check_local_cache(identifier, rule, weight)

        # 计算窗口
        current_time = time.time()
        window = int(current_time // rule.window_seconds)
        key = f"{rule.get_key_prefix()}:fixed:{identifier}:{window}"

        # 原子性增加计数
        pipe = self.redis_client.pipeline()
        pipe.incr(key)
        pipe.expire(key, rule.window_seconds)
        results = await pipe.execute()

        current_count = results[0]
        effective_limit = rule.max_requests + rule.burst_allowance
        allowed = current_count <= effective_limit

        if not allowed and rule.punishment_duration > 0:
            await self._apply_punishment(identifier, rule)

        window_end = (window + 1) * rule.window_seconds

        return RateLimitResult(
            allowed=allowed,
            current_count=current_count,
            limit=rule.max_requests,
            remaining=max(0, effective_limit - current_count),
            reset_time=window_end
        )

    async def _check_local_cache(self, identifier: str, rule: RateLimitRule,
                                 weight: int) -> RateLimitResult:
        """本地缓存检查（Redis不可用时的回退）"""
        current_time = time.time()
        cache_key = f"{rule.name}:{identifier}"

        if cache_key in self.local_cache:
            cache_data = self.local_cache[cache_key]
            if current_time - cache_data['timestamp'] > rule.window_seconds:
                # 窗口重置
                cache_data = {'count': 0, 'timestamp': current_time}
        else:
            cache_data = {'count': 0, 'timestamp': current_time}

        cache_data['count'] += weight
        self.local_cache[cache_key] = cache_data

        allowed = cache_data['count'] <= rule.max_requests + rule.burst_allowance

        return RateLimitResult(
            allowed=allowed,
            current_count=cache_data['count'],
            limit=rule.max_requests,
            remaining=max(0, rule.max_requests - cache_data['count']),
            reset_time=cache_data['timestamp'] + rule.window_seconds
        )

    async def _apply_punishment(self, identifier: str, rule: RateLimitRule):
        """应用惩罚"""
        if rule.punishment_duration <= 0:
            return

        punishment_key = f"{rule.get_key_prefix()}:punishment:{identifier}"
        punishment_end = time.time() + rule.punishment_duration

        if self.redis_client:
            await self.redis_client.setex(
                punishment_key, rule.punishment_duration, str(punishment_end)
            )

        self.logger.warning(
            f"Applied punishment to {identifier} for rule {rule.name}, "
            f"duration: {rule.punishment_duration}s"
        )

    async def get_current_usage(self, identifier: str, rule_name: str) -> Dict[str, Any]:
        """获取当前使用情况"""
        if rule_name not in self.rules:
            return {}

        rule = self.rules[rule_name]

        if rule.limit_type == LimitType.SLIDING_WINDOW:
            key = f"{rule.get_key_prefix()}:sliding:{identifier}"
            if self.redis_client:
                current_time = time.time()
                window_start = current_time - rule.window_seconds
                count = await self.redis_client.zcount(key, window_start, current_time)
                return {
                    "current_count": count,
                    "limit": rule.max_requests,
                    "window_seconds": rule.window_seconds,
                    "type": "sliding_window"
                }

        return {}

    async def whitelist_user(self, identifier: str, duration: int = 3600):
        """将用户加入白名单"""
        whitelist_key = f"rate_limit:whitelist:{identifier}"
        if self.redis_client:
            await self.redis_client.setex(whitelist_key, duration, "1")
        self.logger.info(f"Added {identifier} to whitelist for {duration}s")

    async def is_whitelisted(self, identifier: str) -> bool:
        """检查是否在白名单"""
        if not self.redis_client:
            return False

        whitelist_key = f"rate_limit:whitelist:{identifier}"
        return bool(await self.redis_client.get(whitelist_key))

    async def get_stats(self) -> Dict[str, Any]:
        """获取速率限制统计信息"""
        stats = {
            "rules_count": len(self.rules),
            "enabled_rules": len([r for r in self.rules.values() if r.enabled]),
            "local_cache_size": len(self.local_cache),
            "rules": {}
        }

        for rule_name, rule in self.rules.items():
            stats["rules"][rule_name] = {
                "enabled": rule.enabled,
                "limit_type": rule.limit_type.value,
                "max_requests": rule.max_requests,
                "window_seconds": rule.window_seconds,
                "action_types": [at.value for at in rule.action_types],
                "user_groups": rule.user_groups
            }

        return stats


# 全局速率限制器实例
_rate_limiter: Optional[AdvancedRateLimiter] = None


async def get_rate_limiter() -> AdvancedRateLimiter:
    """获取全局速率限制器"""
    global _rate_limiter
    if _rate_limiter is None:
        # 尝试连接Redis
        redis_client = None
        try:
            redis_url = getattr(settings, 'REDIS_URL', 'redis://localhost:6379')
            redis_client = redis.from_url(redis_url)
            await redis_client.ping()
        except Exception as e:
            logger.warning(f"Redis not available, using local cache: {e}")
            redis_client = None

        _rate_limiter = AdvancedRateLimiter(redis_client)

    return _rate_limiter


async def check_user_rate_limit(user_id: int, action_type: ActionType,
                                user_group: str = "normal", weight: int = 1) -> RateLimitResult:
    """检查用户速率限制的便利函数"""
    limiter = await get_rate_limiter()

    # 先检查白名单
    if await limiter.is_whitelisted(f"user:{user_id}"):
        return RateLimitResult(
            allowed=True,
            current_count=0,
            limit=float('inf'),
            remaining=float('inf'),
            reset_time=time.time()
        )

    return await limiter.check_rate_limit(
        f"user:{user_id}", action_type, user_group, weight
    )


async def check_ip_rate_limit(ip_address: str, action_type: ActionType,
                              weight: int = 1) -> RateLimitResult:
    """检查IP速率限制的便利函数"""
    limiter = await get_rate_limiter()

    # 先检查白名单
    if await limiter.is_whitelisted(f"ip:{ip_address}"):
        return RateLimitResult(
            allowed=True,
            current_count=0,
            limit=float('inf'),
            remaining=float('inf'),
            reset_time=time.time()
        )

    return await limiter.check_rate_limit(
        f"ip:{ip_address}", action_type, "normal", weight
    )