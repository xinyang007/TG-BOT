import time
import asyncio
from typing import Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict
import json
import logging

from .logging_config import get_logger

logger = get_logger("app.cache")


@dataclass
class CacheEntry:
    """缓存条目"""
    value: Any
    timestamp: float
    ttl: int  # 生存时间（秒）
    access_count: int = 0
    last_access: float = 0.0

    def is_expired(self) -> bool:
        """检查是否过期"""
        if self.ttl <= 0:  # 永久缓存
            return False
        return time.time() - self.timestamp > self.ttl

    def access(self) -> Any:
        """访问缓存项，更新访问统计"""
        self.access_count += 1
        self.last_access = time.time()
        return self.value


class MemoryCache:
    """内存缓存实现"""

    def __init__(self, default_ttl: int = 300, max_entries: int = 10000):
        self.default_ttl = default_ttl
        self.max_entries = max_entries
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        self._stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
            "evictions": 0
        }

    async def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        async with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._stats["misses"] += 1
                logger.debug(f"Cache miss: {key}")
                return None

            if entry.is_expired():
                del self._cache[key]
                self._stats["misses"] += 1
                self._stats["evictions"] += 1
                logger.debug(f"Cache expired: {key}")
                return None

            self._stats["hits"] += 1
            logger.debug(f"Cache hit: {key}")
            return entry.access()

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存值"""
        if ttl is None:
            ttl = self.default_ttl

        async with self._lock:
            # 检查是否需要清理空间
            if len(self._cache) >= self.max_entries:
                await self._evict_lru()

            entry = CacheEntry(
                value=value,
                timestamp=time.time(),
                ttl=ttl,
                last_access=time.time()
            )

            self._cache[key] = entry
            self._stats["sets"] += 1
            logger.debug(f"Cache set: {key}, TTL: {ttl}")

    async def delete(self, key: str) -> bool:
        """删除缓存项"""
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._stats["deletes"] += 1
                logger.debug(f"Cache delete: {key}")
                return True
            return False

    async def clear(self) -> None:
        """清空缓存"""
        async with self._lock:
            cleared_count = len(self._cache)
            self._cache.clear()
            logger.info(f"Cache cleared: {cleared_count} entries")

    async def _evict_lru(self) -> None:
        """清理最少使用的条目"""
        if not self._cache:
            return

        # 找到最少使用的条目
        lru_key = min(
            self._cache.keys(),
            key=lambda k: (self._cache[k].access_count, self._cache[k].last_access)
        )

        del self._cache[lru_key]
        self._stats["evictions"] += 1
        logger.debug(f"Cache LRU eviction: {lru_key}")

    async def cleanup_expired(self) -> int:
        """清理过期条目"""
        async with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.is_expired()
            ]

            for key in expired_keys:
                del self._cache[key]

            if expired_keys:
                self._stats["evictions"] += len(expired_keys)
                logger.info(f"Cache cleanup: {len(expired_keys)} expired entries")

            return len(expired_keys)

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total_requests = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total_requests if total_requests > 0 else 0

        return {
            **self._stats,
            "hit_rate": round(hit_rate, 3),
            "total_requests": total_requests,
            "cache_size": len(self._cache),
            "max_entries": self.max_entries
        }

    async def get_detailed_stats(self) -> Dict[str, Any]:
        """获取详细的缓存统计信息"""
        async with self._lock:
            current_time = time.time()

            # 按TTL分组统计
            ttl_groups = defaultdict(int)
            expired_count = 0

            for entry in self._cache.values():
                if entry.is_expired():
                    expired_count += 1
                else:
                    ttl_groups[entry.ttl] += 1

            return {
                **self.get_stats(),
                "expired_entries": expired_count,
                "ttl_distribution": dict(ttl_groups),
                "memory_usage_estimate": self._estimate_memory_usage()
            }

    def _estimate_memory_usage(self) -> int:
        """估算内存使用量（字节）"""
        try:
            # 简单估算，实际情况可能更复杂
            total_size = 0
            for key, entry in self._cache.items():
                key_size = len(key.encode('utf-8'))
                value_size = len(str(entry.value).encode('utf-8'))
                entry_overhead = 100  # 估算的对象开销
                total_size += key_size + value_size + entry_overhead
            return total_size
        except Exception:
            return -1  # 估算失败


class ConversationCache:
    """专门用于对话相关数据的缓存"""

    def __init__(self, cache: MemoryCache):
        self.cache = cache
        self.logger = get_logger("app.cache.conversation")

    async def get_user_ban_status(self, user_id: int) -> Optional[bool]:
        """获取用户拉黑状态"""
        key = f"user_banned:{user_id}"
        return await self.cache.get(key)

    async def set_user_ban_status(self, user_id: int, is_banned: bool, ttl: int = 300):
        """设置用户拉黑状态"""
        key = f"user_banned:{user_id}"
        await self.cache.set(key, is_banned, ttl)
        self.logger.debug(f"Cached user ban status: {user_id} = {is_banned}")

    async def get_conversation_by_entity(self, entity_id: int, entity_type: str) -> Optional[Dict[str, Any]]:
        """获取实体对话信息"""
        key = f"conv_entity:{entity_type}:{entity_id}"
        return await self.cache.get(key)

    async def set_conversation_by_entity(self, entity_id: int, entity_type: str,
                                         conv_data: Dict[str, Any], ttl: int = 600):
        """设置实体对话信息"""
        key = f"conv_entity:{entity_type}:{entity_id}"
        await self.cache.set(key, conv_data, ttl)
        self.logger.debug(f"Cached conversation for {entity_type}:{entity_id}")

    async def get_conversation_by_topic(self, topic_id: int) -> Optional[Dict[str, Any]]:
        """获取话题对话信息"""
        key = f"conv_topic:{topic_id}"
        return await self.cache.get(key)

    async def set_conversation_by_topic(self, topic_id: int, conv_data: Dict[str, Any], ttl: int = 600):
        """设置话题对话信息"""
        key = f"conv_topic:{topic_id}"
        await self.cache.set(key, conv_data, ttl)
        self.logger.debug(f"Cached conversation for topic:{topic_id}")

    async def invalidate_conversation(self, entity_id: int, entity_type: str, topic_id: Optional[int] = None):
        """使对话缓存失效"""
        await self.cache.delete(f"conv_entity:{entity_type}:{entity_id}")
        if topic_id:
            await self.cache.delete(f"conv_topic:{topic_id}")
        self.logger.debug(f"Invalidated conversation cache for {entity_type}:{entity_id}")

    async def get_binding_id(self, custom_id: str) -> Optional[Dict[str, Any]]:
        """获取绑定ID信息"""
        key = f"binding_id:{custom_id}"
        return await self.cache.get(key)

    async def set_binding_id(self, custom_id: str, binding_data: Dict[str, Any], ttl: int = 1800):
        """设置绑定ID信息"""
        key = f"binding_id:{custom_id}"
        await self.cache.set(key, binding_data, ttl)
        self.logger.debug(f"Cached binding ID: {custom_id}")

    async def invalidate_binding_id(self, custom_id: str):
        """使绑定ID缓存失效"""
        await self.cache.delete(f"binding_id:{custom_id}")
        self.logger.debug(f"Invalidated binding ID cache: {custom_id}")


class RateLimitCache:
    """速率限制缓存"""

    def __init__(self, cache: MemoryCache):
        self.cache = cache
        self.logger = get_logger("app.cache.rate_limit")

    async def check_rate_limit(self, identifier: str, max_requests: int, window_seconds: int) -> Tuple[bool, int]:
        """
        检查速率限制
        返回: (是否允许, 当前请求数)
        """
        key = f"rate_limit:{identifier}"
        current_time = time.time()
        window_start = current_time - window_seconds

        # 获取当前请求记录
        requests = await self.cache.get(key) or []

        # 过滤掉窗口外的请求
        valid_requests = [req_time for req_time in requests if req_time > window_start]

        # 检查是否超过限制
        if len(valid_requests) >= max_requests:
            self.logger.warning(f"Rate limit exceeded for {identifier}: {len(valid_requests)}/{max_requests}")
            return False, len(valid_requests)

        # 添加当前请求
        valid_requests.append(current_time)

        # 更新缓存
        await self.cache.set(key, valid_requests, window_seconds + 10)  # 稍微长一点的TTL

        self.logger.debug(f"Rate limit check for {identifier}: {len(valid_requests)}/{max_requests}")
        return True, len(valid_requests)


class CacheManager:
    """缓存管理器"""

    def __init__(self, default_ttl: int = 300, max_entries: int = 10000):
        self.memory_cache = MemoryCache(default_ttl, max_entries)
        self.conversation_cache = ConversationCache(self.memory_cache)
        self.rate_limit_cache = RateLimitCache(self.memory_cache)
        self.logger = get_logger("app.cache.manager")
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start_cleanup_task(self, interval: int = 300):
        """启动清理任务"""
        if self._cleanup_task and not self._cleanup_task.done():
            return

        self._cleanup_task = asyncio.create_task(self._periodic_cleanup(interval))
        self.logger.info(f"Started cache cleanup task with {interval}s interval")

    async def stop_cleanup_task(self):
        """停止清理任务"""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Stopped cache cleanup task")

    async def _periodic_cleanup(self, interval: int):
        """定期清理过期条目"""
        while True:
            try:
                await asyncio.sleep(interval)
                expired_count = await self.memory_cache.cleanup_expired()
                if expired_count > 0:
                    self.logger.info(f"Periodic cleanup removed {expired_count} expired entries")
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {e}", exc_info=True)

    async def get_stats(self) -> Dict[str, Any]:
        """获取所有缓存统计信息"""
        return await self.memory_cache.get_detailed_stats()

    async def clear_all(self):
        """清空所有缓存"""
        await self.memory_cache.clear()
        self.logger.info("Cleared all cache")


# 全局缓存管理器实例
_cache_manager: Optional[CacheManager] = None


def get_cache_manager() -> CacheManager:
    """获取全局缓存管理器"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager()
    return _cache_manager


async def init_cache_manager(default_ttl: int = 300, max_entries: int = 10000):
    """初始化缓存管理器"""
    global _cache_manager
    _cache_manager = CacheManager(default_ttl, max_entries)
    await _cache_manager.start_cleanup_task()
    logger.info("Cache manager initialized")


async def cleanup_cache_manager():
    """清理缓存管理器"""
    global _cache_manager
    if _cache_manager:
        await _cache_manager.stop_cleanup_task()
        await _cache_manager.clear_all()
    logger.info("Cache manager cleaned up")