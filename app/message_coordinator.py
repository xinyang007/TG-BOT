# app/message_coordinator.py

import asyncio
import json
import time
import uuid
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import time
from dataclasses import dataclass

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from .logging_config import get_logger
from .settings import settings
from .message_processor import MessageProcessor, ProcessingResult  # 新增导入

logger = get_logger("app.message_coordinator")


def extract_member_from_zscan_result(item):
    """从 zscan_iter 结果中提取 member 字符串"""
    if isinstance(item, tuple):
        return item[0]  # (member, score) 格式
    else:
        return item  # 直接是 member 字符串


class MessagePriority(Enum):
    """消息优先级"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


class LockType(Enum):
    """锁类型"""
    MESSAGE_PROCESSING = "msg_proc"
    BOT_SELECTION = "bot_sel"
    HEALTH_CHECK = "health_chk"


@dataclass
class QueuedMessage:
    """队列中的消息"""
    message_id: str
    update_id: int
    chat_id: int
    user_id: Optional[int]
    chat_type: str
    priority: MessagePriority
    payload: Dict[str, Any]
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0
    assigned_bot_id: Optional[str] = None
    processing_deadline: Optional[float] = None  # 用于记录处理超时时间

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "message_id": self.message_id,
            "update_id": self.update_id,
            "chat_id": self.chat_id,
            "user_id": self.user_id,
            "chat_type": self.chat_type,
            "priority": self.priority.value,
            "payload": self.payload,
            "created_at": self.created_at,
            "retry_count": self.retry_count,
            "assigned_bot_id": self.assigned_bot_id,
            "processing_deadline": self.processing_deadline
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueuedMessage':
        """从字典创建"""
        return cls(
            message_id=data["message_id"],
            update_id=data["update_id"],
            chat_id=data["chat_id"],
            user_id=data.get("user_id"),
            chat_type=data["chat_type"],
            priority=MessagePriority(data["priority"]),
            payload=data["payload"],
            created_at=data.get("created_at", time.time()),
            retry_count=data.get("retry_count", 0),
            assigned_bot_id=data.get("assigned_bot_id"),
            processing_deadline=data.get("processing_deadline")
        )

    def is_expired(self, max_age_seconds: int = 300) -> bool:
        """检查消息是否过期"""
        return time.time() - self.created_at > max_age_seconds

    def should_retry(self, max_retries: int = 3) -> bool:
        """检查是否应该重试"""
        return self.retry_count < max_retries


@dataclass
class SessionInfo:
    """会话信息"""
    bot_id: str
    last_activity: float
    message_count: int = 0


class DistributedLock:
    """分布式锁"""

    def __init__(self, redis_client: redis.Redis, lock_key: str, timeout: int = 30):
        self.redis_client = redis_client
        self.lock_key = f"lock:{lock_key}"
        self.timeout = timeout
        self.lock_value = str(uuid.uuid4())
        self.logger = get_logger(f"app.lock.{lock_key}")

    async def acquire(self) -> bool:
        """获取锁"""
        try:
            # 使用 SET NX EX 原子操作
            result = await self.redis_client.set(
                self.lock_key, self.lock_value, ex=self.timeout, nx=True
            )
            if result:
                self.logger.debug(f"成功获取锁: {self.lock_key}")
                return True
            else:
                self.logger.debug(f"锁已被占用: {self.lock_key}")
                return False
        except Exception as e:
            self.logger.error(f"获取锁失败: {e}")
            return False

    async def release(self) -> bool:
        """释放锁"""
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            result = await self.redis_client.eval(lua_script, 1, self.lock_key, self.lock_value)
            if result == 1:
                self.logger.debug(f"成功释放锁: {self.lock_key}")
                return True
            else:
                self.logger.warning(f"锁值不匹配或已过期: {self.lock_key}")
                return False
        except Exception as e:
            self.logger.error(f"释放锁失败: {e}")
            return False

    async def extend(self, additional_time: int = 30) -> bool:
        """延长锁的持有时间"""
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        try:
            result = await self.redis_client.eval(
                lua_script, 1, self.lock_key, self.lock_value, str(additional_time)
            )
            return result == 1
        except Exception as e:
            self.logger.error(f"延长锁失败: {e}")
            return False

    async def __aenter__(self):
        """异步上下文管理器入口"""
        success = await self.acquire()
        if not success:
            raise Exception(f"无法获取锁: {self.lock_key}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.release()


class LoadBalancer:
    """智能负载均衡器 - 考虑私聊连续性和群聊平均分配"""

    def __init__(self, bot_manager):
        self.bot_manager = bot_manager
        self.logger = get_logger("app.load_balancer")

        # 会话亲和性：维护用户/群聊与机器人的映射关系
        self._private_sessions: Dict[int, Dict[str, Any]] = {}  # user_id -> session_dict
        self._group_sessions: Dict[int, Dict[str, Any]] = {}  # chat_id -> session_dict

        # 群聊连续使用控制
        self._group_consecutive_count = 0  # 当前机器人连续使用次数
        self._current_group_bot = None  # 当前群聊使用的机器人
        self._max_consecutive = 5  # 最大连续使用次数

        # 机器人使用统计
        self._bot_stats = {}  # bot_id -> {"total_messages": int, "last_used": float}

        # 会话超时设置（30分钟）
        self._session_timeout = 30 * 60

        # 健康度权重配置
        self._health_weights = {
            "healthy": 1.0,
            "available": 0.8,
            "recovering": 0.6,
            "error": 0.3
        }

    async def select_best_bot(self, queued_msg: QueuedMessage) -> Optional[str]:
        """智能选择最佳机器人"""
        available_bots = self.bot_manager.get_available_bots()
        if not available_bots:
            self.logger.warning(f"没有可用的机器人 - 消息: {queued_msg.message_id}, "
                                f"类型: {queued_msg.chat_type}, 时间: {time.time():.3f}")
            return None

        # 清理过期会话
        self._cleanup_expired_sessions()

        # 如果消息已分配给特定机器人且该机器人仍可用
        if queued_msg.assigned_bot_id:
            assigned_bot = self.bot_manager.get_bot_by_id(queued_msg.assigned_bot_id)
            if assigned_bot and assigned_bot.is_available():
                self.logger.debug(f"消息 {queued_msg.message_id} 沿用已分配机器人 {assigned_bot.bot_id}")
                return queued_msg.assigned_bot_id

        # 根据聊天类型选择策略
        if queued_msg.chat_type == "private":
            return await self._select_for_private_chat(queued_msg, available_bots)
        else:
            return await self._select_for_group_chat(queued_msg, available_bots)

    async def _select_for_private_chat(self, queued_msg: QueuedMessage, available_bots: List) -> Optional[str]:
        """私聊选择策略：优先保持会话连续性，新用户优先主机器人"""
        user_id = queued_msg.user_id
        current_time = time.time()

        # 检查是否存在活跃的私聊会话
        if user_id in self._private_sessions:
            session = self._private_sessions[user_id]
            mapped_bot = self.bot_manager.get_bot_by_id(session['bot_id'])

            # 检查会话是否超时
            if current_time - session['last_activity'] > self._session_timeout:
                self.logger.info(f"用户 {user_id} 的会话已超时，清理会话")
                del self._private_sessions[user_id]
            elif mapped_bot and mapped_bot.is_available():
                # 继续使用原机器人
                session['last_activity'] = current_time
                session['message_count'] += 1

                self.logger.info(f"✅ 私聊用户 {user_id} 继续使用机器人 {session['bot_id']} "
                                 f"({mapped_bot.config.name}) - 会话消息数: {session['message_count']}")

                await self._update_bot_stats(session['bot_id'])
                return session['bot_id']
            else:
                # 原机器人不可用，清理会话
                self.logger.warning(f"用户 {user_id} 的原机器人 {session['bot_id']} 不可用，需重新分配")
                del self._private_sessions[user_id]

        # 🔥 新用户分配策略：优先选择主机器人（优先级1）
        primary_bot = None
        for bot in available_bots:
            if bot.config.priority == 1:  # 主机器人通常优先级为1
                primary_bot = bot
                break

        # 如果主机器人可用，优先分配给新用户
        if primary_bot and primary_bot.is_available():
            selected_bot = primary_bot
            self.logger.info(
                f"🎯 为新用户 {user_id} 优先分配主机器人 {selected_bot.bot_id} ({selected_bot.config.name})")
        else:
            # 主机器人不可用，使用智能策略选择备用机器人
            selected_bot = self._select_by_smart_strategy(available_bots, queued_msg)
            if selected_bot:
                self.logger.warning(
                    f"⚠️ 主机器人不可用，为用户 {user_id} 分配备用机器人 {selected_bot.bot_id} ({selected_bot.config.name})")

        if selected_bot:
            # 建立新会话
            self._private_sessions[user_id] = {
                'bot_id': selected_bot.bot_id,
                'last_activity': current_time,
                'message_count': 1
            }

            self.logger.info(f"✅ 为用户 {user_id} 建立新会话: {selected_bot.config.name} ({selected_bot.bot_id})")
            await self._update_bot_stats(selected_bot.bot_id)
            return selected_bot.bot_id

        return None

    async def _select_for_group_chat(self, queued_msg: QueuedMessage, available_bots: List) -> Optional[str]:
        """群聊选择策略：根据配置策略智能分配"""
        strategy = getattr(settings, 'BOT_SELECTION_STRATEGY', 'balanced')

        if strategy == "balanced":
            return await self._select_balanced_for_group(queued_msg, available_bots)
        elif strategy == "health_priority":
            return await self._select_by_health_priority(available_bots)
        elif strategy == "load_based":
            return await self._select_by_load_priority(queued_msg, available_bots)
        else:
            # 默认使用平衡策略
            return await self._select_balanced_for_group(queued_msg, available_bots)

    async def _select_balanced_for_group(self, queued_msg: QueuedMessage, available_bots: List) -> Optional[str]:
        """群聊平衡选择：连续5次同一机器人后切换"""
        # 检查当前机器人是否可以继续使用
        if (self._current_group_bot and
                self._group_consecutive_count < self._max_consecutive):

            current_bot = self.bot_manager.get_bot_by_id(self._current_group_bot)
            if current_bot and current_bot.is_available():
                # 继续使用当前机器人
                self._group_consecutive_count += 1

                self.logger.debug(f"群聊继续使用机器人 {self._current_group_bot} "
                                  f"(连续第 {self._group_consecutive_count} 次)")

                await self._update_bot_stats(self._current_group_bot)
                return self._current_group_bot

        # 需要切换机器人
        other_bots = [bot for bot in available_bots
                      if bot.bot_id != self._current_group_bot]

        if not other_bots:
            # 如果只有一个机器人，重置计数继续使用
            if available_bots:
                selected_bot = available_bots[0]
                self._current_group_bot = selected_bot.bot_id
                self._group_consecutive_count = 1

                self.logger.info(f"只有一个可用机器人，重置计数继续使用 {selected_bot.bot_id}")
                await self._update_bot_stats(selected_bot.bot_id)
                return selected_bot.bot_id
            return None

        # 从其他机器人中选择最佳的
        selected_bot = self._select_by_smart_strategy(other_bots, queued_msg)
        if selected_bot:
            old_bot = self._current_group_bot
            self._current_group_bot = selected_bot.bot_id
            self._group_consecutive_count = 1

            self.logger.info(f"群聊切换机器人：{old_bot} -> {selected_bot.bot_id}")
            await self._update_bot_stats(selected_bot.bot_id)
            return selected_bot.bot_id

        return None

    def _select_by_smart_strategy(self, available_bots: List, queued_msg: QueuedMessage):
        """智能策略选择：综合考虑健康度、负载和优先级"""
        if not available_bots:
            return None

        best_bot = None
        best_score = float('-inf')

        for bot in available_bots:
            # 计算综合评分
            health_score = self._calculate_health_score(bot) * 0.5
            load_score = self._calculate_load_score(bot) * 0.3
            priority_score = self._calculate_priority_score(bot, queued_msg) * 0.2

            total_score = health_score + load_score + priority_score

            self.logger.debug(f"机器人 {bot.bot_id} 评分: "
                              f"健康度={health_score:.2f}, 负载={load_score:.2f}, "
                              f"优先级={priority_score:.2f}, 总分={total_score:.2f}")

            if total_score > best_score:
                best_score = total_score
                best_bot = bot

        if best_bot:
            self.logger.info(f"选择机器人 {best_bot.bot_id} (评分: {best_score:.2f})")

        return best_bot

    async def _select_by_health_priority(self, available_bots: List) -> Optional[str]:
        """健康度优先选择"""
        if not available_bots:
            return None

        sorted_bots = sorted(available_bots,
                             key=lambda b: self._calculate_health_score(b),
                             reverse=True)

        selected_bot = sorted_bots[0]
        self.logger.info(f"基于健康度选择机器人 {selected_bot.bot_id}")

        await self._update_bot_stats(selected_bot.bot_id)
        return selected_bot.bot_id

    async def _select_by_load_priority(self, queued_msg: QueuedMessage, available_bots: List) -> Optional[str]:
        """负载优先选择"""
        if not available_bots:
            return None

        sorted_bots = sorted(available_bots,
                             key=lambda b: b.get_load_score(),
                             reverse=False)  # 分数越低越好，所以不需要反转

        selected_bot = sorted_bots[0]
        self.logger.info(f"基于负载选择机器人 {selected_bot.bot_id}")

        await self._update_bot_stats(selected_bot.bot_id)
        return selected_bot.bot_id

    def _calculate_health_score(self, bot) -> float:
        """计算健康度评分（0-1）"""
        if not hasattr(bot, 'status'):
            return 0.5

        status_str = bot.status.value if hasattr(bot.status, 'value') else str(bot.status)
        return self._health_weights.get(status_str.lower(), 0.5)

    def _calculate_load_score(self, bot) -> float:
        """计算负载评分（0-1，越低负载评分越高）"""
        try:
            load_score = bot.get_load_score()
            normalized_load = min(load_score / 100.0, 1.0)
            return 1.0 - normalized_load
        except:
            return 0.5

    def _calculate_priority_score(self, bot, queued_msg: QueuedMessage) -> float:
        """计算优先级评分（0-1）"""
        base_score = 0.5

        # 机器人配置优先级
        if hasattr(bot, 'config') and hasattr(bot.config, 'priority'):
            priority_score = max(0.2, 1.0 - (bot.config.priority - 1) * 0.2)
            base_score = priority_score

        # 消息优先级加成
        if hasattr(queued_msg, 'priority'):
            priority_value = queued_msg.priority.value if hasattr(queued_msg.priority, 'value') else queued_msg.priority
            if priority_value >= 3:
                base_score += 0.1

        # 管理员消息加成
        if (hasattr(queued_msg, 'user_id') and queued_msg.user_id and
                queued_msg.user_id in getattr(settings, 'ADMIN_USER_IDS', [])):
            base_score += 0.1

        return min(1.0, base_score)

    def calculate_message_weight(self, queued_msg: QueuedMessage) -> int:
        """计算消息权重（向后兼容方法）"""
        weight = 1
        priority_weights = {
            MessagePriority.LOW: 1,
            MessagePriority.NORMAL: 2,
            MessagePriority.HIGH: 3,
            MessagePriority.URGENT: 5
        }
        weight *= priority_weights.get(queued_msg.priority, 1)

        if queued_msg.user_id and queued_msg.user_id in getattr(settings, 'ADMIN_USER_IDS', []):
            weight *= 2

        return weight

    async def _update_bot_stats(self, bot_id: str):
        """更新机器人使用统计"""
        current_time = time.time()

        if bot_id not in self._bot_stats:
            self._bot_stats[bot_id] = {"total_messages": 0, "last_used": current_time}

        self._bot_stats[bot_id]["total_messages"] += 1
        self._bot_stats[bot_id]["last_used"] = current_time

    def _cleanup_expired_sessions(self):
        """清理过期的会话"""
        current_time = time.time()

        expired_users = []
        for user_id, session in self._private_sessions.items():
            if current_time - session['last_activity'] > self._session_timeout:
                expired_users.append(user_id)

        for user_id in expired_users:
            del self._private_sessions[user_id]
            self.logger.debug(f"清理用户 {user_id} 的过期会话")

    def get_session_info(self) -> Dict[str, Any]:
        """获取会话信息统计"""
        return {
            "active_private_sessions": len(self._private_sessions),
            "current_group_bot": self._current_group_bot,
            "group_consecutive_count": self._group_consecutive_count,
            "bot_stats": self._bot_stats.copy(),
            "private_sessions_detail": {
                str(user_id): {
                    "bot_id": session['bot_id'],
                    "message_count": session['message_count'],
                    "duration_minutes": (time.time() - session['last_activity']) / 60
                }
                for user_id, session in self._private_sessions.items()
            }
        }

    def reset_stats(self):
        """重置统计信息（保留会话）"""
        self._bot_stats.clear()
        self._group_consecutive_count = 0
        self.logger.info("负载均衡统计已重置")

    def force_switch_group_bot(self):
        """强制切换群聊机器人（用于测试）"""
        self._group_consecutive_count = self._max_consecutive
        self.logger.info("强制触发群聊机器人切换")

    def clear_user_session(self, user_id: int):
        """清除指定用户的会话（用于调试）"""
        if user_id in self._private_sessions:
            old_bot = self._private_sessions[user_id]['bot_id']
            del self._private_sessions[user_id]
            self.logger.info(f"清除用户 {user_id} 与机器人 {old_bot} 的会话")
            return True
        return False

    def get_assignment_stats(self) -> Dict[str, Any]:
        """获取分配统计（兼容性方法）"""
        session_info = self.get_session_info()

        bot_assignment_count = {}
        for user_id, session in self._private_sessions.items():
            bot_id = session['bot_id']
            bot_assignment_count[bot_id] = bot_assignment_count.get(bot_id, 0) + session['message_count']

        if self._current_group_bot:
            bot_assignment_count[self._current_group_bot] = (
                    bot_assignment_count.get(self._current_group_bot, 0) + self._group_consecutive_count
            )

        return {
            "session_info": session_info,
            "bot_assignment_count": bot_assignment_count,
            "total_assigned": sum(bot_assignment_count.values()),
            "distribution": self._calculate_distribution(bot_assignment_count)
        }

    def _calculate_distribution(self, assignment_count: Dict[str, int]) -> Dict[str, Any]:
        """计算分配分布统计"""
        if not assignment_count:
            return {"balanced": True, "variance": 0.0}

        counts = list(assignment_count.values())
        if not counts:
            return {"balanced": True, "variance": 0.0}

        mean_count = sum(counts) / len(counts)
        variance = sum((count - mean_count) ** 2 for count in counts) / len(counts)

        is_balanced = variance < 5.0

        return {
            "balanced": is_balanced,
            "variance": round(variance, 2),
            "mean": round(mean_count, 2),
            "counts": counts
        }


class MessageQueue:
    """消息队列管理器"""

    def __init__(self, redis_client: Optional['redis.Redis'] = None):
        self.redis_client = redis_client
        self.instance_id = str(uuid.uuid4())[:8]
        self.logger = get_logger("app.message_queue")

        # 队列名称
        self.pending_queue = "mq:pending"  # 有序集合，按分数（优先级+时间戳）排序
        self.processing_queue = "mq:processing"  # 有序集合，存储正在处理的消息及其开始时间
        self.failed_queue = "mq:failed"  # 有序集合，存储处理失败的消息（待重试）
        self.dead_letter_queue = "mq:dead_letter"  # 有序集合，存储已达最大重试次数的消息

    async def enqueue(self, queued_msg: QueuedMessage, priority_boost: bool = False) -> bool:
        """将消息添加到队列"""
        if not self.redis_client:
            self.logger.error("Redis客户端未初始化")
            return False

        try:
            # 序列化消息
            message_data = json.dumps(queued_msg.to_dict())

            # 根据优先级选择分数，同时加入时间戳确保在同优先级下按入队顺序
            # 优先级越高，分数越高，取消息时BZPOPMAX优先取出
            priority_score = queued_msg.priority.value
            if priority_boost:
                priority_score += 10  # 临时优先级提升

            # 分数 = 优先级 * 1000000000 + (max_timestamp_in_nano - current_timestamp_in_nano)
            # 这样可以在同优先级下，时间戳越小（越早入队）的，反而分数越高（因为是倒序），BZPOPMAX会先取
            # Redis的ZADD分数是双精度浮点数，需要注意精度问题。这里简单处理，确保优先级是主要因素
            # 使用一个大数作为基数，确保优先级区分度，同时考虑时间戳的倒序，让更早的消息先被处理
            score = (priority_score * 1_000_000_000) + (int(time.time() * 1_000_000) % 1_000_000_000)  # 时间戳后几位

            # 添加到有序集合
            await self.redis_client.zadd(self.pending_queue, {message_data: score})

            self.logger.info(f"消息 {queued_msg.message_id} 已加入队列，优先级: {queued_msg.priority.value}")
            return True

        except Exception as e:
            self.logger.error(f"消息入队失败: {e}", exc_info=True)
            return False

    async def dequeue(self, timeout: int = 1) -> Optional[QueuedMessage]:
        """从队列中取出消息"""
        if not self.redis_client:
            return None

        try:
            # 使用 BZPOPMAX 阻塞式获取最高优先级消息
            # 返回 (key, member, score)
            result = await self.redis_client.bzpopmax(self.pending_queue, timeout=timeout)

            if not result:
                return None

            _queue_name, message_data_str, _score = result
            message_dict = json.loads(message_data_str)
            queued_msg = QueuedMessage.from_dict(message_dict)

            # 将消息移动到处理队列
            # score 使用当前时间戳，用于清理超时消息
            await self.redis_client.zadd(
                self.processing_queue,
                {message_data_str: time.time()}  # 存储原始的message_data_str
            )

            self.logger.debug(f"从队列取出消息: {queued_msg.message_id}")
            return queued_msg

        except Exception as e:
            self.logger.error(f"消息出队失败: {e}", exc_info=True)
            return None

    async def mark_completed(self, message_id: str) -> bool:
        """标记消息处理完成"""
        if not self.redis_client:
            self.logger.warning(f"Redis客户端不可用，无法标记消息 {message_id} 完成")
            return False

        try:
            self.logger.debug(f"开始标记消息 {message_id} 为完成状态")
            # 从处理队列中移除消息
            # 遍历有序集合的成员，因为我们存储的是整个JSON字符串而不是message_id作为member
            members_to_remove = []
            processing_members = await self.redis_client.zrange(
                self.processing_queue, 0, -1, withscores=False
            )
            for member_str in processing_members:
                try:
                    if isinstance(member_str, bytes):
                        member_str = member_str.decode('utf-8')

                    data = json.loads(member_str)
                    if data.get("message_id") == message_id:
                        members_to_remove.append(member_str)
                        self.logger.debug(f"找到待移除的消息: {message_id}")
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    self.logger.warning(f"处理队列中发现无效数据: {str(e)}")
                    continue

            if members_to_remove:
                # 原子操作：移除所有匹配的消息
                removed_count = await self.redis_client.zrem(self.processing_queue, *members_to_remove)

                if removed_count > 0:
                    self.logger.info(f"✅ 消息 {message_id} 处理完成，已从处理队列移除 ({removed_count} 条记录)")
                    return True
                else:
                    self.logger.warning(f"⚠️ 消息 {message_id} 移除操作返回0，可能已被其他进程处理")
                    return False
            else:
                self.logger.warning(f"⚠️ 消息 {message_id} 未在处理队列中找到")
                # 检查是否在其他队列中
                await self._debug_message_location(
                    message_id)  # Consider removing or making this an actual check for other queues
                return False

        except Exception as e:
            self.logger.error(f"❌ 标记消息 {message_id} 完成失败: {e}", exc_info=True)
            return False

    async def mark_failed(self, message_id: str, error: str) -> bool:
        """标记消息处理失败"""
        if not self.redis_client:
            return False

        try:
            members_to_process = []
            # FIX: Iterate over the processing_queue, not dead_letter_queue
            async for item in self.redis_client.zscan_iter(
                    self.processing_queue):  # Changed from self.dead_letter_queue
                try:
                    member_str = extract_member_from_zscan_result(item)
                    data = json.loads(member_str)
                    if data.get("message_id") == message_id:
                        members_to_process.append(member_str)
                except json.JSONDecodeError:
                    self.logger.warning(f"处理队列中发现无效JSON成员：{member_str[:100]}")
                    continue

            if not members_to_process:
                # This can happen if cleanup_stale_messages already moved it.
                self.logger.warning(
                    f"尝试标记失败消息 {message_id} 但未在处理队列中找到。可能是已被清理或处理。")  # Added more context
                return False

            for member_str in members_to_process:
                # Remove the message from the processing queue
                removed_count = await self.redis_client.zrem(self.processing_queue, member_str)
                if removed_count == 0:
                    self.logger.warning(f"消息 {message_id} 在尝试从处理队列移除时返回0，可能已被其他进程处理或清理。")
                    continue  # Skip to next member_str if this one was already gone

                data = json.loads(member_str)
                # Add error information
                data["error"] = error
                data["failed_at"] = time.time()
                data["retry_count"] = data.get("retry_count", 0) + 1

                queued_msg = QueuedMessage.from_dict(data)

                if queued_msg.should_retry(max_retries=settings.MESSAGE_MAX_RETRIES):
                    # Re-enqueue into pending queue
                    await self.enqueue(queued_msg)
                    self.logger.info(f"消息 {message_id} 将重试，当前重试次数: {queued_msg.retry_count}")
                else:
                    # Move to dead-letter queue
                    await self.redis_client.zadd(
                        self.dead_letter_queue,
                        {json.dumps(data): time.time()}  # Use current time as score for DLQ
                    )
                    self.logger.warning(
                        f"消息 {message_id} 超过最大重试次数 ({settings.MESSAGE_MAX_RETRIES})，移至死信队列")
            return True  # If at least one message was processed

        except Exception as e:
            self.logger.error(f"标记消息失败失败: {e}", exc_info=True)
            return False

    async def cleanup_stale_messages(self, timeout_seconds: Optional[int] = None):
        """清理超时的处理中消息"""
        if not self.redis_client:
            return

        # 使用settings中的配置
        if timeout_seconds is None:
            timeout_seconds = settings.MESSAGE_PROCESSING_TIMEOUT

        try:
            current_time = time.time()
            cutoff_time = current_time - timeout_seconds

            # 获取超时的消息 (member 是 JSON 字符串，score 是处理开始时间)
            stale_members = await self.redis_client.zrangebyscore(
                self.processing_queue, 0, cutoff_time
            )

            if not stale_members:
                self.logger.debug("没有需要清理的超时消息。")
                return

            self.logger.info(f"清理 {len(stale_members)} 条超时处理中消息。")

            for member_str in stale_members:
                try:
                    data = json.loads(member_str)
                    message_id = data.get("message_id", "unknown")

                    # 移除超时消息
                    await self.redis_client.zrem(self.processing_queue, member_str)

                    # 重新加入待处理队列，并增加重试次数
                    data["retry_count"] = data.get("retry_count", 0) + 1
                    data["error"] = "Processing timeout"
                    data["failed_at"] = current_time
                    queued_msg = QueuedMessage.from_dict(data)

                    if queued_msg.should_retry(max_retries=settings.MESSAGE_MAX_RETRIES):
                        await self.enqueue(queued_msg)
                        self.logger.warning(
                            f"超时消息 {message_id} (重试 {queued_msg.retry_count}/{settings.MESSAGE_MAX_RETRIES}) 重新加入队列。")
                    else:
                        await self.redis_client.zadd(
                            self.dead_letter_queue,
                            {json.dumps(data): current_time}
                        )
                        self.logger.error(f"超时消息 {message_id} 超过最大重试次数，移至死信队列。")

                except json.JSONDecodeError:
                    self.logger.error(f"清理超时消息时发现无效JSON成员：{member_str[:100]}")
                    continue

        except Exception as e:
            self.logger.error(f"清理超时消息失败: {e}", exc_info=True)

    async def retry_message_from_dlq(self, message_id: str) -> bool:
        """
        从死信队列中查找并重试指定消息。
        如果找到，将其移回 pending 队列，并重置 retry_count。
        """
        if not self.redis_client:
            self.logger.error("Redis客户端未初始化，无法重试死信队列消息。")
            return False

        try:
            dlq_members_to_process = []
            async for member_str in self.redis_client.zscan_iter(self.dead_letter_queue):
                try:
                    data = json.loads(member_str)
                    if data.get("message_id") == message_id:
                        dlq_members_to_process.append(member_str)
                except json.JSONDecodeError:
                    continue

            if not dlq_members_to_process:
                self.logger.info(f"消息 {message_id} 未在死信队列中找到。")
                return False

            for member_str in dlq_members_to_process:
                await self.redis_client.zrem(self.dead_letter_queue, member_str)
                data = json.loads(member_str)

                # 重置重试计数并重新加入待处理队列
                data["retry_count"] = 0  # 重置重试次数
                data["error"] = None  # 清除错误信息
                data["failed_at"] = None
                queued_msg = QueuedMessage.from_dict(data)
                await self.enqueue(queued_msg)
                self.logger.info(f"消息 {message_id} 已从死信队列移回待处理队列进行重试。")
            return True

        except Exception as e:
            self.logger.error(f"从死信队列重试消息 {message_id} 失败: {e}", exc_info=True)
            return False

    async def get_stats(self) -> Dict[str, Any]:
        """获取队列统计信息"""
        if not self.redis_client:
            return {"error": "Redis not available"}

        try:
            stats = {
                "pending_count": await self.redis_client.zcard(self.pending_queue),
                "processing_count": await self.redis_client.zcard(self.processing_queue),
                "failed_count": await self.redis_client.zcard(self.failed_queue),
                "dead_letter_count": await self.redis_client.zcard(self.dead_letter_queue),
                "instance_id": self.instance_id
            }

            # 计算平均等待时间 (仅适用于pending_queue)
            # 由于分数是优先级与时间戳的组合，这里计算真实时间戳的平均值会更复杂
            # 简单计算最早消息的等待时间
            if stats["pending_count"] > 0:
                oldest_pending = await self.redis_client.zrange(
                    self.pending_queue, 0, 0, withscores=True  # 获取分数最低（最早）的消息
                )
                if oldest_pending:
                    # 分数是 priority * 1_000_000_000 + (time_in_micros % 1_000_000_000)
                    # 需要反向计算出原始时间戳
                    oldest_score = int(oldest_pending[0][1])
                    # 提取时间戳部分
                    oldest_time_micros = oldest_score % 1_000_000_000  # 提取后9位
                    # 重新构建时间戳
                    # 实际的时间戳是 (score % 1_000_000_000) / 1_000_000 + base_time
                    # 这里的 `created_at` 字段可以直接获取原始时间戳，更准确
                    message_data_str = oldest_pending[0][0]
                    message_data = json.loads(message_data_str)
                    original_created_at = message_data.get("created_at", time.time())

                    stats["oldest_pending_age_seconds"] = round(time.time() - original_created_at, 2)
                else:
                    stats["oldest_pending_age_seconds"] = 0.0
            else:
                stats["oldest_pending_age_seconds"] = 0.0

            # 计算处理中消息的平均处理时间 (如果需要，可以通过遍历processing_queue计算)
            # 或者通过记录处理完成的消息的duration来计算 (已在monitoring中实现)

            return stats

        except Exception as e:
            self.logger.error(f"获取队列统计失败: {e}", exc_info=True)
            return {"error": str(e)}


class MessageCoordinator:
    """消息分发协调器主类"""

    def __init__(self, bot_manager, redis_client: Optional['redis.Redis'] = None):
        self.bot_manager = bot_manager
        self.redis_client = redis_client
        self.instance_id = str(uuid.uuid4())[:8]
        self.logger = get_logger("app.message_coordinator")

        # 初始化组件
        self.load_balancer = LoadBalancer(bot_manager)
        self.message_queue = MessageQueue(redis_client)
        # MessageProcessor 实例在 create_coordinated_handler 中创建并传入

        # 后台任务
        self._processing_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

        # 处理器回调，由 create_coordinated_handler 设置
        self._message_processor_callback: Optional[callable] = None

    async def start(self):
        """启动消息协调器"""
        if self._running:
            return

        self.logger.info("启动消息分发协调器...")
        self._running = True

        # 启动后台任务
        self._processing_task = asyncio.create_task(self._message_processing_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        self.logger.info(f"消息协调器已启动，实例ID: {self.instance_id}")

    async def stop(self):
        """停止消息协调器"""
        if not self._running:
            return

        self.logger.info("停止消息分发协调器...")
        self._running = False

        # 取消后台任务
        for task in [self._processing_task, self._cleanup_task]:
            if task and not task.done():
                task.cancel()

        # 等待任务完成
        for task in [self._processing_task, self._cleanup_task]:
            if task and not task.done():
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        self.logger.info("消息协调器已停止")

    def generate_message_id(self, update_id: int, chat_id: int) -> str:
        """生成唯一的消息ID"""
        content = f"{update_id}:{chat_id}:{time.time()}"
        return hashlib.md5(content.encode()).hexdigest()[:16]

    async def coordinate_message(self, raw_update: Dict[str, Any]) -> bool:
        """协调处理单个消息"""
        update_id = raw_update.get("update_id")
        msg_data = raw_update.get("message", {}) or raw_update.get("edited_message", {}) or {}
        chat_id = msg_data.get("chat", {}).get("id")
        user_id = msg_data.get("from", {}).get("id")
        chat_type = msg_data.get("chat", {}).get("type")

        if not all([update_id, chat_id, chat_type]):
            self.logger.warning("消息数据不完整，跳过处理")
            return False

        # 生成消息ID
        message_id = self.generate_message_id(update_id, chat_id)

        # 确保不会重复处理
        lock_key = f"msg_coord_lock:{message_id}"
        # 设置一个较短的锁超时，例如10秒，足够消息入队
        try:
            async with DistributedLock(self.redis_client, lock_key, timeout=settings.COORDINATION_LOCK_TIMEOUT) as lock:
                # 确定消息优先级
                priority = self._determine_priority(user_id, chat_type, msg_data)

                # 创建队列消息
                queued_msg = QueuedMessage(
                    message_id=message_id,
                    update_id=update_id,
                    chat_id=chat_id,
                    user_id=user_id,
                    chat_type=chat_type,
                    priority=priority,
                    payload=raw_update,
                    created_at=time.time()  # 记录入队时间
                )

                # 选择机器人
                selected_bot_id = await self.load_balancer.select_best_bot(queued_msg)
                if not selected_bot_id:
                    self.logger.error(f"无法为消息 {message_id} 选择机器人，入队失败。")
                    return False

                queued_msg.assigned_bot_id = selected_bot_id

                # 加入队列
                success = await self.message_queue.enqueue(queued_msg)
                if success:
                    self.logger.info(f"消息 {message_id} 已协调分配给机器人 {selected_bot_id} 并成功入队。")
                else:
                    self.logger.error(f"消息 {message_id} 协调成功但入队失败。")

                return success
        except Exception as e:
            self.logger.error(f"协调消息 {message_id} 时获取锁或处理异常: {e}", exc_info=True)
            return False

    def _determine_priority(self, user_id: Optional[int], chat_type: str, msg_data: Dict) -> MessagePriority:
        """确定消息优先级"""
        # 使用settings中的配置
        from .settings import settings  # 确保导入

        priority = MessagePriority.NORMAL

        # 管理员消息高优先级
        if settings.ADMIN_MESSAGE_PRIORITY_BOOST and user_id and user_id in settings.ADMIN_USER_IDS:
            priority = MessagePriority.HIGH

        # 私聊消息优先级
        elif chat_type == "private":
            priority = MessagePriority(settings.PRIVATE_CHAT_PRIORITY)

        # 支持群组消息优先级
        chat_id = msg_data.get("chat", {}).get("id")
        if settings.SUPPORT_GROUP_ID and str(
                chat_id) == settings.SUPPORT_GROUP_ID and settings.SUPPORT_GROUP_PRIORITY_BOOST:
            # 如果支持群组优先级高于当前已确定的优先级，则提升
            if priority.value < MessagePriority.HIGH.value:  # 默认支持群组消息为高优先级
                priority = MessagePriority.HIGH

        # 其他群组消息优先级
        elif chat_type in ["group", "supergroup"]:
            priority = MessagePriority(settings.GROUP_CHAT_PRIORITY)

        return priority

    async def _message_processing_loop(self):
        """消息处理循环"""
        while self._running:
            try:
                # 从队列获取消息
                queued_msg = await self.message_queue.dequeue(timeout=1)
                if not queued_msg:
                    # 如果队列为空，等待一小段时间再重试，避免CPU空转
                    self.logger.debug("消息队列为空，等待新的消息...")  # 添加这条日志
                    await asyncio.sleep(0.1)
                    continue

                # 确保回调函数已设置
                if self._message_processor_callback is None:
                    self.logger.error(
                        f"消息 {queued_msg.message_id} 无法处理：Message processor callback 未设置。将其标记为失败。")
                    await self.message_queue.mark_failed(queued_msg.message_id, "Processor callback not set")
                    continue  # 继续循环，尝试处理下一条消息

                # 处理消息
                await self._process_queued_message(queued_msg)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"消息处理循环异常: {e}", exc_info=True)
                await asyncio.sleep(1)  # 出现异常时暂停一下，防止死循环

    async def _process_queued_message(self, queued_msg: QueuedMessage):
        """处理单个队列消息"""
        if self._message_processor_callback is None:
            self.logger.error("Message processor callback not set. Cannot process messages.")
            await self.message_queue.mark_failed(queued_msg.message_id, "Processor not initialized")
            return

        start_time = time.time()

        try:
            # 记录处理开始
            self.logger.debug(f"开始处理队列消息: {queued_msg.message_id}")

            # 获取分配的机器人并验证
            bot = None
            if queued_msg.assigned_bot_id:
                bot = self.bot_manager.get_bot_by_id(queued_msg.assigned_bot_id)

            if not bot or not bot.is_available():
                self.logger.warning(
                    f"消息 {queued_msg.message_id} 的原分配机器人 {queued_msg.assigned_bot_id} 不可用，尝试重新选择。")
                # 重新选择机器人
                new_bot_id = await self.load_balancer.select_best_bot(queued_msg)
                if not new_bot_id:
                    self.logger.error(f"无法为消息 {queued_msg.message_id} 找到可用机器人。")
                    await self.message_queue.mark_failed(queued_msg.message_id, "No available bot for processing")
                    return
                queued_msg.assigned_bot_id = new_bot_id
                bot = self.bot_manager.get_bot_by_id(new_bot_id)
                self.logger.info(f"消息 {queued_msg.message_id} 重新分配给机器人 {new_bot_id}。")

            # 记录机器人请求
            if bot:
                await self.bot_manager.record_bot_request(bot.bot_id)

            # 调用实际的消息处理逻辑
            processing_result = await self._message_processor_callback(queued_msg, bot)

            # 🔥 关键修复：强制标记完成，即使处理过程中有小错误
            if processing_result.success:
                success = await self.message_queue.mark_completed(queued_msg.message_id)
                if success:
                    self.logger.info(f"✅ 消息 {queued_msg.message_id} 处理和标记完成成功")
                else:
                    self.logger.warning(f"⚠️ 消息 {queued_msg.message_id} 处理成功但标记完成失败")
            else:
                self.logger.warning(f"❌ 消息 {queued_msg.message_id} 处理失败: {processing_result.error_message}")
                await self.message_queue.mark_failed(queued_msg.message_id, processing_result.error_message)

        except Exception as e:
            self.logger.error(f"❌ 处理消息 {queued_msg.message_id} 时发生未预期异常: {e}", exc_info=True)
            await self.message_queue.mark_failed(queued_msg.message_id, f"Unexpected error: {str(e)}")
        finally:
            processing_time = time.time() - start_time
            self.logger.debug(f"消息 {queued_msg.message_id} 处理耗时: {processing_time:.3f}秒")

    async def _cleanup_loop(self):
        """清理循环"""
        while self._running:
            try:
                # 使用settings中的清理间隔
                await asyncio.sleep(settings.COORDINATION_CLEANUP_INTERVAL)
                await self.message_queue.cleanup_stale_messages()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"清理循环异常: {e}", exc_info=True)
                await asyncio.sleep(60)  # 错误时暂停一下

    async def get_stats(self) -> Dict[str, Any]:
        """获取协调器统计信息"""
        queue_stats = await self.message_queue.get_stats()
        bot_stats = self.bot_manager.get_stats()

        return {
            "coordinator": {
                "instance_id": self.instance_id,
                "running": self._running
            },
            "queue": queue_stats,
            "bots": bot_stats
        }


# 全局消息协调器实例
_message_coordinator: Optional[MessageCoordinator] = None


async def get_message_coordinator() -> MessageCoordinator:
    """获取全局消息协调器"""
    global _message_coordinator
    if _message_coordinator is None:
        # 获取机器人管理器
        from .bot_manager import get_bot_manager
        bot_manager = await get_bot_manager()

        # 获取Redis客户端
        redis_client = None
        if redis:
            try:
                redis_url = getattr(settings, 'REDIS_URL', 'redis://localhost:6379')
                redis_client = redis.from_url(redis_url)
                await redis_client.ping()
                logger.info("Redis连接成功，消息协调器将使用Redis")
            except Exception as e:
                logger.warning(f"Redis不可用，消息协调器功能受限: {e}")

        _message_coordinator = MessageCoordinator(bot_manager, redis_client)
        # 🔥 自动启动协调器
        try:
            await _message_coordinator.start()
            logger.info(f"消息协调器已自动启动")
        except Exception as e:
            logger.error(f"自动启动消息协调器失败: {e}")

    return _message_coordinator


async def cleanup_message_coordinator():
    """清理消息协调器"""
    global _message_coordinator
    if _message_coordinator:
        await _message_coordinator.stop()
        _message_coordinator = None
        logger.info("消息协调器已清理")