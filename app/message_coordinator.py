import asyncio
import json
import time
import uuid
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import hashlib

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from .logging_config import get_logger
from .settings import settings

logger = get_logger("app.message_coordinator")


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
    processing_deadline: Optional[float] = None

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
    """负载均衡器"""

    def __init__(self, bot_manager):
        self.bot_manager = bot_manager
        self.logger = get_logger("app.load_balancer")

    def calculate_message_weight(self, queued_msg: QueuedMessage) -> int:
        """计算消息权重"""
        weight = 1

        # 根据优先级调整权重
        priority_weights = {
            MessagePriority.LOW: 1,
            MessagePriority.NORMAL: 2,
            MessagePriority.HIGH: 3,
            MessagePriority.URGENT: 5
        }
        weight *= priority_weights.get(queued_msg.priority, 1)

        # 管理员消息权重更高
        if queued_msg.user_id and queued_msg.user_id in getattr(settings, 'ADMIN_USER_IDS', []):
            weight *= 2

        # 群组消息权重较低
        if queued_msg.chat_type in ['group', 'supergroup']:
            weight = max(1, weight // 2)

        return weight

    async def select_best_bot(self, queued_msg: QueuedMessage) -> Optional[str]:
        """选择最佳机器人处理消息"""
        try:
            # 获取可用机器人
            available_bots = self.bot_manager.get_available_bots()
            if not available_bots:
                self.logger.warning("没有可用的机器人")
                return None

            # 如果消息已分配给特定机器人且该机器人仍可用
            if queued_msg.assigned_bot_id:
                assigned_bot = self.bot_manager.get_bot_by_id(queued_msg.assigned_bot_id)
                if assigned_bot and assigned_bot.is_available():
                    return queued_msg.assigned_bot_id

            # 计算消息权重
            message_weight = self.calculate_message_weight(queued_msg)

            # 根据负载和权重选择机器人
            best_bot = None
            best_score = float('inf')

            for bot in available_bots:
                # 计算机器人负载评分
                load_score = bot.get_load_score()

                # 考虑消息权重对负载的影响
                adjusted_score = load_score + (message_weight * 10)

                # 考虑机器人最近的请求时间
                time_since_last_request = time.time() - bot.last_request_time
                if time_since_last_request < 1:  # 1秒内有请求
                    adjusted_score += 50  # 增加负载惩罚

                # 考虑机器人的优先级设置
                priority_bonus = (5 - bot.config.priority) * 10

                final_score = adjusted_score - priority_bonus

                self.logger.debug(
                    f"机器人 {bot.bot_id} 评分: 负载={load_score}, "
                    f"调整后={adjusted_score}, 最终={final_score}"
                )

                if final_score < best_score:
                    best_score = final_score
                    best_bot = bot

            if best_bot:
                self.logger.info(f"为消息 {queued_msg.message_id} 选择机器人 {best_bot.bot_id}")
                return best_bot.bot_id
            else:
                self.logger.warning("无法选择合适的机器人")
                return None

        except Exception as e:
            self.logger.error(f"选择机器人失败: {e}", exc_info=True)
            return None


class MessageQueue:
    """消息队列管理器"""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.instance_id = str(uuid.uuid4())[:8]
        self.logger = get_logger("app.message_queue")

        # 队列名称
        self.pending_queue = "mq:pending"
        self.processing_queue = "mq:processing"
        self.failed_queue = "mq:failed"
        self.dead_letter_queue = "mq:dead_letter"

    async def enqueue(self, queued_msg: QueuedMessage, priority_boost: bool = False) -> bool:
        """将消息添加到队列"""
        if not self.redis_client:
            self.logger.error("Redis客户端未初始化")
            return False

        try:
            # 序列化消息
            message_data = json.dumps(queued_msg.to_dict())

            # 根据优先级选择分数
            priority_score = queued_msg.priority.value
            if priority_boost:
                priority_score += 10

            # 添加时间戳确保唯一性
            score = priority_score * 1000000 + int(time.time() * 1000) % 1000000

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
            result = await self.redis_client.bzpopmax(self.pending_queue, timeout=timeout)

            if not result:
                return None

            queue_name, message_data, score = result
            message_dict = json.loads(message_data)
            queued_msg = QueuedMessage.from_dict(message_dict)

            # 将消息移动到处理队列
            processing_data = message_dict.copy()
            processing_data["processing_started"] = time.time()
            processing_data["processor_instance"] = self.instance_id

            await self.redis_client.zadd(
                self.processing_queue,
                {json.dumps(processing_data): time.time()}
            )

            self.logger.debug(f"从队列取出消息: {queued_msg.message_id}")
            return queued_msg

        except Exception as e:
            self.logger.error(f"消息出队失败: {e}", exc_info=True)
            return None

    async def mark_completed(self, message_id: str) -> bool:
        """标记消息处理完成"""
        if not self.redis_client:
            return False

        try:
            # 从处理队列中移除消息
            members = await self.redis_client.zrange(self.processing_queue, 0, -1)
            for member in members:
                try:
                    data = json.loads(member)
                    if data.get("message_id") == message_id:
                        await self.redis_client.zrem(self.processing_queue, member)
                        self.logger.debug(f"消息 {message_id} 处理完成")
                        return True
                except json.JSONDecodeError:
                    continue

            return False

        except Exception as e:
            self.logger.error(f"标记消息完成失败: {e}", exc_info=True)
            return False

    async def mark_failed(self, message_id: str, error: str) -> bool:
        """标记消息处理失败"""
        if not self.redis_client:
            return False

        try:
            # 从处理队列中找到并移动到失败队列
            members = await self.redis_client.zrange(self.processing_queue, 0, -1)
            for member in members:
                try:
                    data = json.loads(member)
                    if data.get("message_id") == message_id:
                        # 移除原消息
                        await self.redis_client.zrem(self.processing_queue, member)

                        # 添加错误信息
                        data["error"] = error
                        data["failed_at"] = time.time()
                        data["retry_count"] = data.get("retry_count", 0) + 1

                        # 检查是否应该重试
                        queued_msg = QueuedMessage.from_dict(data)
                        if queued_msg.should_retry():
                            # 重新加入待处理队列
                            await self.enqueue(queued_msg)
                            self.logger.info(f"消息 {message_id} 将重试，当前重试次数: {queued_msg.retry_count}")
                        else:
                            # 移动到死信队列
                            await self.redis_client.zadd(
                                self.dead_letter_queue,
                                {json.dumps(data): time.time()}
                            )
                            self.logger.warning(f"消息 {message_id} 超过最大重试次数，移至死信队列")

                        return True
                except json.JSONDecodeError:
                    continue

            return False

        except Exception as e:
            self.logger.error(f"标记消息失败失败: {e}", exc_info=True)
            return False

    async def cleanup_stale_messages(self, timeout_seconds: int = 300):
        """清理超时的处理中消息"""
        if not self.redis_client:
            return

        try:
            current_time = time.time()
            cutoff_time = current_time - timeout_seconds

            # 获取超时的消息
            stale_members = await self.redis_client.zrangebyscore(
                self.processing_queue, 0, cutoff_time
            )

            for member in stale_members:
                try:
                    data = json.loads(member)
                    message_id = data.get("message_id", "unknown")

                    # 移除超时消息
                    await self.redis_client.zrem(self.processing_queue, member)

                    # 重新加入待处理队列
                    data["retry_count"] = data.get("retry_count", 0) + 1
                    queued_msg = QueuedMessage.from_dict(data)

                    if queued_msg.should_retry():
                        await self.enqueue(queued_msg)
                        self.logger.warning(f"超时消息 {message_id} 重新加入队列")
                    else:
                        await self.redis_client.zadd(
                            self.dead_letter_queue,
                            {json.dumps(data): current_time}
                        )
                        self.logger.error(f"超时消息 {message_id} 移至死信队列")

                except json.JSONDecodeError:
                    continue

        except Exception as e:
            self.logger.error(f"清理超时消息失败: {e}", exc_info=True)

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

            # 计算平均等待时间
            if stats["pending_count"] > 0:
                oldest_pending = await self.redis_client.zrange(
                    self.pending_queue, 0, 0, withscores=True
                )
                if oldest_pending:
                    oldest_time = oldest_pending[0][1] % 1000000 / 1000
                    stats["oldest_pending_age"] = time.time() - oldest_time

            return stats

        except Exception as e:
            self.logger.error(f"获取队列统计失败: {e}", exc_info=True)
            return {"error": str(e)}


class MessageCoordinator:
    """消息分发协调器主类"""

    def __init__(self, bot_manager, redis_client: Optional[redis.Redis] = None):
        self.bot_manager = bot_manager
        self.redis_client = redis_client
        self.instance_id = str(uuid.uuid4())[:8]
        self.logger = get_logger("app.message_coordinator")

        # 初始化组件
        self.load_balancer = LoadBalancer(bot_manager)
        self.message_queue = MessageQueue(redis_client)

        # 后台任务
        self._processing_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

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
        msg_data = raw_update.get("message", {})
        chat_id = msg_data.get("chat", {}).get("id")
        user_id = msg_data.get("from", {}).get("id")
        chat_type = msg_data.get("chat", {}).get("type")

        if not all([update_id, chat_id, chat_type]):
            self.logger.warning("消息数据不完整，跳过处理")
            return False

        # 生成消息ID
        message_id = self.generate_message_id(update_id, chat_id)

        # 检查是否已经在处理中
        lock_key = f"msg:{message_id}"
        async with DistributedLock(self.redis_client, lock_key, timeout=60) as lock:

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
                payload=raw_update
            )

            # 选择机器人
            selected_bot_id = await self.load_balancer.select_best_bot(queued_msg)
            if not selected_bot_id:
                self.logger.error(f"无法为消息 {message_id} 选择机器人")
                return False

            queued_msg.assigned_bot_id = selected_bot_id

            # 加入队列
            success = await self.message_queue.enqueue(queued_msg)
            if success:
                self.logger.info(f"消息 {message_id} 已协调分配给机器人 {selected_bot_id}")
            else:
                self.logger.error(f"消息 {message_id} 协调失败")

            return success

    def _determine_priority(self, user_id: Optional[int], chat_type: str, msg_data: Dict) -> MessagePriority:
        """确定消息优先级"""
        # 管理员消息高优先级
        if user_id and user_id in getattr(settings, 'ADMIN_USER_IDS', []):
            return MessagePriority.HIGH

        # 私聊消息中等优先级
        if chat_type == "private":
            return MessagePriority.NORMAL

        # 支持群组消息高优先级
        chat_id = msg_data.get("chat", {}).get("id")
        if str(chat_id) == getattr(settings, 'SUPPORT_GROUP_ID', ''):
            return MessagePriority.HIGH

        # 其他群组消息低优先级
        return MessagePriority.LOW

    async def _message_processing_loop(self):
        """消息处理循环"""
        while self._running:
            try:
                # 从队列获取消息
                queued_msg = await self.message_queue.dequeue(timeout=1)
                if not queued_msg:
                    continue

                # 处理消息
                await self._process_queued_message(queued_msg)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"消息处理循环异常: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _process_queued_message(self, queued_msg: QueuedMessage):
        """处理单个队列消息"""
        try:
            # 获取分配的机器人
            if not queued_msg.assigned_bot_id:
                self.logger.error(f"消息 {queued_msg.message_id} 没有分配机器人")
                await self.message_queue.mark_failed(queued_msg.message_id, "No bot assigned")
                return

            bot = self.bot_manager.get_bot_by_id(queued_msg.assigned_bot_id)
            if not bot or not bot.is_available():
                # 重新选择机器人
                new_bot_id = await self.load_balancer.select_best_bot(queued_msg)
                if not new_bot_id:
                    await self.message_queue.mark_failed(queued_msg.message_id, "No available bot")
                    return
                queued_msg.assigned_bot_id = new_bot_id
                bot = self.bot_manager.get_bot_by_id(new_bot_id)

            # 记录机器人请求
            await self.bot_manager.record_bot_request(queued_msg.assigned_bot_id)

            # 处理消息（这里需要调用实际的消息处理逻辑）
            success = await self._execute_message_processing(queued_msg, bot)

            if success:
                await self.message_queue.mark_completed(queued_msg.message_id)
                self.logger.debug(f"消息 {queued_msg.message_id} 处理成功")
            else:
                await self.message_queue.mark_failed(queued_msg.message_id, "Processing failed")

        except Exception as e:
            self.logger.error(f"处理消息 {queued_msg.message_id} 时异常: {e}", exc_info=True)
            await self.message_queue.mark_failed(queued_msg.message_id, str(e))

    async def _execute_message_processing(self, queued_msg: QueuedMessage, bot) -> bool:
        """执行实际的消息处理"""
        # 这里需要调用原有的消息处理逻辑
        # 暂时返回True表示成功
        self.logger.info(f"使用机器人 {bot.bot_id} 处理消息 {queued_msg.message_id}")
        # TODO: 集成原有的消息处理逻辑
        return True

    async def _cleanup_loop(self):
        """清理循环"""
        while self._running:
            try:
                await self.message_queue.cleanup_stale_messages()
                await asyncio.sleep(60)  # 每分钟清理一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"清理循环异常: {e}", exc_info=True)
                await asyncio.sleep(60)

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


async def get_message_coordinator():
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
        await _message_coordinator.start()

    return _message_coordinator


async def cleanup_message_coordinator():
    """清理消息协调器"""
    global _message_coordinator
    if _message_coordinator:
        await _message_coordinator.stop()
        _message_coordinator = None
        logger.info("消息协调器已清理")