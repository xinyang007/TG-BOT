import time
import asyncio
from typing import Dict, Set
from dataclasses import dataclass
from .tg_utils import tg
from .settings import settings
from .logging_config import get_logger

logger = get_logger("app.rate_limit_notifications")

# 通知冷却时间管理
_notification_cooldowns: Dict[int, float] = {}


@dataclass
class NotificationMessage:
    """通知消息模板"""
    title: str
    content: str
    suggestion: str


class RateLimitNotificationManager:
    """速率限制通知管理器"""

    def __init__(self):
        self.logger = get_logger("app.notifications.rate_limit")

        # 多语言消息模板
        self.messages = {
            "zh": {
                "private": NotificationMessage(
                    title="🚫 消息发送过于频繁",
                    content="📊 您在短时间内发送了过多消息\n⏰ 请等待 {time} 后再试",
                    suggestion="💡 为了更好的服务体验，请适当降低消息发送频率"
                ),
                "group": NotificationMessage(
                    title="🚫 群聊消息限制",
                    content="📊 您在群聊中发送消息过于频繁\n⏰ 限制将在 {time} 后解除",
                    suggestion="💡 请稍后再在群聊中发送消息"
                ),
                "group_public": NotificationMessage(
                    title="⚠️ 消息频率限制",
                    content="用户 {user_name} 消息发送过于频繁\n限制将在 {time} 后解除",
                    suggestion=""
                )
            },
            "en": {
                "private": NotificationMessage(
                    title="🚫 Message Rate Limit",
                    content="📊 You've sent too many messages in a short time\n⏰ Please wait {time} before trying again",
                    suggestion="💡 Please reduce message frequency for better service"
                ),
                "group": NotificationMessage(
                    title="🚫 Group Message Limit",
                    content="📊 You've sent too many messages in this group\n⏰ Restriction will be lifted in {time}",
                    suggestion="💡 Please wait before sending more messages"
                ),
                "group_public": NotificationMessage(
                    title="⚠️ Message Rate Limit",
                    content="User {user_name} has been sending messages too frequently\nRestriction will be lifted in {time}",
                    suggestion=""
                )
            }
        }

    def _format_time(self, seconds: int) -> str:
        """格式化时间显示"""
        if seconds >= 3600:  # 大于1小时
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}小时{minutes}分钟" if settings.RATE_LIMIT_NOTIFICATION_LANGUAGE == "zh" else f"{hours}h {minutes}m"
        elif seconds >= 60:  # 大于1分钟
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            if remaining_seconds > 0:
                return f"{minutes}分{remaining_seconds}秒" if settings.RATE_LIMIT_NOTIFICATION_LANGUAGE == "zh" else f"{minutes}m {remaining_seconds}s"
            else:
                return f"{minutes}分钟" if settings.RATE_LIMIT_NOTIFICATION_LANGUAGE == "zh" else f"{minutes}m"
        else:  # 小于1分钟
            return f"{seconds}秒" if settings.RATE_LIMIT_NOTIFICATION_LANGUAGE == "zh" else f"{seconds}s"

    def _should_send_notification(self, user_id: int, chat_id: int = None) -> bool:
        """检查是否应该发送通知（冷却时间检查）"""
        if not getattr(settings, 'ENABLE_RATE_LIMIT_NOTIFICATIONS', True):
            return False

        current_time = time.time()
        cooldown_duration = getattr(settings, 'RATE_LIMIT_NOTIFICATION_COOLDOWN', 60)

        # 使用 chat_id 和 user_id 组合作为键，这样私聊和群聊可以分别冷却
        cooldown_key = f"{user_id}_{chat_id}" if chat_id else str(user_id)
        last_notification = _notification_cooldowns.get(cooldown_key, 0)

        if current_time - last_notification < cooldown_duration:
            self.logger.debug(f"用户 {user_id} 在聊天 {chat_id} 的通知冷却中，跳过发送")
            return False

        return True

    def _record_notification(self, user_id: int, chat_id: int = None):
        """记录通知发送时间"""
        cooldown_key = f"{user_id}_{chat_id}" if chat_id else str(user_id)
        _notification_cooldowns[cooldown_key] = time.time()

        # 清理过期的冷却记录
        current_time = time.time()
        cooldown_duration = getattr(settings, 'RATE_LIMIT_NOTIFICATION_COOLDOWN', 60)
        expired_keys = [
            key for key, timestamp in _notification_cooldowns.items()
            if current_time - timestamp > cooldown_duration * 2
        ]
        for key in expired_keys:
            del _notification_cooldowns[key]

    async def _send_safe_message(self, chat_id: int, text: str, parse_mode: str = "HTML",
                                 reply_to_message_id: int = None) -> bool:
        """安全发送消息，处理回复失败的情况"""
        # 构建基本payload
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode
        }

        # 如果有回复消息ID，添加回复参数和容错参数
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id
            payload["allow_sending_without_reply"] = True  # 关键：如果回复失败则正常发送

        try:
            await tg("sendMessage", payload)
            self.logger.debug(f"✅ 消息发送成功到聊天 {chat_id}")
            return True

        except Exception as e:
            error_msg = str(e).lower()

            # 检查是否是回复相关错误
            if any(keyword in error_msg for keyword in [
                "message to be replied not found",
                "message not found",
                "reply message not found",
                "replied message not found"
            ]):
                self.logger.warning(
                    f"⚠️ 回复的消息不存在 (chat_id: {chat_id}, reply_to: {reply_to_message_id})，尝试直接发送")

                # 移除回复参数，直接发送
                fallback_payload = {
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": parse_mode
                }

                try:
                    await tg("sendMessage", fallback_payload)
                    self.logger.info(f"✅ 回退发送成功到聊天 {chat_id}")
                    return True
                except Exception as fallback_error:
                    self.logger.error(f"❌ 回退发送也失败: {fallback_error}")
                    return False
            else:
                self.logger.error(f"❌ 发送消息失败 (chat_id: {chat_id}): {e}")
                return False

    async def send_notification(self, user_id: int, user_name: str, chat_type: str,
                                chat_id: int, rate_result, msg_id: int = None):
        """发送速率限制通知"""
        try:
            # 检查是否应该发送通知
            if not self._should_send_notification(user_id, chat_id):
                return

            # 记录通知发送
            self._record_notification(user_id, chat_id)

            # 计算剩余时间
            current_time = time.time()
            remaining_seconds = max(0, int(rate_result.reset_time - current_time))
            time_str = self._format_time(remaining_seconds)

            # 获取语言
            lang = getattr(settings, 'RATE_LIMIT_NOTIFICATION_LANGUAGE', 'zh')
            if lang not in self.messages:
                lang = 'zh'  # 回退到中文

            if chat_type == "private":
                # 私聊 - 直接在私聊中通知
                template = self.messages[lang]["private"]
                notification_text = (
                    f"<b>{template.title}</b>\n\n"
                    f"{template.content.format(time=time_str)}\n\n"
                    f"📈 状态：{rate_result.current_count}/{rate_result.limit} 条消息\n"
                    f"🔄 重置时间：<code>{time_str}</code>\n\n"
                    f"{template.suggestion}"
                )

                success = await self._send_safe_message(
                    chat_id=user_id,
                    text=notification_text
                )

                if success:
                    self.logger.info(f"✅ 已向用户 {user_id} 发送私聊限速通知")
                else:
                    self.logger.error(f"❌ 向用户 {user_id} 发送私聊限速通知失败")

            elif chat_type in ("group", "supergroup"):
                # 群聊 - 在群聊中通知
                template = self.messages[lang]["group_public"]
                display_name = user_name or f"ID{user_id}"

                notification_text = (
                    f"<b>{template.title}</b>\n\n"
                    f"{template.content.format(user_name=display_name, time=time_str)}\n"
                    f"📈 状态：{rate_result.current_count}/{rate_result.limit} 条消息"
                )

                # 安全发送群聊通知（可能回复原消息）
                success = await self._send_safe_message(
                    chat_id=chat_id,
                    text=notification_text,
                    reply_to_message_id=msg_id if msg_id else None
                )

                if success:
                    self.logger.info(f"✅ 已在群聊 {chat_id} 发送用户 {user_id} 的限速通知")
                else:
                    self.logger.error(f"❌ 在群聊 {chat_id} 发送用户 {user_id} 的限速通知失败")

                # 可选：同时私信用户详细信息
                if getattr(settings, 'ALSO_NOTIFY_USER_PRIVATELY', False):
                    private_template = self.messages[lang]["group"]
                    private_text = (
                        f"<b>{private_template.title}</b>\n\n"
                        f"{private_template.content.format(time=time_str)}\n\n"
                        f"📈 状态：{rate_result.current_count}/{rate_result.limit} 条消息\n"
                        f"🏠 群组：<code>{chat_id}</code>\n\n"
                        f"{private_template.suggestion}"
                    )

                    private_success = await self._send_safe_message(
                        chat_id=user_id,
                        text=private_text
                    )

                    if private_success:
                        self.logger.info(f"✅ 已向用户 {user_id} 发送群聊限速私信通知")
                    else:
                        self.logger.warning(f"⚠️ 向用户 {user_id} 发送群聊限速私信通知失败")

        except Exception as e:
            self.logger.error(f"❌ 发送限速通知失败: {e}", exc_info=True)

    async def send_punishment_notification(self, user_id: int, punishment_duration: int):
        """发送惩罚期通知"""
        try:
            if not self._should_send_notification(user_id):
                return

            self._record_notification(user_id)

            lang = getattr(settings, 'RATE_LIMIT_NOTIFICATION_LANGUAGE', 'zh')
            time_str = self._format_time(punishment_duration)

            if lang == "zh":
                text = (
                    f"🚫 <b>临时限制生效</b>\n\n"
                    f"由于频繁发送消息，您已被临时限制\n"
                    f"⏰ 限制时间：<code>{time_str}</code>\n\n"
                    f"💡 限制期间您的消息将不会被处理\n"
                    f"⌛ 请耐心等待限制自动解除"
                )
            else:
                text = (
                    f"🚫 <b>Temporary Restriction</b>\n\n"
                    f"You have been temporarily restricted due to frequent messaging\n"
                    f"⏰ Duration: <code>{time_str}</code>\n\n"
                    f"💡 Your messages will not be processed during restriction\n"
                    f"⌛ Please wait for automatic removal"
                )

            success = await self._send_safe_message(
                chat_id=user_id,
                text=text
            )

            if success:
                self.logger.info(f"✅ 已向用户 {user_id} 发送惩罚期通知，时长：{time_str}")
            else:
                self.logger.error(f"❌ 向用户 {user_id} 发送惩罚期通知失败，时长：{time_str}")

        except Exception as e:
            self.logger.error(f"❌ 发送惩罚期通知失败: {e}", exc_info=True)


# 全局通知管理器实例
_notification_manager = RateLimitNotificationManager()


async def send_rate_limit_notification(user_id: int, user_name: str, chat_type: str,
                                       chat_id: int, rate_result, msg_id: int = None):
    """发送速率限制通知的便利函数"""
    await _notification_manager.send_notification(
        user_id, user_name, chat_type, chat_id, rate_result, msg_id
    )


async def send_punishment_notification(user_id: int, punishment_duration: int):
    """发送惩罚期通知的便利函数"""
    await _notification_manager.send_punishment_notification(user_id, punishment_duration)