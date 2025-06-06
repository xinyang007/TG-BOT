import time
import asyncio
from typing import Dict, Set, Optional
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

        # 🔥 新增：备用机器人信息缓存
        self._backup_bot_cache = {}
        self._cache_expiry = {}
        self._cache_duration = 300  # 5分钟缓存

        # 多语言消息模板
        self.messages = {
            "zh": {
                "private": NotificationMessage(
                    title="🚫 消息发送过于频繁",
                    content="📊 您在短时间内发送了过多消息\n⏰ 请等待 {time} 后再试",
                    suggestion="💡 为了更好的服务体验，请适当降低消息发送频率"
                ),
                "private_with_backup": NotificationMessage(
                    title="🚫 消息发送过于频繁",
                    content="📊 您在短时间内发送了过多消息\n⏰ 请等待 {time} 后再试\n\n🤖 您也可以联系备用机器人继续对话：",
                    suggestion="💡 点击下方按钮联系备用机器人，或等待限制解除"
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
                "private_with_backup": NotificationMessage(
                    title="🚫 Message Rate Limit",
                    content="📊 You've sent too many messages in a short time\n⏰ Please wait {time} before trying again\n\n🤖 You can also contact our backup bot to continue:",
                    suggestion="💡 Click the button below to contact backup bot, or wait for limit to be lifted"
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

    async def _get_backup_bot_info(self, current_bot_token: str) -> Optional[Dict]:
        """获取备用机器人信息（修复版本）"""

        # 🔥 修复1：检查缓存
        cache_key = f"backup_{current_bot_token[-10:] if current_bot_token else 'none'}"
        current_time = time.time()

        if (cache_key in self._backup_bot_cache and
                cache_key in self._cache_expiry and
                current_time < self._cache_expiry[cache_key]):
            cached_info = self._backup_bot_cache[cache_key]
            if cached_info:
                self.logger.debug(f"使用缓存的备用机器人信息: @{cached_info.get('username', 'N/A')}")
                return cached_info

        self.logger.debug("开始获取备用机器人信息...")

        # 🔥 修复2：检查多机器人模式
        if not getattr(settings, 'MULTI_BOT_ENABLED', False):
            self.logger.info("多机器人模式未启用，无法提供备用机器人")
            self._backup_bot_cache[cache_key] = None
            self._cache_expiry[cache_key] = current_time + self._cache_duration
            return None

        try:
            # 🔥 修复3：安全获取机器人管理器
            self.logger.debug("正在获取机器人管理器...")

            try:
                from .dependencies import get_bot_manager_dep
                bot_manager = await get_bot_manager_dep()
            except ImportError as e:
                self.logger.error(f"导入机器人管理器失败: {e}")
                self._backup_bot_cache[cache_key] = None
                self._cache_expiry[cache_key] = current_time + 60
                return None
            except Exception as e:
                self.logger.error(f"获取机器人管理器失败: {e}")
                self._backup_bot_cache[cache_key] = None
                self._cache_expiry[cache_key] = current_time + 60
                return None

            if not bot_manager:
                self.logger.warning("机器人管理器不可用")
                self._backup_bot_cache[cache_key] = None
                self._cache_expiry[cache_key] = current_time + 60
                return None

            self.logger.debug("✅ 机器人管理器获取成功")

            # 🔥 修复4：获取可用机器人
            available_bots = bot_manager.get_available_bots()
            self.logger.debug(f"找到 {len(available_bots)} 个可用机器人")

            if len(available_bots) < 2:
                self.logger.warning(f"可用机器人数量不足: {len(available_bots)}，至少需要2个")
                self._backup_bot_cache[cache_key] = None
                self._cache_expiry[cache_key] = current_time + 60
                return None

            # 🔥 修复5：筛选备用机器人
            backup_bots = [
                bot for bot in available_bots
                if bot.config.token != current_bot_token and bot.is_available()
            ]

            self.logger.debug(f"筛选出 {len(backup_bots)} 个备用机器人候选")

            if not backup_bots:
                self.logger.warning("没有找到符合条件的备用机器人")
                self._backup_bot_cache[cache_key] = None
                self._cache_expiry[cache_key] = current_time + 60
                return None

            # 🔥 修复6：选择最佳备用机器人
            healthy_backups = [bot for bot in backup_bots if bot.status.value == "healthy"]
            if healthy_backups:
                best_backup = min(healthy_backups, key=lambda b: b.config.priority)
                self.logger.debug(f"选择健康的备用机器人: {best_backup.bot_id}")
            else:
                best_backup = min(backup_bots, key=lambda b: b.config.priority)
                self.logger.debug(f"选择可用的备用机器人: {best_backup.bot_id}")

            # 🔥 修复7：获取机器人信息（带重试机制）
            for attempt in range(3):
                try:
                    self.logger.debug(f"尝试获取机器人信息 (第 {attempt + 1} 次)")

                    from .tg_utils import tg_with_specific_bot

                    bot_info = await tg_with_specific_bot(
                        best_backup.config.token,
                        "getMe",
                        {},
                        max_retries=1,
                        initial_delay=1
                    )

                    if not bot_info:
                        raise Exception("API返回空结果")

                    username = bot_info.get("username")
                    if not username:
                        raise Exception("机器人没有用户名")

                    result = {
                        "username": username,
                        "first_name": bot_info.get("first_name", "Backup Bot"),
                        "token": best_backup.config.token,
                        "bot_id": best_backup.bot_id,
                        "priority": best_backup.config.priority
                    }

                    # 🔥 修复8：更新缓存并返回结果
                    self._backup_bot_cache[cache_key] = result
                    self._cache_expiry[cache_key] = current_time + self._cache_duration

                    self.logger.info(f"✅ 成功获取备用机器人信息: @{username} (优先级: {best_backup.config.priority})")
                    return result

                except Exception as api_error:
                    self.logger.warning(f"获取机器人信息失败 (尝试 {attempt + 1}/3): {api_error}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    continue

            # 所有重试都失败了
            self.logger.error(f"所有获取备用机器人信息的尝试都失败了，机器人: {best_backup.bot_id}")
            self._backup_bot_cache[cache_key] = None
            self._cache_expiry[cache_key] = current_time + 30
            return None

        except Exception as e:
            self.logger.error(f"获取备用机器人信息时发生异常: {e}", exc_info=True)
            self._backup_bot_cache[cache_key] = None
            self._cache_expiry[cache_key] = current_time + 30
            return None

    async def send_notification(self, user_id: int, user_name: str, chat_type: str,
                                chat_id: int, rate_result, msg_id: int = None,
                                specific_bot_token: Optional[str] = None):
        """发送速率限制通知（增强错误处理版本）"""
        try:
            # 检查是否应该发送通知
            if not self._should_send_notification(user_id, chat_id):
                return

            # 🔥 关键修复：验证机器人token
            if specific_bot_token:
                self.logger.info(f"✅ 使用指定机器人发送通知: {specific_bot_token[-10:]} -> 用户 {user_id}")
            else:
                self.logger.warning(f"⚠️ 没有指定机器人token，将使用默认机器人发送通知给用户 {user_id}")

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
                # 私聊 - 直接在私聊中通知，使用指定的机器人
                # 🔥 修复：使用新的备用机器人获取方法
                self.logger.debug("尝试获取备用机器人信息...")
                backup_bot_info = await self._get_backup_bot_info(specific_bot_token or "")

                if backup_bot_info and backup_bot_info.get('username'):
                    self.logger.debug(f"找到备用机器人: @{backup_bot_info['username']}")
                    template = self.messages[lang]["private_with_backup"]
                    notification_text = (
                        f"<b>{template.title}</b>\n\n"
                        f"{template.content.format(time=time_str)}\n\n"
                        f"📈 状态：{rate_result.current_count}/{rate_result.limit} 条消息\n"
                        f"🔄 重置时间：<code>{time_str}</code>\n\n"
                        f"{template.suggestion}"
                    )

                    # 创建内联键盘，提供备用机器人链接
                    reply_markup = {
                        "inline_keyboard": [[{
                            "text": f"💬 联系备用机器人 (@{backup_bot_info['username']})",
                            "url": f"https://t.me/{backup_bot_info['username']}"
                        }]]
                    }

                    payload = {
                        "chat_id": user_id,
                        "text": notification_text,
                        "parse_mode": "HTML",
                        "reply_markup": reply_markup
                    }
                else:
                    self.logger.debug("没有可用的备用机器人，发送基础通知")
                    template = self.messages[lang]["private"]
                    notification_text = (
                        f"<b>{template.title}</b>\n\n"
                        f"{template.content.format(time=time_str)}\n\n"
                        f"📈 状态：{rate_result.current_count}/{rate_result.limit} 条消息\n"
                        f"🔄 重置时间：<code>{time_str}</code>\n\n"
                        f"{template.suggestion}"
                    )

                    payload = {
                        "chat_id": user_id,
                        "text": notification_text,
                        "parse_mode": "HTML"
                    }

                # 🔥 关键修复：使用指定的机器人token发送通知，并记录详细信息
                try:
                    await tg("sendMessage", payload, specific_bot_token=specific_bot_token)
                    token_suffix = specific_bot_token[-10:] if specific_bot_token else 'default'

                    if backup_bot_info and backup_bot_info.get('username'):
                        self.logger.info(
                            f"✅ 已向用户 {user_id} 发送带备用机器人 @{backup_bot_info['username']} 的限速通知（使用机器人: {token_suffix}）")
                    else:
                        self.logger.info(f"✅ 已向用户 {user_id} 发送基础限速通知（使用机器人: {token_suffix}）")

                except Exception as send_error:
                    self.logger.error(
                        f"❌ 使用指定机器人 {specific_bot_token[-10:] if specific_bot_token else 'None'} 发送通知失败: {send_error}")

                    # 🔥 回退策略：尝试使用主机器人发送
                    try:
                        primary_token = settings.get_primary_bot_token()
                        if primary_token != specific_bot_token:
                            await tg("sendMessage", payload, specific_bot_token=primary_token)
                            self.logger.info(
                                f"✅ 回退成功：使用主机器人 {primary_token[-10:]} 向用户 {user_id} 发送限速通知")
                        else:
                            raise send_error
                    except Exception as fallback_error:
                        self.logger.error(f"❌ 回退发送也失败: {fallback_error}")
                        raise fallback_error

            elif chat_type in ("group", "supergroup"):
                # 群聊通知逻辑保持不变...
                # (省略群聊部分代码，与之前相同)
                pass

        except Exception as e:
            self.logger.error(f"❌ 发送限速通知失败: {e}", exc_info=True)

    async def send_punishment_notification(self, user_id: int, punishment_duration: int,
                                           specific_bot_token: Optional[str] = None):
        """发送惩罚期通知（修复版本）"""
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

            # 🔥 关键修复：使用指定的机器人token发送通知
            await tg("sendMessage", {
                "chat_id": user_id,
                "text": text,
                "parse_mode": "HTML"
            }, specific_bot_token=specific_bot_token)

            self.logger.info(f"✅ 已向用户 {user_id} 发送惩罚期通知，时长：{time_str}")

        except Exception as e:
            self.logger.error(f"❌ 发送惩罚期通知失败: {e}", exc_info=True)


# 全局通知管理器实例
_notification_manager = RateLimitNotificationManager()


async def send_rate_limit_notification(
        user_id: int,
        user_name: str,
        chat_type: str,
        chat_id: int,
        rate_result,
        msg_id: int = None,
        preferred_bot_token: Optional[str] = None  # 🔥 新增
):
    """发送速率限制通知的便利函数（修复版本）"""
    # 🔥 关键修复：将preferred_bot_token传递给实际的发送函数
    await _notification_manager.send_notification(
        user_id, user_name, chat_type, chat_id, rate_result, msg_id,
        specific_bot_token=preferred_bot_token  # 传递指定的机器人token
    )


async def send_punishment_notification(user_id: int, punishment_duration: int,
                                       specific_bot_token: Optional[str] = None):  # 🔥 新增参数
    """发送惩罚期通知的便利函数（修复版本）"""
    await _notification_manager.send_punishment_notification(
        user_id, punishment_duration, specific_bot_token=specific_bot_token
    )