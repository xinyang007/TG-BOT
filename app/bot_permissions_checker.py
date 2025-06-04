import asyncio
from typing import Dict, List, Optional
from .tg_utils import tg_with_specific_bot
from .settings import settings
from .logging_config import get_logger

logger = get_logger("app.bot_permissions")


class BotPermissionsChecker:
    """机器人权限检查器"""

    def __init__(self):
        self.logger = get_logger("app.bot_permissions")

    async def check_bot_permissions(self, bot_token: str, support_group_id: str) -> Dict[str, bool]:
        """检查机器人在支持群组中的权限"""
        try:
            # 获取机器人在群组中的信息
            chat_member = await tg_with_specific_bot(
                bot_token, "getChatMember",
                {"chat_id": support_group_id, "user_id": await self._get_bot_id(bot_token)}
            )

            permissions = {
                "can_manage_topics": False,
                "can_delete_messages": False,
                "can_edit_messages": False,
                "can_pin_messages": False,
                "can_send_messages": False
            }

            if chat_member.get("status") == "administrator":
                # 检查管理员权限
                admin_permissions = chat_member.get("can_manage_topics", False)
                permissions["can_manage_topics"] = admin_permissions
                permissions["can_delete_messages"] = chat_member.get("can_delete_messages", False)
                permissions["can_edit_messages"] = chat_member.get("can_edit_messages", False)
                permissions["can_pin_messages"] = chat_member.get("can_pin_messages", False)
                permissions["can_send_messages"] = True  # 管理员通常可以发送消息
            elif chat_member.get("status") == "member":
                # 普通成员权限通常有限
                permissions["can_send_messages"] = True
                # 普通成员通常无法管理话题

            return permissions

        except Exception as e:
            self.logger.error(f"检查机器人权限失败: {e}")
            return {
                "can_manage_topics": False,
                "can_delete_messages": False,
                "can_edit_messages": False,
                "can_pin_messages": False,
                "can_send_messages": False,
                "error": str(e)
            }

    async def _get_bot_id(self, bot_token: str) -> int:
        """获取机器人的用户ID"""
        try:
            bot_info = await tg_with_specific_bot(bot_token, "getMe", {})
            return bot_info["id"]
        except Exception as e:
            self.logger.error(f"获取机器人ID失败: {e}")
            raise

    async def check_all_bots_permissions(self) -> Dict[str, Dict]:
        """检查所有机器人的权限"""
        if not getattr(settings, 'MULTI_BOT_ENABLED', False):
            # 单机器人模式
            return {
                "single_bot": await self.check_bot_permissions(
                    settings.BOT_TOKEN, settings.SUPPORT_GROUP_ID
                )
            }

        results = {}
        enabled_bots = settings.get_enabled_bots()

        for bot_config in enabled_bots:
            bot_name = f"{bot_config.name} ({bot_config.token[-10:]})"
            results[bot_name] = await self.check_bot_permissions(
                bot_config.token, settings.SUPPORT_GROUP_ID
            )

        return results

    async def ensure_bot_has_topic_permissions(self, bot_token: str) -> bool:
        """确保机器人有创建话题的权限"""
        try:
            permissions = await self.check_bot_permissions(bot_token, settings.SUPPORT_GROUP_ID)

            if not permissions.get("can_manage_topics", False):
                self.logger.warning(
                    f"机器人 {bot_token[-10:]} 缺少管理话题权限，无法创建话题。"
                    f"请在群组中给予该机器人管理员权限，并启用 'manage_topics' 权限。"
                )
                return False

            return True

        except Exception as e:
            self.logger.error(f"检查机器人话题权限失败: {e}")
            return False


# 全局权限检查器实例
_permissions_checker = BotPermissionsChecker()


async def check_bot_permissions(bot_token: str = None) -> Dict:
    """检查机器人权限的便利函数"""
    if bot_token:
        return await _permissions_checker.check_bot_permissions(bot_token, settings.SUPPORT_GROUP_ID)
    else:
        return await _permissions_checker.check_all_bots_permissions()


async def ensure_all_bots_have_permissions() -> Dict[str, bool]:
    """确保所有机器人都有必要的权限"""
    results = {}

    if not getattr(settings, 'MULTI_BOT_ENABLED', False):
        # 单机器人模式
        results["single_bot"] = await _permissions_checker.ensure_bot_has_topic_permissions(
            settings.BOT_TOKEN
        )
    else:
        # 多机器人模式
        enabled_bots = settings.get_enabled_bots()
        for bot_config in enabled_bots:
            bot_name = f"{bot_config.name}"
            results[bot_name] = await _permissions_checker.ensure_bot_has_topic_permissions(
                bot_config.token
            )

    return results