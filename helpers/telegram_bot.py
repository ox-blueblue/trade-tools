import os
import ssl
from typing import Dict, Any, Optional

import certifi
import aiohttp

BASE_URL = "https://api.telegram.org/bot"

class TelegramBot:
    def __init__(self, token: str, chat_id: str, base_url: Optional[str] = None):
        self.token = token
        self.chat_id = chat_id
        self.base_url = base_url if base_url else BASE_URL
        self.api_url = f"{self.base_url.rstrip('/')}{self.token}"
        self.session = None

    async def __aenter__(self):
        await self._create_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def _create_session(self):
        """创建 aiohttp 会话"""
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            connector=aiohttp.TCPConnector(ssl=ssl_context)
        )

    async def close(self):
        """关闭 aiohttp 会话"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def send_text(self, content: str, parse_mode: str = "HTML") -> Dict[str, Any]:
        """异步发送文本消息到 Telegram"""
        payload = {
            "chat_id": self.chat_id,
            "text": content,
            "parse_mode": parse_mode
        }
        return await self._send_message("sendMessage", payload)

    async def _send_message(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """内部方法，异步发送消息到 Telegram API"""
        url = f"{self.api_url}/{method}"
        
        try:
            # 确保会话已创建
            if not self.session or self.session.closed:
                await self._create_session()
            
            async with self.session.post(url, json=payload) as response:
                response_data = await response.json()
                if not response_data.get("ok", False):
                    print(f"Telegram send message failed: {response_data}")
                return response_data
        except Exception as e:
            print(f"Telegram send message failed: {e}")
            return {"ok": False, "error": str(e)}