"""
服务器管理器

默认使用 Hypercorn 作为 ASGI 服务器
"""

import asyncio
import signal
import sys
from typing import Any, Dict, Optional

from loguru import logger

from ..utils import get_local_ip


class HypercornServer:
    """Hypercorn 服务器"""
    
    def __init__(self, app, host: str = "0.0.0.0", port: int = 8000, **kwargs):
        self.app = app
        self.host = host
        self.port = port
        self.kwargs = kwargs
        self._running = False
        self._import_hypercorn()
    
    def _import_hypercorn(self):
        """动态导入 Hypercorn"""
        try:
            import hypercorn.asyncio
            from hypercorn.config import Config
            self.hypercorn = hypercorn
            self.Config = Config
        except ImportError:
            raise ImportError("Hypercorn 未安装，请运行: pip install hypercorn")
    
    def start(self) -> None:
        """启动 Hypercorn 服务器"""
        try:
            # 配置 Hypercorn
            config = self.Config()
            config.bind = [f"{self.host}:{self.port}"]
            config.use_reloader = self.kwargs.get('reload', False)
            config.workers = self.kwargs.get('workers', 1)
            config.keep_alive_timeout = self.kwargs.get('keep_alive_timeout', 5)
            config.graceful_timeout = self.kwargs.get('graceful_timeout', 30)
            config.max_incomplete_request_size = self.kwargs.get('max_incomplete_request_size', 16 * 1024)
            config.websocket_max_size = self.kwargs.get('websocket_max_size', 16 * 1024 * 1024)
            
            # 应用其他配置
            for key, value in self.kwargs.items():
                if hasattr(config, key) and key not in ['reload']:
                    setattr(config, key, value)
            
            self._running = True
            
            # 运行服务器
            asyncio.run(self.hypercorn.asyncio.serve(self.app, config))
            
        except Exception as e:
            logger.error(f"Hypercorn 服务器启动失败: {e}")
            raise
        finally:
            self._running = False
    
    def stop(self) -> None:
        """停止 Hypercorn 服务器"""
        self._running = False
    
    @property
    def is_running(self) -> bool:
        """检查服务器是否运行中"""
        return self._running
    
    def get_url(self) -> str:
        """获取服务器 URL"""
        # 使用真实 IP 地址显示（如果 host 是 0.0.0.0）
        display_host = get_local_ip() if self.host == "0.0.0.0" else self.host
        return f"http://{display_host}:{self.port}"


class ServerManager:
    """简化的服务器管理器"""
    
    def __init__(self):
        self._current_server: Optional[HypercornServer] = None
    
    def start_server(
        self,
        app,
        host: str = "0.0.0.0",
        port: int = 8000,
        **kwargs
    ) -> None:
        """
        启动 Hypercorn 服务器
        
        Args:
            app: ASGI 应用
            host: 主机地址
            port: 端口号
            **kwargs: 其他配置参数
        """
        if self._current_server and self._current_server.is_running:
            logger.warning("服务器已在运行中，请先停止当前服务器")
            return
        
        self._current_server = HypercornServer(app, host, port, **kwargs)
        
        # 注册信号处理器
        self._register_signal_handlers()
        
        try:
            self._current_server.start()
        except KeyboardInterrupt:
            logger.info("收到中断信号，正在关闭服务器...")
        finally:
            self.stop_server()
    
    def stop_server(self) -> None:
        """停止当前服务器"""
        if self._current_server:
            self._current_server.stop()
            self._current_server = None
    
    def _register_signal_handlers(self) -> None:
        """注册信号处理器"""
        def signal_handler(signum, frame):
            logger.info(f"收到信号 {signum}，正在关闭服务器...")
            self.stop_server()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def get_server_info(self) -> Dict[str, Any]:
        """获取服务器信息"""
        if not self._current_server:
            return {"status": "not_running"}
        
        return {
            "status": "running" if self._current_server.is_running else "stopped",
            "url": self._current_server.get_url(),
            "host": self._current_server.host,
            "port": self._current_server.port,
        }


# 全局服务器管理器实例
server_manager = ServerManager()


def start_server(
    app,
    host: str = "0.0.0.0",
    port: int = 8000,
    **kwargs
) -> None:
    """启动服务器的便捷函数"""
    server_manager.start_server(app, host, port, **kwargs)