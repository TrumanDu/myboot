"""
服务器管理器

默认使用 Hypercorn 作为 ASGI 服务器，支持多 workers 模式
"""

import asyncio
import multiprocessing
import os
import signal
import socket
import sys
from typing import Any, Dict, List, Optional

from loguru import logger

from ..utils import get_local_ip

# 别名映射：调用方传入的名称 → Hypercorn Config 属性名
# application.py 白名单已处理大部分别名，此处仅保留通用兜底映射
# 参考：https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html
HYPERCORN_CONFIG_ALIASES: Dict[str, str] = {
    "reload": "use_reloader",
    "max_incomplete_request_size": "h11_max_incomplete_size",
}

# 序列化 Config 到子进程时跳过：只读或由其他字段推导，子进程 asyncio.serve() 不需/不应重设
_HYPERCORN_SERIALIZE_SKIP = frozenset({
    "ssl_enabled",  # Hypercorn：只读，由 certfile/keyfile 推导
    "workers",      # asyncio.serve() 不用此字段；多进程由外层 fork
})

# myboot 内部消耗、不传给 Hypercorn 的键
_MYBOOT_INTERNAL = {"host", "port", "app_path"}


def _create_socket_with_reuseport(host: str, port: int, backlog: int = 100) -> socket.socket:
    """
    创建启用 SO_REUSEPORT 的 socket（仅 Linux/macOS）
    
    Args:
        host: 绑定的主机地址
        port: 绑定的端口号
        
    Returns:
        已绑定并监听的 socket 对象
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # Linux/macOS 支持 SO_REUSEPORT
    if hasattr(socket, 'SO_REUSEPORT'):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    
    sock.setblocking(False)
    sock.bind((host, port))
    sock.listen(backlog)
    return sock


def _resolve_app_from_path(app_path: str):
    """
    从模块路径解析 ASGI 应用
    
    支持的格式:
        - "module.path:app"                    -> 获取 module.path 模块的 app 属性
        - "module.path:app.get_fastapi_app()"  -> 获取 app 对象后调用 get_fastapi_app()
        - "module.path:create_app()"           -> 调用 module.path 模块的 create_app() 函数
    
    Args:
        app_path: 应用模块路径
        
    Returns:
        ASGI 应用实例
    """
    import importlib
    import re
    
    # 解析模块路径和应用名
    if ":" in app_path:
        module_path, app_expr = app_path.rsplit(":", 1)
    else:
        module_path, app_expr = app_path, "app"
    
    # 导入模块
    # 如果请求的是 main 模块且 __main__ 已存在，直接使用 __main__
    if module_path == "main" and "__main__" in sys.modules:
        module = sys.modules["__main__"]
    else:
        module = importlib.import_module(module_path)
    
    # 解析表达式，支持链式调用如 "app.get_fastapi_app()"
    # 使用简单的解析器处理 attr 和 method() 调用
    parts = re.split(r'\.(?![^(]*\))', app_expr)  # 按 . 分割，但不分割括号内的点
    
    result = module
    for part in parts:
        part = part.strip()
        if not part:
            continue
            
        # 检查是否是方法调用 (以 () 结尾)
        if part.endswith("()"):
            method_name = part[:-2]
            result = getattr(result, method_name)()
        else:
            result = getattr(result, part)
    
    return result


def _worker_serve(
    app_path: str, 
    config_dict: dict, 
    worker_id: int, 
    total_workers: int,
    socket_fd: Optional[int] = None
):
    """
    Worker 进程入口函数
    
    Args:
        app_path: 应用模块路径，格式为 "module.path:app_name"
        config_dict: Hypercorn 配置字典
        worker_id: Worker 进程 ID (从 1 开始)
        total_workers: 总 worker 数量
        socket_fd: 预创建的 socket 文件描述符（Linux/macOS 多 worker 模式）
    """
    # 设置环境变量，供 Application 读取
    os.environ["MYBOOT_WORKER_ID"] = str(worker_id)
    os.environ["MYBOOT_WORKER_COUNT"] = str(total_workers)
    os.environ["MYBOOT_IS_PRIMARY_WORKER"] = "1" if worker_id == 1 else "0"
    
    # 重新初始化 worker 日志（设置环境变量后才能正确检测多 worker 模式）
    from .logger import setup_worker_logging
    setup_worker_logging(worker_id, total_workers)
    
    try:
        import hypercorn.asyncio
        from hypercorn.config import Config
    except ImportError:
        raise ImportError("Hypercorn 未安装，请运行: pip install hypercorn")
    
    # 从模块路径加载 app
    app = _resolve_app_from_path(app_path)
    
    # 重建配置
    config = Config()
    for key, value in config_dict.items():
        if hasattr(config, key):
            setattr(config, key, value)
    
    # 如果提供了 socket_fd，使用 fd:// 绑定方式
    if socket_fd is not None:
        config.bind = [f"fd://{socket_fd}"]
    
    logger.info(f"Worker-{worker_id}/{total_workers} 启动中... (primary={worker_id == 1})")
    
    # 运行服务器
    asyncio.run(hypercorn.asyncio.serve(app, config))


class HypercornServer:
    """Hypercorn 服务器，支持多 workers 模式"""
    
    def __init__(self, app, host: str = "0.0.0.0", port: int = 8000, **kwargs):
        self.app = app
        self.host = host
        self.port = port
        self.kwargs = kwargs
        self._running = False
        self._workers: List[multiprocessing.Process] = []
        self._import_hypercorn()
    
    def _import_hypercorn(self):
        """动态导入 Hypercorn"""
        try:
            import hypercorn.asyncio
            from hypercorn.config import Config
            self.hypercorn = hypercorn
            self._config_class = Config
        except ImportError:
            raise ImportError("Hypercorn 未安装，请运行: pip install hypercorn")
    
    def _build_config(self) -> Any:
        """构建 Hypercorn Config：将 kwargs 通过别名映射写入 Config 属性。"""
        config = self._config_class()
        config.bind = [f"{self.host}:{self.port}"]
        for key, value in self.kwargs.items():
            if key in _MYBOOT_INTERNAL:
                continue
            hypercorn_key = HYPERCORN_CONFIG_ALIASES.get(key, key)
            if hasattr(config, hypercorn_key):
                setattr(config, hypercorn_key, value)
        return config

    def _config_to_dict(self, config) -> dict:
        """将 Hypercorn Config 序列化为字典，供多 worker 子进程重建使用。"""
        out = {}
        for attr in dir(config):
            if attr.startswith("_"):
                continue
            if attr in _HYPERCORN_SERIALIZE_SKIP:
                continue
            val = getattr(config, attr, None)
            if callable(val):
                continue
            if isinstance(val, (str, int, float, bool, list, dict, type(None))):
                out[attr] = val
        return out
    
    def start(self, app_path: Optional[str] = None) -> None:
        """
        启动 Hypercorn 服务器
        
        Args:
            app_path: 应用模块路径（多 workers 模式必需），格式为 "module.path:app_name"
                      例如: "myapp.main:app" 或 "myapp.main:create_app()"
        """
        try:
            config = self._build_config()
            workers = self.kwargs.get('workers', 1)
            
            self._running = True
            
            if workers > 1:
                # 多 workers 模式
                if not app_path:
                    logger.warning(
                        "多 workers 模式需要提供 app_path 参数（如 'myapp.main:app'），"
                        "当前回退到单进程模式"
                    )
                    asyncio.run(self.hypercorn.asyncio.serve(self.app, config))
                else:
                    self._start_multiple_workers(app_path, config, workers)
            else:
                # 单 worker 模式：直接运行
                asyncio.run(self.hypercorn.asyncio.serve(self.app, config))
            
        except Exception as e:
            logger.error(f"Hypercorn 服务器启动失败: {e}")
            raise
        finally:
            self._running = False
            self._cleanup_workers()
    
    def _start_multiple_workers(self, app_path: str, config, workers: int) -> None:
        """
        启动多个 worker 进程
        
        Args:
            app_path: 应用模块路径
            config: Hypercorn 配置
            workers: worker 数量
        """
        # Windows 需要使用 spawn
        if sys.platform == 'win32':
            multiprocessing.set_start_method('spawn', force=True)
        
        config_dict = self._config_to_dict(config)
        socket_fd: Optional[int] = None
        shared_socket: Optional[socket.socket] = None
        
        # Linux/macOS: 使用 SO_REUSEPORT 预创建 socket，允许多个进程绑定同一端口
        backlog = config_dict.get("backlog", 100)
        if sys.platform != 'win32':
            try:
                shared_socket = _create_socket_with_reuseport(self.host, self.port, backlog=backlog)
                socket_fd = shared_socket.fileno()
                logger.info(f"已创建共享 socket (fd={socket_fd}), 启用 SO_REUSEPORT")
            except Exception as e:
                logger.warning(f"创建共享 socket 失败: {e}，回退到普通模式")
                shared_socket = None
                socket_fd = None
        
        logger.info(f"启动 {workers} 个 worker 进程...")
        
        # 创建并启动 worker 进程
        for i in range(workers):
            process = multiprocessing.Process(
                target=_worker_serve,
                args=(app_path, config_dict, i + 1, workers, socket_fd),
                name=f"hypercorn-worker-{i + 1}"
            )
            process.start()
            self._workers.append(process)
            logger.info(f"Worker-{i + 1}/{workers} 已启动 (PID: {process.pid})")
        
        # 主进程等待所有 worker
        try:
            for process in self._workers:
                process.join()
        except KeyboardInterrupt:
            logger.info("收到中断信号，正在关闭所有 workers...")
            self._cleanup_workers()
        finally:
            # 关闭主进程中的共享 socket
            if shared_socket:
                try:
                    shared_socket.close()
                except Exception:
                    pass
    
    def _cleanup_workers(self) -> None:
        """清理所有 worker 进程"""
        for process in self._workers:
            if process.is_alive():
                logger.info(f"终止 Worker (PID: {process.pid})...")
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
        self._workers.clear()
    
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
        app_path: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        启动 Hypercorn 服务器
        
        Args:
            app: ASGI 应用
            host: 主机地址
            port: 端口号
            app_path: 应用模块路径（多 workers 模式必需），格式为 "module.path:app_name"
                      例如: "myapp.main:app"
            **kwargs: 其他配置参数，包括:
                - workers: worker 进程数量，默认 1
                - reload: 是否启用热重载
                - keep_alive_timeout: keep-alive 超时时间
                - graceful_timeout: 优雅关闭超时时间
        """
        if self._current_server and self._current_server.is_running:
            logger.warning("服务器已在运行中，请先停止当前服务器")
            return
        
        self._current_server = HypercornServer(app, host, port, **kwargs)
        
        # 注册信号处理器
        self._register_signal_handlers()
        
        try:
            self._current_server.start(app_path=app_path)
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
    app_path: Optional[str] = None,
    **kwargs
) -> None:
    """
    启动服务器的便捷函数
    
    Args:
        app: ASGI 应用
        host: 主机地址
        port: 端口号
        app_path: 应用模块路径（多 workers 模式必需），格式为 "module.path:app_name"
        **kwargs: 其他配置参数
    
    Example:
        # 单 worker 模式
        start_server(app, host="0.0.0.0", port=8000)
        
        # 多 workers 模式（4个进程）
        start_server(
            app, 
            host="0.0.0.0", 
            port=8000,
            app_path="myapp.main:app",
            workers=4
        )
    """
    server_manager.start_server(app, host, port, app_path=app_path, **kwargs)