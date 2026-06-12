"""
MyBoot 应用程序主类

提供类似 Spring Boot 的自动配置和快速启动功能
"""

import asyncio
import os
import signal
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, List, Optional, Type

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from .auto_configuration import auto_discover, apply_auto_configuration
from .config import get_settings
from .container import Container
from .logger import setup_logging
from .scheduler import Scheduler
from .server import ServerManager
from ..exceptions import MyBootException
from ..utils import get_local_ip
from ..web.middleware import Middleware

# 全局应用实例注册表（用于在路由函数中获取当前应用实例）
_current_app: Optional['Application'] = None


def app() -> 'Application':
    """获取当前应用实例"""
    if _current_app is None:
        raise RuntimeError("应用实例未初始化，请确保应用已创建并启动")
    return _current_app


class Application:
    """MyBoot 应用程序主类"""

    def __init__(
            self,
            name: str = "MyBoot App",
            config_file: Optional[str] = None,
            **kwargs
    ):
        """
        初始化应用程序
        
        Args:
            name: 应用程序名称
            config_file: 配置文件路径
            **kwargs: 其他配置参数
        """
        self.name = name
        self.config = get_settings(config_file)

        # 应用配置
        self._apply_config(kwargs)

        # 获取应用版本号（从配置文件读取，默认 0.0.1）
        self.version = self.config.get("app.version", "0.0.1")

        # 初始化 loguru 日志系统（包括第三方库日志级别配置）
        setup_logging(self.config)

        # Prometheus 多进程指标环境（须在 prometheus_client 被 import 之前配置）
        from ..metrics import setup_multiproc_env
        setup_multiproc_env(self.config, self.name)

        self.logger = logger.bind(name=self.name)
        
        # Worker 信息（多进程模式下由环境变量设置）
        self._worker_id = int(os.environ.get("MYBOOT_WORKER_ID", "1"))
        self._worker_count = int(os.environ.get("MYBOOT_WORKER_COUNT", "1"))
        self._is_primary_worker = os.environ.get("MYBOOT_IS_PRIMARY_WORKER", "1") == "1"
        
        # Scheduler 默认只在 primary worker 启动（可通过 scheduler.on_all_workers
        # 全局配置覆盖）。任务级 all_workers=True 的任务在非 primary worker 注册时，
        # 注册门控会调用 scheduler.enable() 按需启用调度器实例。
        scheduler_on_all_workers = self.config.get("scheduler.on_all_workers", False)
        scheduler_globally_enabled = self.config.get("scheduler.enabled", True)
        self._scheduler_enabled = (
            self._is_primary_worker or scheduler_on_all_workers
        ) and scheduler_globally_enabled
        self.scheduler = Scheduler(config=self.config, enabled=self._scheduler_enabled)

        # 中间件列表
        self.middlewares: List[Middleware] = []

        # 路由处理器
        self.route_handlers: Dict[str, Callable] = {}

        # 服务注册表
        self.services: Dict[str, Any] = {}

        # 模型注册表
        self.models: Dict[str, Any] = {}

        # 客户端注册表
        self.clients: Dict[str, Any] = {}

        # 组件注册表
        self.components: Dict[str, Any] = {}

        # 统一容器接口（支持从 container、services、clients 中获取实例）
        self.container = Container(self)

        # 启动钩子
        self.startup_hooks: List[Callable] = []
        self.shutdown_hooks: List[Callable] = []

        # Worker 生命周期钩子（@on_worker_start / @on_worker_stop）
        # 每个 worker 进程的 lifespan 中各触发一次；单 worker 模式下也触发一次
        self.worker_start_hooks: List[Callable] = []
        self.worker_stop_hooks: List[Callable] = []

        # FastAPI 应用实例
        self._fastapi_app: Optional[FastAPI] = None

        # 服务器实例
        self._server: Optional[Any] = None

        # 注册信号处理器
        self._register_signal_handlers()

        # 创建 FastAPI 应用
        self._fastapi_app = self._create_fastapi_app()

        # 自动配置标志
        self.auto_configuration_enabled = kwargs.get('auto_configuration', True)
        self.auto_discover_package = kwargs.get('auto_discover_package', 'app')

        # 服务器管理器
        self.server_manager = ServerManager()

        # 注册为当前应用实例
        global _current_app
        _current_app = self

    def _apply_config(self, kwargs: Dict[str, Any]) -> None:
        """应用配置参数"""
        for key, value in kwargs.items():
            self.config.set(key, value)

    def _register_signal_handlers(self) -> None:
        """注册信号处理器"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum: int, frame) -> None:
        """信号处理器"""
        self.logger.info(f"收到信号 {signum}，开始优雅关闭...")
        asyncio.create_task(self.shutdown())

    def add_middleware(self, middleware: Middleware) -> None:
        """添加中间件"""
        self.middlewares.append(middleware)
        self.logger.debug(f"已添加中间件: {middleware.__class__.__name__}")

    def add_startup_hook(self, hook: Callable) -> None:
        """添加启动钩子"""
        self.startup_hooks.append(hook)
        self.logger.debug(f"已添加启动钩子: {hook.__name__}")

    def add_shutdown_hook(self, hook: Callable) -> None:
        """添加关闭钩子"""
        self.shutdown_hooks.append(hook)
        self.logger.debug(f"已添加关闭钩子: {hook.__name__}")

    def add_worker_start_hook(self, hook: Callable) -> None:
        """添加 worker 启动钩子

        在每个 worker 进程的 lifespan 启动阶段触发（startup_hooks 之后、
        调度器启动之前），单 worker 模式下也触发一次。
        """
        self.worker_start_hooks.append(hook)
        self.logger.debug(f"已添加 worker 启动钩子: {hook.__name__}")

    def add_worker_stop_hook(self, hook: Callable) -> None:
        """添加 worker 停止钩子

        在每个 worker 进程的 lifespan 关闭阶段触发（调度器停止之后、
        shutdown_hooks 之前）。

        注意：Windows 多 worker 模式下父进程通过 terminate()（硬终止）
        清理 worker 进程，lifespan 关闭阶段可能不会执行，
        因此 worker 停止钩子在 Windows 上不保证触发。
        """
        self.worker_stop_hooks.append(hook)
        self.logger.debug(f"已添加 worker 停止钩子: {hook.__name__}")

    def register_service(self, name: str, service: Any) -> None:
        """注册服务"""
        self.services[name] = service
        self.logger.debug(f"已注册服务: {name}")

    def get_service(self, name: str) -> Any:
        """获取服务

        优先返回 app.services 中的单例实例；非单例（request/factory）服务
        不在 app.services 中预存，回退到 DI 容器按需解析。
        """
        if name in self.services:
            return self.services[name]
        di_container = getattr(self, 'di_container', None)
        if di_container is not None and di_container.has_service(name):
            return di_container.get_service(name)
        return None

    def has_service(self, name: str) -> bool:
        """检查是否有服务"""
        return name in self.services

    def get_client(self, name: str) -> Any:
        """获取客户端"""
        return self.clients.get(name)

    def has_client(self, name: str) -> bool:
        """检查是否有客户端"""
        return name in self.clients

    def get_component(self, name: str) -> Any:
        """获取组件"""
        return self.components.get(name)

    def has_component(self, name: str) -> bool:
        """检查是否有组件"""
        return name in self.components

    def route(
            self,
            path: str,
            methods: Optional[List[str]] = None,
            **kwargs
    ) -> Callable:
        """
        装饰器：注册路由
        
        Args:
            path: 路由路径
            methods: HTTP 方法列表
            **kwargs: 其他 FastAPI 路由参数
        """
        if methods is None:
            methods = ["GET"]

        def decorator(func: Callable) -> Callable:
            # 存储路由处理器
            route_key = f"{','.join(methods)}:{path}"
            self.route_handlers[route_key] = func

            self.logger.debug(f"已注册路由: {methods} {path} -> {func.__name__}")
            return func

        return decorator

    def get(self, path: str, **kwargs) -> Callable:
        """GET 路由装饰器"""
        return self.route(path, ["GET"], **kwargs)

    def post(self, path: str, **kwargs) -> Callable:
        """POST 路由装饰器"""
        return self.route(path, ["POST"], **kwargs)

    def put(self, path: str, **kwargs) -> Callable:
        """PUT 路由装饰器"""
        return self.route(path, ["PUT"], **kwargs)

    def delete(self, path: str, **kwargs) -> Callable:
        """DELETE 路由装饰器"""
        return self.route(path, ["DELETE"], **kwargs)

    def patch(self, path: str, **kwargs) -> Callable:
        """PATCH 路由装饰器"""
        return self.route(path, ["PATCH"], **kwargs)

    def _create_fastapi_app(self) -> FastAPI:
        """创建 FastAPI 应用实例"""

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            """应用生命周期管理"""

            # 执行启动钩子
            for hook in self.startup_hooks:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook()
                    else:
                        hook()
                except Exception as e:
                    self.logger.error(f"启动钩子执行失败: {e}", exc_info=True)

            # 执行 worker 启动钩子（每个 worker 进程的 lifespan 各触发一次，
            # 位于 startup_hooks 之后、调度器启动之前）
            for hook in self.worker_start_hooks:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook()
                    else:
                        hook()
                except Exception as e:
                    self.logger.error(f"Worker 启动钩子执行失败: {e}", exc_info=True)

            # 启动调度器
            if self.scheduler.has_jobs():
                self.scheduler.start()
                self.logger.info("✅ 任务调度器已启动")

            yield

            # 关闭
            self.logger.info(f"🛑 关闭 {self.name}...")

            # 停止调度器
            if self.scheduler.is_running():
                self.scheduler.stop()
                self.logger.info("✅ 任务调度器已停止")

            # 执行 worker 停止钩子（调度器停止之后、shutdown_hooks 之前）
            # 注意：Windows 多 worker 模式下父进程使用 terminate() 硬终止
            # worker，lifespan 关闭阶段可能不执行，stop 钩子不保证触发
            for hook in self.worker_stop_hooks:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook()
                    else:
                        hook()
                except Exception as e:
                    self.logger.error(f"Worker 停止钩子执行失败: {e}", exc_info=True)

            # 自动关闭 client：对具有 close() 方法的 client 实例兜底调用
            # （worker_stop_hooks 之后，shutdown_hooks 之前）。
            # 应用若已自行 close，建议实现为幂等；二次 close 的异常降为 warning
            for client_name, client_instance in list(self.clients.items()):
                close_method = getattr(client_instance, "close", None)
                if not callable(close_method):
                    continue
                try:
                    if asyncio.iscoroutinefunction(close_method):
                        await close_method()
                    else:
                        close_method()
                    self.logger.debug(f"Client 已自动关闭: {client_name}")
                except Exception as e:
                    self.logger.warning(f"Client '{client_name}' close() 异常（已忽略）: {e}")

            # 执行关闭钩子
            for hook in self.shutdown_hooks:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook()
                    else:
                        hook()
                except Exception as e:
                    self.logger.error(f"关闭钩子执行失败: {e}", exc_info=True)

            # Prometheus multiproc：标记本进程退出，清理 gauge 残留文件
            # （内部已兜底 try/except，此处再防御一层确保关闭流程不被打断）
            try:
                from ..metrics import mark_current_process_dead
                mark_current_process_dead()
            except Exception:
                pass

        # 创建 FastAPI 应用

        app = FastAPI(
            title=self.name,
            version=self.version,
            lifespan=lifespan,
        )

        # 添加 CORS 中间件（如果配置了 server.cors）
        cors_config = self.config.get("server.cors")
        if cors_config:
            app.add_middleware(
                CORSMiddleware,
                allow_origins=cors_config.get("allow_origins", ["*"]),
                allow_credentials=cors_config.get("allow_credentials", True),
                allow_methods=cors_config.get("allow_methods", ["*"]),
                allow_headers=cors_config.get("allow_headers", ["*"]),
            )
            self.logger.debug("CORS 中间件已启用")

        # Prometheus 指标（metrics.enabled 时启用，可选依赖缺失只告警不报错）
        from ..metrics import is_available as metrics_available, is_enabled as metrics_enabled
        metrics_path = str(self.config.get("metrics.path", "/metrics") or "/metrics")
        metrics_ready = False
        if metrics_enabled(self.config):
            if metrics_available():
                metrics_ready = True
                from ..metrics import _coerce_bool
                if _coerce_bool(self.config.get("metrics.http_metrics", True), True):
                    from ..metrics import HttpMetricsMiddleware
                    app.add_middleware(HttpMetricsMiddleware, metrics_path=metrics_path)
                    self.logger.debug("HTTP 指标中间件已启用")
            else:
                self.logger.warning(
                    "metrics.enabled=true 但未安装 prometheus-client，"
                    "指标功能不可用。安装方式: pip install myboot[metrics]"
                )

        # 添加自定义中间件
        for middleware in self.middlewares:
            app.add_middleware(middleware.middleware_class, **middleware.kwargs)

        # 添加响应格式化中间件（最后添加，因为它会最先执行）
        # FastAPI 中间件是后进先出（LIFO），所以最后添加的中间件会最先处理响应
        response_format_enabled = self.config.get("server.response_format.enabled", True)
        if response_format_enabled:
            from myboot.web.middleware import ResponseFormatterMiddleware
            exclude_paths = list(self.config.get("server.response_format.exclude_paths", []) or [])
            if metrics_ready:
                # metrics 端点输出 Prometheus 文本格式，不做 JSON 包装
                exclude_paths.append(metrics_path)
            app.add_middleware(
                ResponseFormatterMiddleware,
                exclude_paths=exclude_paths,
                auto_wrap=True
            )
            self.logger.debug("响应格式化中间件已启用")

        # 注册路由
        self._register_routes(app)

        # 注册异常处理器
        self._register_exception_handlers(app)

        # 添加健康检查端点
        self._add_health_endpoints(app)

        # 挂载 Prometheus 指标端点（懒初始化，首次请求才 import prometheus_client）
        if metrics_ready:
            from ..metrics import make_metrics_asgi_app
            app.mount(metrics_path, make_metrics_asgi_app())
            self.logger.debug(f"Prometheus 指标端点已挂载: {metrics_path}")

        return app

    def _register_routes(self, app: FastAPI) -> None:
        """注册路由到 FastAPI 应用"""
        for route_key, handler in self.route_handlers.items():
            methods, path = route_key.split(":", 1)
            method_list = methods.split(",")

            # 添加路由到 FastAPI
            app.add_api_route(
                path,
                handler,
                methods=method_list,
                name=handler.__name__
            )

    def _register_exception_handlers(self, app: FastAPI) -> None:
        """注册异常处理器"""
        
        def _extract_error_messages(errors: list) -> list:
            """从错误列表中提取错误消息"""
            messages = []
            for error in errors:
                msg = error.get('msg', 'Validation Error')
                # 移除 "Value error, " 前缀
                if msg.startswith('Value error, '):
                    msg = msg[len('Value error, '):]
                messages.append(msg)
            return messages

        @app.exception_handler(MyBootException)
        async def myboot_exception_handler(request: Request, exc: MyBootException):
            """MyBoot 异常处理器"""
            self.logger.error(f"MyBoot 异常: {exc}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "code": 500,
                    "message": "Internal Server Error",
                    "data": {
                        "type": exc.__class__.__name__
                    }
                }
            )

        @app.exception_handler(HTTPException)
        async def http_exception_handler(request: Request, exc: HTTPException):
            """HTTP 异常处理器"""
            self.logger.warning(f"HTTP 异常: {exc.status_code} - {exc.detail}")
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "success": False,
                    "code": exc.status_code,
                    "message": "HTTP Error"
                }
            )

        @app.exception_handler(RequestValidationError)
        async def validation_exception_handler(request: Request, exc: RequestValidationError):
            """请求验证异常处理器 - 返回友好的错误信息"""
            errors = exc.errors()
            # 只提取错误消息
            error_messages = _extract_error_messages(errors)
            
            # 使用第一个错误消息作为主要错误信息
            error_msg = error_messages[0] if error_messages else 'Validation Error'
            
            self.logger.warning(f"请求验证失败: {error_messages}")
            return JSONResponse(
                status_code=422,
                content={
                    "success": False,
                    "code": 422,
                    "message": error_msg,
                    "data": {
                        "fieldErrors": error_messages
                    }
                }
            )

        @app.exception_handler(Exception)
        async def global_exception_handler(request: Request, exc: Exception):
            """全局异常处理器"""
            self.logger.error(f"未处理的异常: {exc}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "code": 500,
                    "message": "Internal Server Error",
                    "data": {
                        "type": exc.__class__.__name__
                    }
                }
            )

    def _add_health_endpoints(self, app: FastAPI) -> None:
        """添加健康检查端点"""

        @app.get("/health")
        async def health_check():
            """健康检查端点"""
            return {
                "status": "healthy",
                "app": self.name,
                "version": self.version,
                "uptime": "running"
            }

        @app.get("/health/ready")
        async def readiness_check():
            """就绪检查端点"""
            return {
                "status": "ready",
                "app": self.name,
                "services": {
                    "scheduler": self.scheduler.is_running() if self.scheduler.has_jobs() else "disabled"
                }
            }

        @app.get("/health/live")
        async def liveness_check():
            """存活检查端点"""
            return {
                "status": "alive",
                "app": self.name
            }

    def run(
            self,
            host: str = "0.0.0.0",
            port: int = 8000,
            reload: bool = False,
            workers: int = 1,
            app_path: Optional[str] = None,
            **kwargs
    ) -> None:
        """
        运行应用程序
        
        Args:
            host: 主机地址
            port: 端口号
            reload: 是否开启热重载
            workers: 工作进程数
            app_path: 应用模块路径（多 workers 模式必需），格式为 "module.path:app_name"
                      例如: "main:app" 表示从 main.py 导入 app 变量
                      如果 app 是通过 get_fastapi_app() 获取，使用 "main:app.get_fastapi_app()"
            **kwargs: 其他服务器参数
            
        Example:
            # 单进程模式（默认）
            app.run(host="0.0.0.0", port=8000)
            
            # 多进程模式（4个 workers）
            app.run(
                host="0.0.0.0",
                port=8000,
                workers=4,
                app_path="main:app.get_fastapi_app()"
            )
        """
        # 从配置中获取参数
        host = self.config.get("server.host", host)
        port = self.config.get("server.port", port)
        reload = self.config.get("server.reload", reload)
        workers = self.config.get("server.workers", workers)
        app_path = self.config.get("server.app_path", app_path)

        # 白名单方式收集 server.* 中的 Hypercorn 配置
        # 别名转换（如 max_incomplete_request_size）由 server.py 的 HYPERCORN_CONFIG_ALIASES 统一处理
        server_section = self.config.get("server") or {}
        server_kwargs = {}
        for k in ("backlog", "read_timeout", "keep_alive_timeout", "graceful_timeout",
                   "reloader", "use_reloader", "max_incomplete_request_size"):
            if k in server_section:
                server_kwargs[k] = server_section[k]
        # 合并：配置文件 < run(**kwargs) 覆盖
        run_kwargs = {**server_kwargs, **kwargs}

        # 自动发现和配置
        # 多 worker 模式（workers > 1 且提供了 app_path）下，父进程只做 AST 自动发现
        # （无副作用，预热 .myboot_cache_*.json 供子进程使用），实例化（apply_auto_configuration）
        # 延迟到每个 worker 进程内的 bootstrap_worker() 执行，
        # 避免 fork 模式下所有 worker 共享父进程预先创建的 client/service 实例（issue #11）。
        # 单 worker 模式与缺少 app_path 的回退路径（父进程自己 serve）保持原有行为不变。
        if self.auto_configuration_enabled:
            self.logger.info("🔍 开始自动发现组件...")
            auto_discover(self.auto_discover_package)
            if workers > 1 and app_path:
                self.logger.info("⏩ 多 worker 模式：自动配置延迟到各 worker 进程内执行")
            else:
                apply_auto_configuration(self)

        # 获取真实 IP 用于日志显示（服务器仍然使用配置的 host 绑定）
        display_host = get_local_ip() if host == "0.0.0.0" else host

        # 显示服务器信息
        self.logger.info(f"🌐 服务器启动: http://{display_host}:{port}")
        self.logger.info(f"📚 API 文档: http://{display_host}:{port}/docs")
        self.logger.info(f"🔍 健康检查: http://{display_host}:{port}/health")
        self.logger.info(f"⚙️ 服务器类型: Hypercorn")
        self.logger.info(f"🔧 工作进程: {workers}")
        
        if workers > 1 and not app_path:
            self.logger.warning(
                "⚠️ 多 workers 模式需要提供 app_path 参数，"
                "例如: app.run(workers=4, app_path='main:app.get_fastapi_app()')"
            )

        # 启动服务器
        try:
            self.server_manager.start_server(
                app=self._fastapi_app,
                host=host,
                port=port,
                reload=reload,
                workers=workers,
                app_path=app_path,
                **run_kwargs
            )
        except KeyboardInterrupt:
            self.logger.info("收到中断信号，正在关闭...")
        finally:
            asyncio.run(self.shutdown())

    async def shutdown(self) -> None:
        """优雅关闭应用程序"""
        if self._server:
            # 服务器关闭逻辑
            pass

        # 停止调度器
        if self.scheduler.is_running():
            self.scheduler.stop()

        self.logger.info("应用程序已关闭")

    def add_route(self, path: str, handler: Callable, methods: List[str] = None, **kwargs) -> None:
        """添加路由到 FastAPI 应用"""
        if self._fastapi_app is None:
            self._fastapi_app = self._create_fastapi_app()

        if methods is None:
            methods = ['GET']

        # 使用 FastAPI 的 add_api_route 方法
        self._fastapi_app.add_api_route(path, handler, methods=methods, **kwargs)

    def get_fastapi_app(self) -> FastAPI:
        """获取 FastAPI 应用实例"""
        if self._fastapi_app is None:
            self._fastapi_app = self._create_fastapi_app()
        return self._fastapi_app

    def bootstrap_worker(self) -> FastAPI:
        """Worker 进程内引导（多 worker 模式下由 server._worker_serve 调用）

        spawn（Windows）与 fork（Linux/macOS）两种模式在此收敛：

        1. 重读 MYBOOT_WORKER_ID 等环境变量——它们在 _worker_serve 中设置，
           晚于 Application 构造（spawn 子进程在 multiprocessing 引导阶段
           重新 import 用户主模块时即构造 Application），因此 __init__ 读到
           的是默认值，必须在这里刷新。
        2. 重算 scheduler 门控并重建 Scheduler——修复「每个 worker 都自认
           primary、调度器多跑」的问题；同时丢弃 fork 继承的、绑定到
           fork 前实例的任务注册。
        3. 防御性重置——若检测到 fork 继承的预引导状态（父进程曾手动执行
           apply_auto_configuration），清空 DI 容器与各注册表并重建
           FastAPI 应用，丢弃绑定到 fork 前控制器实例的路由。
        4. 在 worker 进程内执行自动配置——所有 client/service/component/
           controller 在本 worker 内实例化（issue #11 核心修复），同时修复
           Windows spawn 模式下 worker 无用户路由的问题。

        Returns:
            本 worker 进程内完成引导的 FastAPI 应用实例
        """
        # 1. 重读 worker 环境变量
        self._worker_id = int(os.environ.get("MYBOOT_WORKER_ID", "1"))
        self._worker_count = int(os.environ.get("MYBOOT_WORKER_COUNT", "1"))
        self._is_primary_worker = os.environ.get("MYBOOT_IS_PRIMARY_WORKER", "1") == "1"

        # 2. 重算调度器门控并重建调度器（默认仅 primary worker 启用，
        #    可通过 scheduler.on_all_workers 配置覆盖；任务级 all_workers=True
        #    的任务在本 worker 注册时由注册门控按需调用 scheduler.enable()）
        scheduler_on_all_workers = self.config.get("scheduler.on_all_workers", False)
        scheduler_globally_enabled = self.config.get("scheduler.enabled", True)
        self._scheduler_enabled = (
            self._is_primary_worker or scheduler_on_all_workers
        ) and scheduler_globally_enabled
        self.scheduler = Scheduler(config=self.config, enabled=self._scheduler_enabled)

        # 3. 防御性重置：fork 子进程若继承了父进程已引导的状态
        #    （正常延迟引导路径下这些注册表都是空的，不会进入此分支）
        di_container = getattr(self, 'di_container', None)
        inherited = bool(
            (di_container is not None and di_container.service_providers)
            or self.services or self.clients or self.components
        )
        if inherited:
            self.logger.warning(
                "检测到 fork 继承的预引导状态，重置注册表后在 worker 内重建实例..."
            )
            if di_container is not None:
                di_container.clear()
            self.services.clear()
            self.clients.clear()
            self.components.clear()
            if hasattr(self, '_client_type_map'):
                self._client_type_map.clear()
            if hasattr(self, '_component_type_map'):
                self._component_type_map.clear()
            # 注意：worker 钩子由 _auto_register_worker_hooks 重新注册，
            # 先清空避免重复（此分支下父进程已经 apply 过一次）
            self.worker_start_hooks.clear()
            self.worker_stop_hooks.clear()
            # 重建 FastAPI 应用，丢弃绑定到 fork 前控制器实例的路由
            self._fastapi_app = self._create_fastapi_app()

        # 4. 在 worker 进程内执行自动配置
        #    - spawn 子进程：全局管理器是全新的，auto_discover 执行扫描（命中父进程缓存）
        #    - fork 子进程：管理器已发现完成，auto_discover 幂等守卫直接返回
        if self.auto_configuration_enabled:
            self.logger.info(f"🔍 Worker-{self._worker_id} 开始进程内引导...")
            auto_discover(self.auto_discover_package)
            apply_auto_configuration(self)

        return self.get_fastapi_app()

    # ==================== Worker 信息 ====================
    
    @property
    def worker_id(self) -> int:
        """
        获取当前 Worker ID（从 1 开始）
        
        单进程模式下返回 1
        """
        return self._worker_id
    
    @property
    def worker_count(self) -> int:
        """
        获取 Worker 总数
        
        单进程模式下返回 1
        """
        return self._worker_count
    
    @property
    def is_primary_worker(self) -> bool:
        """
        是否为主 Worker（Worker ID = 1）
        
        适用于只需要在一个 worker 执行的任务（如定时任务、初始化任务）
        
        Example:
            if app.is_primary_worker:
                # 只在主 worker 执行
                await init_cache()
        """
        return self._is_primary_worker
    
    @property
    def is_multi_worker_mode(self) -> bool:
        """是否运行在多 Worker 模式"""
        return self._worker_count > 1


# 便捷函数
def create_app(
        name: str = "MyBoot App",
        config_file: Optional[str] = None,
        **kwargs
) -> Application:
    """创建 MyBoot 应用程序实例"""
    return Application(name, config_file, **kwargs)


def get_service(name: str):
    return _current_app.get_service(name)


def get_client(name: str):
    return _current_app.get_client(name)


def get_container() -> Container:
    """获取容器实例"""
    if _current_app is None:
        raise RuntimeError("应用实例未初始化，请确保应用已创建并启动")
    return _current_app.container
