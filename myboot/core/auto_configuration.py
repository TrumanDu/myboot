"""
自动配置模块

实现约定优于配置的设计理念，提供自动发现和配置功能

设计参考 Spring Boot：
- 使用 AST 静态分析替代 import（发现阶段不执行代码）
- 延迟导入：只在注册阶段才导入需要的模块
- 缓存 AST 分析结果，缓存命中时完全跳过分析
"""

import os
import time
import inspect
from pathlib import Path
from typing import Dict, List, Type, Any

from loguru import logger as loguru_logger

from ._internal import ast_analyzer, component_scanner, scan_cache
from ._internal.scan_cache import _CACHE_VERSION

logger = loguru_logger.bind(name=__name__)


class AutoConfigurationError(Exception):
    """自动配置失败异常

    当自动注册组件失败时抛出此异常，导致应用启动失败
    """
    pass

# MyBoot 装饰器到组件类型的映射
# 注意：@cron/@interval/@once 装饰器只能在 @component 类中使用
# job 方法的注册在 _auto_register_components 中动态进行
_DECORATOR_MAPPING = {
    'service': 'services',
    'client': 'clients',
    'model': 'models',
    'component': 'components',
    'rest_controller': 'rest_controllers',
    'route': 'routes',
    'get': 'routes',
    'post': 'routes',
    'put': 'routes',
    'delete': 'routes',
    'patch': 'routes',
    'middleware': 'middleware',
    'on_worker_start': 'worker_hooks',
    'on_worker_stop': 'worker_hooks',
}


from ..utils.naming import camel_to_snake as _camel_to_snake


def _find_project_root() -> str:
    """查找项目根目录"""
    # 从当前文件所在目录开始向上查找
    current_dir = Path(__file__).parent.absolute()
    
    # 向上查找，直到找到包含 pyproject.toml 或 requirements.txt 的目录
    while current_dir.parent != current_dir:
        if (current_dir / 'pyproject.toml').exists() or (current_dir / 'requirements.txt').exists():
            return str(current_dir)
        current_dir = current_dir.parent
    
    # 如果没找到，返回当前工作目录
    return os.getcwd()


class AutoConfigurationManager:
    """
    自动配置管理器
    
    设计参考 Spring Boot：
    - 发现阶段：使用 AST 静态分析，不执行 import
    - 注册阶段：延迟导入，只导入需要的模块
    - 缓存：缓存 AST 分析结果，缓存命中时完全跳过分析
    """
    
    def __init__(self, app_root: str = None, use_cache: bool = True, parallel_import: bool = False):
        self.app_root = app_root or _find_project_root()
        self.use_cache = use_cache
        self.parallel_import = parallel_import
        # 组件元数据（不包含实际类对象，只有模块路径和名称）
        self._component_metadata: Dict[str, List[dict]] = {
            'routes': [],
            'middleware': [],
            'services': [],
            'models': [],
            'clients': [],
            'components': [],
            'rest_controllers': [],
            'worker_hooks': []
        }
        # 已加载的组件（包含实际类对象，延迟填充）
        self.discovered_components: Dict[str, List[dict]] = {
            'routes': [],
            'middleware': [],
            'services': [],
            'models': [],
            'clients': [],
            'components': [],
            'rest_controllers': [],
            'worker_hooks': []
        }
        self.auto_configured = False
        self._modules_loaded = False
    
    def _get_cache_path(self, package_name: str) -> Path:
        """获取缓存文件路径"""
        return scan_cache.get_cache_path(self.app_root, package_name)

    def _collect_source_files(self, package_path: Path) -> Dict[str, float]:
        """收集所有源文件及其修改时间"""
        return scan_cache.collect_source_files(package_path)

    def _is_cache_valid(self, cache_path: Path, package_path: Path) -> bool:
        """检查缓存是否有效"""
        return scan_cache.is_cache_valid(cache_path, package_path)

    def _save_cache(self, cache_path: Path, package_path: Path) -> None:
        """保存元数据缓存（不包含类对象）"""
        scan_cache.save_cache(cache_path, package_path, self._component_metadata)

    def _load_cache(self, cache_path: Path) -> bool:
        """从缓存加载元数据（不导入模块）"""
        loaded = scan_cache.load_cache(cache_path, self._component_metadata.keys())
        if loaded is None:
            return False
        self._component_metadata = loaded
        return True

    def _parse_decorators(self, node) -> List[str]:
        """解析装饰器名称"""
        return ast_analyzer.parse_decorators(node)

    def _scan_file_ast(self, file_path: Path, module_name: str) -> None:
        """使用 AST 静态分析扫描单个文件（不执行 import）"""
        ast_analyzer.scan_file_ast(
            file_path, module_name, self._component_metadata, _DECORATOR_MAPPING
        )

    def _scan_package_ast(self, package_path: Path) -> None:
        """使用 AST 递归扫描包（不执行 import）"""
        ast_analyzer.scan_package_ast(
            package_path, self._component_metadata, _DECORATOR_MAPPING
        )

    def _load_modules(self) -> None:
        """延迟加载模块，将元数据转换为实际的类对象"""
        if self._modules_loaded:
            return

        self.discovered_components = component_scanner.build_discovered_components(
            self._component_metadata, self.parallel_import
        )

        self._modules_loaded = True

    def _resolve_scan_target(self, package_name: str):
        """解析扫描目标：单模块文件或包目录

        Returns:
            (scan_path, scan_type, module_name)
            scan_type 为 'file' 或 'package'；目录扫描时 module_name 为 None
        """
        app_root = Path(self.app_root)

        if package_name.endswith('.py'):
            file_path = app_root / package_name.replace('/', os.sep)
            if file_path.is_file():
                module_name = str(
                    file_path.relative_to(app_root).with_suffix('')
                ).replace(os.sep, '.')
                return file_path, 'file', module_name

        dotted_path = app_root / package_name.replace('.', os.sep)
        py_file = dotted_path.with_suffix('.py')
        if py_file.is_file():
            return py_file, 'file', package_name

        dir_path = dotted_path if dotted_path.is_dir() else app_root / package_name
        if dir_path.is_dir():
            return dir_path, 'package', None

        return None, None, None
    
    def auto_discover(self, package_name: str = "app") -> None:
        """
        自动发现应用组件（AST 静态分析，不执行 import）
        
        模块的实际导入延迟到 apply_auto_configuration 时进行。
        package_name 支持目录包（如 app、examples/convention）或单模块
        （如 examples.convention_app、examples/convention_app.py）。
        """
        # 幂等守卫：fork 子进程（bootstrap_worker）重复调用时直接返回，
        # 避免向 _component_metadata 追加重复条目
        if self.auto_configured:
            logger.debug("自动发现已完成，跳过重复扫描")
            return

        start_time = time.perf_counter()
        logger.info(f"开始自动发现 {package_name} 包中的组件...")
        
        try:
            scan_path, scan_type, module_name = self._resolve_scan_target(package_name)
            if scan_path is None:
                logger.warning(f"扫描目标不存在: {package_name}")
                return
            
            cache_path = self._get_cache_path(package_name)
            
            # 尝试使用缓存（只读取元数据，不导入模块）
            if self.use_cache and self._is_cache_valid(cache_path, scan_path):
                if self._load_cache(cache_path):
                    self.auto_configured = True
                    elapsed = (time.perf_counter() - start_time) * 1000
                    logger.info(f"自动发现完成（从缓存加载），耗时: {elapsed:.2f}ms")
                    return
            
            # AST 静态分析扫描（不执行 import）
            if scan_type == 'file':
                self._scan_file_ast(scan_path, module_name)
            else:
                self._scan_package_ast(scan_path)
            
            # 保存缓存
            if self.use_cache:
                self._save_cache(cache_path, scan_path)
            
            self.auto_configured = True
            elapsed = (time.perf_counter() - start_time) * 1000
            logger.info(f"自动发现完成（AST 扫描），耗时: {elapsed:.2f}ms")
            
        except Exception as e:
            logger.error(f"自动发现失败: {e}", exc_info=True)
    
    def apply_auto_configuration(self, app) -> None:
        """应用自动配置（此时才执行模块导入）"""
        if not self.auto_configured:
            logger.warning("自动发现未完成，跳过自动配置")
            return
        
        start_time = time.perf_counter()
        
        # 延迟加载模块（将元数据转换为实际类对象）
        load_start = time.perf_counter()
        self._load_modules()
        load_elapsed = (time.perf_counter() - load_start) * 1000
        logger.info(f"模块加载完成，耗时: {load_elapsed:.2f}ms")
        
        self._auto_register_routes(app)
        self._auto_register_models(app)
        self._auto_register_clients(app)
        self._auto_register_services(app)  # 先注册服务，支持依赖注入
        self._auto_register_components(app)  # 注册组件并注册其中的 job 方法
        self._auto_register_middleware(app)
        self._auto_register_rest_controllers(app)
        # 最后注册 worker 钩子（此时容器/服务已就绪，钩子内可解析依赖）
        self._auto_register_worker_hooks(app)

        elapsed = (time.perf_counter() - start_time) * 1000
        logger.info(f"自动配置应用完成，耗时: {elapsed:.2f}ms")
    
    def _auto_register_rest_controllers(self, app) -> None:
        """自动注册 REST 控制器
        
        只注册显式使用 @get、@post 等装饰器的方法，不自动根据方法名生成路由
        """
        import inspect as inspect_module
        
        for controller_info in self.discovered_components['rest_controllers']:
            try:
                cls = controller_info['class']
                controller_config = getattr(cls, '__myboot_rest_controller__')
                base_path = controller_config['base_path']
                base_kwargs = controller_config.get('kwargs', {})
                
                # 创建控制器实例（支持依赖注入）
                instance = self._get_class_instance(cls, app)
                
                # 检查类中的所有方法，只处理显式使用路由装饰器的方法
                for method_name, method in inspect_module.getmembers(
                    instance, 
                    predicate=lambda x: inspect_module.ismethod(x) and not x.__name__.startswith('_')
                ):
                    # 只处理有 __myboot_route__ 属性的方法（即显式使用 @get、@post 等装饰器的方法）
                    if hasattr(method, '__myboot_route__'):
                        route_config = getattr(method, '__myboot_route__')
                        method_path = route_config['path']
                        methods = route_config.get('methods', ['GET'])
                        route_kwargs = {**base_kwargs, **route_config.get('kwargs', {})}
                        route_kwargs.setdefault(
                            'operation_id',
                            f"{controller_info['module'].replace('.', '_')}_"
                            f"{cls.__name__}_{method_name}"
                        )
                        
                        # 合并基础路径和方法路径
                        # 如果方法路径是绝对路径（以 // 开头），则直接使用（去掉一个 /）
                        # 否则，将方法路径追加到基础路径
                        if method_path.startswith('//'):
                            # 绝对路径，去掉一个 / 后使用
                            full_path = method_path[1:]
                        elif method_path.startswith('/'):
                            # 以 / 开头但非绝对路径，去掉开头的 / 后追加到基础路径
                            full_path = f"{base_path}{method_path}".replace('//', '/')
                        else:
                            # 相对路径，追加到基础路径
                            full_path = f"{base_path}/{method_path}".replace('//', '/')
                        
                        # 注册路由
                        app.add_route(
                            path=full_path,
                            handler=method,
                            methods=methods,
                            **route_kwargs
                        )
                        
                        logger.debug(f"自动注册 REST 路由: {methods} {full_path} -> {controller_info['module']}.{cls.__name__}.{method_name}")
                
                logger.info(f"自动注册 REST 控制器: {controller_info['module']}.{cls.__name__}")
            except Exception as e:
                logger.error(f"自动注册 REST 控制器失败 {controller_info['module']}: {e}", exc_info=True)
                raise AutoConfigurationError(
                    f"自动注册 REST 控制器失败 '{controller_info['module']}': {e}"
                ) from e
    
    def _auto_register_routes(self, app) -> None:
        """自动注册路由"""
        for route_info in self.discovered_components['routes']:
            try:
                # 前缀匹配：AST 扫描产生的类型为 function_get/function_post/
                # function_route 等（issue #8 修复同款逻辑，精确匹配会漏掉
                # 模块级 @get/@post 函数路由）
                if route_info['type'].startswith('function_'):
                    # 函数路由
                    func = route_info['function']
                    route_config = getattr(func, '__myboot_route__')
                    app.add_route(
                        path=route_config['path'],
                        handler=func,
                        methods=route_config.get('methods', ['GET']),
                        **route_config.get('kwargs', {})
                    )
                    logger.debug(f"自动注册路由: {route_info['module']}.{func.__name__}")
                elif route_info['type'].startswith('class_'):
                    # 类路由（class 条目没有 'function' 键，日志只在各方法处打印）
                    cls = route_info['class']
                    route_config = getattr(cls, '__myboot_route__')
                    instance = cls()
                    for method_name, method in inspect.getmembers(instance, predicate=inspect.ismethod):
                        if hasattr(method, '__myboot_route__'):
                            method_config = getattr(method, '__myboot_route__')
                            app.add_route(
                                path=method_config['path'],
                                handler=method,
                                methods=method_config.get('methods', ['GET']),
                                **method_config.get('kwargs', {})
                            )
                            logger.debug(f"自动注册路由: {route_info['module']}.{cls.__name__}.{method_name}")
            except Exception as e:
                logger.error(f"自动注册路由失败 {route_info['module']}: {e}", exc_info=True)
                raise AutoConfigurationError(
                    f"自动注册路由失败 '{route_info['module']}': {e}"
                ) from e
    
    def _is_job_enabled(self, _func, job_config: dict) -> bool:
        """
        检查任务是否启用
        
        Args:
            _func: 任务函数（保留用于未来扩展，如从函数名读取配置）
            job_config: 任务配置
            
        Returns:
            bool: 任务是否启用
        """
        # 检查装饰器中直接指定的 enabled
        enabled = job_config.get('enabled')
        if enabled is not None:
            # 支持布尔值和字符串
            if isinstance(enabled, bool):
                return enabled
            if isinstance(enabled, str):
                return enabled.lower() in ('true', '1', 'yes', 'on', 'enabled')
            return bool(enabled)
        
        # 默认启用
        return True

    def _get_class_instance(self, cls: Type, app) -> Any:
        """
        获取类实例，支持依赖注入
        
        从 di_container 获取 service 依赖，从 app.components、app.clients 获取依赖
        找不到直接报错，不尝试初始化
        
        Args:
            cls: 类
            app: 应用实例
            
        Returns:
            类实例
            
        Raises:
            RuntimeError: 如果 di_container 不存在或依赖注入失败
            KeyError: 如果必需依赖未注册
        """
        # 检查 di_container 是否存在
        if not hasattr(app, 'di_container'):
            raise RuntimeError(f"无法实例化 {cls.__name__}：应用未配置依赖注入容器")
        
        di_container = app.di_container
        
        # 检查类是否有 @service 装饰器
        if hasattr(cls, '__myboot_service__'):
            service_config = getattr(cls, '__myboot_service__')
            service_name = service_config.get('name', _camel_to_snake(cls.__name__))
            if di_container.has_service(service_name):
                return di_container.get_service(service_name)
            raise KeyError(f"服务 '{service_name}' 未在 DI 容器中注册")
        
        # 检查类是否有 @client 装饰器
        if hasattr(cls, '__myboot_client__'):
            client_config = getattr(cls, '__myboot_client__')
            client_name = client_config.get('name', _camel_to_snake(cls.__name__))
            if hasattr(app, 'clients') and client_name in app.clients:
                return app.clients[client_name]
            raise KeyError(f"客户端 '{client_name}' 未注册")
        
        # 检查类是否有 @component 装饰器且已注册
        if hasattr(cls, '__myboot_component__'):
            component_config = getattr(cls, '__myboot_component__')
            component_name = component_config.get('name', _camel_to_snake(cls.__name__))
            if hasattr(app, 'components') and component_name in app.components:
                return app.components[component_name]
            # 组件尚未注册，继续创建新实例
        
        # 检查构造函数是否有依赖
        from myboot.core.di.decorators import get_injectable_params
        params = get_injectable_params(cls.__init__)
        
        if not params:
            # 没有依赖参数，直接实例化
            return cls()
        
        # 有依赖参数，从 DI 容器和 clients 获取依赖
        dependencies = {}
        for param_name, param_info in params.items():
            dependency_name = param_info.get('service_name')
            if not dependency_name:
                continue
            
            is_optional = param_info.get('is_optional', False)
            dependency_instance = None
            found = False
            
            # 优先从 DI 容器获取 service
            if di_container.has_service(dependency_name):
                try:
                    dependency_instance = di_container.get_service(dependency_name)
                    logger.debug(f"从 DI 容器注入 service 依赖: {param_name} = {dependency_name}")
                    found = True
                except Exception as e:
                    if not is_optional:
                        raise RuntimeError(
                            f"无法实例化 {cls.__name__}："
                            f"获取 service 依赖 '{dependency_name}' (参数 '{param_name}') 失败: {e}"
                        ) from e
                    logger.debug(f"获取可选 service 依赖 '{dependency_name}' 失败: {e}")
            
            # 如果没找到 service，尝试从 components 获取
            if not found and hasattr(app, 'components'):
                # 先按名称查找
                if dependency_name in app.components:
                    dependency_instance = app.components[dependency_name]
                    logger.debug(f"从 components 注入依赖: {param_name} = {dependency_name}")
                    found = True
                # 再尝试按类型查找
                elif hasattr(app, '_component_type_map'):
                    param_type = param_info.get('type')
                    if param_type and param_type in app._component_type_map:
                        actual_name = app._component_type_map[param_type]
                        dependency_instance = app.components[actual_name]
                        found = True
            
            # 如果没找到 component，尝试从 clients 获取
            if not found and hasattr(app, 'clients'):
                # 先按名称查找
                if dependency_name in app.clients:
                    dependency_instance = app.clients[dependency_name]
                    logger.debug(f"从 clients 注入依赖: {param_name} = {dependency_name}")
                    found = True
                # 再尝试按类型查找
                elif hasattr(app, '_client_type_map'):
                    param_type = param_info.get('type')
                    if param_type and param_type in app._client_type_map:
                        actual_name = app._client_type_map[param_type]
                        dependency_instance = app.clients[actual_name]
                        found = True
            
            # 如果都没找到且不是可选的，报错
            if not found and not is_optional:
                raise KeyError(
                    f"无法实例化 {cls.__name__}："
                    f"必需依赖 '{dependency_name}' (参数 '{param_name}') 未在 DI 容器、components 或 clients 中注册"
                )
            
            if found and dependency_instance is not None:
                dependencies[param_name] = dependency_instance
        
        # 使用依赖实例化
        logger.debug(f"使用依赖注入实例化 {cls.__name__}: {list(dependencies.keys())}")
        return cls(**dependencies)
    
    def _auto_register_middleware(self, app) -> None:
        """自动注册中间件"""
        from myboot.web.middleware import FunctionMiddleware
        
        if not self.discovered_components['middleware']:
            return
        
        # 按照 order 排序中间件
        middleware_list = []
        for middleware_info in self.discovered_components['middleware']:
            try:
                func = middleware_info['function']
                middleware_config = getattr(func, '__myboot_middleware__')
                order = middleware_config.get('order', 0)
                middleware_list.append({
                    'func': func,
                    'config': middleware_config,
                    'order': order,
                    'module': middleware_info['module']
                })
            except Exception as e:
                logger.error(f"解析中间件配置失败 {middleware_info['module']}: {e}", exc_info=True)
                raise AutoConfigurationError(
                    f"解析中间件配置失败 '{middleware_info['module']}': {e}"
                ) from e
        
        # 按 order 排序
        middleware_list.sort(key=lambda x: x['order'])
        
        # 注册中间件（FastAPI 的中间件是后进先出的，所以需要反向注册）
        for middleware_item in reversed(middleware_list):
            try:
                func = middleware_item['func']
                config = middleware_item['config']
                module = middleware_item['module']
                
                # 创建动态中间件类，包装 FunctionMiddleware
                middleware_name = config.get('name', func.__name__)
                
                # 使用闭包捕获变量
                def make_init(middleware_func, middleware_config):
                    def __init__(self, app):
                        FunctionMiddleware.__init__(
                            self,
                            app=app,
                            middleware_func=middleware_func,
                            path_filter=middleware_config.get('path_filter'),
                            methods=middleware_config.get('methods'),
                            condition=middleware_config.get('condition'),
                            order=middleware_config.get('order', 0),
                            **middleware_config.get('kwargs', {})
                        )
                    return __init__
                
                # 动态创建中间件类
                middleware_class = type(
                    f"Middleware_{middleware_name}",
                    (FunctionMiddleware,),
                    {'__init__': make_init(func, config)}
                )
                
                # 添加到 FastAPI 应用
                app._fastapi_app.add_middleware(middleware_class)
                
                logger.info(
                    f"自动注册中间件: '{middleware_name}' "
                    f"(order={config.get('order', 0)}, "
                    f"module={module})"
                )
            except Exception as e:
                logger.error(f"自动注册中间件失败 {middleware_item['module']}: {e}", exc_info=True)
                raise AutoConfigurationError(
                    f"自动注册中间件失败 '{middleware_item['module']}': {e}"
                ) from e
        
        logger.info(f"成功注册 {len(middleware_list)} 个中间件")
    
    def _auto_register_services(self, app) -> None:
        """自动注册服务（支持依赖注入）"""
        try:
            from myboot.core.di import DependencyContainer
            
            # 检查应用是否有依赖注入容器
            if not hasattr(app, 'di_container'):
                app.di_container = DependencyContainer()
            
            di_container = app.di_container
            
            # 第一步：注册所有服务到容器（不创建实例）
            for service_info in self.discovered_components['services']:
                try:
                    cls = service_info['class']
                    service_config = getattr(cls, '__myboot_service__')
                    service_name = service_config.get('name', cls.__name__.lower())
                    
                    # 获取服务作用域（默认单例）
                    scope = service_config.get('scope', 'singleton')
                    
                    # 注册到依赖注入容器
                    di_container.register_service(
                        service_class=cls,
                        service_name=service_name,
                        scope=scope,
                        config=service_config
                    )
                    
                    logger.debug(f"已注册服务到容器: '{service_name}' ({service_info['module']}.{cls.__name__})")
                except Exception as e:
                    logger.error(f"注册服务到容器失败 {service_info['module']}: {e}", exc_info=True)
            
            # 第二步：构建依赖注入容器
            try:
                di_container.build_container()
                logger.info("依赖注入容器构建成功")
            except Exception as e:
                logger.error(f"构建依赖注入容器失败: {e}", exc_info=True)
                raise AutoConfigurationError(f"构建依赖注入容器失败: {e}") from e
            
            # 第三步：获取所有服务实例并注册到应用上下文
            for service_info in self.discovered_components['services']:
                try:
                    cls = service_info['class']
                    service_config = getattr(cls, '__myboot_service__')
                    service_name = service_config.get('name', cls.__name__.lower())

                    # 非单例（request/factory）服务跳过预实例化：
                    # 按需通过 app.di_container / Container.get 解析，
                    # 不会出现在 app.services 字典中
                    scope = service_config.get('scope', 'singleton')
                    if scope != 'singleton':
                        logger.info(
                            f"自动注册服务（{scope} 作用域，按需解析）: '{service_name}' "
                            f"({service_info['module']}.{cls.__name__})"
                        )
                        continue

                    # 从容器获取服务实例（自动注入依赖）
                    instance = di_container.get_service(service_name)
                    app.services[service_name] = instance
                    
                    # 确保当前应用实例已注册
                    from myboot.core.application import _current_app
                    if _current_app != app:
                        # 更新当前应用实例
                        import myboot.core.application
                        myboot.core.application._current_app = app
                    
                    logger.info(f"自动注册服务（依赖注入）: '{service_name}' ({service_info['module']}.{cls.__name__})")
                except Exception as e:
                    logger.error(f"获取服务实例失败 {service_info['module']}: {e}", exc_info=True)
                    raise AutoConfigurationError(
                        f"自动注册服务失败 '{service_name}' ({service_info['module']}): {e}"
                    ) from e
        
        except ImportError as e:
            logger.error(f"dependency_injector 未安装: {e}", exc_info=True)
            raise AutoConfigurationError(
                "依赖注入需要 dependency_injector 库，请运行: pip install dependency-injector"
            ) from e
        except AutoConfigurationError:
            # 重新抛出 AutoConfigurationError
            raise
        except Exception as e:
            logger.error(f"依赖注入服务注册失败: {e}", exc_info=True)
            raise AutoConfigurationError(f"依赖注入服务注册失败: {e}") from e
    
    def _auto_register_models(self, app) -> None:
        """自动注册模型"""
        for model_info in self.discovered_components['models']:
            try:
                cls = model_info['class']
                model_config = getattr(cls, '__myboot_model__')
                
                # 注册模型到应用上下文
                model_name = model_config.get('name', cls.__name__.lower())
                app.models[model_name] = cls
                
                logger.info(f"自动注册模型: {model_info['module']}")
            except Exception as e:
                logger.error(f"自动注册模型失败 {model_info['module']}: {e}", exc_info=True)
                raise AutoConfigurationError(
                    f"自动注册模型失败 '{model_info['module']}': {e}"
                ) from e
    
    def _auto_register_clients(self, app) -> None:
        """自动注册客户端"""
        # 初始化类型到名称的映射（用于按类型查找）
        if not hasattr(app, '_client_type_map'):
            app._client_type_map = {}
        
        # 初始化 DI 容器（如果还没有）
        if not hasattr(app, 'di_container'):
            from myboot.core.di import DependencyContainer
            app.di_container = DependencyContainer()
        
        for client_info in self.discovered_components['clients']:
            try:
                cls = client_info['class']
                client_config = getattr(cls, '__myboot_client__')
                # 优先使用用户自定义名称，否则使用 _camel_to_snake 自动生成
                client_name = client_config.get('name', _camel_to_snake(cls.__name__))
                auto_name = _camel_to_snake(cls.__name__)

                # 非单例（request/factory）客户端：注册类到 DI 容器按需解析，
                # 不在此处创建实例，也不出现在 app.clients 字典中
                scope = client_config.get('scope', 'singleton')
                if scope != 'singleton':
                    app.di_container.register_service(
                        service_class=cls,
                        service_name=client_name,
                        scope=scope,
                        config=client_config
                    )
                    if auto_name != client_name and not app.di_container.has_service(auto_name):
                        app.di_container.register_service(
                            service_class=cls,
                            service_name=auto_name,
                            scope=scope,
                            config=client_config
                        )
                    logger.info(
                        f"自动注册客户端（{scope} 作用域，按需解析）: '{client_name}' "
                        f"({client_info['module']}.{cls.__name__})"
                    )
                    continue

                # 创建客户端实例并注册到应用上下文
                instance = cls()
                app.clients[client_name] = instance
                
                # 同时记录类型映射，用于按类型查找
                app._client_type_map[cls] = client_name
                # 也用自动转换的名称注册（如果不同），方便按类型名查找
                if auto_name != client_name and auto_name not in app.clients:
                    app.clients[auto_name] = instance
                
                # 将 client 实例注册到 DI 容器（作为已创建的单例）
                # 这样 Service 可以通过 DI 容器获取 Client 依赖
                app.di_container.register_instance(client_name, instance)
                if auto_name != client_name:
                    app.di_container.register_instance(auto_name, instance)
                
                logger.info(f"自动注册客户端: '{client_name}' ({client_info['module']}.{cls.__name__})")
            except Exception as e:
                logger.error(f"自动注册客户端失败 {client_info['module']}: {e}", exc_info=True)
                raise AutoConfigurationError(
                    f"自动注册客户端失败 '{client_info['module']}': {e}"
                ) from e
    
    def _auto_register_components(self, app) -> None:
        """自动注册组件（支持依赖注入）"""
        # 初始化类型到名称的映射（用于按类型查找）
        if not hasattr(app, '_component_type_map'):
            app._component_type_map = {}
        
        for component_info in self.discovered_components['components']:
            try:
                cls = component_info['class']
                component_config = getattr(cls, '__myboot_component__')
                
                # 获取组件配置
                component_name = component_config.get('name', _camel_to_snake(cls.__name__))
                lazy = component_config.get('lazy', False)
                # scope 配置用于未来支持 prototype 模式
                
                # 懒加载的组件跳过立即实例化
                if lazy:
                    # 记录组件信息，延迟创建
                    app._lazy_components = getattr(app, '_lazy_components', {})
                    app._lazy_components[component_name] = {
                        'class': cls,
                        'config': component_config,
                        'module': component_info['module']
                    }
                    logger.debug(f"已注册懒加载组件: '{component_name}' ({component_info['module']}.{cls.__name__})")
                    continue
                
                # 创建组件实例（支持依赖注入）
                instance = self._get_class_instance(cls, app)
                
                # 注册到组件注册表
                app.components[component_name] = instance
                
                # 记录类型映射，用于按类型查找
                app._component_type_map[cls] = component_name
                
                # 也用自动转换的名称注册（如果不同）
                auto_name = _camel_to_snake(cls.__name__)
                if auto_name != component_name and auto_name not in app.components:
                    app.components[auto_name] = instance
                
                # 将组件实例注册到 DI 容器（作为已创建的单例）
                # 这样其他组件可以通过 DI 容器获取依赖
                if hasattr(app, 'di_container'):
                    app.di_container.register_instance(component_name, instance)
                    if auto_name != component_name:
                        app.di_container.register_instance(auto_name, instance)
                
                # 注册组件内的 job 方法（@cron/@interval/@once）
                self._register_component_jobs(app, instance, cls, component_info['module'])
                
                logger.info(f"自动注册组件: '{component_name}' ({component_info['module']}.{cls.__name__})")
            except Exception as e:
                logger.error(f"自动注册组件失败 {component_info['module']}: {e}", exc_info=True)
                raise AutoConfigurationError(
                    f"自动注册组件失败 '{component_info['module']}': {e}"
                ) from e
    
    def _register_component_jobs(self, app, instance, cls: Type, module_name: str) -> None:
        """
        注册组件内的 job 方法
        
        扫描组件实例中使用 @cron/@interval/@once 装饰器的方法，并注册到调度器
        
        Args:
            app: 应用实例
            instance: 组件实例
            cls: 组件类
            module_name: 模块名称
        """
        import inspect as inspect_module
        
        for method_name, method in inspect_module.getmembers(instance, predicate=inspect_module.ismethod):
            # 跳过私有方法
            if method_name.startswith('_'):
                continue
            
            # 检查是否有 job 装饰器
            if not hasattr(method, '__myboot_job__'):
                continue
            
            job_config = getattr(method, '__myboot_job__')
            
            # 检查任务是否启用
            if not self._is_job_enabled(method, job_config):
                logger.info(f"任务已禁用，跳过注册: {cls.__name__}.{method_name} ({module_name})")
                continue
            
            # 注册任务
            try:
                if job_config['type'] == 'cron':
                    app.scheduler.add_cron_job(
                        func=method,
                        cron=job_config['cron'],
                        **job_config.get('kwargs', {})
                    )
                elif job_config['type'] == 'interval':
                    app.scheduler.add_interval_job(
                        func=method,
                        interval=job_config['interval'],
                        **job_config.get('kwargs', {})
                    )
                elif job_config['type'] == 'once':
                    app.scheduler.add_date_job(
                        func=method,
                        run_date=job_config['run_date'],
                        **job_config.get('kwargs', {})
                    )
                
                logger.info(f"自动注册任务（组件方法）: {cls.__name__}.{method_name} ({module_name})")
            except Exception as e:
                logger.error(f"注册任务失败 {cls.__name__}.{method_name}: {e}", exc_info=True)

    def _auto_register_worker_hooks(self, app) -> None:
        """自动注册 worker 生命周期钩子（@on_worker_start / @on_worker_stop）

        模块级函数钩子按 order 排序后追加到 app.worker_start_hooks /
        app.worker_stop_hooks，由每个 worker 进程的 lifespan 触发。
        """
        hook_infos = self.discovered_components.get('worker_hooks', [])
        if not hook_infos:
            return

        start_hooks = []
        stop_hooks = []
        for hook_info in hook_infos:
            func = hook_info.get('function')
            if func is None:
                continue
            hook_config = getattr(func, '__myboot_worker_hook__', None)
            if not hook_config:
                continue
            order = hook_config.get('order', 0)
            if hook_config.get('event') == 'start':
                start_hooks.append((order, func, hook_info['module']))
            else:
                stop_hooks.append((order, func, hook_info['module']))

        for order, func, module in sorted(start_hooks, key=lambda x: x[0]):
            app.add_worker_start_hook(func)
            logger.info(f"自动注册 worker 启动钩子: {module}.{func.__name__} (order={order})")
        for order, func, module in sorted(stop_hooks, key=lambda x: x[0]):
            app.add_worker_stop_hook(func)
            logger.info(f"自动注册 worker 停止钩子: {module}.{func.__name__} (order={order})")


# 全局自动配置管理器实例
_auto_configuration_manager = AutoConfigurationManager()


def auto_discover(package_name: str = "app") -> None:
    """自动发现应用组件"""
    _auto_configuration_manager.auto_discover(package_name)


def apply_auto_configuration(app) -> None:
    """应用自动配置"""
    _auto_configuration_manager.apply_auto_configuration(app)
