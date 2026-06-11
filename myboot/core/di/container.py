"""
依赖注入容器

管理 dependency_injector Container 和服务的生命周期
"""

from typing import Dict, Type, Any, Optional
from dependency_injector import containers, providers
from loguru import logger

from .registry import ServiceRegistry
from .providers import ServiceProvider


class DependencyContainer:
    """依赖注入容器管理器"""
    
    def __init__(self):
        """初始化依赖注入容器"""
        self.container = containers.DynamicContainer()
        self.registry = ServiceRegistry()
        self.service_providers: Dict[str, ServiceProvider] = {}
        self.service_instances: Dict[str, Any] = {}
        self._wired = False
    
    def register_service(
        self,
        service_class: Type,
        service_name: str,
        scope: str = ServiceProvider.SINGLETON,
        config: dict = None
    ) -> None:
        """
        注册服务到容器
        
        Args:
            service_class: 服务类
            service_name: 服务名称
            scope: 生命周期范围 (singleton/factory)
            config: 服务配置
        """
        # 注册到注册表
        self.registry.register_service(service_class, service_name, config)

        # 创建服务提供者
        # 注意：装饰器元数据（config）中可能含顶层 'scope' 键，
        # 与显式 scope 参数冲突，这里剔除（以显式参数为准）
        config_kwargs = {k: v for k, v in (config or {}).items() if k != 'scope'}
        provider = ServiceProvider(service_class, service_name, scope, **config_kwargs)
        self.service_providers[service_name] = provider
    
    def register_instance(self, name: str, instance: Any) -> None:
        """
        注册已创建的实例到容器（用于 Client 等外部创建的实例）
        
        Args:
            name: 实例名称
            instance: 实例对象
        """
        # 直接缓存实例
        self.service_instances[name] = instance
        
        # 通知注册表（避免依赖检查时产生警告）
        self.registry.known_instances.add(name)
        
        # 创建一个返回已有实例的提供者
        di_provider = providers.Object(instance)
        setattr(self.container, name, di_provider)
        
        # 创建占位的 ServiceProvider（用于 has_service 检查）
        placeholder = ServiceProvider(type(instance), name, ServiceProvider.SINGLETON)
        placeholder._provider = di_provider
        self.service_providers[name] = placeholder
    
    def build_container(self) -> None:
        """
        构建依赖注入容器
        
        按照依赖顺序注册所有服务到 Container
        """
        # 构建依赖图
        self.registry.build_dependency_graph()
        
        # 获取初始化顺序
        try:
            init_order = self.registry.get_initialization_order()
        except ValueError as e:
            logger.error(f"无法构建依赖注入容器: {e}")
            raise
        
        # 获取服务的参数信息，用于正确映射依赖
        service_params = {}
        for service_name in self.service_providers.keys():
            service_class = self.registry.get_service_class(service_name)
            if service_class and hasattr(service_class, '__init__'):
                from .decorators import get_injectable_params
                params = get_injectable_params(service_class.__init__)
                service_params[service_name] = params
        
        # 按顺序注册服务
        for service_name in init_order:
            if service_name not in self.service_providers:
                continue
            
            provider = self.service_providers[service_name]
            deps = self.registry.get_dependencies(service_name)
            
            # 构建依赖字典（使用参数名作为键）
            dependencies = {}
            params = service_params.get(service_name, {})
            
            for param_name, param_info in params.items():
                dep_service_name = param_info.get('service_name')
                if dep_service_name and dep_service_name in self.service_providers:
                    # 从容器中获取依赖的提供者
                    dep_provider = self.service_providers[dep_service_name]
                    if not dep_provider.get_provider():
                        # 如果依赖的提供者还未创建，先创建它
                        dep_deps = self.registry.get_dependencies(dep_service_name)
                        dep_dependencies = {}
                        dep_params = service_params.get(dep_service_name, {})
                        for dep_param_name, dep_param_info in dep_params.items():
                            nested_dep_name = dep_param_info.get('service_name')
                            if nested_dep_name and nested_dep_name in self.service_providers:
                                nested_provider = self.service_providers[nested_dep_name]
                                if nested_provider.get_provider():
                                    dep_dependencies[dep_param_name] = nested_provider.get_provider()
                        dep_provider.create_provider(dep_dependencies if dep_dependencies else None)
                    
                    # 使用参数名作为键（dependency_injector 需要参数名匹配）
                    dependencies[param_name] = dep_provider.get_provider()
            
            # 创建提供者
            di_provider = provider.create_provider(dependencies if dependencies else None)
            
            # 注册到容器
            setattr(self.container, service_name, di_provider)
            
            logger.debug(f"已注册服务提供者: {service_name} (依赖: {deps})")
    
    def wire_modules(self, *modules) -> None:
        """
        连接模块以启用依赖注入
        
        Args:
            *modules: 要连接的模块列表
        """
        if not self._wired:
            self.container.wire(modules=modules)
            self._wired = True
            logger.debug(f"已连接模块: {modules}")
    
    def unwire_modules(self) -> None:
        """断开模块连接"""
        if self._wired:
            self.container.unwire()
            self._wired = False
            logger.debug("已断开模块连接")
    
    def get_service(self, service_name: str) -> Any:
        """
        获取服务实例
        
        Args:
            service_name: 服务名称
            
        Returns:
            服务实例
            
        Raises:
            KeyError: 如果服务不存在
        """
        # 先检查是否有已缓存的实例（包括通过 register_instance 注册的）
        if service_name in self.service_instances:
            return self.service_instances[service_name]
        
        if service_name not in self.service_providers:
            raise KeyError(f"服务 '{service_name}' 未注册")
        
        # 如果是单例，缓存实例
        provider = self.service_providers[service_name]
        if provider.scope == ServiceProvider.SINGLETON:
            di_provider = getattr(self.container, service_name, None)
            if di_provider:
                self.service_instances[service_name] = di_provider()
            else:
                raise RuntimeError(f"服务 '{service_name}' 的提供者未正确配置")
            return self.service_instances[service_name]
        else:
            # 工厂模式，每次创建新实例
            di_provider = getattr(self.container, service_name, None)
            if di_provider:
                return di_provider()
            else:
                raise RuntimeError(f"服务 '{service_name}' 的提供者未正确配置")
    
    def has_service(self, service_name: str) -> bool:
        """
        检查服务是否已注册
        
        Args:
            service_name: 服务名称
            
        Returns:
            是否存在
        """
        return service_name in self.service_providers or service_name in self.service_instances
    
    def get_all_services(self) -> Dict[str, Any]:
        """
        获取所有服务实例（仅单例）
        
        Returns:
            服务名称到实例的字典
        """
        result = {}
        for service_name in self.service_providers.keys():
            try:
                result[service_name] = self.get_service(service_name)
            except Exception as e:
                logger.warning(f"无法获取服务 '{service_name}': {e}")
        return result
    
    def clear(self) -> None:
        """清空容器"""
        self.unwire_modules()
        self.container = containers.DynamicContainer()
        self.registry.clear()
        self.service_providers.clear()
        self.service_instances.clear()
        self._wired = False

