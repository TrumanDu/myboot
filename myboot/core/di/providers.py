"""
服务提供者配置

定义服务的提供者配置和生命周期管理
"""

from typing import Any, Type, Optional
from dependency_injector import providers


class ServiceProvider:
    """服务提供者配置"""
    
    SINGLETON = 'singleton'
    REQUEST = 'request'
    FACTORY = 'factory'

    def __init__(
        self,
        service_class: Type,
        service_name: str,
        scope: str = SINGLETON,
        **kwargs
    ):
        """
        初始化服务提供者

        Args:
            service_class: 服务类
            service_name: 服务名称
            scope: 生命周期范围 (singleton/request/factory)
            **kwargs: 其他配置参数
        """
        self.service_class = service_class
        self.service_name = service_name
        self.scope = scope
        self.kwargs = kwargs
        self._provider: Optional[Any] = None

    def create_provider(self, dependencies: dict = None) -> Any:
        """
        创建 dependency_injector Provider

        scope 到 Provider 的映射:
            - singleton -> providers.Singleton（默认）
            - request   -> providers.ContextLocalSingleton（基于 contextvars，
                           每个 asyncio 任务/HTTP 请求内单例）
            - 其他      -> providers.Factory（每次创建新实例）

        Args:
            dependencies: 依赖的服务提供者字典

        Returns:
            dependency_injector Provider 实例
        """
        if self.scope == self.SINGLETON:
            provider_class = providers.Singleton
        elif self.scope == self.REQUEST:
            provider_class = providers.ContextLocalSingleton
        else:
            provider_class = providers.Factory

        if dependencies:
            self._provider = provider_class(self.service_class, **dependencies)
        else:
            self._provider = provider_class(self.service_class)

        return self._provider
    
    def get_provider(self) -> Any:
        """获取提供者实例"""
        return self._provider

