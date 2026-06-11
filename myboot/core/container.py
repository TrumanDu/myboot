"""
统一容器模块

提供统一的容器接口，支持从 container、components、services、clients 中获取实例
"""

from typing import Any, Dict, List, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from .application import Application


class Container:
    """统一容器类，支持从 container、components、services、clients 中获取实例"""
    
    def __init__(self, app: 'Application'):
        """
        初始化容器
        
        Args:
            app: 应用实例
        """
        self._app = app
        # 容器自己的存储字典
        self._storage: Dict[str, Any] = {}
    
    def put(self, name: str, instance: Any) -> None:
        """
        将实例放入容器
        
        Args:
            name: 实例名称
            instance: 实例对象
        """
        self._storage[name] = instance
        self._app.logger.debug(f"已放入容器: {name} ({type(instance).__name__})")
    
    def get(self, name: str, default: Any = None) -> Any:
        """
        从容器中获取实例（优先从 container，然后从 components、services，最后从 clients）
        
        Args:
            name: 实例名称
            default: 如果不存在时返回的默认值
            
        Returns:
            实例对象，如果不存在则返回 default
        """
        # 优先从 container 中获取
        if name in self._storage:
            return self._storage[name]
        
        # 然后从 components 中获取
        if name in self._app.components:
            return self._app.components[name]
        
        # 然后从 services 中获取
        if name in self._app.services:
            return self._app.services[name]
        
        # 最后从 clients 中获取
        if name in self._app.clients:
            return self._app.clients[name]

        # 非单例（request/factory）的 service/client 不在上述字典中预存，
        # 回退到 DI 容器按需解析
        di_container = getattr(self._app, 'di_container', None)
        if di_container is not None and di_container.has_service(name):
            return di_container.get_service(name)

        return default
    
    def get_or_raise(self, name: str) -> Any:
        """
        从容器中获取实例，如果不存在则抛出异常
        
        Args:
            name: 实例名称
            
        Returns:
            实例对象
            
        Raises:
            KeyError: 如果实例不存在
        """
        instance = self.get(name)
        if instance is None:
            raise KeyError(f"容器中不存在实例: {name} (已检查 container、components、services、clients)")
        return instance
    
    def has(self, name: str) -> bool:
        """
        检查容器中是否存在指定名称的实例（检查 container、components、services、clients）
        
        Args:
            name: 实例名称
            
        Returns:
            是否存在
        """
        return (name in self._storage or 
                name in self._app.components or
                name in self._app.services or 
                name in self._app.clients)
    
    def remove(self, name: str) -> bool:
        """
        从容器中移除实例（按顺序从 container、components、services、clients 中移除）
        
        Args:
            name: 实例名称
            
        Returns:
            是否成功移除
        """
        removed = False
        if name in self._storage:
            del self._storage[name]
            self._app.logger.debug(f"已从容器移除: {name}")
            removed = True
        if name in self._app.components:
            del self._app.components[name]
            self._app.logger.debug(f"已从组件移除: {name}")
            removed = True
        if name in self._app.services:
            del self._app.services[name]
            self._app.logger.debug(f"已从服务移除: {name}")
            removed = True
        if name in self._app.clients:
            del self._app.clients[name]
            self._app.logger.debug(f"已从客户端移除: {name}")
            removed = True
        return removed
    
    def get_by_type(self, instance_type: Type) -> List[Any]:
        """
        根据类型获取容器中所有匹配的实例（从 container、components、services、clients 中查找）
        
        Args:
            instance_type: 实例类型
            
        Returns:
            匹配的实例列表
        """
        instances = []
        # 从 container 中查找
        instances.extend([instance for instance in self._storage.values() 
                          if isinstance(instance, instance_type)])
        # 从 components 中查找
        instances.extend([instance for instance in self._app.components.values() 
                          if isinstance(instance, instance_type)])
        # 从 services 中查找
        instances.extend([instance for instance in self._app.services.values() 
                          if isinstance(instance, instance_type)])
        # 从 clients 中查找
        instances.extend([instance for instance in self._app.clients.values() 
                          if isinstance(instance, instance_type)])
        return instances
    
    def list_all(self) -> Dict[str, Any]:
        """
        列出容器中所有实例（包括 container、components、services、clients）
        
        Returns:
            所有实例的字典（名称 -> 实例）
        """
        all_instances = {}
        all_instances.update(self._storage)
        all_instances.update(self._app.components)
        all_instances.update(self._app.services)
        all_instances.update(self._app.clients)
        return all_instances
    
    def clear(self) -> None:
        """清空容器（清空 container、components、services、clients）"""
        self._storage.clear()
        self._app.components.clear()
        self._app.services.clear()
        self._app.clients.clear()
        self._app.logger.debug("容器已清空")
    
    def __getitem__(self, name: str) -> Any:
        """支持使用 [] 语法获取实例"""
        return self.get_or_raise(name)
    
    def __setitem__(self, name: str, instance: Any) -> None:
        """支持使用 [] 语法设置实例"""
        self.put(name, instance)
    
    def __contains__(self, name: str) -> bool:
        """支持使用 in 语法检查实例是否存在"""
        return self.has(name)
    
    def __delitem__(self, name: str) -> None:
        """支持使用 del 语法删除实例"""
        if not self.remove(name):
            raise KeyError(f"容器中不存在实例: {name}")

