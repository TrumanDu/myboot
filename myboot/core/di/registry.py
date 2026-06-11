"""
服务注册表

负责扫描服务、分析依赖关系、构建依赖图
"""

import inspect
from typing import Dict, List, Type, Set, Optional, Tuple
from collections import defaultdict, deque
from loguru import logger

from .decorators import get_injectable_params


class ServiceRegistry:
    """服务注册表"""
    
    def __init__(self):
        """初始化服务注册表"""
        self.services: Dict[str, Type] = {}  # service_name -> service_class
        self.service_configs: Dict[str, dict] = {}  # service_name -> config
        self.dependencies: Dict[str, Set[str]] = {}  # service_name -> set of dependency names
        self.dependents: Dict[str, Set[str]] = {}  # service_name -> set of dependent names
        self._dependency_graph: Optional[Dict[str, Set[str]]] = None
        self.known_instances: Set[str] = set()  # 已知的外部实例名称（如 Client）
    
    def register_service(self, service_class: Type, service_name: str, config: dict = None) -> None:
        """
        注册服务类
        
        Args:
            service_class: 服务类
            service_name: 服务名称
            config: 服务配置
        """
        self.services[service_name] = service_class
        self.service_configs[service_name] = config or {}
        self.dependencies[service_name] = set()
        # 不覆盖已有 dependents：可能已有「先注册的依赖方」写入的条目，覆盖会导致拓扑序错误
        self.dependents.setdefault(service_name, set[str]())

        # 分析依赖关系
        self._analyze_dependencies(service_name, service_class)

        # 注册时即检测循环依赖：只告警不报错（报错时机仍在容器构建时），
        # 让问题在注册阶段就可见，便于定位
        for cycle in self._find_cycles_involving(service_name):
            logger.warning(
                f"检测到循环依赖: {' -> '.join(cycle)}。"
                f"容器构建时将会失败，请重构代码以消除循环依赖。"
            )

    def _find_cycles_involving(self, service_name: str) -> List[List[str]]:
        """查找包含指定服务的循环依赖链（基于当前已注册的依赖关系，不使用缓存图）"""
        cycles: List[List[str]] = []
        path: List[str] = []
        rec_stack: Set[str] = set()

        def dfs(node: str) -> None:
            if node in rec_stack:
                cycle_start = path.index(node)
                cycles.append(path[cycle_start:] + [node])
                return
            rec_stack.add(node)
            path.append(node)
            for neighbor in self.dependencies.get(node, set()):
                if neighbor in self.services and not cycles:
                    dfs(neighbor)
            rec_stack.discard(node)
            path.pop()

        dfs(service_name)
        return cycles
    
    def _analyze_dependencies(self, service_name: str, service_class: Type) -> None:
        """
        分析服务的依赖关系
        
        Args:
            service_name: 服务名称
            service_class: 服务类
        """
        if not hasattr(service_class, '__init__'):
            return
        
        init_method = service_class.__init__
        params = get_injectable_params(init_method)
        
        for param_name, param_info in params.items():
            dep_service_name = param_info['service_name']
            
            if dep_service_name:
                # 检查依赖的服务是否存在（可能还未注册）
                # 先记录依赖关系，稍后在构建依赖图时验证
                self.dependencies[service_name].add(dep_service_name)
                if dep_service_name not in self.dependents:
                    self.dependents[dep_service_name] = set()
                self.dependents[dep_service_name].add(service_name)
    
    def build_dependency_graph(self) -> Dict[str, Set[str]]:
        """
        构建依赖关系图
        
        Returns:
            依赖关系图字典
        """
        if self._dependency_graph is not None:
            return self._dependency_graph
        
        self._dependency_graph = {}
        
        # 验证所有依赖的服务都已注册（排除已知的外部实例如 Client）
        known_names = set(self.services.keys()) | self.known_instances
        for service_name, deps in self.dependencies.items():
            missing_deps = deps - known_names
            if missing_deps:
                logger.warning(
                    f"服务 '{service_name}' 的依赖 '{missing_deps}' 未找到，"
                    f"将在运行时检查"
                )
        
        # 构建依赖图
        for service_name in self.services.keys():
            self._dependency_graph[service_name] = self.dependencies.get(service_name, set())
        
        return self._dependency_graph
    
    def detect_circular_dependencies(self) -> List[List[str]]:
        """
        检测循环依赖
        
        Returns:
            循环依赖列表，每个元素是一个循环依赖链
        """
        graph = self.build_dependency_graph()
        cycles = []
        visited = set()
        rec_stack = set()
        path = []
        
        def dfs(node: str) -> None:
            if node in rec_stack:
                # 找到循环
                cycle_start = path.index(node)
                cycle = path[cycle_start:] + [node]
                cycles.append(cycle)
                return
            
            if node in visited:
                return
            
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in graph.get(node, set()):
                if neighbor in self.services:  # 只检查已注册的服务
                    dfs(neighbor)
            
            rec_stack.remove(node)
            path.pop()
        
        for service_name in self.services.keys():
            if service_name not in visited:
                dfs(service_name)
        
        return cycles
    
    def get_initialization_order(self) -> List[str]:
        """
        获取服务初始化顺序（拓扑排序）
        
        Returns:
            服务名称列表，按初始化顺序排列
            
        Raises:
            ValueError: 如果存在循环依赖
        """
        cycles = self.detect_circular_dependencies()
        if cycles:
            cycle_str = ' -> '.join(cycles[0])
            raise ValueError(
                f"检测到循环依赖: {cycle_str}。"
                f"请重构代码以消除循环依赖。"
            )
        
        graph = self.build_dependency_graph()
        in_degree = defaultdict(int)
        
        # 计算入度
        for service_name in self.services.keys():
            in_degree[service_name] = 0
        
        for service_name, deps in graph.items():
            for dep in deps:
                if dep in self.services:  # 只考虑已注册的服务
                    in_degree[service_name] += 1
        
        # 拓扑排序
        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        result = []
        
        while queue:
            service_name = queue.popleft()
            result.append(service_name)
            
            # 更新依赖此服务的其他服务的入度
            for dependent in self.dependents.get(service_name, set()):
                if dependent in in_degree:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)
        
        # 检查是否所有服务都已处理
        if len(result) != len(self.services):
            remaining = set(self.services.keys()) - set(result)
            logger.warning(f"以下服务无法确定初始化顺序: {remaining}")
            # 将剩余的服务添加到末尾
            result.extend(remaining)
        
        return result
    
    def get_dependencies(self, service_name: str) -> Set[str]:
        """
        获取服务的依赖列表
        
        Args:
            service_name: 服务名称
            
        Returns:
            依赖的服务名称集合
        """
        return self.dependencies.get(service_name, set())
    
    def get_dependents(self, service_name: str) -> Set[str]:
        """
        获取依赖此服务的服务列表
        
        Args:
            service_name: 服务名称
            
        Returns:
            依赖此服务的服务名称集合
        """
        return self.dependents.get(service_name, set())
    
    def get_service_class(self, service_name: str) -> Optional[Type]:
        """
        获取服务类
        
        Args:
            service_name: 服务名称
            
        Returns:
            服务类，如果不存在则返回 None
        """
        return self.services.get(service_name)
    
    def get_service_config(self, service_name: str) -> dict:
        """
        获取服务配置
        
        Args:
            service_name: 服务名称
            
        Returns:
            服务配置字典
        """
        return self.service_configs.get(service_name, {})
    
    def has_service(self, service_name: str) -> bool:
        """
        检查服务是否已注册
        
        Args:
            service_name: 服务名称
            
        Returns:
            是否存在
        """
        return service_name in self.services
    
    def clear(self) -> None:
        """清空注册表"""
        self.services.clear()
        self.service_configs.clear()
        self.dependencies.clear()
        self.dependents.clear()
        self._dependency_graph = None

