"""
装饰器模块

提供约定优于配置的装饰器，用于自动注册组件
"""

import re
from functools import wraps
from typing import Any, Dict, List, Optional, Union, Callable


from ..utils.naming import camel_to_snake as _camel_to_snake


def route(path: str, methods: List[str] = None, **kwargs):
    """
    路由装饰器
    
    Args:
        path: 路由路径
        methods: HTTP 方法列表
        **kwargs: 其他路由参数
    """
    if methods is None:
        methods = ['GET']
    
    def decorator(func):
        func.__myboot_route__ = {
            'path': path,
            'methods': methods,
            'kwargs': kwargs
        }
        return func
    return decorator


def get(path: str, **kwargs):
    """GET 路由装饰器"""
    return route(path, methods=['GET'], **kwargs)


def post(path: str, **kwargs):
    """POST 路由装饰器"""
    return route(path, methods=['POST'], **kwargs)


def put(path: str, **kwargs):
    """PUT 路由装饰器"""
    return route(path, methods=['PUT'], **kwargs)


def delete(path: str, **kwargs):
    """DELETE 路由装饰器"""
    return route(path, methods=['DELETE'], **kwargs)


def patch(path: str, **kwargs):
    """PATCH 路由装饰器"""
    return route(path, methods=['PATCH'], **kwargs)


def cron(cron_expression: str, enabled: Optional[bool] = None, **kwargs):
    """
    Cron 任务装饰器
    
    支持在函数和类方法中使用：
    - 函数：@cron("0 0 * * *") 或 @cron("0 0 * * * *")
    - 类方法：在类的方法上使用 @cron("0 0 * * *")
    
    Args:
        cron_expression: Cron 表达式
            - 标准5位格式：分 时 日 月 周（如 "0 0 * * *" 表示每小时）
            - 6位格式：秒 分 时 日 月 周（如 "0 0 * * * *" 表示每小时，兼容旧格式）
        enabled: 是否启用任务，如果为 None 则默认启用
                 可以通过手动获取配置来传递，例如：
                 from myboot.core.config import get_config
                 enabled = get_config('jobs.heartbeat.enabled', True)
        **kwargs: 其他任务参数
    """
    def decorator(func):
        func.__myboot_job__ = {
            'type': 'cron',
            'cron': cron_expression,
            'enabled': enabled,
            'kwargs': kwargs
        }
        return func
    return decorator


def interval(seconds: int = None, minutes: int = None, hours: int = None, enabled: Optional[bool] = None, **kwargs):
    """
    间隔任务装饰器
    
    支持在函数和类方法中使用：
    - 函数：@interval(seconds=60)
    - 类方法：在类的方法上使用 @interval(seconds=60)
    
    Args:
        seconds: 秒数
        minutes: 分钟数
        hours: 小时数
        enabled: 是否启用任务，如果为 None 则默认启用
                 可以通过手动获取配置来传递，例如：
                 from myboot.core.config import get_config
                 enabled = get_config('jobs.heartbeat.enabled', True)
        **kwargs: 其他任务参数
    """
    def decorator(func):
        interval_value = seconds or (minutes * 60) or (hours * 3600)
        func.__myboot_job__ = {
            'type': 'interval',
            'interval': interval_value,
            'enabled': enabled,
            'kwargs': kwargs
        }
        return func
    return decorator


def once(run_date: str = None, enabled: Optional[bool] = None, **kwargs):
    """
    一次性任务装饰器
    
    支持在函数和类方法中使用：
    - 函数：@once('2025-12-31 23:59:59')
    - 类方法：在类的方法上使用 @once('2025-12-31 23:59:59')
    
    Args:
        run_date: 运行日期
        enabled: 是否启用任务，如果为 None 则默认启用
                 可以通过手动获取配置来传递，例如：
                 from myboot.core.config import get_config
                 enabled = get_config('jobs.heartbeat.enabled', True)
        **kwargs: 其他任务参数
    """
    def decorator(func):
        func.__myboot_job__ = {
            'type': 'once',
            'run_date': run_date,
            'enabled': enabled,
            'kwargs': kwargs
        }
        return func
    return decorator


def service(name: str = None, **kwargs):
    """
    服务装饰器
    
    Args:
        name: 服务名称
        **kwargs: 其他服务参数
    """
    def decorator(cls):
        cls.__myboot_service__ = {
            'name': name or _camel_to_snake(cls.__name__),
            'kwargs': kwargs
        }
        return cls
    return decorator


def model(name: str = None, **kwargs):
    """
    模型装饰器
    
    Args:
        name: 模型名称
        **kwargs: 其他模型参数
    """
    def decorator(cls):
        cls.__myboot_model__ = {
            'name': name or _camel_to_snake(cls.__name__),
            'kwargs': kwargs
        }
        return cls
    return decorator


def client(name: str = None, **kwargs):
    """
    客户端装饰器
    
    Args:
        name: 客户端名称
        **kwargs: 其他客户端参数
    """
    def decorator(cls):
        cls.__myboot_client__ = {
            'name': name or _camel_to_snake(cls.__name__),
            'kwargs': kwargs
        }
        return cls
    return decorator


def component(
    name: str = None,
    scope: str = 'singleton',
    primary: bool = False,
    lazy: bool = False,
    **kwargs
):
    """
    组件装饰器
    
    用于将类注册为通用组件，支持依赖注入。
    可用于任意需要托管的类（Job 实例、工具类、配置类等）。
    
    Args:
        name: 组件名称，默认使用类名的 snake_case 形式
        scope: 生命周期范围
            - 'singleton': 单例模式（默认），整个应用生命周期内只创建一个实例
            - 'prototype': 原型模式，每次获取时创建新实例
        primary: 当按类型获取有多个匹配时，是否为首选
        lazy: 是否懒加载，True 时在首次使用时才创建实例
        **kwargs: 其他组件参数
    
    Examples:
        # 基本用法
        @component()
        class EmailHelper:
            def send(self, to: str, content: str):
                pass
        
        # 带依赖注入
        @component(name='redis_cache')
        class RedisCache:
            def __init__(self, redis_client: RedisClient):
                self.redis = redis_client
        
        # 包含定时任务的组件
        @component()
        class DataSyncJobs:
            def __init__(self, data_service: DataService):
                self.data_service = data_service
            
            @cron("0 2 * * *")
            def sync_daily(self):
                self.data_service.sync()
    """
    def decorator(cls):
        cls.__myboot_component__ = {
            'name': name or _camel_to_snake(cls.__name__),
            'scope': scope,
            'primary': primary,
            'lazy': lazy,
            'kwargs': kwargs
        }
        return cls
    return decorator


def middleware(
    name: str = None,
    order: int = 0,
    path_filter: Union[str, List[str]] = None,
    methods: List[str] = None,
    condition: Callable = None,
    **kwargs
):
    """
    中间件装饰器
    
    Args:
        name: 中间件名称
        order: 执行顺序，数字越小越先执行（默认 0）
        path_filter: 路径过滤，支持字符串、字符串列表或正则表达式模式
                     例如: '/api/*', ['/api/*', '/admin/*']
        methods: HTTP 方法过滤，如 ['GET', 'POST']（默认 None，处理所有方法）
        condition: 条件函数，接收 request 对象，返回 bool 决定是否执行中间件
        **kwargs: 其他中间件参数
    
    Examples:
        @middleware(order=1, path_filter='/api/*')
        def api_middleware(request, next_handler):
            return next_handler(request)
        
        @middleware(order=2, methods=['POST', 'PUT'])
        def post_middleware(request, next_handler):
            return next_handler(request)
    """
    def decorator(func):
        func.__myboot_middleware__ = {
            'name': name or func.__name__,
            'order': order,
            'path_filter': path_filter,
            'methods': methods,
            'condition': condition,
            'kwargs': kwargs
        }
        return func
    return decorator


def rest_controller(base_path: str, **kwargs):
    """
    REST 控制器装饰器
    
    用于标记 REST 控制器类，为类中的方法提供基础路径。
    类中的方法需要显式使用 @get、@post、@put、@delete、@patch 等装饰器才会生成路由。
    
    路径合并规则：
    - 方法路径以 // 开头：作为绝对路径使用（去掉一个 /）
    - 方法路径以 / 开头：去掉开头的 / 后追加到基础路径
    - 方法路径不以 / 开头：直接追加到基础路径
    
    示例:
        @rest_controller('/api/reports')
        class ReportController:
            @post('/generate')  # 最终路径: POST /api/reports/generate
            def create_report(self, report_type: str):
                return {"message": "报告生成任务已创建"}
            
            @get('/status/{job_id}')  # 最终路径: GET /api/reports/status/{job_id}
            def get_status(self, job_id: str):
                return {"status": "completed"}
    
    Args:
        base_path: 基础路径
        **kwargs: 其他路由参数
    """
    def decorator(cls):
        cls.__myboot_rest_controller__ = {
            'base_path': base_path.rstrip('/'),
            'kwargs': kwargs
        }
        return cls
    return decorator
