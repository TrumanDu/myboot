"""
Web 异常模块

提供 Web 相关的异常类

ValidationError / ConfigurationError 自 0.2.0 起与 myboot.exceptions
中的定义收敛为同一个类（此前两处各自定义、互不兼容），从本模块 import
仍然有效。
"""

from typing import Any, Dict, Optional

from ..exceptions import ConfigurationError, ValidationError  # noqa: F401


class HTTPException(Exception):
    """HTTP 异常基类"""
    
    def __init__(
        self,
        status_code: int,
        message: str = "HTTP Error",
        details: Optional[Dict[str, Any]] = None
    ):
        self.status_code = status_code
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class BadRequestError(HTTPException):
    """400 错误请求异常"""
    
    def __init__(self, message: str = "错误请求", details: Optional[Dict[str, Any]] = None):
        super().__init__(400, message, details)


class UnauthorizedError(HTTPException):
    """401 未授权异常"""
    
    def __init__(self, message: str = "未授权", details: Optional[Dict[str, Any]] = None):
        super().__init__(401, message, details)


class ForbiddenError(HTTPException):
    """403 禁止访问异常"""
    
    def __init__(self, message: str = "禁止访问", details: Optional[Dict[str, Any]] = None):
        super().__init__(403, message, details)


class NotFoundError(HTTPException):
    """404 未找到异常"""
    
    def __init__(self, message: str = "未找到", details: Optional[Dict[str, Any]] = None):
        super().__init__(404, message, details)


class MethodNotAllowedError(HTTPException):
    """405 方法不允许异常"""
    
    def __init__(self, message: str = "方法不允许", details: Optional[Dict[str, Any]] = None):
        super().__init__(405, message, details)


class ConflictError(HTTPException):
    """409 冲突异常"""
    
    def __init__(self, message: str = "资源冲突", details: Optional[Dict[str, Any]] = None):
        super().__init__(409, message, details)


class UnprocessableEntityError(HTTPException):
    """422 无法处理的实体异常"""
    
    def __init__(self, message: str = "无法处理的实体", details: Optional[Dict[str, Any]] = None):
        super().__init__(422, message, details)


class TooManyRequestsError(HTTPException):
    """429 请求过多异常"""
    
    def __init__(self, message: str = "请求过多", details: Optional[Dict[str, Any]] = None):
        super().__init__(429, message, details)


class InternalServerError(HTTPException):
    """500 内部服务器错误异常"""
    
    def __init__(self, message: str = "内部服务器错误", details: Optional[Dict[str, Any]] = None):
        super().__init__(500, message, details)


class ServiceUnavailableError(HTTPException):
    """503 服务不可用异常"""
    
    def __init__(self, message: str = "服务不可用", details: Optional[Dict[str, Any]] = None):
        super().__init__(503, message, details)


class AuthenticationError(Exception):
    """认证错误异常"""
    
    def __init__(self, message: str = "认证失败"):
        self.message = message
        super().__init__(self.message)


class AuthorizationError(Exception):
    """授权错误异常"""
    
    def __init__(self, message: str = "授权失败"):
        self.message = message
        super().__init__(self.message)


class RateLimitError(Exception):
    """限流错误异常"""
    
    def __init__(self, message: str = "请求频率过高", retry_after: Optional[int] = None):
        self.message = message
        self.retry_after = retry_after
        super().__init__(self.message)


class BusinessError(Exception):
    """业务错误异常"""
    
    def __init__(
        self,
        message: str = "业务处理失败",
        code: str = "BUSINESS_ERROR",
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(self.message)


class ExternalServiceError(Exception):
    """外部服务错误异常"""
    
    def __init__(
        self,
        service_name: str,
        message: str = "外部服务调用失败",
        status_code: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.service_name = service_name
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class DatabaseError(Exception):
    """数据库错误异常"""
    
    def __init__(
        self,
        message: str = "数据库操作失败",
        operation: Optional[str] = None,
        table: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.operation = operation
        self.table = table
        self.details = details or {}
        super().__init__(self.message)


class CacheError(Exception):
    """缓存错误异常"""
    
    def __init__(
        self,
        message: str = "缓存操作失败",
        operation: Optional[str] = None,
        key: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.operation = operation
        self.key = key
        self.details = details or {}
        super().__init__(self.message)


class TimeoutError(Exception):
    """超时错误异常"""
    
    def __init__(
        self,
        message: str = "操作超时",
        timeout: Optional[float] = None,
        operation: Optional[str] = None
    ):
        self.message = message
        self.timeout = timeout
        self.operation = operation
        super().__init__(self.message)


# 异常映射
HTTP_STATUS_EXCEPTIONS = {
    400: BadRequestError,
    401: UnauthorizedError,
    403: ForbiddenError,
    404: NotFoundError,
    405: MethodNotAllowedError,
    409: ConflictError,
    422: UnprocessableEntityError,
    429: TooManyRequestsError,
    500: InternalServerError,
    503: ServiceUnavailableError,
}


def create_http_exception(status_code: int, message: str, details: Optional[Dict[str, Any]] = None) -> HTTPException:
    """根据状态码创建 HTTP 异常

    已知状态码映射到对应子类（子类构造签名为 ``(message, details)``，
    status_code 已内置）；未知状态码回退到基类 ``HTTPException``。
    """
    exception_class = HTTP_STATUS_EXCEPTIONS.get(status_code)
    if exception_class is not None:
        return exception_class(message, details)
    return HTTPException(status_code, message, details)
