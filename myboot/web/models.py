"""
Web 数据模型

提供请求和响应的数据模型
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class BaseResponse(BaseModel):
    """基础响应模型"""
    
    success: bool = Field(default=True, description="是否成功")
    message: str = Field(default="操作成功", description="响应消息")
    data: Optional[Any] = Field(default=None, description="响应数据")
    timestamp: datetime = Field(default_factory=datetime.now, description="时间戳")

class ErrorResponse(BaseResponse):
    """错误响应模型"""
    
    success: bool = Field(default=False, description="是否成功")
    message: str = Field(default="操作失败", description="错误消息")
    error_code: Optional[str] = Field(default=None, description="错误代码")
    details: Optional[Dict[str, Any]] = Field(default=None, description="错误详情")


class PaginationRequest(BaseModel):
    """分页请求模型"""
    
    page: int = Field(default=1, ge=1, description="页码")
    size: int = Field(default=10, ge=1, le=100, description="每页大小")
    sort: Optional[str] = Field(default=None, description="排序字段")
    order: str = Field(default="asc", pattern="^(asc|desc)$", description="排序方向")
    
    @field_validator('page')
    def validate_page(cls, v):
        if v < 1:
            raise ValueError('页码必须大于 0')
        return v
    
    @field_validator('size')
    def validate_size(cls, v):
        if v < 1 or v > 100:
            raise ValueError('每页大小必须在 1-100 之间')
        return v


class PaginationResponse(BaseResponse):
    """分页响应模型"""
    
    data: List[Any] = Field(default=[], description="数据列表")
    pagination: Dict[str, Any] = Field(description="分页信息")
    
    @classmethod
    def create(
        cls,
        data: List[Any],
        total: int,
        page: int,
        size: int,
        message: str = "查询成功"
    ) -> "PaginationResponse":
        """创建分页响应"""
        total_pages = (total + size - 1) // size
        
        pagination = {
            "total": total,
            "page": page,
            "size": size,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1
        }
        
        return cls(
            success=True,
            message=message,
            data=data,
            pagination=pagination
        )


class HealthCheckResponse(BaseModel):
    """健康检查响应模型"""
    
    status: str = Field(description="服务状态")
    app: str = Field(description="应用名称")
    version: str = Field(description="应用版本")
    uptime: str = Field(description="运行时间")
    timestamp: datetime = Field(default_factory=datetime.now, description="检查时间")

class RequestInfo(BaseModel):
    """请求信息模型"""
    
    method: str = Field(description="HTTP 方法")
    url: str = Field(description="请求 URL")
    headers: Dict[str, str] = Field(description="请求头")
    query_params: Dict[str, Any] = Field(default={}, description="查询参数")
    path_params: Dict[str, Any] = Field(default={}, description="路径参数")
    body: Optional[Any] = Field(default=None, description="请求体")
    client_ip: Optional[str] = Field(default=None, description="客户端 IP")
    user_agent: Optional[str] = Field(default=None, description="用户代理")
    timestamp: datetime = Field(default_factory=datetime.now, description="请求时间")

class ResponseInfo(BaseModel):
    """响应信息模型"""
    
    status_code: int = Field(description="状态码")
    headers: Dict[str, str] = Field(description="响应头")
    body: Optional[Any] = Field(default=None, description="响应体")
    process_time: float = Field(description="处理时间（秒）")
    timestamp: datetime = Field(default_factory=datetime.now, description="响应时间")

class ValidationErrorDetail(BaseModel):
    """验证错误详情模型"""
    
    field: str = Field(description="字段名")
    message: str = Field(description="错误消息")
    value: Any = Field(description="字段值")
    type: str = Field(description="错误类型")


class ValidationErrorResponse(ErrorResponse):
    """验证错误响应模型"""
    
    message: str = Field(default="请求参数验证失败", description="错误消息")
    error_code: str = Field(default="VALIDATION_ERROR", description="错误代码")
    details: List[ValidationErrorDetail] = Field(description="验证错误详情")


class APIError(BaseModel):
    """API 错误模型"""
    
    code: str = Field(description="错误代码")
    message: str = Field(description="错误消息")
    details: Optional[Dict[str, Any]] = Field(default=None, description="错误详情")
    timestamp: datetime = Field(default_factory=datetime.now, description="错误时间")

class SuccessResponse(BaseResponse):
    """成功响应模型"""
    
    success: bool = Field(default=True, description="是否成功")
    message: str = Field(default="操作成功", description="成功消息")
    data: Optional[Any] = Field(default=None, description="响应数据")


class CreatedResponse(SuccessResponse):
    """创建成功响应模型"""
    
    message: str = Field(default="创建成功", description="成功消息")
    status_code: int = Field(default=201, description="状态码")


class UpdatedResponse(SuccessResponse):
    """更新成功响应模型"""
    
    message: str = Field(default="更新成功", description="成功消息")
    status_code: int = Field(default=200, description="状态码")


class DeletedResponse(SuccessResponse):
    """删除成功响应模型"""
    
    message: str = Field(default="删除成功", description="成功消息")
    status_code: int = Field(default=204, description="状态码")
    data: Optional[Any] = Field(default=None, description="响应数据")


# 便捷函数
def success_response(data: Any = None, message: str = "操作成功") -> SuccessResponse:
    """创建成功响应"""
    return SuccessResponse(data=data, message=message)


def error_response(
    message: str = "操作失败",
    error_code: str = "UNKNOWN_ERROR",
    details: Optional[Dict[str, Any]] = None
) -> ErrorResponse:
    """创建错误响应"""
    return ErrorResponse(
        message=message,
        error_code=error_code,
        details=details
    )


def validation_error_response(errors: List[ValidationErrorDetail]) -> ValidationErrorResponse:
    """创建验证错误响应"""
    return ValidationErrorResponse(details=errors)


def pagination_response(
    data: List[Any],
    total: int,
    page: int,
    size: int,
    message: str = "查询成功"
) -> PaginationResponse:
    """创建分页响应"""
    return PaginationResponse.create(data, total, page, size, message)
