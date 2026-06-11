"""
REST API 响应格式封装

提供统一的 REST API 响应格式，确保所有 API 返回一致的格式：
{
    "success": true/false,
    "code": 200,
    "message": "操作成功",
    "data": {...}
}
"""

from typing import Any, Dict, Optional, Union
from pydantic import BaseModel, ConfigDict, Field


class ApiResponse(BaseModel):
    """统一的 REST API 响应格式"""
    
    model_config = ConfigDict(exclude_none=True)
    
    success: bool = Field(description="是否成功")
    code: int = Field(description="HTTP 状态码")
    message: Optional[str] = Field(default=None, description="响应消息")
    data: Optional[Any] = Field(default=None, description="响应数据")


class ResponseWrapper:
    """响应包装器
    
    提供便捷方法创建统一格式的响应
    """
    
    @staticmethod
    def success(
        data: Any = None,
        message: Optional[str] = None,
        code: int = 200
    ) -> Dict[str, Any]:
        """
        创建成功响应
        
        Args:
            data: 响应数据
            message: 响应消息
            code: HTTP 状态码
        
        Returns:
            统一格式的响应字典
        """
        result = {
            "success": True,
            "code": code,
        }
        if message is not None:
            result["message"] = message
        if data is not None:
            result["data"] = data
        return result
    
    @staticmethod
    def error(
        message: Optional[str] = None,
        code: int = 500,
        data: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        创建错误响应
        
        Args:
            message: 错误消息
            code: HTTP 状态码
            data: 错误详情数据
        
        Returns:
            统一格式的响应字典
        """
        result = {
            "success": False,
            "code": code,
        }
        if message is not None:
            result["message"] = message
        if data is not None:
            result["data"] = data
        return result
    
    @staticmethod
    def created(
        data: Any = None,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """创建成功响应（201）"""
        return ResponseWrapper.success(data=data, message=message, code=201)
    
    @staticmethod
    def updated(
        data: Any = None,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """更新成功响应（200）"""
        return ResponseWrapper.success(data=data, message=message, code=200)
    
    @staticmethod
    def deleted(
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """删除成功响应（200）"""
        return ResponseWrapper.success(data=None, message=message, code=200)
    
    @staticmethod
    def no_content() -> Dict[str, Any]:
        """无内容响应（204）"""
        return ResponseWrapper.success(data=None, message=None, code=204)
    
    @staticmethod
    def pagination(
        data: list,
        total: int,
        page: int,
        size: int,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        创建分页响应
        
        Args:
            data: 数据列表
            total: 总记录数
            page: 当前页码
            size: 每页大小
            message: 响应消息
        
        Returns:
            统一格式的分页响应
        """
        total_pages = (total + size - 1) // size if size > 0 else 0
        pagination_data = {
            "list": data,
            "pagination": {
                "total": total,
                "page": page,
                "size": size,
                "totalPages": total_pages,
                "hasNext": page < total_pages,
                "hasPrev": page > 1
            }
        }
        return ResponseWrapper.success(data=pagination_data, message=message, code=200)
    
    @staticmethod
    def wrap(data: Any, message: Optional[str] = None, code: int = 200) -> Dict[str, Any]:
        """
        包装任意数据为统一格式
        
        Args:
            data: 要包装的数据
            message: 响应消息（如果为 None，返回中不包含该字段）
            code: HTTP 状态码
        
        Returns:
            统一格式的响应字典
        """
        # 如果已经是统一格式，直接返回
        if isinstance(data, dict) and all(key in data for key in ["success", "code"]):
            return data
        
        return ResponseWrapper.success(data=data, message=message, code=code)


# 全局响应包装器实例
response = ResponseWrapper()

