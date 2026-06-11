#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
依赖注入示例应用

展示 MyBoot 框架的依赖注入功能
"""

import sys
from pathlib import Path
from typing import Optional

from myboot.core.application import Application
from myboot.core.decorators import service, rest_controller, get, post

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 创建应用实例
app = Application(
    name="依赖注入示例",
    auto_configuration=True,
    auto_discover_package="examples.dependency_injection_example"
)


# ==================== 基础服务层 ====================

@service()
class DatabaseClient:
    """数据库客户端"""
    
    def __init__(self):
        self.connection = "connected"
        print("✅ DatabaseClient 已初始化")
    
    def query(self, sql: str):
        print(f"📊 执行查询: {sql}")
        return [{"id": 1, "name": "用户1"}]


@service()
class CacheService:
    """缓存服务"""
    
    def __init__(self):
        self.cache = {}
        print("✅ CacheService 已初始化")
    
    def get(self, key: str):
        return self.cache.get(key)
    
    def set(self, key: str, value: any):
        self.cache[key] = value


# ==================== 仓储层 ====================

@service()
class UserRepository:
    """用户仓储 - 依赖 DatabaseClient"""
    
    def __init__(self, db: DatabaseClient):
        self.db = db
        print("✅ UserRepository 已初始化（依赖: DatabaseClient）")
    
    def find_by_id(self, user_id: int):
        result = self.db.query(f"SELECT * FROM users WHERE id = {user_id}")
        return result[0] if result else {"id": user_id, "name": f"用户{user_id}"}


# ==================== 服务层 ====================

@service()
class UserService:
    """用户服务 - 依赖 UserRepository 和可选的 CacheService"""
    
    def __init__(
        self,
        user_repo: UserRepository,
        cache: Optional[CacheService] = None
    ):
        self.user_repo = user_repo
        self.cache = cache
        print("✅ UserService 已初始化（依赖: UserRepository, CacheService）")
    
    def get_user(self, user_id: int):
        # 尝试从缓存获取
        if self.cache:
            cached = self.cache.get(f"user:{user_id}")
            if cached:
                print(f"📦 从缓存获取用户 {user_id}")
                return cached
        
        # 从数据库获取
        user = self.user_repo.find_by_id(user_id)
        
        # 存入缓存
        if self.cache:
            self.cache.set(f"user:{user_id}", user)
            print(f"💾 用户 {user_id} 已存入缓存")
        
        return user


@service()
class EmailService:
    """邮件服务"""
    
    def __init__(self):
        print("✅ EmailService 已初始化")
    
    def send_email(self, to: str, subject: str, body: str):
        print(f"📧 发送邮件到 {to}: {subject} - {body}")
        return {"status": "sent", "to": to, "subject": subject}


@service()
class OrderService:
    """订单服务 - 依赖 UserService 和 EmailService"""
    
    def __init__(self, user_service: UserService, email_service: EmailService):
        self.user_service = user_service
        self.email_service = email_service
        print("✅ OrderService 已初始化（依赖: UserService, EmailService）")
    
    def create_order(self, user_id: int, product: str):
        # 获取用户信息
        user = self.user_service.get_user(user_id)
        
        # 发送邮件通知
        self.email_service.send_email(
            to=user.get('email', 'user@example.com'),
            subject="订单创建",
            body=f"您的订单 {product} 已创建"
        )
        
        return {
            "order_id": 12345,
            "user_id": user_id,
            "product": product,
            "status": "created"
        }


# ==================== REST 控制器 ====================
# 注意：路由必须在 @rest_controller 装饰的类中定义，支持依赖注入

@rest_controller('/')
class HomeController:
    """首页控制器"""
    
    @get('/')
    def home(self):
        """首页（依赖注入示例）"""
        return {
            "message": "依赖注入示例应用",
            "features": [
                "自动依赖注入",
                "多级依赖支持",
                "可选依赖支持",
                "循环依赖检测",
                "REST 控制器"
            ],
            "endpoints": [
                "GET /api/users/{user_id} - 获取用户信息",
                "POST /api/orders - 创建订单"
            ]
        }


@rest_controller('/api/users')
class UserController:
    """用户控制器 - 自动注入 UserService"""
    
    def __init__(self, user_service: UserService):
        self.user_service = user_service
    
    @get('/{user_id}')
    def get_user(self, user_id: int):
        """获取用户信息 - GET /api/users/{user_id}"""
        return self.user_service.get_user(user_id)


@rest_controller('/api/orders')
class OrderController:
    """订单控制器 - 自动注入 OrderService"""
    
    def __init__(self, order_service: OrderService):
        self.order_service = order_service
    
    @post('/')
    def create_order(self, user_id: int, product: str):
        """创建订单 - POST /api/orders"""
        return self.order_service.create_order(user_id, product)


if __name__ == "__main__":
    print("\n" + "="*60)
    print("依赖注入示例应用")
    print("="*60 + "\n")
    
    # 运行应用
    app.run(host="0.0.0.0", port=8000, reload=False)

