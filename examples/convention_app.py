#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
约定优于配置示例应用

展示 MyBoot 框架的约定优于配置特性
"""

import sys
import threading
from pathlib import Path

from myboot.core.application import Application
from myboot.core.config import get_config
from myboot.core.decorators import (
    get, post, put, delete,
    cron, interval, once,
    service, client, middleware,
    rest_controller
)
from myboot.jobs.scheduled_job import ScheduledJob
from myboot.core.scheduler import get_scheduler

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 创建应用实例
app = Application(
    name="约定优于配置示例",
    auto_configuration=True,  # 启用自动配置
    auto_discover_package="examples"  # 自动发现当前包
)


# ==================== 服务层 ====================

@service()
class UserService:
    """用户服务 - 自动注册为 'user_service'"""

    def __init__(self):
        self.users = {}
        print("✅ UserService 已初始化")

    def get_user(self, user_id: int):
        """获取用户"""
        return self.users.get(user_id, {"id": user_id, "name": f"用户{user_id}"})

    def create_user(self, name: str, email: str):
        """创建用户"""
        user_id = len(self.users) + 1
        user = {"id": user_id, "name": name, "email": email}
        self.users[user_id] = user
        return user

    def update_user(self, user_id: int, **kwargs):
        """更新用户"""
        if user_id in self.users:
            self.users[user_id].update(kwargs)
            return self.users[user_id]
        return None

    def delete_user(self, user_id: int):
        """删除用户"""
        return self.users.pop(user_id, None)


@service('email_service')
class EmailService:
    """邮件服务 - 注册为 'email_service'"""

    def __init__(self):
        print("✅ EmailService 已初始化")

    def send_email(self, to: str, subject: str, body: str):
        """发送邮件"""
        print(f"📧 发送邮件到 {to}: {subject}")
        return {"status": "sent", "to": to, "subject": subject}


# ==================== 客户端层 ====================

@client()
class DatabaseClient:
    """数据库客户端 - 自动注册为 'database_client'"""

    def __init__(self):
        print("✅ DatabaseClient 已初始化")

    def connect(self):
        """连接数据库"""
        print("🔗 连接数据库")
        return True

    def query(self, sql: str):
        """执行查询"""
        print(f"📊 执行查询: {sql}")
        return []


@client('redis_client')
class RedisClient:
    """Redis 客户端 - 注册为 'redis_client'"""

    def __init__(self):
        print("✅ RedisClient 已初始化")
        self.value = None

    def get(self, key: str):
        """获取缓存"""
        print(f"📦 获取缓存: {key}")
        return self.value

    def set(self, key: str, value: str):
        """设置缓存"""
        print(f"💾 设置缓存: {key} = {value}")
        self.value = value


# ==================== 中间件 ====================

@middleware(order=1)
async def logging_middleware(request, next_handler):
    """日志中间件 - 自动注册，处理所有请求"""
    print(f"📝 请求日志: {request.method} {request.url}")
    response = await next_handler(request)
    print(f"📝 响应日志: {response.status_code}")
    return response


@middleware(order=2)
async def timing_middleware(request, next_handler):
    """计时中间件 - 自动注册，处理所有请求"""
    import time
    start_time = time.time()
    response = await next_handler(request)
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"⏱️ 请求耗时: {elapsed:.3f}s - {request.method} {request.url.path}")
    return response


@middleware(order=3, path_filter='/api/*')
async def api_middleware(request, next_handler):
    """API 中间件 - 只处理 /api/* 路径的请求"""
    print(f"🔌 API 请求: {request.method} {request.url.path}")
    # 可以在这里添加 API 特定的逻辑，如认证、限流等
    response = await next_handler(request)
    return response


@middleware(order=4, methods=['POST', 'PUT', 'PATCH'])
async def modify_middleware(request, next_handler):
    """修改操作中间件 - 只处理 POST、PUT、PATCH 请求"""
    print(f"✏️ 修改操作: {request.method} {request.url.path}")
    response = await next_handler(request)
    return response


@middleware(
    order=5,
    path_filter=['/users/*', '/api/products/*'],
    condition=lambda req: req.headers.get('user-agent', '').startswith('Mozilla')
)
async def browser_only_middleware(request, next_handler):
    """浏览器专用中间件 - 只处理浏览器请求且匹配特定路径"""
    print(f"🌐 浏览器请求: {request.headers.get('user-agent', 'Unknown')}")
    response = await next_handler(request)
    return response


# ==================== 路由层 ====================

@get('/')
def home():
    """首页 - 自动注册为 GET /"""
    return {
        "message": "欢迎使用 MyBoot 约定优于配置示例",
        "features": [
            "自动发现组件",
            "约定优于配置",
            "零配置启动",
            "自动注册服务"
        ]
    }


@get('/users')
def get_users():
    """获取用户列表 - 自动注册为 GET /users"""
    from myboot.core.application import app
    user_service = app().get_service('user_service')
    return {"users": list(user_service.users.values())}


@get('/users/{user_id}')
def get_user(user_id: int):
    """获取单个用户 - 自动注册为 GET /users/{user_id}"""
    from myboot.core.application import get_service
    user_service = get_service('user_service')
    return user_service.get_user(user_id)


@post('/users')
def create_user(name: str, email: str):
    """创建用户 - 自动注册为 POST /users"""
    from myboot.core.application import get_service
    user_service = get_service('user_service')
    email_service = get_service('email_service')
    user = user_service.create_user(name, email)
    email_service.send_email(email, "欢迎注册", f"欢迎 {name} 注册我们的服务！")

    return {"message": "用户创建成功", "user": user}


@put('/users/{user_id}')
def update_user(user_id: int, name: str = None, email: str = None):
    """更新用户 - 自动注册为 PUT /users/{user_id}"""
    from myboot.core.application import get_service
    user_service = get_service('user_service')

    update_data = {}
    if name:
        update_data['name'] = name
    if email:
        update_data['email'] = email

    user = user_service.update_user(user_id, **update_data)
    if user:
        return {"message": "用户更新成功", "user": user}
    else:
        return {"error": "用户不存在"}


@delete('/users/{user_id}')
def delete_user(user_id: int):
    """删除用户 - 自动注册为 DELETE /users/{user_id}"""
    from myboot.core.application import get_service
    user_service = get_service('user_service')

    user = user_service.delete_user(user_id)

    if user:
        return {"message": "用户删除成功", "user": user}
    else:
        return {"error": "用户不存在"}


# ==================== 定时任务 ====================

@cron('0 */1 * * * *', enabled=True)  # 每分钟执行
def heartbeat():
    """心跳任务 - 自动注册"""
    print("💓 心跳检测 - 系统运行正常")


# 每10分钟执行
# 从配置文件读取 enabled 状态
@interval(minutes=10, enabled=get_config('jobs.cleanup_task.enabled', True))
def cleanup_task():
    """清理任务 - 自动注册"""
    print("🧹 执行清理任务")


@once('2025-11-5 17:41:00')
def new_year_task():
    """新年任务 - 自动注册（已禁用）"""
    print("🎉 新年任务执行")


# ==================== REST 控制器 ====================

@rest_controller('/api/products')
class ProductController:
    """产品控制器 - 自动生成 RESTful 路由"""

    def __init__(self):
        self.products = {
            1: {"id": 1, "name": "产品1", "price": 100},
            2: {"id": 2, "name": "产品2", "price": 200}
        }

    @get("/api/products")
    def list(self):
        """GET /api/products 或 GET /api/products/{product_id}"""
        from myboot.core.application import get_client
        redis_client = get_client('redis_client')
        if redis_client:
            print(redis_client.get('app_status'))
        return {"products": list(self.products.values())}

    @get("/api/products/{product_id}")
    def get(self, product_id: int = None):
        """GET /api/products 或 GET /api/products/{product_id}"""
        if product_id:
            return self.products.get(product_id, {"error": "产品不存在"})
        return {"products": list(self.products.values())}

    @post("/api/products")
    def post(self, name: str, price: float):
        """POST /api/products"""
        product_id = max(self.products.keys()) + 1
        product = {"id": product_id, "name": name, "price": price}
        self.products[product_id] = product
        return {"message": "产品创建成功", "product": product}

    @put("/api/products/{product_id}")
    def put(self, product_id: int, name: str = None, price: float = None):
        """PUT /api/products/{product_id}"""
        if product_id not in self.products:
            return {"error": "产品不存在"}

        if name:
            self.products[product_id]['name'] = name
        if price:
            self.products[product_id]['price'] = price

        return {"message": "产品更新成功", "product": self.products[product_id]}

    @delete("/api/products/{product_id}")
    def delete(self, product_id: int):
        """DELETE /api/products/{product_id}"""
        if product_id in self.products:
            product = self.products.pop(product_id)
            return {"message": "产品删除成功", "product": product}
        return {"error": "产品不存在"}


def generate_report(report_type: str):
    """生成报告任务"""
    import time
    print(f"开始生成 {report_type} 报告")
    time.sleep(10)  # 模拟报告生成
    return {"type": report_type, "status": "completed"}


@rest_controller('/api/reports')
class ReportController:
    """报告控制器"""

    def __init__(self):
        self.scheduler = get_scheduler()

    @post('/generate')
    def create_report(self, report_type: str):
        """创建报告生成任务"""
        # 创建自定义 ScheduledJob
        class ReportJob(ScheduledJob):
            def __init__(self, report_type: str):
                super().__init__(
                    name=f"生成{report_type}报告",
                    timeout=300  # 5分钟超时
                )
                self.report_type = report_type
            
            def run(self, *args, **kwargs):
                return generate_report(self.report_type)
        
        # 创建任务实例
        job = ReportJob(report_type)
        
        # 添加到调度器并执行
        job_id = self.scheduler.add_scheduled_job(job)
        thread = threading.Thread(target=job.execute)
        thread.daemon = True
        thread.start()

        return {"message": "报告生成任务已创建", "job_id": job_id}

    @get('/status/{job_id}')
    def get_status(self, job_id: str):
        """查询任务状态"""
        job = self.scheduler.get_scheduled_job(job_id)
        if job:
            return job.get_info()
        return {"error": "任务不存在"}


# ==================== 启动钩子 ====================
@app.add_startup_hook
def startup_hook():
    """启动钩子"""
    print("🚀 应用启动钩子执行")

    from myboot.core.application import get_client

    # 初始化数据库连接
    db_client = get_client('database_client')
    if db_client:
        db_client.connect()

    # 初始化 Redis 连接
    redis_client = get_client('redis_client')
    if redis_client:
        redis_client.set('app_status', 'running')


@app.add_shutdown_hook
def shutdown_hook():
    """关闭钩子"""
    print("🛑 应用关闭钩子执行")


# ==================== 主程序 ====================

if __name__ == "__main__":
    print("=" * 60)
    print("🎯 MyBoot 约定优于配置示例")
    print("=" * 60)
    print()
    print("✨ 特性展示:")
    print("  • 自动发现和注册组件")
    print("  • 约定优于配置")
    print("  • 零配置启动")
    print("  • 自动服务注入")
    print("  • 自动路由注册")
    print("  • 自动任务调度")
    print()
    print("🌐 访问地址:")
    print("  • 应用: http://localhost:8000")
    print("  • API 文档: http://localhost:8000/docs")
    print("  • 健康检查: http://localhost:8000/health")
    print()
    print("📚 API 端点:")
    print("  • GET  /                    - 首页")
    print("  • GET  /users               - 用户列表")
    print("  • GET  /users/{id}          - 获取用户")
    print("  • POST /users               - 创建用户")
    print("  • PUT  /users/{id}          - 更新用户")
    print("  • DELETE /users/{id}        - 删除用户")
    print("  • GET  /api/products        - 产品列表")
    print("  • POST /api/products        - 创建产品")
    print("  • PUT  /api/products/{id}   - 更新产品")
    print("  • DELETE /api/products/{id} - 删除产品")
    print()
    print("⏰ 定时任务:")
    print("  • 心跳检测 (每分钟)")
    print("  • 清理任务 (每2分钟)")
    print("  • 新年任务 (2024-12-31 23:59:59)")
    print()
    print("=" * 60)

    # 启动应用
    app.run(host="0.0.0.0", port=8000, reload=False)
