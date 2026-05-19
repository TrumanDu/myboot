# MyBoot - 类似 Spring Boot 的 Python 快速开发框架

[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![PyPI Version](https://img.shields.io/pypi/v/myboot.svg)](https://pypi.org/project/myboot/)

MyBoot 是一个功能丰富的 Python Web 框架，提供类似 Spring Boot 的自动配置和快速开发功能。它集成了 Web API、定时任务、日志管理、配置管理等核心功能，让您能够快速构建现代化的 Python 应用程序。

## ✨ 主要特性

- 🚀 **快速启动**: 类似 Spring Boot 的自动配置和快速启动
- 🎯 **约定优于配置**: 遵循约定，减少配置工作，自动发现和注册组件
- 🌐 **Web API**: 基于 FastAPI 的高性能 Web API 开发
- ⚡ **高性能服务器**: 默认使用 Hypercorn 服务器，支持 HTTP/2 和多进程
- ⏰ **定时任务**: 强大的任务调度系统，支持 Cron 表达式和间隔任务（详见 [任务调度器使用说明](docs/scheduler.md)）
- 📝 **日志管理**: 基于 loguru 的强大日志系统，支持结构化日志和第三方库日志控制
- ⚙️ **配置管理**: 基于 Dynaconf 的强大配置系统，支持 YAML 配置、环境变量覆盖和远程配置
- 🔧 **中间件支持**: 丰富的中间件生态，包括 CORS、限流、安全等
- 📊 **健康检查**: 内置健康检查、就绪检查和存活检查
- 🎯 **依赖注入**: 简单的依赖注入和组件管理
- 🔄 **优雅关闭**: 支持优雅关闭和资源清理
- 📚 **自动文档**: 自动生成 API 文档和交互式界面

## 🚀 快速开始

### 安装

```bash
pip install myboot
```

### 命令行工具

MyBoot 提供了便捷的命令行工具用于初始化项目：

```bash
# 显示帮助信息
myboot --help

# 初始化新项目（交互式）
myboot init

# 使用指定模板初始化项目
myboot init --name my-app --template basic    # 基础模板
myboot init --name my-app --template api      # API 项目模板
myboot init --name my-app --template full     # 完整项目模板

# 显示框架信息
myboot info
```

### 创建应用

使用 `myboot init` 初始化项目后，在 `main.py` 中创建应用：

```python
"""main.py - 应用入口文件"""
from myboot.core.application import create_app

# 创建应用实例
app = create_app(name="我的应用")

# 运行应用
if __name__ == "__main__":
    app.run()
```

在 `app/api/` 目录中定义路由（使用 `@rest_controller` 装饰器）：

```python
"""app/api/routes.py"""
from myboot.core.decorators import rest_controller, get, post

@rest_controller('/api')
class HelloController:
    """Hello 控制器"""

    @get('/')
    def hello(self):
        """Hello World 接口 - GET /api"""
        return {"message": "Hello, MyBoot!", "status": "success"}

@rest_controller('/api/users')
class UserController:
    """用户控制器 - 支持依赖注入"""

    def __init__(self, user_service: UserService):
        self.user_service = user_service

    @get('/{user_id}')
    def get_user(self, user_id: int):
        """获取用户 - GET /api/users/{user_id}"""
        return self.user_service.get_user(user_id)
```

### 运行应用

应用入口文件位于项目根目录的 `main.py`：

```bash
# 直接运行
python main.py

# 启用自动重载（开发环境）
python main.py --reload

# 指定端口和主机
python main.py --host 0.0.0.0 --port 8080
```

访问 http://localhost:8000 查看您的应用！

## 🎯 约定优于配置

MyBoot 框架的核心设计理念是"约定优于配置"，让您能够快速开发而无需复杂的配置。

### 自动发现和注册

```python
from myboot.core.decorators import service, rest_controller, get, cron, component

@service()
class UserService:
    """用户服务 - 自动注册为 'user_service'"""
    def get_user(self, user_id):
        return {"id": user_id, "name": f"用户{user_id}"}

@rest_controller('/api/users')
class UserController:
    """用户控制器 - 支持依赖注入"""

    def __init__(self, user_service: UserService):
        self.user_service = user_service

    @get('/{user_id}')
    def get_user(self, user_id: int):
        """获取用户 - GET /api/users/{user_id}"""
        return self.user_service.get_user(user_id)

@component()
class ScheduledJobs:
    """定时任务组件 - 使用 @component 装饰器定义定时任务"""

    @cron('0 */5 * * * *')
    def cleanup_task(self):
        """清理任务 - 自动注册定时任务"""
        print("执行清理任务")
```

### 零配置启动

```python
from myboot.core.application import Application

# 创建应用，自动发现和配置所有组件
app = Application(
    name="我的应用",
    auto_configuration=True,  # 启用自动配置
    auto_discover_package="app"  # 自动发现 app 包
)

# 直接运行，无需手动注册
app.run()
```

### 依赖注入和服务管理

MyBoot 提供了基于 `dependency_injector` 的强大依赖注入机制，支持自动依赖解析和注入，让您可以轻松管理服务之间的依赖关系。

#### 自动依赖注入

框架会自动检测服务的依赖关系并自动注入，无需手动获取：

```python
from myboot.core.decorators import service

@service()
class UserService:
    def __init__(self):
        self.users = {}

@service()
class EmailService:
    def send_email(self, to: str, subject: str):
        print(f"发送邮件到 {to}: {subject}")

@service()
class OrderService:
    # 自动注入 UserService 和 EmailService
    def __init__(self, user_service: UserService, email_service: EmailService):
        self.user_service = user_service
        self.email_service = email_service

    def create_order(self, user_id: int):
        user = self.user_service.get_user(user_id)
        self.email_service.send_email(user['email'], "订单创建", "您的订单已创建")
```

**特性：**

- ✅ 自动检测依赖关系
- ✅ 自动处理依赖顺序
- ✅ 支持多级依赖
- ✅ 支持可选依赖（`Optional[Type]`）
- ✅ 自动检测循环依赖
- ✅ 向后兼容，现有代码无需修改

**更多信息：** 查看 [依赖注入使用指南](docs/dependency-injection.md)

#### 获取服务 (get_service)

服务是通过 `@service()` 装饰器自动注册的。**推荐方式：在控制器构造函数中通过类型注解自动注入。**

**方式一：依赖注入（推荐）**

```python
from myboot.core.decorators import rest_controller, get, service

@service()
class UserService:
    def get_user(self, user_id: int):
        return {"user_id": user_id}

@rest_controller('/api/users')
class UserController:
    def __init__(self, user_service: UserService):
        # 通过构造函数自动注入服务
        self.user_service = user_service

    @get('/{user_id}')
    def get_user(self, user_id: int):
        return self.user_service.get_user(user_id)
```

**方式二：通过全局函数（适用于非控制器场景）**

```python
from myboot.core.application import get_service

# 在启动钩子或其他地方获取服务
def some_function():
    user_service = get_service('user_service')
    return user_service.get_user(1)
```

#### 获取客户端 (get_client)

客户端是通过 `@client()` 装饰器自动注册的。**推荐方式：在控制器构造函数中通过类型注解自动注入。**

**方式一：依赖注入（推荐）**

```python
from myboot.core.decorators import rest_controller, get, client

@client()
class RedisClient:
    def get(self, key: str):
        return None

@rest_controller('/api/products')
class ProductController:
    def __init__(self, redis_client: RedisClient):
        # 通过构造函数自动注入客户端
        self.redis_client = redis_client

    @get('/')
    def get_products(self):
        cache_data = self.redis_client.get('products')
        return {"products": cache_data or []}
```

**方式二：通过全局函数（适用于非控制器场景）**

```python
from myboot.core.application import get_client

# 在启动钩子或其他地方获取客户端
def some_function():
    redis_client = get_client('redis_client')
    return redis_client.get('products')
```

#### 完整示例

```python
from myboot.core.decorators import service, client, rest_controller, get, post

# 定义服务
@service()
class UserService:
    """用户服务 - 自动注册为 'user_service'"""
    def get_user(self, user_id: int):
        return {"id": user_id, "name": f"用户{user_id}"}

    def create_user(self, name: str, email: str):
        return {"name": name, "email": email}

@service()
class EmailService:
    """邮件服务"""
    def send_email(self, to: str, subject: str, body: str):
        print(f"发送邮件到 {to}")

# 定义客户端
@client('redis_client')
class RedisClient:
    """Redis 客户端 - 注册为 'redis_client'"""
    def get(self, key: str):
        return None

# 控制器中使用依赖注入
@rest_controller('/api/users')
class UserController:
    """用户控制器 - 自动注入服务和客户端"""

    def __init__(self, user_service: UserService, email_service: EmailService, redis_client: RedisClient):
        self.user_service = user_service
        self.email_service = email_service
        self.redis_client = redis_client

    @get('/{user_id}')
    def get_user(self, user_id: int):
        """获取用户"""
        # 先检查缓存
        cache_key = f"user:{user_id}"
        cached = self.redis_client.get(cache_key)
        if cached:
            return cached
        return self.user_service.get_user(user_id)

    @post('/')
    def create_user(self, name: str, email: str):
        """创建用户"""
        user = self.user_service.create_user(name, email)
        self.email_service.send_email(email, "欢迎", f"欢迎 {name}")
        return {"message": "用户创建成功", "user": user}
```

#### 服务命名规则

- **默认命名**: 如果未指定名称，服务名会自动转换为类名的小写形式，并使用下划线分隔
  - `UserService` → `'user_service'`
  - `EmailService` → `'email_service'`
  - `DatabaseClient` → `'database_client'`
  - `RedisClient` → `'redis_client'`
- **自定义命名**: 可以通过装饰器参数指定名称
  - `@service('email_service')` → `'email_service'`
  - `@client('redis_client')` → `'redis_client'`

#### 注意事项

1. **推荐依赖注入**: 在控制器中推荐使用构造函数依赖注入，代码更清晰、可测试性更好
2. **服务必须已注册**: 确保服务或客户端已经通过装饰器注册
3. **全局函数适用场景**: `get_service()` 和 `get_client()` 适用于启动钩子、工具函数等非控制器场景
4. **路由定义**: 所有路由必须在 `@rest_controller` 装饰的类中定义

### 约定规则

- **服务命名**: 类名自动转换为下划线分隔的小写形式作为服务名（如 `UserService` → `user_service`）
- **路由映射**: 使用 `@rest_controller` 装饰器定义路由，方法装饰器 `@get`、`@post` 等定义具体端点
- **任务调度**: 在 `@component` 类中使用 `@cron`、`@interval`、`@once` 装饰器（详见 [任务调度器使用说明](docs/scheduler.md)）
- **组件扫描**: 自动扫描指定包中的所有组件

## ⚡ 高性能服务器

MyBoot 默认使用 Hypercorn 作为 ASGI 服务器，提供卓越的性能和特性：

### 服务器特性

- **高性能**: 基于 Hypercorn 的高性能 ASGI 服务器
- **HTTP/2 支持**: 支持现代 HTTP 协议
- **WebSocket 支持**: 支持实时通信
- **多进程支持**: 支持多工作进程，适合生产环境
- **自动重载**: 开发环境支持自动重载
- **优雅关闭**: 支持优雅关闭和资源清理

### 使用示例

```python
from myboot.core.application import Application

# 创建应用
app = Application(name="我的应用")

# 开发环境（单进程 + 自动重载）
app.run(host="0.0.0.0", port=8000, reload=True, workers=1)

# 生产环境（多进程）
app.run(host="0.0.0.0", port=8000, workers=4)

# 或者直接运行 main.py
# python main.py --reload  # 开发环境
# python main.py --workers 4  # 生产环境
```

## ⚙️ 配置管理

MyBoot 使用 Dynaconf 提供强大的配置管理功能：

### 基本使用

```python
from myboot.core.config import get_settings, get_config

# 直接使用 Dynaconf settings（自动查找配置文件）
settings = get_settings()
app_name = settings.app.name
server_port = settings.server.port

# 使用便捷函数
database_url = get_config('database.url', 'sqlite:///./app.db')
debug_mode = get_config('app.debug', False)

# 指定配置文件路径
settings = get_settings('custom_config.yaml')

# 通过环境变量指定配置文件
# export CONFIG_FILE=/path/to/config.yaml
# 或
# export CONFIG_FILE=https://example.com/config.yaml
```

### 环境变量覆盖

环境变量可以直接覆盖配置值（使用 `__` 作为分隔符），优先级高于所有配置文件：

```bash
# 使用环境变量覆盖配置值
export APP__NAME="MyApp"
export SERVER__PORT=9000
export LOGGING__LEVEL=DEBUG

# 嵌套配置使用双下划线分隔
export SERVER__CORS__ALLOW_ORIGINS='["http://localhost:3000"]'
```

**注意**：环境变量覆盖配置值的优先级最高，会覆盖所有配置文件中的对应值。

### 远程配置

```python
from myboot.core.config import get_settings

# 从远程 URL 加载配置
settings = get_settings('https://example.com/config.yaml')
```

### 配置优先级

MyBoot 按照以下优先级查找和加载配置文件：

1. **环境变量 `CONFIG_FILE`**（最高优先级）

   - 通过环境变量指定配置文件路径或 URL

   ```bash
   export CONFIG_FILE=/path/to/config.yaml
   # 或
   export CONFIG_FILE=https://example.com/config.yaml
   ```

2. **参数指定的配置文件**

   - 通过 `create_app()` 或 `get_settings()` 的 `config_file` 参数指定

   ```python
   app = create_app(name="我的应用", config_file="custom_config.yaml")
   ```

3. **项目根目录 `/conf` 目录下的配置文件**

   - `项目根目录/conf/config.yaml`
   - `项目根目录/conf/config.yml`

4. **项目根目录下的配置文件**

   - `项目根目录/config.yaml`
   - `项目根目录/config.yml`

5. **默认配置**
   - 内置的默认配置值

**注意**：环境变量还可以直接覆盖配置值（使用 `__` 作为分隔符），优先级高于所有配置文件：

```bash
export APP__NAME="MyApp"
export SERVER__PORT=9000
export LOGGING__LEVEL=DEBUG
```

## 📖 详细文档

- [📚 完整文档](docs/README.md) - 文档中心
- [⚡ REST API 异步任务](docs/rest-api-async-tasks.md) - REST API 中使用异步任务指南
- [🔧 依赖注入](docs/dependency-injection.md) - 依赖注入使用指南
- [⏰ 任务调度器](docs/scheduler.md) - Cron / 间隔 / 一次性任务与配置说明

### 1. Web API 开发

**重要**：路由必须在 `@rest_controller` 装饰的类中定义，支持依赖注入。

#### REST 控制器（推荐方式）

```python
from myboot.core.decorators import rest_controller, get, post, put, delete, service
from myboot.web.models import BaseResponse


@service()
class UserService:
    """用户服务"""
    def get_users(self):
        return []

    def get_user(self, user_id: int):
        return {"user_id": user_id, "name": f"用户{user_id}"}

    def create_user(self, name: str, email: str):
        return {"name": name, "email": email}

    def update_user(self, user_id: int, **kwargs):
        return {"user_id": user_id, **kwargs}

    def delete_user(self, user_id: int):
        return {"user_id": user_id}


@rest_controller('/api/users')
class UserController:
    """用户控制器 - 自动注入 UserService"""

    def __init__(self, user_service: UserService):
        self.user_service = user_service

    @get('/')
    def get_users(self):
        """获取用户列表 - GET /api/users"""
        users = self.user_service.get_users()
        return BaseResponse(success=True, message="获取用户列表成功", data={"users": users})

    @get('/{user_id}')
    def get_user(self, user_id: int):
        """获取单个用户 - GET /api/users/{user_id}"""
        user = self.user_service.get_user(user_id)
        return BaseResponse(success=True, message="获取用户成功", data=user)

    @post('/')
    def create_user(self, name: str, email: str):
        """创建用户 - POST /api/users"""
        user = self.user_service.create_user(name, email)
        return BaseResponse(success=True, message="用户创建成功", data=user)

    @put('/{user_id}')
    def update_user(self, user_id: int, name: str = None, email: str = None):
        """更新用户 - PUT /api/users/{user_id}"""
        update_data = {}
        if name:
            update_data['name'] = name
        if email:
            update_data['email'] = email
        user = self.user_service.update_user(user_id, **update_data)
        return BaseResponse(success=True, message=f"用户 {user_id} 更新成功", data=user)

    @delete('/{user_id}')
    def delete_user(self, user_id: int):
        """删除用户 - DELETE /api/users/{user_id}"""
        user = self.user_service.delete_user(user_id)
        return BaseResponse(success=True, message=f"用户 {user_id} 删除成功", data=user)


# 约定优于配置说明：
# 1. 使用 @rest_controller 装饰器定义控制器类和基础路径
# 2. 使用 @get, @post, @put, @delete 装饰器定义路由方法
# 3. 构造函数参数自动进行依赖注入
# 4. 框架自动发现和注册控制器
# 5. 统一的响应格式和错误处理
```

#### REST 控制器

使用 `@rest_controller` 装饰器可以创建 REST 控制器类，为类中的方法提供统一的基础路径。类中的方法需要显式使用 `@get`、`@post`、`@put`、`@delete`、`@patch` 等装饰器才会生成路由。

**基本用法：**

```python
from myboot.core.decorators import rest_controller, get, post, put, delete
from myboot.web.models import BaseResponse

@rest_controller('/api/users')
class UserController:
    """用户控制器"""

    def __init__(self):
        # 可以在这里初始化服务、客户端等
        pass

    @get('/')
    def list_users(self):
        """获取用户列表 - GET /api/users"""
        return BaseResponse(
            success=True,
            message="获取用户列表成功",
            data={"users": []}
        )

    @get('/{user_id}')
    def get_user(self, user_id: int):
        """获取单个用户 - GET /api/users/{user_id}"""
        return BaseResponse(
            success=True,
            message="获取用户成功",
            data={"user_id": user_id, "name": f"用户{user_id}"}
        )

    @post('/')
    def create_user(self, name: str, email: str):
        """创建用户 - POST /api/users"""
        return BaseResponse(
            success=True,
            message="用户创建成功",
            data={"name": name, "email": email}
        )

    @put('/{user_id}')
    def update_user(self, user_id: int, name: str = None, email: str = None):
        """更新用户 - PUT /api/users/{user_id}"""
        return BaseResponse(
            success=True,
            message=f"用户 {user_id} 更新成功",
            data={"user_id": user_id, "name": name, "email": email}
        )

    @delete('/{user_id}')
    def delete_user(self, user_id: int):
        """删除用户 - DELETE /api/users/{user_id}"""
        return BaseResponse(
            success=True,
            message=f"用户 {user_id} 删除成功",
            data={"user_id": user_id}
        )
```

**路径合并规则：**

- 方法路径以 `//` 开头：作为绝对路径使用（去掉一个 `/`）
- 方法路径以 `/` 开头：去掉开头的 `/` 后追加到基础路径
- 方法路径不以 `/` 开头：直接追加到基础路径

**示例：**

```python
@rest_controller('/api/reports')
class ReportController:
    """报告控制器"""

    @post('/generate')  # 最终路径: POST /api/reports/generate
    def create_report(self, report_type: str):
        return {"message": "报告生成任务已创建", "type": report_type}

    @get('/status/{job_id}')  # 最终路径: GET /api/reports/status/{job_id}
    def get_status(self, job_id: str):
        return {"status": "completed", "job_id": job_id}

    @get('//health')  # 最终路径: GET /health (绝对路径)
    def health_check(self):
        return {"status": "ok"}
```

**在控制器中使用依赖注入：**

```python
from myboot.core.decorators import rest_controller, get, post, service, client
from myboot.web.models import BaseResponse

@service()
class ProductService:
    def get_all(self):
        return []
    def create(self, name: str, price: float):
        return {"name": name, "price": price}

@client()
class RedisClient:
    def set(self, key: str, value):
        pass

@rest_controller('/api/products')
class ProductController:
    """产品控制器 - 使用依赖注入"""

    def __init__(self, product_service: ProductService, redis_client: RedisClient):
        # 通过构造函数自动注入
        self.product_service = product_service
        self.redis_client = redis_client

    @get('/')
    def list_products(self):
        """获取产品列表"""
        products = self.product_service.get_all()
        return BaseResponse(success=True, data={"products": products})

    @post('/')
    def create_product(self, name: str, price: float):
        """创建产品"""
        product = self.product_service.create(name, price)
        self.redis_client.set(f"product:{product['name']}", product)
        return BaseResponse(success=True, data={"product": product})
```

**注意事项：**

1. **显式装饰器**：类中的方法必须显式使用 `@get`、`@post` 等装饰器才会生成路由
2. **路径合并**：方法路径会自动与基础路径合并，形成最终的路由路径
3. **自动注册**：控制器类会被自动发现和注册，无需手动配置
4. **依赖注入**：在构造函数中声明类型注解，框架自动注入服务和客户端

#### 数据模型

```python
from pydantic import BaseModel
from typing import Optional
from myboot.core.decorators import rest_controller, post
from myboot.web.models import BaseResponse

class User(BaseModel):
    """用户数据模型"""
    id: Optional[int] = None
    name: str
    email: str
    age: Optional[int] = None

@rest_controller('/api/users')
class UserController:
    @post('/')
    def create_user(self, user: User):
        """创建用户"""
        return BaseResponse(success=True, message="用户创建成功", data=user.dict())
```

#### 分页处理

```python
from myboot.core.decorators import rest_controller, get
from myboot.web.models import BaseResponse
from typing import Optional

@rest_controller('/api/users')
class UserController:
    @get('/')
    def get_users(self, page: int = 1, size: int = 10, search: Optional[str] = None):
        """获取用户列表（分页） - GET /api/users"""
        # 处理分页逻辑
        return BaseResponse(
            success=True,
            message="获取用户列表成功",
            data={"users": [], "total": 0, "page": page, "size": size}
        )
```

### 2. 定时任务

**重要**：定时任务必须在 `@component` 装饰的类中定义，支持依赖注入。完整说明见 [任务调度器使用说明](docs/scheduler.md)。

#### Cron 表达式任务

```python
from myboot.core.decorators import component, cron, interval, once
from myboot.core.config import get_config

@component()
class ScheduledJobs:
    """定时任务组件"""

    @cron("0 0 * * * *", enabled=True)  # 每小时执行
    def hourly_task(self):
        print("每小时任务")

    # 从配置文件读取 enabled 状态
    @cron("0 0 2 * * *", enabled=get_config('jobs.cleanup_task.enabled', True))
    def daily_backup(self):
        """每天凌晨2点执行"""
        print("每日备份")
```

#### 间隔任务

```python
@component()
class MonitorJobs:
    """监控任务组件"""

    @interval(seconds=30, enabled=True)  # 每30秒执行
    def heartbeat(self):
        print("心跳检测")

    @interval(minutes=5, enabled=get_config('jobs.monitor.enabled', True))
    def monitor(self):
        """每5分钟执行"""
        print("系统监控")
```

#### 一次性任务

```python
@component()
class OneTimeJobs:
    """一次性任务组件"""

    @once("2025-12-31 23:59:59", enabled=True)
    def new_year_task(self):
        """新年任务 - 过期后不再执行"""
        print("新年任务")
```

#### 带依赖注入的定时任务

```python
from myboot.core.decorators import component, service, cron

@service()
class DataService:
    def sync_data(self):
        print("同步数据...")

@component()
class DataSyncJobs:
    """数据同步任务 - 自动注入 DataService"""

    def __init__(self, data_service: DataService):
        self.data_service = data_service

    @cron("0 2 * * *")  # 每天凌晨2点
    def sync_daily(self):
        self.data_service.sync_data()
```

### 3. 配置管理

#### 配置文件 (config.yaml)

```yaml
# 应用配置
app:
  name: "我的应用"
  version: "1.0.0"
  debug: true

# 服务器配置
server:
  host: "0.0.0.0"
  port: 8000
  reload: true

# 数据库配置
database:
  url: "sqlite:///./app.db"
  pool_size: 10

# 任务调度配置
scheduler:
  enabled: true # 是否启用调度器
  timezone: "Asia/Shanghai" # 时区设置（可选，需要安装 pytz）
  max_workers: 10 # 最大工作线程数

# 任务启用配置（可选）
jobs:
  heartbeat:
    enabled: true
  cleanup_task:
    enabled: false
  monitor:
    enabled: true

# 日志配置
logging:
  level: "INFO" # 日志级别: DEBUG, INFO, WARNING, ERROR, CRITICAL
  format: "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
  file: "logs/app.log" # 可选，如果配置会自动添加文件 handler
  # 第三方库日志级别配置
  third_party:
    urllib3: "WARNING"
    requests: "WARNING"
    hypercorn: "WARNING"
```

#### 配置使用

```python
from myboot.core.config import get_settings, get_config, get_config_bool, get_config_str

# 方式一：使用 get_settings() 获取完整配置对象
settings = get_settings()
port = settings.get("server.port", 8000)
debug = settings.get("app.debug", False)

# 方式二：使用便捷函数（推荐）
port = get_config("server.port", 8000)
debug = get_config_bool("app.debug", False)
db_url = get_config_str("database.url", "sqlite:///./app.db")

# 在应用实例中也可以直接使用
port = app.config.get("server.port", 8000)
```

#### 调度器配置

Cron 表达式格式、星期字段含义及多 Worker 行为见 [任务调度器使用说明](docs/scheduler.md)。

```yaml
# 任务调度配置
scheduler:
  enabled: true # 是否启用调度器
  timezone: "Asia/Shanghai" # 时区设置（需要安装 pytz）
  max_workers: 10 # 最大工作线程数
```

```python
# 获取调度器配置
config = app.scheduler.get_config()
print(config)  # {'enabled': True, 'timezone': 'Asia/Shanghai', ...}

# 列出所有任务
jobs = app.scheduler.list_all_jobs()
for job in jobs:
    print(job)

# 获取单个任务信息
job_info = app.scheduler.get_job_info('cron_heartbeat')
print(job_info)
```

#### 任务启用控制

任务装饰器支持 `enabled` 参数，可以控制任务是否启用：

```python
from myboot.core.decorators import component, cron, interval, once
from myboot.core.config import get_config

@component()
class TaskControlDemo:
    """任务控制示例"""

    # 方式一：直接指定
    @cron("0 */1 * * * *", enabled=True)  # 启用
    def enabled_task(self):
        print("启用状态")

    @interval(minutes=2, enabled=False)  # 禁用
    def disabled_task(self):
        print("禁用状态")

    # 方式二：从配置文件读取
    @once("2025-01-01 00:00:00", enabled=get_config('jobs.my_task.enabled', True))
    def configurable_task(self):
        print("可配置任务")
```

**注意**：

- 定时任务必须在 `@component` 装饰的类中定义
- 如果 `enabled` 为 `None`，默认启用
- 一次性任务如果时间已过期，将自动标记为过期不再执行
- 已执行的一次性任务不会重复执行

### 4. 日志管理

MyBoot 使用 [loguru](https://github.com/Delgan/loguru) 作为日志系统，提供强大的日志功能和优雅的 API。

#### 基本使用

```python
# 使用框架导出的 logger
from myboot.core.logger import logger

logger.info("应用启动")
logger.error("发生错误")
logger.debug("调试信息")
logger.warning("警告信息")
```

#### 日志配置

日志系统会在应用启动时自动根据配置文件初始化，无需手动配置。

**配置文件示例 (config.yaml):**

```yaml
# 日志配置
logging:
  # 日志级别: DEBUG, INFO, WARNING, ERROR, CRITICAL
  level: "INFO"

  # 日志格式（支持 loguru 格式或标准 logging 格式，会自动转换）
  # 如果设置了 json: true，此选项将被忽略
  format: "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"

  # 是否使用 JSON 格式输出（适用于日志聚合和分析工具）
  # 设置为 true 时，日志以 JSON 格式输出，包含完整的结构化信息
  json: false

  # 日志文件路径（可选，如果配置会自动添加文件 handler，支持自动轮转）
  file: "logs/app.log"

  # 第三方库日志级别配置（用于控制第三方库的日志输出）
  third_party:
    urllib3: "WARNING" # 只显示 WARNING 及以上级别
    requests: "WARNING" # 只显示 WARNING 及以上级别
    hypercorn: "WARNING" # 只显示 WARNING 及以上级别
    hypercorn.error: "WARNING" # hypercorn.error logger
    asyncio: "INFO" # 显示 INFO 及以上级别
```

#### JSON 格式日志

启用 JSON 格式后，日志会以结构化 JSON 格式输出，便于日志聚合和分析工具（如 ELK、Loki、Grafana 等）处理：

```yaml
logging:
  level: "INFO"
  json: true # 启用 JSON 格式输出
  file: "logs/app.log"
```

JSON 格式日志包含以下字段：

- `text`: 格式化的日志文本
- `record`: 完整的日志记录对象
  - `time`: 时间戳
  - `level`: 日志级别
  - `message`: 日志消息
  - `name`: logger 名称
  - `module`: 模块名
  - `function`: 函数名
  - `file`: 文件名和路径
  - `line`: 行号
  - `process`: 进程信息
  - `thread`: 线程信息
  - `exception`: 异常信息（如果有）

#### 日志格式说明

**Loguru 格式（推荐）：**

```python
format: "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
```

**标准 logging 格式（会自动转换）：**

```python
format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

#### 高级功能

```python
from myboot.core.logger import logger

# 结构化日志
logger.info("用户登录", user_id=123, username="admin")

# 异常日志（自动包含堆栈跟踪）
try:
    1 / 0
except:
    logger.exception("发生错误")

# 绑定上下文信息
logger.bind(user_id=123).info("用户操作")

# 临时修改日志级别
with logger.contextualize(level="DEBUG"):
    logger.debug("这是调试信息")

# 添加自定义 handler（保留用户自定义 loguru 的能力）
from myboot.core.logger import logger
logger.add("custom.log", rotation="100 MB", retention="30 days")
```

#### 手动初始化（可选）

如果需要手动初始化日志系统：

```python
from myboot.core.logger import setup_logging

# 使用默认配置初始化
setup_logging()

# 使用指定配置文件初始化
setup_logging("custom_config.yaml")
```

#### 配置参数说明

| 参数                            | 类型 | 说明                                                                 | 默认值          |
| ------------------------------- | ---- | -------------------------------------------------------------------- | --------------- |
| `logging.level`                 | str  | 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)                         | INFO            |
| `logging.format`                | str  | 日志格式（支持 loguru 格式或标准 logging 格式，json 为 true 时忽略） | loguru 默认格式 |
| `logging.json`                  | bool | 是否使用 JSON 格式输出（适用于日志聚合和分析工具）                   | false           |
| `logging.file`                  | str  | 日志文件路径，如果配置会自动添加文件 handler                         | 无              |
| `logging.third_party.{library}` | str  | 第三方库日志级别，支持设置任意第三方库的日志级别                     | 无              |

#### 文件日志特性

如果配置了 `logging.file`，loguru 会自动提供：

- **自动轮转**: 当日志文件达到 10MB 时自动轮转
- **自动压缩**: 旧日志文件自动压缩为 zip
- **自动清理**: 保留 7 天的日志文件
- **异常信息**: 自动包含完整的堆栈跟踪

#### 第三方库日志控制

通过 `logging.third_party` 配置可以控制第三方库的日志输出级别：

```yaml
logging:
  third_party:
    urllib3: "WARNING" # 隐藏 urllib3 的 INFO 和 DEBUG 日志
    requests: "WARNING" # 隐藏 requests 的 INFO 和 DEBUG 日志
    hypercorn: "WARNING" # 隐藏 hypercorn 的 INFO 和 DEBUG 日志
    asyncio: "INFO" # 只显示 asyncio 的 INFO 及以上级别
```

这样可以有效减少第三方库的噪音日志，让日志更加清晰。

### 5. 中间件

MyBoot 支持通过装饰器定义中间件，中间件会自动注册：

```python
from myboot.core.decorators import middleware
from fastapi import Request

@middleware(order=1, path_filter='/api/*')
def api_middleware(request: Request, next_handler):
    """API 中间件 - 只处理 /api/* 路径"""
    # 前置处理
    print(f"处理请求: {request.method} {request.url}")

    # 调用下一个处理器
    response = next_handler(request)

    # 后置处理
    print(f"响应状态: {response.status_code}")
    return response

@middleware(order=2, methods=['POST', 'PUT'])
def post_middleware(request: Request, next_handler):
    """POST/PUT 中间件 - 只处理 POST 和 PUT 请求"""
    # 可以在这里添加请求验证、日志记录等
    return next_handler(request)
```

**中间件参数说明：**

- `order`: 执行顺序，数字越小越先执行（默认 0）
- `path_filter`: 路径过滤，支持字符串、字符串列表或正则表达式，如 `'/api/*'`, `['/api/*', '/admin/*']`
- `methods`: HTTP 方法过滤，如 `['GET', 'POST']`（默认 None，处理所有方法）
- `condition`: 条件函数，接收 request 对象，返回 bool 决定是否执行中间件

**注意**：CORS 中间件可以通过配置文件启用，无需手动添加。

### 6. 生命周期钩子

```python
from myboot.core.application import create_app

app = create_app(name="我的应用")

# 添加启动钩子
def startup_hook():
    """应用启动时执行"""
    print("应用启动")

app.add_startup_hook(startup_hook)

# 添加关闭钩子
def shutdown_hook():
    """应用关闭时执行"""
    print("应用关闭")

app.add_shutdown_hook(shutdown_hook)
```

## 📁 项目结构

### 标准项目结构（推荐）

使用 `myboot init` 命令创建的标准项目结构：

```
my-app/
├── main.py              # 应用入口（根目录）
├── pyproject.toml        # 项目配置文件
├── .gitignore           # Git 忽略文件
├── app/                  # 应用代码
│   ├── api/              # API 路由
│   ├── service/          # 业务逻辑层
│   ├── model/            # 数据模型
│   ├── jobs/             # 定时任务
│   └── client/           # 客户端（第三方API调用等）
├── conf/                 # 配置文件目录
│   └── config.yaml       # 主配置文件
└── tests/                # 测试代码
```

### 目录说明

- **main.py**: 应用入口文件，位于项目根目录
- **app/api/**: API 路由层，存放所有路由定义
- **app/service/**: 业务逻辑层，存放业务服务类
- **app/model/**: 数据模型层，存放 Pydantic 模型等
- **app/jobs/**: 定时任务组件，存放使用 `@component` 装饰的类，类中方法可使用 `@cron`、`@interval` 等装饰器
- **app/client/**: 客户端层，存放第三方服务客户端（如 Redis、HTTP 客户端等）
- **conf/**: 配置文件目录，存放 YAML 配置文件
- **tests/**: 测试代码目录

## 🔧 高级功能

### 1. 自定义中间件

使用装饰器定义中间件（推荐方式）：

```python
from myboot.core.decorators import middleware
from fastapi import Request

@middleware(order=1)
def custom_middleware(request: Request, next_handler):
    """自定义中间件"""
    # 前置处理
    print(f"请求: {request.method} {request.url}")

    # 调用下一个处理器
    response = next_handler(request)

    # 后置处理
    print(f"响应: {response.status_code}")
    return response
```

或者使用 FastAPI 的 BaseHTTPMiddleware：

```python
from myboot.web.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request

class CustomMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 中间件逻辑
        response = await call_next(request)
        return response

# 在应用初始化时添加
app.add_middleware(Middleware(CustomMiddleware))
```

### 2. 异步任务

在 REST API 中使用异步任务，避免阻塞请求响应。

#### 快速启动后台任务

```python
from myboot.core.decorators import post
from myboot.utils.async_utils import asyn_run
import time

def process_data(data: dict):
    """耗时的数据处理任务"""
    print(f"开始处理数据: {data}")
    time.sleep(5)  # 模拟耗时操作
    print(f"数据处理完成: {data}")
    return {"processed": True, "data": data}

@post('/api/tasks')
def create_task(data: dict):
    """创建异步任务 - 立即返回，任务在后台执行"""
    asyn_run(process_data, data, task_name="数据处理任务")
    return {"message": "任务已创建，正在后台处理"}
```

#### 使用 ScheduledJob

对于需要跟踪任务状态的场景：

```python
from myboot.core.decorators import post, get, rest_controller
from myboot.jobs.scheduled_job import ScheduledJob
from myboot.core.scheduler import get_scheduler
import threading

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
                """生成报告任务"""
                import time
                print(f"开始生成 {self.report_type} 报告")
                time.sleep(10)  # 模拟报告生成
                return {"type": self.report_type, "status": "completed"}

        # 创建任务实例
        job = ReportJob(report_type)

        # 添加到调度器（用于状态跟踪，非定时任务）
        job_id = self.scheduler.add_job_object(job)
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
```

更多详细内容请参考 [REST API 异步任务文档](docs/rest-api-async-tasks.md)。

### 3. 任务管理

#### 调度器任务管理

更多 API 与排查说明见 [任务调度器使用说明](docs/scheduler.md)。

```python
# 获取调度器配置
config = app.scheduler.get_config()
print(config)  # {'enabled': True, 'timezone': 'Asia/Shanghai', 'running': True, 'job_count': 3}

# 列出所有任务
jobs = app.scheduler.list_all_jobs()
for job in jobs:
    print(f"任务ID: {job['job_id']}, 类型: {job['type']}, 函数: {job['func_name']}")

# 获取单个任务信息
job_info = app.scheduler.get_job_info('cron_heartbeat')
if job_info:
    print(f"Cron表达式: {job_info.get('cron')}")
    print(f"是否已执行: {job_info.get('executed', False)}")
    print(f"是否已过期: {job_info.get('expired', False)}")

# 检查调度器是否启用
if app.scheduler.is_enabled():
    print("调度器已启用")
```

#### 使用 ScheduledJob 管理任务

```python
from myboot.jobs.scheduled_job import ScheduledJob
from myboot.core.scheduler import get_scheduler
import threading

# 获取调度器
scheduler = get_scheduler()

# 创建自定义 ScheduledJob
class MyTask(ScheduledJob):
    def __init__(self, data: dict):
        super().__init__(name="my_task")
        self.data = data

    def run(self, *args, **kwargs):
        """任务函数"""
        print(f"处理数据: {self.data}")
        return {"status": "completed", "data": self.data}

# 创建任务实例
job = MyTask({"key": "value"})

# 对于非定时任务，可以直接执行
# 如果需要跟踪状态，可以添加到调度器
job_id = scheduler.add_job_object(job)

# 执行任务
result = job.execute()

# 获取任务状态
status = job.status

# 获取任务信息
job_info = job.get_info()

# 获取所有 ScheduledJob 信息
all_jobs = [job.get_info() for job in scheduler.get_all_scheduled_jobs()]

# 在后台执行任务
thread = threading.Thread(target=job.execute)
thread.daemon = True
thread.start()
```

#### 任务特性

- **过期任务处理**: 一次性任务如果时间已过期，将自动标记为过期不再执行
- **已执行任务**: 一次性任务执行后不会重复执行
- **时区支持**: 支持配置时区（需要安装 pytz）
- **任务状态查询**: 可以查询任务的执行状态、是否过期等信息

## 📚 示例应用

- **基础示例** (`examples/convention_app.py`): 展示基本功能
- **依赖注入示例** (`examples/dependency_injection_example.py`): 展示依赖注入功能

## 🤝 贡献

欢迎贡献代码！请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 了解如何参与。

## 📄 许可证

本项目采用 Apache-2.0 license 许可证。查看 [LICENSE](LICENSE) 文件了解详情。

## 🙏 致谢

感谢以下开源项目：

- [FastAPI](https://fastapi.tiangolo.com/) - 现代、快速的 Web 框架
- [APScheduler](https://apscheduler.readthedocs.io/) - Python 任务调度库
- [Pydantic](https://pydantic-docs.helpmanual.io/) - 数据验证库
- [Loguru](https://github.com/Delgan/loguru) - 现代、强大的日志库

## 📞 支持

如果您遇到问题或有建议，请：

1. 查看 [文档](https://github.com/TrumanDu/myboot)
2. 搜索 [Issues](https://github.com/TrumanDu/myboot/issues)
3. 创建新的 [Issue](https://github.com/TrumanDu/myboot/issues/new)

---

**MyBoot** - 让企业级应用开发更简单、更快速！
🚀

要解决的问题

1.  [x] 配置文件
2.  [x] 日志问题
3.  [x] web 快速开发框架
4.  [x] 自动注入
5.  [x] 异步任务
6.  [x] job 管理
