#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MyBoot CLI 工具

提供项目模板初始化功能
"""

import click
import sys
from pathlib import Path


@click.group()
def cli():
    """MyBoot 命令行工具 - 项目模板初始化"""
    pass


@cli.command()
@click.option('--name', prompt='项目名称', help='项目名称')
@click.option('--dir', default='.', help='项目目录（默认为当前目录）')
@click.option('--template', type=click.Choice(['basic', 'api', 'full']), default='basic', 
              help='项目模板: basic(基础), api(API项目), full(完整项目)')
@click.option('--force', is_flag=True, help='如果目录已存在则覆盖')
def init(name: str, dir: str, template: str, force: bool):
    """初始化新的 MyBoot 项目"""
    
    project_dir = Path(dir) / name
    
    # 检查目录是否存在
    if project_dir.exists() and not force:
        click.echo(f"❌ 错误: 目录 '{project_dir}' 已存在", err=True)
        click.echo("   使用 --force 选项可以覆盖现有目录")
        sys.exit(1)
    
    click.echo(f"📦 正在初始化项目: {name}")
    click.echo(f"   模板: {template}")
    click.echo(f"   目录: {project_dir}")
    click.echo()
    
    try:
        # 创建目录结构
        dirs = ['app', 'app/api', 'app/service', 'app/model', 'app/jobs', 'app/client', 'conf', 'tests']
        for d in dirs:
            (project_dir / d).mkdir(parents=True, exist_ok=True)
            # 创建 __init__.py
            init_file = project_dir / d / '__init__.py'
            if not init_file.exists():
                init_file.write_text('', encoding='utf-8')
        
        click.echo("✓ 创建目录结构")
        
        # 创建配置文件
        config_content = f"""# {name} 配置文件

# 应用配置
app:
  name: "{name}"
  version: "0.1.0"

# 服务器配置
server:
  port: 8000
  reload: true
  workers: 1
  keep_alive_timeout: 5
  graceful_timeout: 30
  response_format:
    enabled: true
    exclude_paths:
      - "/docs"

# 日志配置
logging:
  level: "INFO"

# 任务调度配置
scheduler:
  enabled: true
  timezone: "UTC"
  max_workers: 10
"""
        config_file = project_dir / 'conf' / 'config.yaml'
        config_file.write_text(config_content, encoding='utf-8')
        click.echo("✓ 创建配置文件: conf/config.yaml")
        
        # 创建主应用文件（放在根目录）
        app_content = f'''"""主应用文件"""
from myboot.core.application import create_app

app = create_app(name="{name}")

if __name__ == "__main__":
    app.run()
'''
        main_file = project_dir / 'main.py'
        main_file.write_text(app_content, encoding='utf-8')
        click.echo("✓ 创建主应用文件: main.py")
        
        # 根据模板创建不同的文件
        if template in ['api', 'full']:
            # 创建示例路由
            api_content = '''"""API 路由示例"""
from myboot.core.application import app

@app.get("/")
def hello():
    """Hello World 接口"""
    return {"message": "Hello, MyBoot!", "status": "success"}

@app.get("/health")
def health():
    """健康检查接口"""
    return {"status": "healthy", "service": "running"}
'''
            routes_file = project_dir / 'app' / 'api' / 'routes.py'
            routes_file.write_text(api_content, encoding='utf-8')
            click.echo("✓ 创建示例路由: app/api/routes.py")
        
        if template == 'full':
            # 创建示例服务
            service_content = '''"""服务层示例"""
from typing import Dict, Any


class UserService:
    """用户服务示例"""
    
    def get_user(self, user_id: int) -> Dict[str, Any]:
        """获取用户信息"""
        return {
            "id": user_id,
            "name": "示例用户",
            "email": "user@example.com"
        }
    
    def create_user(self, name: str, email: str) -> Dict[str, Any]:
        """创建用户"""
        return {
            "id": 1,
            "name": name,
            "email": email,
            "status": "created"
        }
'''
            service_file = project_dir / 'app' / 'service' / 'user_service.py'
            service_file.write_text(service_content, encoding='utf-8')
            click.echo("✓ 创建示例服务: app/service/user_service.py")
            
            # 创建示例模型
            model_content = '''"""数据模型示例"""
from pydantic import BaseModel, EmailStr
from typing import Optional


class User(BaseModel):
    """用户模型"""
    id: Optional[int] = None
    name: str
    email: EmailStr
    status: str = "active"
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "张三",
                "email": "zhangsan@example.com"
            }
        }
'''
            model_file = project_dir / 'app' / 'model' / 'user.py'
            model_file.write_text(model_content, encoding='utf-8')
            click.echo("✓ 创建示例模型: app/model/user.py")
            
            # 创建示例客户端
            client_content = '''"""客户端示例"""
from typing import Dict, Any
import requests


class ApiClient:
    """API 客户端示例"""
    
    def __init__(self, base_url: str):
        """初始化客户端"""
        self.base_url = base_url
    
    def get(self, endpoint: str) -> Dict[str, Any]:
        """GET 请求"""
        response = requests.get(f"{self.base_url}/{endpoint}")
        response.raise_for_status()
        return response.json()
    
    def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """POST 请求"""
        response = requests.post(f"{self.base_url}/{endpoint}", json=data)
        response.raise_for_status()
        return response.json()
'''
            client_file = project_dir / 'app' / 'client' / 'api_client.py'
            client_file.write_text(client_content, encoding='utf-8')
            click.echo("✓ 创建示例客户端: app/client/api_client.py")
            
            # 创建示例定时任务
            job_content = '''"""定时任务示例"""
from myboot.core.decorators import cron


@cron("0 */5 * * * *")  # 每5分钟执行一次
def cleanup_task():
    """清理任务示例"""
    print("执行清理任务...")


@cron("0 0 * * * *")  # 每小时执行一次
def hourly_task():
    """每小时任务示例"""
    print("执行每小时任务...")
'''
            job_file = project_dir / 'app' / 'jobs' / 'tasks.py'
            job_file.write_text(job_content, encoding='utf-8')
            click.echo("✓ 创建示例任务: app/jobs/tasks.py")
        
        # 创建 README
        readme_content = f"""# {name}

MyBoot 项目

## 快速开始

### 安装依赖

```bash
pip install myboot
```

### 运行应用

```bash
# 使用默认设置启动
python main.py

# 或者使用 myboot 命令（如果已安装）
myboot run
```

### 开发模式

```bash
# 启用自动重载
python main.py --reload

# 或者
myboot dev --reload
```

## 项目结构

```
{name}/
├── main.py           # 应用入口
├── pyproject.toml    # 项目配置文件
├── .gitignore        # Git 忽略文件
├── app/              # 应用代码
│   ├── api/          # API 路由
│   ├── service/      # 业务逻辑层
│   ├── model/        # 数据模型
│   ├── jobs/         # 定时任务
│   └── client/       # 客户端（第三方API调用等）
├── conf/             # 配置文件
│   └── config.yaml   # 主配置文件
└── tests/            # 测试代码
```

## 配置说明

配置文件位于 `conf/config.yaml`，支持以下配置：

- **app**: 应用配置（名称、版本等）
- **server**: 服务器配置（端口、工作进程等）
- **logging**: 日志配置
- **scheduler**: 任务调度配置

## 更多信息

- [MyBoot 文档](https://github.com/your-org/myboot)
- [快速开始指南](https://github.com/your-org/myboot/docs)
"""
        readme_file = project_dir / 'README.md'
        readme_file.write_text(readme_content, encoding='utf-8')
        click.echo("✓ 创建 README: README.md")
        
        # 创建 .gitignore
        gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environment
venv/
ENV/
env/
.venv

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Logs
*.log
logs/

# OS
.DS_Store
Thumbs.db

# MyBoot
.pytest_cache/
.coverage
htmlcov/
"""
        gitignore_file = project_dir / '.gitignore'
        gitignore_file.write_text(gitignore_content, encoding='utf-8')
        click.echo("✓ 创建 .gitignore")
        
        # 创建 pyproject.toml
        project_name = name.lower().replace(' ', '-').replace('_', '-')
        pyproject_content = f"""[project]
name = "{project_name}"
version = "0.1.0"
description = "{name} - MyBoot 项目"
authors = [
    {{name = "Your Name", email = "your.email@example.com"}}
]
readme = "README.md"
license = {{text = "MIT"}}
requires-python = ">=3.9"
keywords = ["myboot", "web", "api"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]

dependencies = [
    "myboot>=0.1.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
    "pytest-asyncio>=0.21.0",
    "black>=23.0",
    "isort>=5.12",
    "flake8>=6.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["app"]

[tool.black]
line-length = 88
target-version = ['py39', 'py310', 'py311', 'py312']
include = '\\.pyi?$'

[tool.isort]
profile = "black"
line_length = 88
known_first_party = ["app"]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py", "*_test.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "--strict-markers",
    "--verbose",
]
"""
        pyproject_file = project_dir / 'pyproject.toml'
        pyproject_file.write_text(pyproject_content, encoding='utf-8')
        click.echo("✓ 创建 pyproject.toml")
        
        click.echo()
        click.echo(f"✅ 项目 '{name}' 初始化完成！")
        click.echo()
        click.echo("下一步:")
        click.echo(f"  cd {name}")
        click.echo("  python main.py")
        click.echo()
        
    except Exception as e:
        click.echo(f"❌ 初始化失败: {e}", err=True)
        import traceback
        if '--debug' in sys.argv:
            traceback.print_exc()
        sys.exit(1)


@cli.command()
def info():
    """显示 MyBoot 信息"""
    click.echo("🎯 MyBoot - 类似 Spring Boot 的企业级Python快速开发框架")
    click.echo()
    click.echo("✨ 主要特性:")
    click.echo("  • 快速启动和自动配置")
    click.echo("  • 约定优于配置")
    click.echo("  • 高性能 Hypercorn 服务器")
    click.echo("  • Web API 开发")
    click.echo("  • 定时任务调度")
    click.echo("  • 日志管理")
    click.echo("  • 配置管理")
    click.echo()
    click.echo("🚀 快速开始:")
    click.echo("  myboot init                    # 初始化新项目")
    click.echo("  myboot init --template api     # 使用 API 模板")
    click.echo("  myboot init --template full    # 使用完整模板")
    click.echo()


if __name__ == '__main__':
    cli()
