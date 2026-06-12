"""
配置管理模块

使用 Dynaconf 提供强大的配置管理功能
支持远程加载文件、配置文件优先级、环境变量覆盖等
"""

import os
import tempfile
from pathlib import Path
from typing import Any, Optional

import requests
from dynaconf import Dynaconf


def _find_project_root() -> str:
    """查找项目根目录"""
    current_dir = Path(__file__).parent.absolute()
    
    while current_dir.parent != current_dir:
        if (current_dir / 'pyproject.toml').exists():
            return str(current_dir)
        current_dir = current_dir.parent
    
    return os.getcwd()


def _is_url(path: str) -> bool:
    """检查是否为 URL"""
    return path and path.startswith(('http://', 'https://'))


def _download_config(url: str, cache_dir: str) -> str:
    """下载配置文件到缓存"""
    import hashlib
    
    url_hash = hashlib.md5(url.encode()).hexdigest()
    cache_path = os.path.join(cache_dir, f"{url_hash}.yaml")
    
    try:
        print(f"正在下载配置文件: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        print(f"配置文件已更新并缓存到: {cache_path}")
        return cache_path
    except Exception as e:
        print(f"下载配置文件失败: {e}")
        
        if os.path.exists(cache_path):
            print(f"网络连接失败，使用缓存的配置文件: {cache_path}")
            return cache_path
        else:
            print(f"无可用缓存文件，下载失败: {e}")
            raise


def _get_config_files(config_file: Optional[str] = None) -> list:
    """获取配置文件列表（供 Dynaconf ``settings_files`` 使用）。

    Dynaconf 按列表顺序依次加载，**后加载的文件会覆盖先加载的同名字段**。
    因此列表顺序为「基底 → 越来越高优先级」，而不是把「最重要」的文件放在最前。

    合并结果上的优先级（后者覆盖前者）：
    项目根目录 ``config.yaml`` < ``conf/config.yaml`` < 参数 ``config_file`` < 环境变量 ``CONFIG_FILE``
    """
    project_root = _find_project_root()
    cache_dir = os.path.join(tempfile.gettempdir(), 'myboot_config_cache')
    os.makedirs(cache_dir, exist_ok=True)
    
    config_files = []
    added_paths = set()  # 用于去重
    
    # 与 Dynaconf 加载顺序一致：靠前的先加载（作默认），靠后的覆盖同名键
    config_paths = [
        # 1. 项目根目录（最先加载）
        os.path.join(project_root, 'config.yaml'),
        os.path.join(project_root, 'config.yml'),
        # 2. conf 目录
        os.path.join(project_root, 'conf', 'config.yaml'),
        os.path.join(project_root, 'conf', 'config.yml'),
        # 3. 调用方显式传入
        config_file,
        # 4. CONFIG_FILE 环境变量（最后加载，覆盖上述来源中的同名字段）
        os.getenv('CONFIG_FILE'),
    ]
    
    for config_path in config_paths:
        if not config_path:
            continue
            
        # 处理 URL 配置
        if _is_url(config_path):
            downloaded_path = _download_config(config_path, cache_dir)
            if downloaded_path and downloaded_path not in added_paths:
                config_files.append(downloaded_path)
                added_paths.add(downloaded_path)
        # 处理文件路径
        elif os.path.exists(config_path) and config_path not in added_paths:
            config_files.append(config_path)
            added_paths.add(config_path)
    
    return config_files


def create_settings(config_file: Optional[str] = None) -> Dynaconf:
    """创建 Dynaconf 设置实例"""
    config_files = _get_config_files(config_file)
    
    # 创建 Dynaconf 配置
    settings = Dynaconf(
        # 配置文件列表
        settings_files=config_files,
        
        # 环境变量前缀（禁用前缀）
        envvar_prefix=False,
        
        # 环境变量分隔符
        envvar_separator="__",
        
        # 是否自动转换环境变量类型
        env_parse_values=True,
        
        # 是否忽略空值
        ignore_unknown_envvars=True,
        
        # 是否合并环境变量
        merge_enabled=True,

        # 自动加载项目根目录 .env（不覆盖已存在的真实环境变量，
        # 与容器部署习惯一致：真实 env > .env > 配置文件）
        load_dotenv=True,
        dotenv_path=os.path.join(_find_project_root(), ".env"),
        dotenv_override=False,

        # 默认值：以大写 kwargs 传入（Dynaconf 将任意大写 kwarg 注册为默认配置项）。
        # 注意不能用 default_settings= —— Dynaconf 没有该参数，
        # 它会被当成一个名为 DEFAULT_SETTINGS 的普通配置项，默认值整体失效
        APP={
            "name": "MyBoot App",
            "version": "0.1.0"
        },
        SERVER={
            "host": "0.0.0.0",
            "port": 8000,
            "reload": True,
            "workers": 1,
            "keep_alive_timeout": 5,
            "graceful_timeout": 30,
            "response_format": {
                "enabled": True,
                "exclude_paths": ["/docs"]
            }
        },
        LOGGING={
            "level": "INFO"
        },
        METRICS={
            "enabled": False,
            "path": "/metrics",
            "multiproc_dir": None,
            "http_metrics": True
        },
        SCHEDULER={
            "enabled": True,
            "timezone": "UTC",
            "max_workers": 10
        }
    )
    
    return settings


# 全局配置实例
_settings: Optional[Dynaconf] = None


def get_settings(config_file: Optional[str] = None) -> Dynaconf:
    """获取 Dynaconf 设置实例"""
    global _settings
    
    if _settings is None:
        _settings = create_settings(config_file)
    
    return _settings


# 为了向后兼容，保留一些便捷函数
def get_config(key: str, default: Any = None) -> Any:
    """获取配置值的便捷函数"""
    return get_settings().get(key, default)


def get_config_str(key: str, default: str = "") -> str:
    """获取字符串配置值的便捷函数"""
    value = get_config(key, default)
    return str(value) if value is not None else default


def get_config_int(key: str, default: int = 0) -> int:
    """获取整数配置值的便捷函数"""
    value = get_config(key, default)
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def get_config_bool(key: str, default: bool = False) -> bool:
    """获取布尔配置值的便捷函数"""
    value = get_config(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    return bool(value)


def reload_config() -> None:
    """重新加载配置"""
    global _settings
    _settings = None