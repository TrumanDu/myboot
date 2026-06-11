"""
扫描缓存读写

负责 AST 分析结果（组件元数据）的缓存持久化：
- 缓存版本号 _CACHE_VERSION（修改扫描逻辑时递增以使旧缓存失效）
- 缓存文件路径计算
- 源文件清单收集（用于失效判断）
- 缓存有效性检查、保存与加载

这些函数为纯函数，由 AutoConfigurationManager 调用并传入所需状态，
不依赖 auto_configuration 模块，避免循环导入。
"""

import os
import json
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger as loguru_logger

logger = loguru_logger.bind(name=__name__)


# 缓存版本号，修改扫描逻辑时递增以使旧缓存失效
# 3.1: 新增 worker_hooks 组件类型（@on_worker_start/@on_worker_stop）
_CACHE_VERSION = "3.1"


def get_cache_path(app_root: str, package_name: str) -> Path:
    """获取缓存文件路径"""
    return Path(app_root) / f".myboot_cache_{package_name}.json"


def collect_source_files(package_path: Path) -> Dict[str, float]:
    """收集扫描范围内的源文件及其修改时间（支持单文件或目录）"""
    if package_path.is_file():
        return {str(package_path): package_path.stat().st_mtime}

    files = {}
    for item in package_path.rglob("*.py"):
        if not item.name.startswith("__"):
            files[str(item)] = item.stat().st_mtime
    return files


def is_cache_valid(cache_path: Path, package_path: Path) -> bool:
    """检查缓存是否有效"""
    if not cache_path.exists():
        return False
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        if cache.get('version') != _CACHE_VERSION:
            return False
        current_files = collect_source_files(package_path)
        cached_files = cache.get('source_files', {})
        return current_files == cached_files
    except Exception:
        return False


def save_cache(cache_path: Path, package_path: Path, component_metadata: Dict[str, List[dict]]) -> None:
    """保存元数据缓存（不包含类对象）"""
    try:
        cache = {
            'version': _CACHE_VERSION,
            'source_files': collect_source_files(package_path),
            'components': component_metadata
        }
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")


def load_cache(cache_path: Path, component_keys) -> Optional[Dict[str, List[dict]]]:
    """从缓存加载元数据（不导入模块）

    成功时返回加载并规范化后的组件元数据字典（补齐 component_keys 中缺失的
    组件类型键，避免后续 KeyError）；失败时返回 None。

    Args:
        cache_path: 缓存文件路径
        component_keys: 需要补齐的组件类型键集合（通常为当前元数据字典的 keys）
    """
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        loaded = cache.get('components', {})
        # 规范化：补齐缺失的组件类型键，避免后续 KeyError
        for key in component_keys:
            loaded.setdefault(key, [])
        return loaded
    except Exception as e:
        logger.warning(f"加载缓存失败: {e}")
        return None
