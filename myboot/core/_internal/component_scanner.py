"""
组件模块发现与导入

负责注册阶段的延迟导入逻辑：
- import_modules: 批量导入模块（支持并行/串行），并报告慢模块
- build_discovered_components: 将组件元数据转换为包含实际类/函数对象的组件条目

这些函数为纯函数，由 AutoConfigurationManager 调用并传入所需状态，
不依赖 auto_configuration 模块，避免循环导入。
"""

import time
import importlib
from typing import Dict, List, Any, Set

from loguru import logger as loguru_logger

logger = loguru_logger.bind(name=__name__)


def _import_single(module_name: str):
    """导入单个模块并返回结果"""
    try:
        start = time.perf_counter()
        module = importlib.import_module(module_name)
        elapsed = (time.perf_counter() - start) * 1000
        return module_name, module, elapsed, None
    except Exception as e:
        return module_name, None, 0, e


def import_modules(modules_to_import: Set[str], parallel_import: bool = False) -> Dict[str, Any]:
    """批量导入模块（支持并行/串行），返回 模块名 -> 模块 的映射

    导入失败的模块会记录 error 日志并被跳过；导入耗时超过 100ms 的模块
    会汇总到一条 warning 日志中。
    """
    imported_modules: Dict[str, Any] = {}
    slow_modules = []  # 记录慢模块

    if parallel_import and len(modules_to_import) > 1:
        # 并行导入（对 I/O 密集的模块有帮助）
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=min(8, len(modules_to_import))) as executor:
            futures = {executor.submit(_import_single, m): m for m in modules_to_import}
            for future in as_completed(futures):
                module_name, module, elapsed, error = future.result()
                if error:
                    logger.error(f"导入模块失败 {module_name}: {error}")
                else:
                    imported_modules[module_name] = module
                    if elapsed > 100:
                        slow_modules.append((module_name, elapsed))
    else:
        # 串行导入
        for module_name in modules_to_import:
            module_name, module, elapsed, error = _import_single(module_name)
            if error:
                logger.error(f"导入模块失败 {module_name}: {error}")
            else:
                imported_modules[module_name] = module
                if elapsed > 100:
                    slow_modules.append((module_name, elapsed))

    # 输出慢模块报告
    if slow_modules:
        slow_modules.sort(key=lambda x: x[1], reverse=True)
        report = ", ".join([f"{name}({ms:.0f}ms)" for name, ms in slow_modules[:10]])
        logger.warning(f"慢模块导入: {report}")

    return imported_modules


def build_discovered_components(
    component_metadata: Dict[str, List[dict]],
    parallel_import: bool = False,
) -> Dict[str, List[dict]]:
    """将组件元数据转换为包含实际类对象的组件条目

    收集需导入的模块并去重，执行导入后将元数据解析为实际的类/函数/方法对象。
    返回与 component_metadata 同结构的 discovered_components 字典。
    """
    # 收集需要导入的模块（去重）
    modules_to_import: Set[str] = set()
    for items in component_metadata.values():
        for item in items:
            modules_to_import.add(item['module'])

    # 批量导入模块
    imported_modules = import_modules(modules_to_import, parallel_import)

    # 将元数据转换为包含实际类对象的组件
    discovered_components: Dict[str, List[dict]] = {key: [] for key in component_metadata.keys()}
    for component_type, items in component_metadata.items():
        for item in items:
            module = imported_modules.get(item['module'])
            if not module:
                continue

            entry = {'module': item['module'], 'type': item['type']}

            if 'class_name' in item:
                cls = getattr(module, item['class_name'], None)
                if cls:
                    entry['class'] = cls
                else:
                    continue

            if 'func_name' in item:
                func = getattr(module, item['func_name'], None)
                if func:
                    entry['function'] = func
                else:
                    continue

            if 'method_name' in item:
                entry['method_name'] = item['method_name']
                if 'class' in entry:
                    entry['method'] = getattr(entry['class'], item['method_name'], None)

            discovered_components[component_type].append(entry)

    return discovered_components
