"""
AST 静态分析

在发现阶段使用 AST 静态分析替代 import（不执行模块代码）：
- parse_decorators: 提取节点上的装饰器名称
- scan_file_ast: 扫描单个文件，提取被 MyBoot 装饰器标注的类/函数元数据
- scan_package_ast: 递归扫描整个包

这些函数为纯分析函数，由调用方传入组件元数据容器与装饰器映射，
不依赖 auto_configuration 模块，避免循环导入。
"""

import os
import ast
from pathlib import Path
from typing import Dict, List

from loguru import logger as loguru_logger

logger = loguru_logger.bind(name=__name__)


def parse_decorators(node: ast.AST) -> List[str]:
    """解析装饰器名称"""
    decorators = []
    decorator_list = getattr(node, 'decorator_list', [])
    for dec in decorator_list:
        if isinstance(dec, ast.Name):
            decorators.append(dec.id)
        elif isinstance(dec, ast.Call):
            if isinstance(dec.func, ast.Name):
                decorators.append(dec.func.id)
            elif isinstance(dec.func, ast.Attribute):
                decorators.append(dec.func.attr)
    return decorators


def scan_file_ast(
    file_path: Path,
    module_name: str,
    component_metadata: Dict[str, List[dict]],
    decorator_mapping: Dict[str, str],
) -> None:
    """使用 AST 静态分析扫描单个文件（不执行 import）

    将发现的组件元数据就地追加到 component_metadata 中。
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            source = f.read()
        tree = ast.parse(source, filename=str(file_path))
    except Exception as e:
        logger.warning(f"AST 解析失败 {file_path}: {e}")
        return

    # 只遍历模块顶层节点（避免 ast.walk 的问题）
    # 注意：job 方法（@cron/@interval/@once）只能在 @component 类中定义
    # job 方法的注册在 _auto_register_components 中动态进行，不在 AST 扫描阶段处理
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            decorators = parse_decorators(node)
            for dec_name in decorators:
                if dec_name in decorator_mapping:
                    component_type = decorator_mapping[dec_name]
                    component_metadata[component_type].append({
                        'module': module_name,
                        'class_name': node.name,
                        'type': f'class_{dec_name}'
                    })

        elif isinstance(node, ast.FunctionDef):
            # 模块级函数
            decorators = parse_decorators(node)
            for dec_name in decorators:
                if dec_name in decorator_mapping:
                    component_type = decorator_mapping[dec_name]
                    component_metadata[component_type].append({
                        'module': module_name,
                        'func_name': node.name,
                        'type': f'function_{dec_name}'
                    })


def scan_package_ast(
    package_path: Path,
    component_metadata: Dict[str, List[dict]],
    decorator_mapping: Dict[str, str],
) -> None:
    """使用 AST 递归扫描包（不执行 import）

    将发现的组件元数据就地追加到 component_metadata 中。
    """
    for item in package_path.rglob("*.py"):
        if item.name.startswith("__"):
            continue
        # 计算模块名
        rel_path = item.relative_to(package_path.parent)
        module_name = str(rel_path.with_suffix('')).replace(os.sep, '.')
        scan_file_ast(item, module_name, component_metadata, decorator_mapping)
