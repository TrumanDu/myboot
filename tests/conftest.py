"""
pytest 公共 fixture

提供临时配置文件、干净环境变量等测试基础设施。
"""

import os
import sys
import textwrap

import pytest

# 确保从源码导入 myboot（而非已安装版本）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture
def tmp_config_file(tmp_path):
    """创建临时 YAML 配置文件，返回写入函数

    用法:
        path = tmp_config_file("app:\\n  name: demo")
    """

    def _write(content: str, filename: str = "config.yaml") -> str:
        config_path = tmp_path / filename
        config_path.write_text(textwrap.dedent(content), encoding="utf-8")
        return str(config_path)

    return _write


@pytest.fixture
def clean_myboot_env(monkeypatch):
    """清除 myboot 相关环境变量，避免测试间相互污染"""
    for key in list(os.environ.keys()):
        if key.startswith("MYBOOT_") or key == "CONFIG_FILE":
            monkeypatch.delenv(key, raising=False)
    return monkeypatch
