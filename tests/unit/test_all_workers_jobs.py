"""
定时任务任务级 all_workers 支持（F4）单元测试

覆盖：
1. @cron/@interval/@once 的 all_workers 元数据写入（顶层键，默认 False）
2. 注册门控：非 primary worker 只注册 all_workers=True 的任务
3. scheduler.on_all_workers=true 全局配置：非 primary 注册全部任务
4. scheduler.enabled=false：任何 worker 都不注册任何任务
"""

import os

import pytest

from myboot.core.application import Application
from myboot.core.auto_configuration import AutoConfigurationManager
from myboot.core.decorators import cron, interval, once


# ---------------------------------------------------------------------------
# 公共 fixture（与 test_multiworker.py 保持一致）
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_worker_env():
    """每个测试前后清理 MYBOOT_* 环境变量"""
    saved = {k: v for k, v in os.environ.items() if k.startswith("MYBOOT_")}
    for key in saved:
        del os.environ[key]
    yield
    for key in [k for k in os.environ if k.startswith("MYBOOT_")]:
        del os.environ[key]
    os.environ.update(saved)


@pytest.fixture
def set_config():
    """临时修改全局配置，测试结束后恢复原值"""
    from myboot.core.config import get_settings

    settings = get_settings()
    changed = {}

    def _set(key, value):
        if key not in changed:
            changed[key] = settings.get(key)
        settings.set(key, value)

    yield _set

    for key, old in changed.items():
        settings.set(key, old)


# ---------------------------------------------------------------------------
# 测试用组件：一个 all_workers 任务 + 一个默认任务
# ---------------------------------------------------------------------------

class JobsComponent:
    @interval(seconds=60, all_workers=True)
    def refresh_local_cache(self):
        pass

    @interval(seconds=60)
    def primary_only_job(self):
        pass


def _register(app):
    manager = AutoConfigurationManager()
    instance = JobsComponent()
    manager._register_component_jobs(app, instance, JobsComponent, "tests.jobs")
    return app.scheduler.list_jobs()


# ---------------------------------------------------------------------------
# 1. 装饰器元数据
# ---------------------------------------------------------------------------

class TestAllWorkersMetadata:
    def test_cron_all_workers_top_level(self):
        @cron("0 0 * * *", all_workers=True)
        def job():
            pass

        meta = job.__myboot_job__
        assert meta["all_workers"] is True
        assert "all_workers" not in meta["kwargs"]

    def test_interval_all_workers_top_level(self):
        @interval(seconds=30, all_workers=True)
        def job():
            pass

        meta = job.__myboot_job__
        assert meta["all_workers"] is True
        assert "all_workers" not in meta["kwargs"]

    def test_once_all_workers_top_level(self):
        @once("2099-01-01 00:00:00", all_workers=True)
        def job():
            pass

        meta = job.__myboot_job__
        assert meta["all_workers"] is True
        assert "all_workers" not in meta["kwargs"]

    def test_all_workers_defaults_to_false(self):
        @cron("0 0 * * *")
        def cron_job():
            pass

        @interval(seconds=1)
        def interval_job():
            pass

        @once("2099-01-01 00:00:00")
        def once_job():
            pass

        assert cron_job.__myboot_job__["all_workers"] is False
        assert interval_job.__myboot_job__["all_workers"] is False
        assert once_job.__myboot_job__["all_workers"] is False


# ---------------------------------------------------------------------------
# 2. 注册门控
# ---------------------------------------------------------------------------

class TestRegistrationGate:
    def test_non_primary_registers_only_all_workers_jobs(self):
        os.environ["MYBOOT_IS_PRIMARY_WORKER"] = "0"
        app = Application(name="aw-nonprimary", auto_configuration=False)
        assert app.is_primary_worker is False

        job_ids = _register(app)
        assert len(job_ids) == 1
        assert "refresh_local_cache" in job_ids[0]
        # 非 primary worker 因 all_workers 任务而启用调度器实例
        assert app.scheduler.is_enabled() is True

    def test_primary_registers_all_jobs(self):
        os.environ["MYBOOT_IS_PRIMARY_WORKER"] = "1"
        app = Application(name="aw-primary", auto_configuration=False)
        assert app.is_primary_worker is True

        job_ids = _register(app)
        assert len(job_ids) == 2
        assert any("refresh_local_cache" in j for j in job_ids)
        assert any("primary_only_job" in j for j in job_ids)

    def test_on_all_workers_config_registers_all_on_non_primary(self, set_config):
        set_config("scheduler.on_all_workers", True)
        os.environ["MYBOOT_IS_PRIMARY_WORKER"] = "0"
        app = Application(name="aw-globalcfg", auto_configuration=False)

        job_ids = _register(app)
        assert len(job_ids) == 2
        assert app.scheduler.is_enabled() is True

    def test_scheduler_disabled_registers_nothing(self, set_config):
        set_config("scheduler.enabled", False)

        # primary worker 也不注册
        app = Application(name="aw-disabled-primary", auto_configuration=False)
        assert _register(app) == []
        assert app.scheduler.is_enabled() is False

        # 非 primary worker（含 all_workers 任务）同样不注册
        os.environ["MYBOOT_IS_PRIMARY_WORKER"] = "0"
        app2 = Application(name="aw-disabled-nonprimary", auto_configuration=False)
        assert _register(app2) == []
        assert app2.scheduler.is_enabled() is False
