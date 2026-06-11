"""
多 worker 服务独立实例化（issue #11）单元测试

覆盖：
1. @service/@client 的 scope 元数据透传（顶层键，装饰期校验）
2. ServiceProvider 的 scope -> Provider 映射（request -> ContextLocalSingleton）
3. Application.run() 多 worker 模式下延迟引导（父进程跳过 apply_auto_configuration）
4. Application.bootstrap_worker() 环境变量重读 / 调度器门控 / fork 残留重置
5. server._worker_serve 与 bootstrap_worker 的桥接
6. lifespan 中 worker 启动/停止钩子的触发顺序
7. auto_discover 幂等守卫与 @on_worker_start/@on_worker_stop 的 AST 发现
"""

import asyncio
import os
import textwrap

import pytest
from fastapi.testclient import TestClient
from dependency_injector import providers as di_providers

import myboot.core.application as application_module
import myboot.core.server as server_module
from myboot.core.application import Application
from myboot.core.auto_configuration import AutoConfigurationManager
from myboot.core.decorators import (
    service,
    client,
    on_worker_start,
    on_worker_stop,
)
from myboot.core.di import DependencyContainer
from myboot.core.di.providers import ServiceProvider


# ---------------------------------------------------------------------------
# 公共 fixture
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_worker_env():
    """每个测试前后清理 MYBOOT_* 环境变量

    _worker_serve / 测试本身会写入 MYBOOT_WORKER_ID 等变量，
    若不清理会污染后续 Application 构造。
    """
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
# 1. scope 元数据透传
# ---------------------------------------------------------------------------

class TestScopeDecorators:
    def test_service_scope_defaults_to_singleton(self):
        @service()
        class AlphaService:
            pass

        assert AlphaService.__myboot_service__["scope"] == "singleton"

    def test_service_scope_stored_as_top_level_key(self):
        """scope 必须是顶层键（而非落入 kwargs），注册时才能读到"""

        @service(scope="request")
        class BetaService:
            pass

        meta = BetaService.__myboot_service__
        assert meta["scope"] == "request"
        assert "scope" not in meta["kwargs"]

    def test_service_invalid_scope_raises_at_decoration(self):
        with pytest.raises(ValueError, match="scope"):
            @service(scope="sigleton")  # 故意拼错
            class TypoService:
                pass

    def test_client_scope_defaults_to_singleton(self):
        @client()
        class AlphaClient:
            pass

        assert AlphaClient.__myboot_client__["scope"] == "singleton"

    def test_client_scope_stored_as_top_level_key(self):
        @client(scope="factory")
        class BetaClient:
            pass

        meta = BetaClient.__myboot_client__
        assert meta["scope"] == "factory"
        assert "scope" not in meta["kwargs"]

    def test_client_invalid_scope_raises_at_decoration(self):
        with pytest.raises(ValueError, match="scope"):
            @client(scope="prototype")
            class TypoClient:
                pass


# ---------------------------------------------------------------------------
# 2. Provider 映射
# ---------------------------------------------------------------------------

class TestProviderScopes:
    class _Dummy:
        pass

    def test_singleton_scope_creates_singleton_provider(self):
        sp = ServiceProvider(self._Dummy, "dummy", scope="singleton")
        assert isinstance(sp.create_provider(), di_providers.Singleton)

    def test_request_scope_creates_context_local_singleton(self):
        sp = ServiceProvider(self._Dummy, "dummy", scope="request")
        assert isinstance(sp.create_provider(), di_providers.ContextLocalSingleton)

    def test_factory_scope_creates_factory_provider(self):
        sp = ServiceProvider(self._Dummy, "dummy", scope="factory")
        provider = sp.create_provider()
        assert isinstance(provider, di_providers.Factory)
        assert not isinstance(provider, di_providers.Singleton)

    def test_request_scope_instances_per_asyncio_task(self):
        """request 作用域：跨 asyncio 任务不同实例，同一任务内同一实例"""

        class ReqService:
            pass

        container = DependencyContainer()
        container.register_service(ReqService, "req_service", scope="request")
        container.build_container()

        async def resolve_twice():
            first = container.get_service("req_service")
            second = container.get_service("req_service")
            return first, second

        async def main():
            (a1, a2), (b1, b2) = await asyncio.gather(
                resolve_twice(), resolve_twice()
            )
            return a1, a2, b1, b2

        a1, a2, b1, b2 = asyncio.run(main())
        assert a1 is a2  # 同一任务（请求）内单例
        assert b1 is b2
        assert a1 is not b1  # 跨任务（请求）不同实例
        # request 作用域不进入单例缓存
        assert "req_service" not in container.service_instances

    def test_register_service_accepts_decorator_metadata_with_scope(self):
        """回归：装饰器元数据（含顶层 scope 键）作为 config 传入时
        不得与 register_service 的显式 scope 参数冲突"""

        @service(scope="request")
        class MetaService:
            pass

        meta = MetaService.__myboot_service__
        container = DependencyContainer()
        container.register_service(
            MetaService, meta["name"], scope=meta["scope"], config=meta
        )
        container.build_container()

        provider = container.service_providers[meta["name"]]
        assert provider.scope == "request"

    def test_factory_scope_not_cached_by_container(self):
        class FactService:
            pass

        container = DependencyContainer()
        container.register_service(FactService, "fact_service", scope="factory")
        container.build_container()

        first = container.get_service("fact_service")
        second = container.get_service("fact_service")
        assert first is not second
        assert "fact_service" not in container.service_instances


# ---------------------------------------------------------------------------
# 3. run() 延迟引导
# ---------------------------------------------------------------------------

class TestRunDefersBootstrap:
    def _run(self, monkeypatch, set_config, workers, app_path):
        calls = []
        monkeypatch.setattr(
            application_module, "auto_discover",
            lambda pkg: calls.append(("discover", pkg)),
        )
        monkeypatch.setattr(
            application_module, "apply_auto_configuration",
            lambda app: calls.append(("apply", app)),
        )

        app = Application(name="run-defer-test")
        monkeypatch.setattr(
            app.server_manager, "start_server",
            lambda **kwargs: calls.append(("serve", kwargs)),
        )
        # conf/config.yaml 中 server.workers=1 会覆盖 run() 入参，显式设置
        set_config("server.workers", workers)
        app.run(workers=workers, app_path=app_path)
        return app, calls

    def test_multi_worker_with_app_path_skips_apply(self, monkeypatch, set_config):
        app, calls = self._run(monkeypatch, set_config, workers=2, app_path="main:app")
        events = [name for name, _ in calls]
        assert "discover" in events  # 父进程仍做 AST 发现（预热缓存）
        assert "apply" not in events  # 实例化延迟到 worker 内
        assert "serve" in events
        serve_kwargs = dict(calls)["serve"]
        assert serve_kwargs["workers"] == 2
        assert serve_kwargs["app_path"] == "main:app"

    def test_single_worker_applies_in_parent(self, monkeypatch, set_config):
        app, calls = self._run(monkeypatch, set_config, workers=1, app_path=None)
        events = [name for name, _ in calls]
        assert "discover" in events
        assert "apply" in events

    def test_multi_worker_without_app_path_falls_back_to_parent_apply(
        self, monkeypatch, set_config
    ):
        """无 app_path 的回退路径（父进程自己 serve）保持原有引导行为"""
        app, calls = self._run(monkeypatch, set_config, workers=2, app_path=None)
        events = [name for name, _ in calls]
        assert "apply" in events


# ---------------------------------------------------------------------------
# 4. bootstrap_worker()
# ---------------------------------------------------------------------------

class TestBootstrapWorker:
    def test_reads_worker_env_and_disables_scheduler_for_non_primary(self):
        app = Application(name="bw-env-test", auto_configuration=False)
        # 构造时无环境变量：默认自认 primary（这正是被修复的 bug 场景）
        assert app.is_primary_worker is True

        os.environ["MYBOOT_WORKER_ID"] = "2"
        os.environ["MYBOOT_WORKER_COUNT"] = "3"
        os.environ["MYBOOT_IS_PRIMARY_WORKER"] = "0"

        old_scheduler = app.scheduler
        result = app.bootstrap_worker()

        assert app.worker_id == 2
        assert app.worker_count == 3
        assert app.is_primary_worker is False
        # 调度器被重建且按 primary 门控禁用
        assert app.scheduler is not old_scheduler
        assert app.scheduler.is_enabled() is False
        assert result is app.get_fastapi_app()

    def test_primary_worker_keeps_scheduler_enabled(self):
        app = Application(name="bw-primary-test", auto_configuration=False)
        os.environ["MYBOOT_WORKER_ID"] = "1"
        os.environ["MYBOOT_WORKER_COUNT"] = "3"
        os.environ["MYBOOT_IS_PRIMARY_WORKER"] = "1"

        app.bootstrap_worker()
        assert app.is_primary_worker is True
        assert app.scheduler.is_enabled() is True

    def test_scheduler_on_all_workers_overrides_primary_gate(self, set_config):
        set_config("scheduler.on_all_workers", True)
        app = Application(name="bw-allworkers-test", auto_configuration=False)
        os.environ["MYBOOT_WORKER_ID"] = "2"
        os.environ["MYBOOT_WORKER_COUNT"] = "2"
        os.environ["MYBOOT_IS_PRIMARY_WORKER"] = "0"

        app.bootstrap_worker()
        assert app.is_primary_worker is False
        assert app.scheduler.is_enabled() is True

    def test_defensive_reset_on_fork_inherited_state(self):
        """fork 子进程继承父进程已引导状态时：清空注册表并重建 FastAPI 应用"""
        app = Application(name="bw-reset-test", auto_configuration=False)

        class InheritedService:
            pass

        app.di_container = DependencyContainer()
        app.di_container.register_service(InheritedService, "inherited_service")
        app.services["inherited_service"] = InheritedService()
        app.clients["inherited_client"] = object()
        app.components["inherited_component"] = object()
        app._client_type_map = {InheritedService: "inherited_client"}
        app._component_type_map = {InheritedService: "inherited_component"}
        app.worker_start_hooks.append(lambda: None)
        app.worker_stop_hooks.append(lambda: None)
        old_fastapi_app = app._fastapi_app

        result = app.bootstrap_worker()

        assert app.services == {}
        assert app.clients == {}
        assert app.components == {}
        assert app._client_type_map == {}
        assert app._component_type_map == {}
        assert app.worker_start_hooks == []
        assert app.worker_stop_hooks == []
        assert not app.di_container.service_providers
        # FastAPI 应用被重建（丢弃绑定到 fork 前实例的路由）
        assert app._fastapi_app is not old_fastapi_app
        assert result is app._fastapi_app

    def test_clean_state_keeps_pristine_fastapi_app(self):
        """正常延迟引导路径：注册表为空时不触发重置，沿用原 FastAPI 实例"""
        app = Application(name="bw-clean-test", auto_configuration=False)
        old_fastapi_app = app._fastapi_app

        result = app.bootstrap_worker()
        assert app._fastapi_app is old_fastapi_app
        assert result is old_fastapi_app

    def test_runs_auto_configuration_inside_worker(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            application_module, "auto_discover",
            lambda pkg: calls.append(("discover", pkg)),
        )
        monkeypatch.setattr(
            application_module, "apply_auto_configuration",
            lambda app: calls.append(("apply", app)),
        )
        app = Application(name="bw-autoconf-test")
        app.bootstrap_worker()

        events = [name for name, _ in calls]
        assert "discover" in events
        assert "apply" in events

    def test_auto_configuration_disabled_skips_bootstrap_apply(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            application_module, "apply_auto_configuration",
            lambda app: calls.append("apply"),
        )
        app = Application(name="bw-noautoconf-test", auto_configuration=False)
        app.bootstrap_worker()
        assert calls == []


# ---------------------------------------------------------------------------
# 5. _worker_serve 桥接
# ---------------------------------------------------------------------------

class TestWorkerServe:
    @pytest.fixture
    def serve_recorder(self, monkeypatch):
        """mock hypercorn.asyncio.serve 与 setup_worker_logging"""
        import importlib

        import hypercorn.asyncio

        # 注意：myboot.core 包的 `from .logger import logger` 会用 loguru
        # 实例遮蔽包属性 myboot.core.logger，必须经 importlib 拿到真实模块
        logger_module = importlib.import_module("myboot.core.logger")

        served = {}

        async def fake_serve(app, config):
            served["app"] = app
            served["config"] = config

        monkeypatch.setattr(hypercorn.asyncio, "serve", fake_serve)
        monkeypatch.setattr(
            logger_module, "setup_worker_logging", lambda *a, **k: None
        )
        return served

    def test_myboot_app_served_via_bootstrap_worker(
        self, monkeypatch, serve_recorder
    ):
        resolved = object()
        monkeypatch.setattr(
            server_module, "_resolve_app_from_path", lambda path: resolved
        )

        bootstrapped_app = object()
        bootstrap_calls = []

        class FakeMybootApp:
            def bootstrap_worker(self):
                bootstrap_calls.append(True)
                return bootstrapped_app

        monkeypatch.setattr(application_module, "_current_app", FakeMybootApp())

        server_module._worker_serve("main:app", {}, 2, 3)

        # 桥接：serve 的是 bootstrap_worker() 的返回值，而非解析出的原对象
        assert bootstrap_calls == [True]
        assert serve_recorder["app"] is bootstrapped_app
        # 环境变量在引导前已设置
        assert os.environ["MYBOOT_WORKER_ID"] == "2"
        assert os.environ["MYBOOT_WORKER_COUNT"] == "3"
        assert os.environ["MYBOOT_IS_PRIMARY_WORKER"] == "0"

    def test_primary_worker_env_flag(self, monkeypatch, serve_recorder):
        monkeypatch.setattr(
            server_module, "_resolve_app_from_path", lambda path: object()
        )
        monkeypatch.setattr(application_module, "_current_app", None)

        server_module._worker_serve("main:app", {}, 1, 2)
        assert os.environ["MYBOOT_IS_PRIMARY_WORKER"] == "1"

    def test_plain_asgi_app_path_untouched(self, monkeypatch, serve_recorder):
        """非 myboot 应用（_current_app 为 None）：直接 serve 解析出的对象"""
        resolved = object()
        monkeypatch.setattr(
            server_module, "_resolve_app_from_path", lambda path: resolved
        )
        monkeypatch.setattr(application_module, "_current_app", None)

        server_module._worker_serve("asgi_module:app", {}, 2, 2)
        assert serve_recorder["app"] is resolved


# ---------------------------------------------------------------------------
# 6. worker 生命周期钩子
# ---------------------------------------------------------------------------

class TestWorkerHookDecorators:
    def test_bare_decorator_marks_function(self):
        @on_worker_start
        def hook():
            pass

        assert hook.__myboot_worker_hook__ == {"event": "start", "order": 0}

    def test_decorator_with_order(self):
        @on_worker_stop(order=5)
        def hook():
            pass

        assert hook.__myboot_worker_hook__ == {"event": "stop", "order": 5}

    def test_auto_register_worker_hooks_sorted_by_order(self):
        class FakeApp:
            def __init__(self):
                self.worker_start_hooks = []
                self.worker_stop_hooks = []

            def add_worker_start_hook(self, hook):
                self.worker_start_hooks.append(hook)

            def add_worker_stop_hook(self, hook):
                self.worker_stop_hooks.append(hook)

        @on_worker_start(order=2)
        def start_late():
            pass

        @on_worker_start(order=1)
        def start_early():
            pass

        @on_worker_stop
        def stop_hook():
            pass

        manager = AutoConfigurationManager(use_cache=False)
        manager.discovered_components["worker_hooks"] = [
            {"function": start_late, "module": "m", "type": "function_on_worker_start"},
            {"function": start_early, "module": "m", "type": "function_on_worker_start"},
            {"function": stop_hook, "module": "m", "type": "function_on_worker_stop"},
        ]

        fake_app = FakeApp()
        manager._auto_register_worker_hooks(fake_app)

        assert fake_app.worker_start_hooks == [start_early, start_late]
        assert fake_app.worker_stop_hooks == [stop_hook]


class TestWorkerHooksInLifespan:
    def test_hook_firing_order_in_lifespan(self):
        """startup -> worker_start(按 order) -> scheduler.start ...
        scheduler.stop -> worker_stop -> shutdown"""
        events = []

        class StubScheduler:
            def has_jobs(self):
                return True

            def start(self):
                events.append("scheduler_start")

            def is_running(self):
                return True

            def stop(self):
                events.append("scheduler_stop")

        app = Application(name="hooks-lifespan-test", auto_configuration=False)
        app.scheduler = StubScheduler()

        def startup_hook():
            events.append("startup")

        def worker_start_sync():
            events.append("worker_start_sync")

        async def worker_start_async():
            events.append("worker_start_async")

        async def worker_stop_async():
            events.append("worker_stop_async")

        def shutdown_hook():
            events.append("shutdown")

        app.add_startup_hook(startup_hook)
        app.add_worker_start_hook(worker_start_sync)
        app.add_worker_start_hook(worker_start_async)
        app.add_worker_stop_hook(worker_stop_async)
        app.add_shutdown_hook(shutdown_hook)

        with TestClient(app.get_fastapi_app()):
            assert events == [
                "startup",
                "worker_start_sync",
                "worker_start_async",
                "scheduler_start",
            ]

        assert events == [
            "startup",
            "worker_start_sync",
            "worker_start_async",
            "scheduler_start",
            "scheduler_stop",
            "worker_stop_async",
            "shutdown",
        ]


# ---------------------------------------------------------------------------
# 7. auto_discover 幂等 + worker 钩子 AST 发现
# ---------------------------------------------------------------------------

class TestAutoDiscover:
    def _make_package(self, tmp_path):
        pkg = tmp_path / "demo_pkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("", encoding="utf-8")
        (pkg / "stuff.py").write_text(
            textwrap.dedent(
                """
                from myboot.core.decorators import (
                    service, on_worker_start, on_worker_stop
                )


                @service()
                class DemoService:
                    pass


                @on_worker_start
                def start_hook():
                    pass


                @on_worker_stop(order=1)
                def stop_hook():
                    pass
                """
            ),
            encoding="utf-8",
        )
        return pkg

    def test_auto_discover_is_idempotent(self, tmp_path):
        self._make_package(tmp_path)
        manager = AutoConfigurationManager(app_root=str(tmp_path), use_cache=False)

        manager.auto_discover("demo_pkg")
        assert len(manager._component_metadata["services"]) == 1
        assert len(manager._component_metadata["worker_hooks"]) == 2

        # 重复调用（fork 子进程场景）：不追加重复条目
        manager.auto_discover("demo_pkg")
        assert len(manager._component_metadata["services"]) == 1
        assert len(manager._component_metadata["worker_hooks"]) == 2

    def test_worker_hooks_discovered_via_ast(self, tmp_path):
        self._make_package(tmp_path)
        manager = AutoConfigurationManager(app_root=str(tmp_path), use_cache=False)
        manager.auto_discover("demo_pkg")

        hook_types = sorted(
            item["type"] for item in manager._component_metadata["worker_hooks"]
        )
        assert hook_types == [
            "function_on_worker_start",
            "function_on_worker_stop",
        ]
