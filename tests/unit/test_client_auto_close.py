"""
0.2.x F2：lifespan 关闭阶段自动调用 client.close()

约定：app.clients 中具有可调用 close() 方法的实例，在 worker_stop_hooks
之后、shutdown_hooks 之前由框架兜底关闭；close 异常降为 warning 不阻断。
"""

import pytest
from fastapi.testclient import TestClient

from myboot.core.application import Application


@pytest.fixture
def make_app(tmp_path, monkeypatch):
    """构造禁用自动配置的最小 Application"""
    import myboot.core.config as config_module

    monkeypatch.setattr(config_module, "_find_project_root", lambda: str(tmp_path))
    config_module.reload_config()

    def _make():
        return Application(name="auto-close-test", auto_configuration=False)

    yield _make
    config_module.reload_config()


class _ClosableClient:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


class _NoCloseClient:
    pass


class _FailingClient:
    def close(self):
        raise RuntimeError("boom")


def _run_lifespan(app: Application) -> None:
    """走一遍完整 lifespan（TestClient 上下文进入/退出即 startup/shutdown）"""
    with TestClient(app.get_fastapi_app()):
        pass


def test_client_with_close_is_auto_closed(make_app):
    app = make_app()
    client = _ClosableClient()
    app.clients["closable"] = client

    _run_lifespan(app)

    assert client.closed == 1


def test_client_without_close_is_skipped(make_app):
    app = make_app()
    app.clients["no_close"] = _NoCloseClient()

    _run_lifespan(app)  # 不抛错即通过


def test_failing_close_does_not_block_others(make_app):
    app = make_app()
    good = _ClosableClient()
    # dict 保序：失败的排在前面，验证不阻断后续
    app.clients["bad"] = _FailingClient()
    app.clients["good"] = good

    _run_lifespan(app)

    assert good.closed == 1


def test_close_runs_after_worker_stop_hooks_before_shutdown_hooks(make_app):
    app = make_app()
    order = []

    class _OrderClient:
        def close(self):
            order.append("client_close")

    app.clients["order"] = _OrderClient()
    app.add_worker_stop_hook(lambda: order.append("worker_stop"))
    app.add_shutdown_hook(lambda: order.append("shutdown"))

    _run_lifespan(app)

    assert order == ["worker_stop", "client_close", "shutdown"]


def test_async_close_supported(make_app):
    app = make_app()
    closed = []

    class _AsyncClient:
        async def close(self):
            closed.append(True)

    app.clients["async"] = _AsyncClient()

    _run_lifespan(app)

    assert closed == [True]
