"""
F5 内置 Prometheus 指标模块单元测试

覆盖：
- prometheus-client 未安装时的优雅退化（no-op 桩、Application 只告警不挂）
- setup_multiproc_env 的各种生效/不生效分支
- get_counter / get_histogram 幂等缓存
- HttpMetricsMiddleware 路由模板标签、unmatched 归类与 metrics 自身路径排除
"""

import importlib.util
import os
import sys
import uuid

import pytest
from fastapi.testclient import TestClient

import myboot.metrics as metrics
from myboot.core.application import Application
from myboot.core.config import get_settings


def _uniq(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class FakeConfig:
    """支持点号 key 的最小配置桩"""

    def __init__(self, data=None):
        self.data = data or {}

    def get(self, key, default=None):
        return self.data.get(key, default)


# ==================== 可用性与 no-op 退化 ====================


class TestAvailability:
    def test_is_available_true_when_installed(self):
        # prometheus-client 已随 test extra 安装
        assert metrics.is_available() is True

    def test_is_available_false_when_not_installed(self, monkeypatch):
        monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
        assert metrics.is_available() is False

    def test_get_counter_noop_when_unavailable(self, monkeypatch):
        monkeypatch.setattr(metrics, "is_available", lambda: False)
        counter = metrics.get_counter(_uniq("noop_counter"), "doc", ("a", "b"))
        # 链式调用不报错
        counter.labels(a="1", b="2").inc()
        counter.inc(5)
        assert isinstance(counter, metrics._NoopMetric)

    def test_get_histogram_noop_when_unavailable(self, monkeypatch):
        monkeypatch.setattr(metrics, "is_available", lambda: False)
        hist = metrics.get_histogram(_uniq("noop_hist"), "doc", ("stage",))
        hist.labels(stage="x").observe(0.1)
        assert isinstance(hist, metrics._NoopMetric)

    def test_observe_stage_and_time_stage_noop_when_unavailable(self, monkeypatch):
        monkeypatch.setattr(metrics, "is_available", lambda: False)
        metrics.observe_stage("recall", 0.05)
        with metrics.time_stage("rank"):
            pass

    def test_application_only_warns_when_enabled_but_not_installed(self, monkeypatch):
        monkeypatch.setattr(metrics, "is_available", lambda: False)
        settings = get_settings()
        settings.set("metrics.enabled", True)
        try:
            app = Application(name="metrics-warn-test", auto_configuration=False)
            fastapi_app = app.get_fastapi_app()
            # 未安装 → 不挂载 /metrics、不加中间件，应用可正常服务
            client = TestClient(fastapi_app)
            resp = client.get("/health")
            assert resp.status_code == 200
            resp = client.get("/metrics")
            assert resp.status_code == 404
        finally:
            settings.set("metrics.enabled", False)


# ==================== is_enabled ====================


class TestIsEnabled:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("True", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            ("false", False),
            ("0", False),
            (None, False),
        ],
    )
    def test_bool_and_string_values(self, value, expected):
        cfg = FakeConfig({"metrics.enabled": value})
        assert metrics.is_enabled(cfg) is expected

    def test_default_false(self):
        assert metrics.is_enabled(FakeConfig()) is False


# ==================== setup_multiproc_env ====================


@pytest.fixture
def clean_prom_env(monkeypatch):
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    monkeypatch.delenv("MYBOOT_WORKER_ID", raising=False)
    return monkeypatch


class TestSetupMultiprocEnv:
    def _cfg(self, workers=4, enabled=True, multiproc_dir=None):
        return FakeConfig(
            {
                "metrics.enabled": enabled,
                "server.workers": workers,
                "metrics.multiproc_dir": multiproc_dir,
            }
        )

    def test_single_worker_noop(self, clean_prom_env):
        clean_prom_env.setattr(sys, "platform", "linux")
        metrics.setup_multiproc_env(self._cfg(workers=1), "demo")
        assert "PROMETHEUS_MULTIPROC_DIR" not in os.environ

    def test_disabled_noop(self, clean_prom_env):
        clean_prom_env.setattr(sys, "platform", "linux")
        metrics.setup_multiproc_env(self._cfg(workers=4, enabled=False), "demo")
        assert "PROMETHEUS_MULTIPROC_DIR" not in os.environ

    def test_win32_noop(self, clean_prom_env):
        clean_prom_env.setattr(sys, "platform", "win32")
        metrics.setup_multiproc_env(self._cfg(workers=4), "demo")
        assert "PROMETHEUS_MULTIPROC_DIR" not in os.environ

    def test_multiworker_sets_env_and_cleans_stale_db(self, clean_prom_env, tmp_path):
        clean_prom_env.setattr(sys, "platform", "linux")
        target = tmp_path / "prom_dir"
        target.mkdir()
        stale = target / "counter_12345.db"
        stale.write_bytes(b"stale")
        keep = target / "note.txt"
        keep.write_text("keep")

        metrics.setup_multiproc_env(
            self._cfg(workers=4, multiproc_dir=str(target)), "demo"
        )

        assert os.environ["PROMETHEUS_MULTIPROC_DIR"] == str(target)
        assert target.is_dir()
        assert not stale.exists()  # 陈旧 .db 被父进程清理
        assert keep.exists()  # 非 .db 文件保留

    def test_worker_process_does_not_clean(self, clean_prom_env, tmp_path):
        clean_prom_env.setattr(sys, "platform", "linux")
        clean_prom_env.setenv("MYBOOT_WORKER_ID", "2")
        target = tmp_path / "prom_dir"
        target.mkdir()
        stale = target / "counter_12345.db"
        stale.write_bytes(b"stale")

        metrics.setup_multiproc_env(
            self._cfg(workers=4, multiproc_dir=str(target)), "demo"
        )
        assert stale.exists()  # 非父进程不清理

    def test_user_preset_env_respected(self, clean_prom_env, tmp_path):
        clean_prom_env.setattr(sys, "platform", "linux")
        user_dir = tmp_path / "user_dir"
        clean_prom_env.setenv("PROMETHEUS_MULTIPROC_DIR", str(user_dir))
        metrics.setup_multiproc_env(
            self._cfg(workers=4, multiproc_dir=str(tmp_path / "other")), "demo"
        )
        assert os.environ["PROMETHEUS_MULTIPROC_DIR"] == str(user_dir)
        assert user_dir.is_dir()

    def test_default_dir_under_tempdir_with_app_slug(self, clean_prom_env):
        import tempfile

        clean_prom_env.setattr(sys, "platform", "linux")
        metrics.setup_multiproc_env(self._cfg(workers=2), "My Demo App!")
        value = os.environ["PROMETHEUS_MULTIPROC_DIR"]
        assert value.startswith(tempfile.gettempdir())
        assert "myboot_prometheus_my_demo_app" in value


# ==================== 指标工厂幂等性 ====================


class TestMetricFactories:
    def test_get_counter_idempotent(self):
        name = _uniq("idem_counter")
        c1 = metrics.get_counter(name, "doc", ("x",))
        c2 = metrics.get_counter(name, "doc", ("x",))
        assert c1 is c2
        c1.labels(x="a").inc()

    def test_get_histogram_idempotent(self):
        name = _uniq("idem_hist")
        h1 = metrics.get_histogram(name, "doc", ("x",))
        h2 = metrics.get_histogram(name, "doc", ("x",))
        assert h1 is h2
        h1.labels(x="a").observe(0.5)

    def test_observe_stage_negative_ignored(self):
        metrics.observe_stage("noop", -1.0)  # 不报错

    def test_time_stage_records(self):
        with metrics.time_stage(_uniq("stage")):
            pass


# ==================== HttpMetricsMiddleware 端到端 ====================


class TestHttpMetricsMiddleware:
    @pytest.fixture
    def metrics_app(self):
        settings = get_settings()
        settings.set("metrics.enabled", True)
        try:
            app = Application(name="metrics-mw-test", auto_configuration=False)

            async def get_item(item_id: str):
                return {"id": item_id}

            app.add_route("/items/{item_id}", get_item, ["GET"])
            yield app
        finally:
            settings.set("metrics.enabled", False)

    def test_http_metrics_collected(self, metrics_app):
        client = TestClient(metrics_app.get_fastapi_app())

        assert client.get("/items/1").status_code == 200
        assert client.get("/items/2").status_code == 200
        assert client.get("/no/such/route").status_code == 404

        scrape = client.get("/metrics")
        assert scrape.status_code == 200
        text = scrape.text

        # path 标签是路由模板，两个不同 id 归并到同一序列
        template_lines = [
            line
            for line in text.splitlines()
            if line.startswith("myboot_http_requests_total")
            and 'path="/items/{item_id}"' in line
        ]
        assert len(template_lines) == 1
        assert 'method="GET"' in template_lines[0]
        assert 'status="200"' in template_lines[0]
        assert template_lines[0].rstrip().endswith("2.0")
        # 具体 id 不出现为独立标签（避免高基数）
        assert 'path="/items/1"' not in text
        assert 'path="/items/2"' not in text

        # 404 归入 unmatched
        assert 'path="unmatched"' in text

        # metrics 自身路径不被统计
        assert 'path="/metrics"' not in text

        # 耗时直方图存在
        assert "myboot_http_request_duration_seconds" in text

    def test_metrics_endpoint_not_wrapped_by_response_formatter(self, metrics_app):
        client = TestClient(metrics_app.get_fastapi_app())
        resp = client.get("/metrics")
        assert resp.status_code == 200
        # Prometheus 文本格式而非统一 JSON 包装
        assert "text/plain" in resp.headers.get("content-type", "")
