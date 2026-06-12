"""run_primary_first / clear_markers 单元测试"""

import os
import threading
import time

import pytest

from myboot.utils import worker_sync
from myboot.utils.worker_sync import run_primary_first, clear_markers


@pytest.fixture(autouse=True)
def isolated_marker_dir(tmp_path, monkeypatch):
    """每个测试使用独立的标记目录，避免相互污染。"""
    import tempfile
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setenv("MYBOOT_APP_NAME", "testapp")
    monkeypatch.delenv("MYBOOT_IS_PRIMARY_WORKER", raising=False)
    yield tmp_path


def _marker_dir(tmp_path):
    return tmp_path / "myboot_worker_sync_testapp"


def test_single_process_passthrough():
    """无环境变量时默认 primary，直通执行并返回值。"""
    assert run_primary_first("m1", lambda: 42) == 42


def test_primary_writes_done(monkeypatch, isolated_marker_dir):
    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "1")
    assert run_primary_first("model", lambda: "ok") == "ok"
    done = _marker_dir(isolated_marker_dir) / "model.done"
    assert done.exists()
    assert done.read_text(encoding="utf-8")  # ISO 时间戳非空


def test_secondary_waits_then_runs_secondary_fn(monkeypatch, isolated_marker_dir):
    """secondary 在另一线程先等待，primary 后完成 → secondary 执行 secondary_fn。"""
    results = {}

    def secondary_thread():
        monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "0")
        results["secondary"] = run_primary_first(
            "model",
            primary_fn=lambda: "heavy",
            secondary_fn=lambda: "light",
            timeout=5.0,
            poll_interval=0.05,
        )

    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "0")
    t = threading.Thread(target=secondary_thread)
    t.start()
    time.sleep(0.2)
    # primary 完成
    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "1")
    assert run_primary_first("model", lambda: "heavy") == "heavy"
    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "0")
    t.join(timeout=5)
    assert not t.is_alive()
    assert results["secondary"] == "light"


def test_secondary_defaults_to_primary_fn(monkeypatch, isolated_marker_dir):
    """secondary_fn 为 None 时 secondary 执行 primary_fn。"""
    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "1")
    run_primary_first("m2", lambda: 1)
    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "0")
    assert run_primary_first("m2", lambda: 1, timeout=1.0, poll_interval=0.05) == 1


def test_primary_failure_writes_failed_and_raises(monkeypatch, isolated_marker_dir):
    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "1")

    def boom():
        raise ValueError("download failed")

    with pytest.raises(ValueError, match="download failed"):
        run_primary_first("model", boom)

    failed = _marker_dir(isolated_marker_dir) / "model.failed"
    assert failed.exists()
    assert "download failed" in failed.read_text(encoding="utf-8")

    # secondary 检测到 .failed → RuntimeError 含异常信息
    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "0")
    with pytest.raises(RuntimeError, match="download failed"):
        run_primary_first("model", lambda: 1, timeout=2.0, poll_interval=0.05)


def test_secondary_timeout(monkeypatch):
    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "0")
    with pytest.raises(TimeoutError):
        run_primary_first("never", lambda: 1, timeout=0.3, poll_interval=0.05)


def test_stale_done_marker_ignored(monkeypatch, isolated_marker_dir):
    """上一轮遗留的旧 .done（mtime 过旧）不会让 secondary 跳过等待。"""
    d = _marker_dir(isolated_marker_dir)
    d.mkdir(parents=True, exist_ok=True)
    stale = d / "model.done"
    stale.write_text("old-run", encoding="utf-8")
    old = worker_sync._MODULE_LOAD_TIME - 100
    os.utime(stale, (old, old))

    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "0")
    with pytest.raises(TimeoutError):
        run_primary_first("model", lambda: 1, timeout=0.3, poll_interval=0.05)


def test_primary_clears_stale_markers(monkeypatch, isolated_marker_dir):
    """primary 执行前清理旧标记，成功后 .failed 不残留。"""
    d = _marker_dir(isolated_marker_dir)
    d.mkdir(parents=True, exist_ok=True)
    (d / "model.failed").write_text("old failure", encoding="utf-8")

    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "1")
    assert run_primary_first("model", lambda: "ok") == "ok"
    assert not (d / "model.failed").exists()
    assert (d / "model.done").exists()


def test_clear_markers(monkeypatch, isolated_marker_dir):
    monkeypatch.setenv("MYBOOT_IS_PRIMARY_WORKER", "1")
    run_primary_first("a", lambda: 1)
    run_primary_first("b", lambda: 1)
    d = _marker_dir(isolated_marker_dir)
    clear_markers("a")
    assert not (d / "a.done").exists()
    assert (d / "b.done").exists()
    clear_markers()
    assert not (d / "b.done").exists()
