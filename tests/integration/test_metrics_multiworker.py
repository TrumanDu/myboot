"""
Prometheus multiproc 多进程聚合集成测试（F5）

父进程设置 PROMETHEUS_MULTIPROC_DIR 后 spawn 两个子进程，各自
get_counter().inc() 一次后退出；父进程通过 make_metrics_asgi_app()
（MultiProcessCollector）scrape，断言聚合值为 2 且目录下存在多个
pid 的 db 文件。

注意：Windows 多 worker 不支持 multiproc 聚合，本文件在 win32 跳过，
由 CI（Linux）执行。

运行方式: uv run pytest tests/integration -m integration
"""

import asyncio
import multiprocessing
import os
import sys

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows 不支持 Prometheus multiproc 文件聚合",
    ),
]

COUNTER_NAME = "myboot_multiproc_itest_total"


def _child_inc(multiproc_dir: str) -> None:
    """子进程入口：multiproc 模式下计数一次

    必须为模块级函数以兼容 spawn 启动方式。env 在 import
    prometheus_client 之前设置，确保选用 mmap 文件后端。
    """
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = multiproc_dir

    from myboot import metrics

    counter = metrics.get_counter(COUNTER_NAME, "multiproc integration test")
    counter.inc()
    metrics.mark_current_process_dead()


def test_multiproc_counter_aggregation(tmp_path, monkeypatch):
    multiproc_dir = str(tmp_path)
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", multiproc_dir)

    # 用 spawn 确保子进程在设置 env 后全新 import prometheus_client
    # （fork 会继承父进程可能已按非 multiproc 模式初始化的模块状态）
    ctx = multiprocessing.get_context("spawn")
    procs = [
        ctx.Process(target=_child_inc, args=(multiproc_dir,)) for _ in range(2)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0

    # 目录下应有来自多个 pid 的 counter db 文件
    db_files = list(tmp_path.glob("counter_*.db"))
    assert len(db_files) >= 2, f"期望至少 2 个 counter db 文件，实际: {db_files}"

    # 父进程 scrape：MultiProcessCollector 聚合两个子进程的计数
    import httpx

    from myboot.metrics import make_metrics_asgi_app

    asgi_app = make_metrics_asgi_app()

    async def _scrape() -> str:
        transport = httpx.ASGITransport(app=asgi_app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            resp = await client.get("/")
            assert resp.status_code == 200
            return resp.text

    text = asyncio.run(_scrape())

    lines = [
        line
        for line in text.splitlines()
        if line.startswith(COUNTER_NAME) and not line.startswith("#")
    ]
    assert lines, f"scrape 输出中未找到 {COUNTER_NAME}: {text[:500]}"
    value = float(lines[0].rsplit(" ", 1)[-1])
    assert value == 2.0
