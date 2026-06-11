"""
多 worker 真实进程集成测试（issue #11）

启动 tests/integration/fixtures/multiworker 下的 fixture 应用（workers=2），
通过 HTTP 与临时文件断言：

1. 每个 worker 进程独立实例化 client（实例 id 与创建 pid 互不相同）
2. @on_worker_start 钩子在每个 worker 各触发一次
3. 调度任务只在 primary worker 执行
4. （POSIX）@on_worker_stop 钩子在优雅关闭时每个 worker 各触发一次；
   Windows 下父进程 terminate() 硬终止 worker，跳过 stop 断言

运行方式: uv run pytest tests/integration -m integration
"""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "multiworker"
WORKERS = 2
STARTUP_TIMEOUT = 120  # spawn 模式启动较慢，宽松等待（轮询，就绪即继续）


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _http_get_json(url: str, timeout: float = 5.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    # ResponseFormatterMiddleware 会包一层 {success, code, message, data}
    if isinstance(payload, dict) and "data" in payload and "success" in payload:
        return payload["data"]
    return payload


def _kill_process_tree(proc: subprocess.Popen) -> None:
    """终止 fixture 父进程及其全部 worker 子进程"""
    if proc.poll() is not None:
        return
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                capture_output=True,
                timeout=30,
            )
        else:
            import signal

            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception:
        proc.kill()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        pass


def _tail(path: Path, lines: int = 60) -> str:
    try:
        content = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(content[-lines:])
    except OSError:
        return "<no log>"


@pytest.fixture
def fixture_app(tmp_path):
    """启动 workers=2 的 fixture 应用，返回 (port, hook_dir, job_dir, proc, log)"""
    port = _free_port()
    hook_dir = tmp_path / "hooks"
    job_dir = tmp_path / "jobs"
    hook_dir.mkdir()
    job_dir.mkdir()
    log_path = tmp_path / "fixture_app.log"

    env = {
        **os.environ,
        "MW_PORT": str(port),
        "MW_WORKERS": str(WORKERS),
        "MW_HOOK_DIR": str(hook_dir),
        "MW_JOB_DIR": str(job_dir),
    }
    popen_kwargs = {}
    if sys.platform != "win32":
        popen_kwargs["start_new_session"] = True

    log_file = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, "main.py"],
        cwd=str(FIXTURE_DIR),
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        **popen_kwargs,
    )
    try:
        yield port, hook_dir, job_dir, proc, log_path
    finally:
        _kill_process_tree(proc)
        log_file.close()


def _wait_for_startup(port, hook_dir, proc, log_path) -> None:
    """轮询等待：两个 worker 的 start 钩子文件出现且 /health 可访问"""
    deadline = time.time() + STARTUP_TIMEOUT
    health_ok = False
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(
                f"fixture 应用提前退出 (returncode={proc.returncode})，"
                f"日志尾部:\n{_tail(log_path)}"
            )
        start_files = list(hook_dir.glob("start_*.txt"))
        if not health_ok:
            try:
                _http_get_json(f"http://127.0.0.1:{port}/health", timeout=2)
                health_ok = True
            except (urllib.error.URLError, OSError, ValueError):
                pass
        if health_ok and len(start_files) >= WORKERS:
            return
        time.sleep(0.5)
    pytest.fail(
        f"启动超时: health_ok={health_ok}, "
        f"start_hooks={len(list(hook_dir.glob('start_*.txt')))}/{WORKERS}，"
        f"日志尾部:\n{_tail(log_path)}"
    )


def _read_start_records(hook_dir: Path):
    """解析 start 钩子记录: {pid: (worker_id, client_instance_id)}"""
    records = {}
    duplicate_lines = []
    for path in hook_dir.glob("start_*.txt"):
        lines = [
            line for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()
        ]
        if len(lines) != 1:
            duplicate_lines.append((path.name, lines))
        pid_str, worker_id_str, instance_id = lines[0].split(",")
        records[int(pid_str)] = (int(worker_id_str), instance_id)
    assert not duplicate_lines, f"start 钩子在同一 worker 内触发多次: {duplicate_lines}"
    return records


@pytest.mark.integration
def test_multiworker_per_worker_instances_hooks_and_scheduler(fixture_app):
    port, hook_dir, job_dir, proc, log_path = fixture_app
    _wait_for_startup(port, hook_dir, proc, log_path)

    # ---- 1. start 钩子：每个 worker 各触发一次，worker_id 覆盖 1..N ----
    start_records = _read_start_records(hook_dir)
    assert len(start_records) == WORKERS, f"期望 {WORKERS} 个 worker 触发 start 钩子: {start_records}"
    worker_ids = sorted(wid for wid, _ in start_records.values())
    assert worker_ids == list(range(1, WORKERS + 1)), start_records

    # ---- 2. client 实例独立：不同 worker 的实例 id 互不相同 ----
    instance_ids = [iid for _, iid in start_records.values()]
    assert all(instance_ids), f"worker 内未解析到 client 实例: {start_records}"
    assert len(set(instance_ids)) == WORKERS, (
        f"不同 worker 共享了同一 client 实例（issue #11 回归）: {start_records}"
    )

    # ---- 3. HTTP 轮询 /whoami：pid -> 实例 id 是单射，且实例创建于本进程 ----
    observed = {}  # pid -> set(instance_id)
    for _ in range(50):
        data = _http_get_json(f"http://127.0.0.1:{port}/api/whoami")
        pid = data["pid"]
        observed.setdefault(pid, set()).add(data["client_instance_id"])
        # client 实例必须创建于响应请求的同一进程（而非 fork 前的父进程）
        assert data["client_created_pid"] == pid, data
        # 响应进程必须是已知的 worker 进程，且实例 id 与钩子记录一致
        assert pid in start_records, (pid, start_records)
        assert data["client_instance_id"] == start_records[pid][1], data
        assert data["worker_id"] == start_records[pid][0], data
    for pid, ids in observed.items():
        assert len(ids) == 1, f"同一 worker 返回了多个 client 实例 id: {pid} -> {ids}"
    # 注：Windows 下多 socket 绑定同端口（SO_REUSEADDR）不保证连接均匀分发，
    # HTTP 观测到的 pid 数量不做强制断言；跨 worker 独立性已由钩子记录覆盖

    # ---- 4. 调度任务仅 primary worker 执行 ----
    deadline = time.time() + 30
    while time.time() < deadline and not list(job_dir.glob("job_*.txt")):
        time.sleep(0.5)
    job_files = list(job_dir.glob("job_*.txt"))
    assert job_files, f"调度任务未执行，日志尾部:\n{_tail(log_path)}"
    # 再观察几秒，确认没有其他 worker 也在跑任务
    time.sleep(3)
    job_pids = sorted(
        int(path.stem.split("_")[1]) for path in job_dir.glob("job_*.txt")
    )
    primary_pid = next(
        pid for pid, (wid, _) in start_records.items() if wid == 1
    )
    assert job_pids == [primary_pid], (
        f"调度任务应只在 primary worker (pid={primary_pid}) 执行，"
        f"实际执行进程: {job_pids}"
    )

    # ---- 5. （POSIX）优雅关闭后 stop 钩子每个 worker 各触发一次 ----
    if sys.platform != "win32":
        proc.terminate()  # SIGTERM -> 优雅关闭 -> lifespan 关闭阶段
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pass
        deadline = time.time() + 15
        while time.time() < deadline and len(list(hook_dir.glob("stop_*.txt"))) < WORKERS:
            time.sleep(0.5)
        stop_files = list(hook_dir.glob("stop_*.txt"))
        assert len(stop_files) == WORKERS, (
            f"期望 {WORKERS} 个 worker 触发 stop 钩子，实际 {len(stop_files)}"
        )
    # Windows: 父进程 terminate() 硬终止 worker，stop 钩子不保证触发，跳过断言
