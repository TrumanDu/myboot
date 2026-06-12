"""
Worker 同步工具：primary-first 初始化协调

多 worker 部署下常见需求：primary worker 负责重型初始化（如下载/加载模型），
其余 worker 等待 primary 完成后再做轻量加载。本模块通过文件标记实现跨进程协调，
仅依赖标准库，兼容 Windows（不使用 fcntl），spawn / fork 两种启动模式均可用。

典型用法::

    from myboot.utils import run_primary_first

    model = run_primary_first(
        "sas_rec_model",
        primary_fn=service.initialize_model,      # primary: 下载并初始化
        secondary_fn=service.load_model_from_disk # 其他 worker: 轻量加载
    )
"""

import datetime
import hashlib
import os
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional, TypeVar

from loguru import logger

T = TypeVar("T")

_logger = logger.bind(name="worker_sync")

# 模块加载时间，用于陈旧标记判定（见 run_primary_first docstring）。
# spawn 模式下每个 worker 进程独立导入本模块，该时间即 worker 启动时间附近；
# fork 模式下可能继承父进程导入时刻，比 worker 启动更早——仍然早于本轮
# primary 写入标记的时刻，因此不会误拒本轮标记。
_MODULE_LOAD_TIME = time.time()

# 陈旧标记判定容差（秒）：mtime 早于 (_MODULE_LOAD_TIME - 容差) 的 .done 视为陈旧
_STALE_TOLERANCE = 2.0


def _slug() -> str:
    """标记目录 slug：优先 MYBOOT_APP_NAME，否则取当前工作目录路径的 hash。

    两者在 spawn / fork 模式下父子进程均一致：环境变量被子进程继承（spawn 下
    multiprocessing 会复制父进程环境），工作目录同样被继承。
    """
    app_name = os.environ.get("MYBOOT_APP_NAME")
    if app_name:
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in app_name)
        return safe[:64]
    return hashlib.md5(os.getcwd().encode("utf-8")).hexdigest()[:16]


def _marker_dir() -> Path:
    d = Path(tempfile.gettempdir()) / f"myboot_worker_sync_{_slug()}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def clear_markers(name: Optional[str] = None) -> None:
    """删除标记文件。

    Args:
        name: 指定时只删除该 name 的 .done / .failed；为 None 时清空整个标记目录。
    """
    d = _marker_dir()
    if name is not None:
        patterns = [f"{name}.done", f"{name}.failed"]
        files = [d / p for p in patterns]
    else:
        files = list(d.glob("*.done")) + list(d.glob("*.failed"))
    for f in files:
        try:
            f.unlink()
        except FileNotFoundError:
            pass
        except OSError as e:  # pragma: no cover - 极端权限/占用情况
            _logger.warning(f"无法删除标记文件 {f}: {e!r}")


def _is_fresh(path: Path) -> bool:
    """判断标记文件是否属于本轮启动（基于 mtime）。"""
    try:
        return path.stat().st_mtime >= _MODULE_LOAD_TIME - _STALE_TOLERANCE
    except OSError:
        return False


def run_primary_first(
    name: str,
    primary_fn: Callable[[], T],
    secondary_fn: Optional[Callable[[], T]] = None,
    *,
    timeout: float = 300.0,
    poll_interval: float = 0.5,
) -> T:
    """primary-first 初始化协调：primary worker 执行重型初始化，其余 worker 等待。

    判定规则：``os.environ.get("MYBOOT_IS_PRIMARY_WORKER", "1") == "1"`` 视为
    primary（单进程模式无此变量 → 默认 primary，工具退化为直接调用 primary_fn）。

    - primary：先清理本 name 的旧标记，执行 primary_fn()；成功写
      ``<标记目录>/<name>.done``（内容为 ISO 时间戳）并返回结果；
      异常时写 ``<name>.failed``（内容为异常 repr）后重新抛出。
    - 非 primary：以 poll_interval 间隔轮询。发现新鲜的 .done → 执行
      secondary_fn()（为 None 时执行 primary_fn）并返回其结果；发现新鲜的
      .failed → 抛 RuntimeError（含 primary 的异常信息）；超过 timeout →
      抛 TimeoutError。

    陈旧标记的保证与限制（双重防护）：

    1. primary 在执行 primary_fn 之前删除本 name 的旧标记（clear_markers）。
    2. secondary 仅认可 mtime 不早于「本模块加载时间 - 2 秒容差」的标记，
       上一轮运行遗留的 .done/.failed 因 mtime 过旧会被忽略并继续等待。

    保证：只要新一轮启动距上一轮结束超过容差（2 秒），secondary 不会被上一轮
    遗留的标记误导；即使 secondary 比 primary 先启动并先于 primary 清理动作
    看到旧标记，mtime 检查也会将其判为陈旧。

    限制：
    - fork 模式下模块加载时间可能取自父进程导入时刻（早于 worker fork），
      若上一轮标记写入晚于父进程导入（如 2 秒内重启），理论上可能误认旧标记；
      实际部署中重启间隔远大于容差，且 primary 启动后会立即删除旧标记，
      该窗口极小。
    - 依赖文件系统 mtime 精度与各 worker 共享同一台机器的 temp 目录；
      不适用于跨机器协调。

    Args:
        name: 协调任务名，同一应用内唯一（用作标记文件名）。
        primary_fn: primary worker 执行的初始化函数。
        secondary_fn: 非 primary worker 在 primary 完成后执行的函数；
            为 None 时执行 primary_fn。
        timeout: 非 primary 等待 .done 的最长秒数。
        poll_interval: 轮询间隔秒数。

    Returns:
        primary_fn 或 secondary_fn 的返回值。

    Raises:
        TimeoutError: 非 primary 等待超时。
        RuntimeError: 非 primary 检测到 primary 初始化失败。
        Exception: primary 侧 primary_fn 抛出的原始异常。
    """
    is_primary = os.environ.get("MYBOOT_IS_PRIMARY_WORKER", "1") == "1"
    d = _marker_dir()
    done_file = d / f"{name}.done"
    failed_file = d / f"{name}.failed"

    if is_primary:
        clear_markers(name)
        _logger.info(f"[{name}] primary worker 开始初始化")
        try:
            result = primary_fn()
        except BaseException as e:
            failed_file.write_text(repr(e), encoding="utf-8")
            _logger.error(f"[{name}] primary 初始化失败: {e!r}")
            raise
        done_file.write_text(
            datetime.datetime.now().isoformat(), encoding="utf-8"
        )
        _logger.info(f"[{name}] primary 初始化完成，已写入标记 {done_file}")
        return result

    # 非 primary：轮询等待
    _logger.info(f"[{name}] secondary worker 等待 primary 完成 (timeout={timeout}s)")
    deadline = time.monotonic() + timeout
    while True:
        if done_file.exists() and _is_fresh(done_file):
            _logger.info(f"[{name}] 检测到 primary 完成标记，开始 secondary 加载")
            fn = secondary_fn if secondary_fn is not None else primary_fn
            return fn()
        if failed_file.exists() and _is_fresh(failed_file):
            detail = ""
            try:
                detail = failed_file.read_text(encoding="utf-8")
            except OSError:
                pass
            raise RuntimeError(
                f"primary worker 初始化 '{name}' 失败: {detail}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"等待 primary worker 完成 '{name}' 超时 ({timeout}s)"
            )
        time.sleep(poll_interval)
