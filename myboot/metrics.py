"""
内置 Prometheus 指标模块（F5）

特性：
- 可选依赖：未安装 prometheus-client 时全部 API 退化为 no-op，不报错
- 多 worker 模式（Linux/macOS）自动配置 PROMETHEUS_MULTIPROC_DIR 实现指标聚合
- 懒初始化：本模块 import 时绝不 import prometheus_client，
  保证 setup_multiproc_env 的环境变量先于 prometheus_client 初始化生效
- 内置 HTTP 指标中间件与 /metrics 端点（由 Application 在 metrics.enabled 时接入）

配置（conf/config.yaml）::

    metrics:
      enabled: true          # 默认 false
      path: /metrics         # 指标暴露路径
      http_metrics: true     # 是否启用 HTTP 请求指标中间件
      multiproc_dir: null    # multiproc 目录（默认系统临时目录下自动生成）

安装: pip install myboot[metrics]
"""

import importlib.util
import os
import re
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple

from loguru import logger

from .exceptions import MyBootException

# 默认耗时直方图分桶（秒）
DEFAULT_DURATION_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)

STAGE_HISTOGRAM_NAME = "myboot_stage_duration_seconds"


class MetricsNotAvailableError(MyBootException):
    """prometheus-client 未安装"""

    def __init__(self, message: Optional[str] = None):
        super().__init__(
            message
            or "prometheus-client 未安装，指标功能不可用。"
               "安装方式: pip install myboot[metrics]",
            "METRICS_NOT_AVAILABLE",
        )


def is_available() -> bool:
    """prometheus-client 是否已安装（不触发实际 import）"""
    return importlib.util.find_spec("prometheus_client") is not None


def _coerce_bool(value: Any, default: bool = False) -> bool:
    """布尔/字符串宽容转换（与 get_config_bool 语义一致）"""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(value)


def is_enabled(config) -> bool:
    """读取 metrics.enabled 配置（默认 False）"""
    return _coerce_bool(config.get("metrics.enabled", False))


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(name)).strip("_").lower()
    return slug or "app"


def setup_multiproc_env(config, app_name: str) -> None:
    """配置 Prometheus 多进程聚合环境变量

    须在 prometheus_client 被 import 之前调用（Application.__init__ 早期）。
    仅当满足以下全部条件时生效，否则 no-op：

    - metrics.enabled 为真
    - prometheus-client 已安装
    - server.workers > 1
    - 非 Windows（win32 多 worker 为 spawn 模式，multiproc 文件聚合不受支持）

    本函数自身绝不 import prometheus_client。
    """
    if not is_enabled(config):
        return
    if not is_available():
        return

    try:
        workers = int(config.get("server.workers", 1) or 1)
    except (TypeError, ValueError):
        workers = 1
    if workers <= 1:
        return
    if sys.platform == "win32":
        logger.warning(
            "Windows 多 worker 模式不支持 Prometheus multiproc 聚合，"
            "各 worker 将仅暴露本进程指标"
        )
        return

    # 用户已自行设置 → 尊重，不覆盖、不清理
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        Path(os.environ["PROMETHEUS_MULTIPROC_DIR"]).mkdir(parents=True, exist_ok=True)
        return

    if "prometheus_client" in sys.modules:
        logger.warning(
            "prometheus_client 已在 PROMETHEUS_MULTIPROC_DIR 设置之前被 import，"
            "多进程指标聚合可能失效（请避免在应用创建前 import prometheus_client）"
        )

    multiproc_dir = config.get("metrics.multiproc_dir", None)
    if multiproc_dir:
        target = Path(str(multiproc_dir))
    else:
        target = Path(tempfile.gettempdir()) / f"myboot_prometheus_{_slugify(app_name)}"
    target.mkdir(parents=True, exist_ok=True)

    # 仅父进程（MYBOOT_WORKER_ID 尚未设置时）清理陈旧 db 文件
    if "MYBOOT_WORKER_ID" not in os.environ:
        for stale in target.glob("*.db"):
            try:
                stale.unlink()
            except OSError:
                pass

    os.environ["PROMETHEUS_MULTIPROC_DIR"] = str(target)
    logger.debug(f"Prometheus multiproc 目录: {target}")


def _use_multiproc() -> bool:
    return bool(os.environ.get("PROMETHEUS_MULTIPROC_DIR"))


def make_metrics_asgi_app():
    """构建 /metrics ASGI 应用（懒初始化包装器）

    首次请求时才 import prometheus_client 并构建真实 app：
    - multiproc 模式（环境变量已设置）→ MultiProcessCollector 聚合所有 worker
    - 否则使用默认全局 REGISTRY
    """
    state: Dict[str, Any] = {"app": None}

    async def metrics_app(scope, receive, send):
        if state["app"] is None:
            if not is_available():
                raise MetricsNotAvailableError()
            from prometheus_client import CollectorRegistry, REGISTRY, make_asgi_app

            if _use_multiproc():
                from prometheus_client import multiprocess

                registry = CollectorRegistry()
                multiprocess.MultiProcessCollector(registry)
            else:
                registry = REGISTRY
            state["app"] = make_asgi_app(registry=registry)
        await state["app"](scope, receive, send)

    return metrics_app


def mark_current_process_dead() -> None:
    """multiproc 模式下标记本进程退出（清理 gauge 残留文件），其余情况 no-op"""
    try:
        if not _use_multiproc() or not is_available():
            return
        from prometheus_client import multiprocess

        multiprocess.mark_process_dead(os.getpid())
    except Exception as e:  # 退出路径绝不抛错
        logger.debug(f"mark_current_process_dead 失败（已忽略）: {e}")


# ==================== 指标工厂（带 no-op 退化） ====================


class _NoopMetric:
    """prometheus-client 未安装时的 no-op 桩对象，支持链式调用"""

    def labels(self, *args, **kwargs) -> "_NoopMetric":
        return self

    def inc(self, *args, **kwargs) -> None:
        pass

    def observe(self, *args, **kwargs) -> None:
        pass

    def set(self, *args, **kwargs) -> None:
        pass


_NOOP_METRIC = _NoopMetric()

# 进程内指标缓存，防止同名重复注册
_metrics_cache: Dict[str, Any] = {}


def get_counter(name: str, documentation: str, labelnames: Sequence[str] = ()):
    """获取（或创建）Counter；未安装 prometheus-client 时返回 no-op 桩"""
    if name in _metrics_cache:
        return _metrics_cache[name]
    if not is_available():
        return _NOOP_METRIC
    from prometheus_client import Counter

    metric = Counter(name, documentation, list(labelnames))
    _metrics_cache[name] = metric
    return metric


def get_histogram(
    name: str,
    documentation: str,
    labelnames: Sequence[str] = (),
    buckets: Optional[Tuple[float, ...]] = None,
):
    """获取（或创建）Histogram；未安装 prometheus-client 时返回 no-op 桩"""
    if name in _metrics_cache:
        return _metrics_cache[name]
    if not is_available():
        return _NOOP_METRIC
    from prometheus_client import Histogram

    metric = Histogram(
        name,
        documentation,
        list(labelnames),
        buckets=buckets or DEFAULT_DURATION_BUCKETS,
    )
    _metrics_cache[name] = metric
    return metric


def observe_stage(stage: str, seconds: float, **labels) -> None:
    """记录某个处理阶段耗时到内置 myboot_stage_duration_seconds 直方图

    Args:
        stage: 阶段名（如 "recall"、"rank"）
        seconds: 耗时（秒），负值忽略
        **labels: 预留扩展，当前版本忽略额外标签
    """
    if seconds < 0:
        return
    histogram = get_histogram(
        STAGE_HISTOGRAM_NAME,
        "MyBoot 处理阶段耗时（秒）",
        labelnames=("stage",),
    )
    histogram.labels(stage=stage).observe(seconds)


@contextmanager
def time_stage(stage: str) -> Iterator[None]:
    """上下文管理器：自动计时 with 体并调用 observe_stage"""
    t0 = time.perf_counter()
    try:
        yield
    finally:
        observe_stage(stage, time.perf_counter() - t0)


# ==================== HTTP 指标中间件 ====================

try:  # starlette 是 fastapi 的必备依赖，正常总是可用
    from starlette.middleware.base import BaseHTTPMiddleware
except ImportError:  # pragma: no cover
    BaseHTTPMiddleware = object  # type: ignore


class HttpMetricsMiddleware(BaseHTTPMiddleware):
    """HTTP 请求指标中间件

    - myboot_http_requests_total{method, path, status} 请求计数
    - myboot_http_request_duration_seconds{method, path} 请求耗时直方图

    path 标签使用路由模板（如 /items/{id}）避免高基数；未匹配路由归入
    "unmatched"；metrics 自身路径不统计。指标对象懒创建——middleware
    import / 构造时不触碰 prometheus_client。
    """

    def __init__(self, app, metrics_path: str = "/metrics"):
        super().__init__(app)
        self.metrics_path = metrics_path
        self._requests_total = None
        self._request_duration = None

    def _ensure_metrics(self) -> None:
        if self._requests_total is None:
            self._requests_total = get_counter(
                "myboot_http_requests_total",
                "HTTP 请求总数",
                labelnames=("method", "path", "status"),
            )
            self._request_duration = get_histogram(
                "myboot_http_request_duration_seconds",
                "HTTP 请求耗时（秒）",
                labelnames=("method", "path"),
            )

    @staticmethod
    def _route_path(scope) -> str:
        route = scope.get("route")
        if route is None:
            return "unmatched"
        # FastAPI APIRoute → path_format（模板，如 /items/{id}）；Mount → path
        path = getattr(route, "path_format", None) or getattr(route, "path", None)
        return path or "unmatched"

    async def dispatch(self, request, call_next):
        raw_path = request.url.path
        if raw_path == self.metrics_path or raw_path.startswith(self.metrics_path + "/"):
            return await call_next(request)

        t0 = time.perf_counter()
        response = await call_next(request)
        elapsed = time.perf_counter() - t0

        try:
            path = self._route_path(request.scope)
            self._ensure_metrics()
            self._requests_total.labels(
                method=request.method, path=path, status=str(response.status_code)
            ).inc()
            self._request_duration.labels(
                method=request.method, path=path
            ).observe(elapsed)
        except Exception as e:  # 指标采集绝不影响业务请求
            logger.debug(f"HTTP 指标采集失败（已忽略）: {e}")

        return response
