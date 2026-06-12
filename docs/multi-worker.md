# 多 Worker 模式指南

myboot 0.2.0 起，多 worker（`workers > 1`）模式下**每个 worker 进程独立完成组件实例化**：
client / service / controller 都在各自的 worker 进程内创建，不再共享父进程 fork 前的实例。
本文是多 worker 相关能力的一站式说明。

## 启动方式

```python
# main.py
app = create_app(name="my-app", auto_discover_package="app")

if __name__ == "__main__":
    app.run(workers=4, app_path="main:app.get_fastapi_app()")
```

```yaml
# conf/config.yaml 亦可
server:
  workers: 4
```

无需再手动调用 `auto_discover()` / `apply_auto_configuration()`——框架在每个
worker 内自动完成引导（`bootstrap_worker()`）。

## Worker 信息

```python
app.worker_id          # 当前 worker ID，从 1 开始；单进程为 1
app.worker_count       # worker 总数
app.is_primary_worker  # 是否主 worker（适合"只跑一份"的逻辑）
```

## Client 生命周期

**建连可以直接写在 `__init__` 里**——0.2.0 起 client 在各 worker 内实例化，
不存在 fork 共享连接问题：

```python
@client()
class RedisClient:
    def __init__(self):
        self.conn = redis.Redis(...)   # 安全：本 worker 进程内新建

    def close(self):                   # 可选：定义即获得自动清理
        self.conn.close()
```

定义了 `close()` 方法（同步或 async）的 client，框架会在 worker 停止时
（`worker_stop_hooks` 之后、`shutdown_hooks` 之前）自动调用。若你也在自己的
shutdown 钩子里关闭，建议把 `close()` 实现为幂等——框架的兜底调用会把二次
close 的异常降为 warning，不影响关闭流程。

## Worker 生命周期钩子

```python
from myboot.core.decorators import on_worker_start, on_worker_stop

@on_worker_start
def warm_up():
    """每个 worker 启动时各触发一次（startup_hooks 之后、调度器启动之前）"""

@on_worker_stop(order=1)
def cleanup():
    """每个 worker 停止时各触发一次（调度器停止之后）"""
```

> Windows 限制：多 worker 下父进程以 `terminate()` 硬终止 worker，
> `@on_worker_stop` 与 client 自动 close 在 Windows 上不保证执行；
> 关键清理逻辑不要只依赖它们。

## Primary-first 初始化协调：`run_primary_first`

典型场景：4 个 worker 都去下载同一个 3GB 模型是浪费——应该 primary 下载，
其余 worker 等它完成后直接从本地加载：

```python
from myboot.utils import run_primary_first
from myboot.core.decorators import on_worker_start

@on_worker_start
def load_model():
    run_primary_first(
        "sasrec-model",                  # 协调标识（同一标识共享一次协调）
        primary_fn=download_and_load,    # 仅 primary 执行
        secondary_fn=load_from_local,    # 其余 worker 等 primary 完成后执行
        timeout=600,                     # 等待上限（秒）
    )
```

语义保证：

- `secondary_fn` 缺省时其余 worker 也执行 `primary_fn`（即"等完再做同样的事"）；
- primary 失败 → 其余 worker 立即抛 `RuntimeError`（含原始异常信息），不会傻等到超时；
- 等待超时 → `TimeoutError`；
- 单进程模式退化为直接调用 `primary_fn()`，代码无需分支；
- 基于临时目录标记文件实现，跨 spawn/fork、跨平台（不依赖 fcntl），
  但**不支持跨机器**协调。

## 定时任务与多 worker：任务级 `all_workers`

默认所有定时任务**只在 primary worker 执行一份**（防止重复）。但"刷新本 worker
内存态"的任务必须每个 worker 各跑一份，否则其余 worker 永远用旧数据：

```python
@component()
class RefreshJobs:
    @interval(hours=24, all_workers=True)   # 每个 worker 各执行
    def refresh_local_cache(self):
        self.items = load_items()           # 刷新的是本进程内存

    @cron("0 2 * * *")                       # 默认：仅 primary 执行一份
    def daily_report(self):
        send_report()
```

怎么选：

| 任务类型 | 配置 | 原因 |
|---|---|---|
| 刷新进程内缓存/内存清单 | `all_workers=True` | 数据在各 worker 自己内存里 |
| 发报表 / 写数据库 / 调外部 API | 默认 | 跑多份会重复副作用 |

全局开关 `scheduler.on_all_workers: true`（所有任务都在全部 worker 跑）仍然有效，
但通常任务级参数是更对的粒度。`scheduler.enabled: false` 全局禁用一切定时任务。

## 服务作用域 `scope`

```python
@service()                      # 默认 singleton：每个 worker 进程内一个实例
@service(scope="request")       # 每个请求（asyncio 任务上下文）一个实例
@service(scope="factory")       # 每次注入解析都创建新实例
```

`request` 作用域适合存放请求级上下文，避免单例服务里的共享可变状态。
`@client` 同样支持 `scope` 参数。
