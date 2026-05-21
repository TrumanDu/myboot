# 任务调度器使用说明

MyBoot 内置基于 [APScheduler](https://apscheduler.readthedocs.io/) 的任务调度器，支持 Cron、固定间隔与一次性任务。应用启动时，若已注册任务且调度器已启用，会自动启动调度。

## 1. 快速开始（推荐）

定时任务**必须**定义在 `@component` 装饰的类中，由自动配置在组件注册时扫描并加入 `app.scheduler`。

```python
from myboot.core.decorators import component, cron, interval, once
from myboot.core.config import get_config

@component()
class DataSyncJobs:
    """定时任务组件"""

    @cron("0 2 * * *")  # 每天 02:00（5 位 Cron）
    def sync_daily(self):
        print("每日同步")

    @interval(minutes=30)  # 每 30 分钟
    def health_check(self):
        print("健康检查")

    @once("2025-12-31 23:59:59")  # 指定时刻执行一次
    def year_end_task(self):
        print("年末任务")

    @cron("0 */5 * * *", enabled=get_config("jobs.report.enabled", True))
    def report(self):
        """enabled 可从配置读取，为 False 时不注册"""
        print("报表任务")
```

**要求与约定：**

| 项 | 说明 |
|----|------|
| 类装饰器 | 必须使用 `@component()`，不支持模块级函数或 `@service` 类中的 `@cron` |
| 方法可见性 | 以 `_` 开头的私有方法不会被扫描 |
| 依赖注入 | 可在组件 `__init__` 中注入 `@service`，在任务方法内使用 |
| 包扫描 | 任务类需位于自动发现包内（默认 `app`），见项目启动与 `auto_discover` 配置 |

应用生命周期中，存在已注册任务且调度器启用时，会在启动钩子阶段调用 `scheduler.start()`，关闭时 `scheduler.stop()`。

## 2. 配置

在 `config.yaml` 或环境变量中配置（环境变量嵌套键使用双下划线 `__`，详见 [配置管理使用说明](./configuration.md)）。

```yaml
scheduler:
  enabled: true              # 是否允许启动调度器
  timezone: "Asia/Shanghai"  # 任务触发时区（建议显式设置）
  max_workers: 10            # 线程池大小
  on_all_workers: false      # 多 worker 时是否在非 primary 进程也启用
```

| 配置项 | 环境变量示例 | 默认值 | 说明 |
|--------|----------------|--------|------|
| `scheduler.enabled` | `SCHEDULER__ENABLED=false` | `true` | 为 `false` 时调度器不启动 |
| `scheduler.timezone` | `SCHEDULER__TIMEZONE=Asia/Shanghai` | `UTC` | 所有触发器使用的时区 |
| `scheduler.max_workers` | `SCHEDULER__MAX_WORKERS=20` | `10` | 并发执行任务的上限 |
| `scheduler.on_all_workers` | `SCHEDULER__ON_ALL_WORKERS=true` | `false` | 见下文「多 Worker」 |

`config.py` 中默认片段：

```yaml
scheduler:
  enabled: true
  timezone: "UTC"
  max_workers: 10
```

## 3. Cron 表达式

解析逻辑见 `myboot/core/scheduler.py` 中 `_parse_cron`：

1. 优先使用 `CronTrigger.from_crontab`（**5 位**，Unix/APScheduler 风格）
2. 解析失败时回退为手动构造 `CronTrigger`（支持 **5 位** 或 **6 位**）

### 3.1 五位格式（推荐）

顺序：**分 · 时 · 日 · 月 · 周**

```
分  时  日  月  周
│   │   │   │   └── 星期（0=周一 … 6=周日，见下表）
│   │   │   └────── 月（1-12 或 *）
│   │   └────────── 日（1-31 或 *）
│   └────────────── 时（0-23）
└────────────────── 分（0-59）
```

**星期字段（APScheduler `from_crontab`）与 Linux crontab 不同：**

| 值 | APScheduler（本项目 5 位默认路径） | 传统 Linux crontab |
|----|-----------------------------------|---------------------|
| 0 | 周一 | 周日 |
| 1 | 周二 | 周一 |
| 2 | 周三 | 周二 |
| … | … | … |
| 6 | 周日 | 周六 |

> 使用 5 位表达式时，请按 **APScheduler 星期编号**理解，避免与系统 `crontab` 的「周日=0」混淆。

**常用示例（5 位，时区以 `scheduler.timezone` 为准）：**

| 表达式 | 含义 |
|--------|------|
| `0 * * * *` | 每小时整点 |
| `0 2 * * *` | 每天 02:00 |
| `0 23 * * 1` | 每周二 23:00（周字段 `1` = 周二） |
| `30 8 * * 0` | 每周一 08:30 |
| `*/15 * * * *` | 每 15 分钟 |
| `0 9-17 * * 1-5` | 工作日 09:00–17:00 每小时整点 |

### 3.2 六位格式（兼容旧写法）

顺序：**秒 · 分 · 时 · 日 · 月 · 周**

仅在 `from_crontab` 无法解析时走手动分支，例如：

| 表达式 | 含义 |
|--------|------|
| `0 0 * * * *` | 每小时整点（秒=0，分=0） |
| `0 */5 * * * *` | 每 5 分钟 |
| `0 0 2 * * *` | 每天 02:00:00 |

### 3.3 字段支持

支持 APScheduler 常见写法：`*`、`,`、`-`、`/` 及范围。复杂表达式以 [APScheduler Cron 文档](https://apscheduler.readthedocs.io/en/stable/modules/triggers/cron.html) 为准。

## 4. 装饰器 API

### 4.1 `@cron`

```python
@cron(cron_expression: str, enabled: bool | None = None, **kwargs)
```

- `cron_expression`：5 位或 6 位 Cron 字符串
- `enabled`：`False` 时跳过注册；`None` 默认启用
- `**kwargs`：传给 APScheduler `add_job` 的额外参数（如 `name`、`max_instances`）

### 4.2 `@interval`

```python
@interval(seconds=None, minutes=None, hours=None, enabled=None, **kwargs)
```

三者至少指定其一，内部统一换算为秒。例如 `@interval(minutes=5)` 每 5 分钟执行。

### 4.3 `@once`

```python
@once(run_date: str, enabled=None, **kwargs)
```

`run_date` 支持格式：

- `YYYY-MM-DD HH:MM:SS`
- `YYYY-MM-DD HH:MM`
- `YYYY-MM-DD`（当天 00:00:00）

时间为 **naive**，由调度器全局时区 `scheduler.timezone` 解释。

## 5. 多 Worker 与进程模型

多进程部署（`server.workers > 1`）时：

- 默认**仅 primary worker**（`MYBOOT_IS_PRIMARY_WORKER=1`）启用调度器，避免重复执行
- 设置 `scheduler.on_all_workers: true` 可在每个 worker 都运行调度（一般仅特殊场景需要）

任务在**线程池**中执行；`max_instances` 默认为 3（APScheduler `job_defaults`），同一任务并发实例数受此限制。

## 6. 编程式 API

除装饰器外，可通过 `app.scheduler` 或 `get_scheduler()` 动态管理任务。

```python
from myboot.core.scheduler import get_scheduler

scheduler = get_scheduler()  # 注意：与 Application 可能不是同一实例，推荐用 app.scheduler

job_id = scheduler.add_cron_job(func=my_func, cron="0 2 * * *")
scheduler.add_interval_job(func=my_func, interval=60)
scheduler.add_date_job(func=my_func, run_date="2025-12-31 23:59:59")

scheduler.remove_job(job_id)
info = scheduler.get_job_info(job_id)
all_jobs = scheduler.list_all_jobs()
```

| 方法 | 说明 |
|------|------|
| `add_cron_job(func, cron, job_id=None, **kwargs)` | 添加 Cron 任务 |
| `add_interval_job(func, interval, job_id=None, **kwargs)` | `interval` 单位为秒 |
| `add_date_job(func, run_date, job_id=None, **kwargs)` | 一次性任务 |
| `remove_job(job_id)` | 移除任务 |
| `get_job_info(job_id)` | 任务类型、下次执行时间等 |
| `list_all_jobs()` | 所有任务摘要 |
| `start()` / `stop()` | 启停（应用生命周期通常自动处理） |
| `is_enabled()` / `is_running()` / `has_jobs()` | 状态查询 |
| `get_config()` | 当前调度器配置摘要 |

未指定 `job_id` 时，默认可为 `cron_{函数名}`、`interval_{函数名}` 等。

### 6.1 `ScheduledJob` 类

继承 `myboot.jobs.scheduled_job.ScheduledJob` 可实现带重试、状态跟踪的任务，并通过 `add_scheduled_job` 注册：

```python
from myboot.jobs.scheduled_job import ScheduledJob

class CleanupJob(ScheduledJob):
    def run(self):
        # 业务逻辑
        return "ok"

job = CleanupJob(name="cleanup", trigger="0 3 * * *")  # 或 trigger={'type': 'cron', 'cron': '...'}
app.scheduler.add_scheduled_job(job)
```

`trigger` 可为 Cron 字符串或字典：`cron` / `interval`（seconds/minutes/hours/days）/ `date`（`run_date`）。

## 7. 运维与排查

```python
# 应用内
print(app.scheduler.get_config())
for job in app.scheduler.list_all_jobs():
    print(job["job_id"], job.get("type"), job.get("next_run_time"))

# 健康检查等可结合 application 状态中的 scheduler 字段
```

日志：调度器绑定 logger 名 `scheduler`，注册任务时会输出 `已添加 Cron 任务` 等信息。

**任务未执行时检查：**

1. `scheduler.enabled` 是否为 `true`
2. 多 worker 下当前进程是否为 primary（或已开启 `on_all_workers`）
3. 装饰器 `enabled=False` 是否被跳过
4. Cron 位数与星期编号是否符合上文约定
5. `timezone` 是否与预期一致
6. 应用启动时 `has_jobs()` 是否为真（无任务不会 `start()`）

## 8. 最佳实践

1. **统一用 5 位 Cron**，减少与 6 位混用带来的理解成本。
2. **显式设置 `scheduler.timezone`**（如 `Asia/Shanghai`），避免默认 UTC 造成「时间差 8 小时」。
3. **任务逻辑保持幂等**；错过触发时 APScheduler 有 `misfire_grace_time`（默认 30 秒），但仍可能补跑。
4. **长耗时任务**注意 `max_workers` 与 `max_instances`，避免占满线程池。
5. **配置开关**用 `enabled=get_config('jobs.xxx.enabled', True)`，便于按环境关闭任务。
6. **IO 密集或阻塞操作**在任务内自行控制超时与异常，避免拖垮调度线程。

## 9. 相关文档与代码

| 资源 | 说明 |
|------|------|
| [scheduler_refactor_analysis.md](./scheduler_refactor_analysis.md) | APScheduler 重构与能力对照（偏设计） |
| [dependency-injection.md](./dependency-injection.md) | 含定时任务的 `@component` 示例 |
| `myboot/core/scheduler.py` | 调度器实现 |
| `myboot/core/decorators.py` | `@cron` / `@interval` / `@once` |
| `examples/convention_app.py` | 完整示例 `ScheduledJobs` 组件 |
