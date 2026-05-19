# 调度器重构可行性分析：使用 APScheduler

> **使用说明**：日常开发与配置 Cron/间隔任务请参阅 [scheduler.md](./scheduler.md)。

## 1. 当前实现分析

### 1.1 现有功能
- **任务类型支持**：
  - Cron 任务（6位表达式：秒 分 时 日 月 周）
  - 间隔任务（interval，支持秒/分/小时）
  - 一次性任务（date，指定日期时间执行）
  
- **API 接口**：
  - `add_cron_job(func, cron, job_id, **kwargs)`
  - `add_interval_job(func, interval, job_id, **kwargs)`
  - `add_date_job(func, run_date, job_id, **kwargs)`
  - `remove_job(job_id)`
  - `get_job(job_id)`
  - `list_jobs()`
  - `start()` / `stop()`
  - `add_scheduled_job(job: ScheduledJob)`
  
- **集成点**：
  - 与 `Application` 生命周期集成（启动/停止）
  - 自动配置系统自动注册装饰器标记的任务
  - 支持 `ScheduledJob` 类对象
  - 配置支持（时区、最大工作线程数、启用/禁用）

### 1.2 当前实现的问题
- 自定义实现，维护成本高
- Cron 表达式解析逻辑复杂，可能存在边界情况
- 时区处理需要手动实现（依赖 pytz）
- 任务执行状态跟踪有限
- 缺少任务持久化能力
- 错误处理和重试机制简单

## 2. APScheduler 能力分析

### 2.1 APScheduler 核心特性
- **触发器（Triggers）**：
  - `CronTrigger`：完整的 Cron 表达式支持（5位或6位）
  - `IntervalTrigger`：固定间隔执行
  - `DateTrigger`：指定日期时间执行
  
- **执行器（Executors）**：
  - `ThreadPoolExecutor`：线程池执行
  - `ProcessPoolExecutor`：进程池执行
  - `AsyncIOExecutor`：异步执行
  - `GeventExecutor`：Gevent 协程执行
  
- **任务存储（Job Stores）**：
  - `MemoryJobStore`：内存存储（默认）
  - `SQLAlchemyJobStore`：数据库持久化
  - `RedisJobStore`：Redis 持久化
  - `MongoDBJobStore`：MongoDB 持久化
  
- **调度器（Schedulers）**：
  - `BlockingScheduler`：阻塞式调度器
  - `BackgroundScheduler`：后台线程调度器（推荐）
  - `AsyncIOScheduler`：异步调度器
  - `GeventScheduler`：Gevent 调度器
  - `TornadoScheduler`：Tornado 调度器
  - `TwistedScheduler`：Twisted 调度器

### 2.2 APScheduler 优势
- ✅ 成熟稳定，社区活跃
- ✅ 完整的 Cron 表达式支持（包括复杂表达式）
- ✅ 内置时区支持（无需手动处理）
- ✅ 任务持久化能力
- ✅ 丰富的任务状态和事件监听
- ✅ 更好的错误处理和日志
- ✅ 支持任务暂停/恢复
- ✅ 支持任务修改（修改触发器）

## 3. 功能映射关系

### 3.1 任务类型映射

| 当前实现 | APScheduler | 映射方式 |
|---------|------------|---------|
| `add_cron_job(func, cron, ...)` | `scheduler.add_job(func, CronTrigger.from_crontab(cron), ...)` | ✅ 直接映射 |
| `add_interval_job(func, interval, ...)` | `scheduler.add_job(func, IntervalTrigger(seconds=interval), ...)` | ✅ 直接映射 |
| `add_date_job(func, run_date, ...)` | `scheduler.add_job(func, DateTrigger(run_date=...), ...)` | ✅ 直接映射 |

### 3.2 API 接口映射

| 当前方法 | APScheduler 对应方法 | 兼容性 |
|---------|-------------------|--------|
| `add_cron_job()` | `add_job(..., trigger=CronTrigger(...))` | ✅ 可封装保持兼容 |
| `add_interval_job()` | `add_job(..., trigger=IntervalTrigger(...))` | ✅ 可封装保持兼容 |
| `add_date_job()` | `add_job(..., trigger=DateTrigger(...))` | ✅ 可封装保持兼容 |
| `remove_job(job_id)` | `remove_job(job_id)` | ✅ 完全兼容 |
| `get_job(job_id)` | `get_job(job_id)` | ✅ 完全兼容 |
| `list_jobs()` | `get_jobs()` | ✅ 可封装保持兼容 |
| `start()` | `start()` | ✅ 完全兼容 |
| `stop()` | `shutdown()` | ⚠️ 需要封装（方法名不同） |

### 3.3 配置映射

| 当前配置 | APScheduler 配置 | 映射方式 |
|---------|-----------------|---------|
| `scheduler.enabled` | 通过 `start()` 控制 | ✅ 逻辑控制 |
| `scheduler.timezone` | `timezone` 参数 | ✅ 直接映射 |
| `scheduler.max_workers` | `executors['default']['max_workers']` | ✅ 直接映射 |

## 4. 重构方案设计

### 4.1 方案一：完全封装（推荐）

**设计思路**：保持现有 API 接口不变，内部使用 APScheduler 实现。

**优点**：
- ✅ 完全向后兼容，现有代码无需修改
- ✅ 可以逐步迁移，降低风险
- ✅ 保留自定义扩展能力

**实现要点**：
```python
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

class Scheduler:
    def __init__(self, config):
        # 创建 APScheduler 实例
        self._scheduler = BackgroundScheduler(
            timezone=config.get('scheduler.timezone', 'UTC'),
            executors={
                'default': {
                    'type': 'threadpool',
                    'max_workers': config.get('scheduler.max_workers', 10)
                }
            }
        )
        # 保持兼容性的映射
        self._jobs = {}  # job_id -> APScheduler Job
        self._scheduled_jobs = {}  # ScheduledJob 对象
    
    def add_cron_job(self, func, cron, job_id=None, **kwargs):
        # 转换 cron 表达式格式（6位 -> APScheduler 格式）
        trigger = self._parse_cron(cron)
        job = self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            **kwargs
        )
        self._jobs[job_id] = job
        return job_id
```

### 4.2 方案二：直接使用 APScheduler

**设计思路**：直接暴露 APScheduler 的 API，简化封装层。

**优点**：
- ✅ 代码更简洁
- ✅ 可以直接使用 APScheduler 的高级特性

**缺点**：
- ❌ 需要修改现有代码
- ❌ 破坏向后兼容性

### 4.3 推荐方案：方案一（完全封装）

**理由**：
1. 保持 API 兼容性，现有代码无需修改
2. 可以逐步增强功能（如添加持久化）
3. 保留扩展空间

## 5. 实施步骤

### 5.1 阶段一：基础重构（保持兼容）
1. **创建新的 Scheduler 类**（基于 APScheduler）
   - 实现所有现有 API 方法
   - 保持方法签名和返回值一致
   - 内部使用 APScheduler

2. **Cron 表达式转换**
   - 当前格式：`"秒 分 时 日 月 周"`（6位）
   - APScheduler 格式：`"分 时 日 月 周"`（5位）或 `CronTrigger` 对象
   - 需要实现转换函数

3. **测试验证**
   - 单元测试覆盖所有 API
   - 集成测试验证现有功能
   - 确保装饰器和自动配置正常工作

### 5.2 阶段二：功能增强（可选）
1. **添加任务持久化**
   - 支持 SQLAlchemyJobStore
   - 支持 RedisJobStore

2. **增强任务管理**
   - 任务暂停/恢复
   - 任务修改触发器
   - 任务执行历史

3. **事件监听**
   - 任务执行成功/失败事件
   - 任务错过执行事件

### 5.3 阶段三：优化和文档
1. 性能优化
2. 文档更新
3. 示例代码更新

## 6. 关键技术点

### 6.1 Cron 表达式转换

**当前格式**（6位）：
```
"秒 分 时 日 月 周"
例如："0 0 * * * *"  # 每小时
```

**APScheduler 格式**（5位或 CronTrigger）：
```python
# 方式1：5位字符串（标准 Cron）
"分 时 日 月 周"
"0 * * * *"  # 每小时

# 方式2：CronTrigger 对象（推荐）
CronTrigger(second=0, minute=0, hour='*', day='*', month='*', day_of_week='*')
```

**转换函数**：
```python
def _parse_cron(self, cron_expr: str) -> CronTrigger:
    """将6位 Cron 表达式转换为 APScheduler CronTrigger"""
    parts = cron_expr.split()
    if len(parts) == 6:
        second, minute, hour, day, month, weekday = parts
        return CronTrigger(
            second=second,
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=weekday
        )
    elif len(parts) == 5:
        # 标准5位格式，秒默认为0
        return CronTrigger.from_crontab(cron_expr)
    else:
        raise ValueError(f"无效的 Cron 表达式: {cron_expr}")
```

### 6.2 ScheduledJob 集成

需要将 `ScheduledJob.execute()` 方法适配到 APScheduler：

```python
def add_scheduled_job(self, job: ScheduledJob, job_id: Optional[str] = None):
    # 包装 execute 方法，保持状态跟踪
    def wrapped_execute():
        return job.execute()
    
    # 根据 trigger 类型添加任务
    if isinstance(job.trigger, str):
        return self.add_cron_job(wrapped_execute, job.trigger, job_id)
    # ... 其他类型
```

### 6.3 时区处理

APScheduler 内置时区支持，无需手动处理：

```python
from pytz import timezone

scheduler = BackgroundScheduler(
    timezone=timezone('Asia/Shanghai')  # 直接支持时区对象
)
```

### 6.4 任务状态管理

APScheduler 提供任务状态：
- `pending`：等待执行
- `running`：正在执行
- `completed`：已完成
- `failed`：执行失败

可以通过 `get_job(job_id).next_run_time` 获取下次执行时间。

## 7. 风险评估

### 7.1 兼容性风险
- **风险**：Cron 表达式格式差异
- **缓解**：实现转换函数，确保兼容

### 7.2 行为差异风险
- **风险**：APScheduler 的执行时机可能与当前实现略有差异
- **缓解**：充分测试，特别是边界情况

### 7.3 性能风险
- **风险**：APScheduler 可能有额外的开销
- **缓解**：性能测试，通常 APScheduler 性能更好

## 8. 测试策略

### 8.1 单元测试
- 所有 API 方法的测试
- Cron 表达式转换测试
- 任务添加/删除测试

### 8.2 集成测试
- 装饰器自动注册测试
- ScheduledJob 集成测试
- 应用生命周期集成测试

### 8.3 兼容性测试
- 现有示例代码运行测试
- 现有项目迁移测试

## 9. 结论

### 9.1 可行性评估
✅ **高度可行**

**理由**：
1. APScheduler 功能完全覆盖现有需求
2. API 可以完全兼容
3. 项目已依赖 APScheduler
4. 重构收益明显（稳定性、功能、维护性）

### 9.2 推荐方案
**采用方案一（完全封装）**，分阶段实施：
1. 第一阶段：基础重构，保持兼容
2. 第二阶段：功能增强（可选）
3. 第三阶段：优化和文档

### 9.3 预期收益
- ✅ 减少代码量（约 200+ 行 → 100+ 行）
- ✅ 提高稳定性（使用成熟库）
- ✅ 增强功能（持久化、事件监听等）
- ✅ 降低维护成本
- ✅ 更好的错误处理

### 9.4 实施建议
1. **先创建新实现**，保留旧实现作为备份
2. **充分测试**后再替换
3. **逐步迁移**，可以先支持两种实现并存
4. **文档更新**，说明新特性

## 10. 参考资源

- APScheduler 官方文档：https://apscheduler.readthedocs.io/
- APScheduler GitHub：https://github.com/agronholm/apscheduler
- Cron 表达式参考：https://en.wikipedia.org/wiki/Cron

