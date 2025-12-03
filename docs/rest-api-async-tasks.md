# REST API 中使用异步任务

在 REST API 中，当需要执行耗时操作时，可以使用异步任务来避免阻塞请求响应。MyBoot 提供了多种方式来在 REST API 中使用异步任务。

## 目录

- [快速启动后台任务](#快速启动后台任务)
- [使用 ScheduledJob](#使用-scheduledjob)
- [异步路由处理](#异步路由处理)
- [任务状态查询](#任务状态查询)
- [完整示例](#完整示例)

## 快速启动后台任务

使用 `asyn_run` 函数可以快速在后台启动异步任务，适用于不需要跟踪任务状态的场景。

### 基本用法

```python
from myboot.core.decorators import post, rest_controller
from myboot.utils.async_utils import asyn_run
import time

def process_data(data: dict):
    """耗时的数据处理任务"""
    print(f"开始处理数据: {data}")
    time.sleep(5)  # 模拟耗时操作
    print(f"数据处理完成: {data}")
    return {"processed": True, "data": data}

@rest_controller('/api/tasks')
class TaskController:
    """任务控制器"""

    @post('/process')
    def create_process_task(self, data: dict):
        """创建数据处理任务"""
        # 立即返回，任务在后台执行
        task = asyn_run(process_data, data, task_name="数据处理任务")

        return {
            "message": "任务已创建，正在后台处理",
            "task_id": str(id(task)),
            "status": "pending"
        }
```

### 带参数的任务

```python
from myboot.utils.async_utils import asyn_run

def send_email(to: str, subject: str, content: str):
    """发送邮件任务"""
    print(f"发送邮件到 {to}: {subject}")
    # 模拟邮件发送
    time.sleep(2)
    return {"sent": True, "to": to}

@post('/api/emails')
def send_email_async(to: str, subject: str, content: str):
    """异步发送邮件"""
    # 启动后台任务
    asyn_run(send_email, to, subject, content, task_name=f"发送邮件给{to}")

    return {
        "message": "邮件发送任务已创建",
        "recipient": to
    }
```

## 使用 ScheduledJob

对于需要跟踪和管理任务状态的场景，建议使用 `ScheduledJob`。

### 使用 ScheduledJob

```python
from myboot.core.decorators import post, get, rest_controller
from myboot.jobs.scheduled_job import ScheduledJob
from myboot.core.scheduler import get_scheduler
import time

@rest_controller('/api/reports')
class ReportController:
    """报告控制器"""

    def __init__(self):
        self.scheduler = get_scheduler()

    @post('/generate')
    def generate_report_task(self, report_type: str, filters: dict = None):
        """创建报告生成任务"""
        # 创建自定义 ScheduledJob
        class ReportJob(ScheduledJob):
            def __init__(self, report_type: str, filters: dict):
                super().__init__(
                    name=f"生成{report_type}报告",
                    description=f"生成类型为 {report_type} 的报告",
                    max_retries=3,
                    timeout=300  # 5分钟超时
                )
                self.report_type = report_type
                self.filters = filters or {}
            
            def run(self, *args, **kwargs):
                """生成报告任务"""
                print(f"开始生成 {self.report_type} 报告")
                time.sleep(10)  # 模拟报告生成
                return {
                    "type": self.report_type,
                    "filters": self.filters,
                    "status": "completed"
                }

        # 创建任务实例
        job = ReportJob(report_type, filters)

        # 添加到调度器（用于状态跟踪，非定时任务）
        job_id = self.scheduler.add_job_object(job)

        # 在后台执行任务
        import threading
        thread = threading.Thread(target=job.execute)
        thread.daemon = True
        thread.start()

        return {
            "message": "报告生成任务已创建",
            "job_id": job_id,
            "status": "pending"
        }

    @get('/status/{job_id}')
    def get_report_status(self, job_id: str):
        """查询任务状态"""
        job = self.scheduler.get_scheduled_job(job_id)

        if not job:
            return {
                "error": "任务不存在"
            }

        job_info = job.get_info()
        return {
            "job_id": job_id,
            "status": job_info["status"],
            "progress": self._calculate_progress(job_info),
            "created_at": job_info["created_at"],
            "started_at": job_info["started_at"],
            "completed_at": job_info["completed_at"]
        }

    def _calculate_progress(self, job_info: dict) -> float:
        """计算任务进度（示例）"""
        if job_info["status"] == "completed":
            return 100.0
        elif job_info["status"] == "running":
            # 可以根据实际业务逻辑计算进度
            return 50.0
        else:
            return 0.0
```

### 使用自定义 ScheduledJob 类

```python
from myboot.jobs.scheduled_job import ScheduledJob
from myboot.core.decorators import post, get, rest_controller
from myboot.core.scheduler import get_scheduler

class DataImportJob(ScheduledJob):
    """数据导入任务"""

    def __init__(self, file_path: str, **kwargs):
        super().__init__(
            name="数据导入",
            description=f"从文件 {file_path} 导入数据",
            **kwargs
        )
        self.file_path = file_path

    def run(self, *args, **kwargs):
        """执行数据导入"""
        import time
        print(f"开始导入文件: {self.file_path}")

        # 模拟数据导入过程
        for i in range(10):
            time.sleep(1)
            print(f"导入进度: {(i+1)*10}%")

        return {
            "file_path": self.file_path,
            "records_imported": 1000,
            "status": "completed"
        }

@rest_controller('/api/import')
class ImportController:
    """数据导入控制器"""

    def __init__(self):
        self.scheduler = get_scheduler()

    @post('/start')
    def start_import(self, file_path: str):
        """启动数据导入任务"""
        job = DataImportJob(file_path)
        
        # 添加到调度器（用于状态跟踪，非定时任务）
        job_id = self.scheduler.add_job_object(job)

        # 在后台执行
        import threading
        thread = threading.Thread(target=job.execute)
        thread.daemon = True
        thread.start()

        return {
            "message": "数据导入任务已启动",
            "job_id": job_id,
            "file_path": file_path
        }

    @get('/jobs')
    def list_jobs(self):
        """列出所有任务"""
        # 获取所有 ScheduledJob 对象
        jobs = self.scheduler.get_all_scheduled_jobs()
        all_jobs = [job.get_info() for job in jobs]
        
        return {
            "jobs": all_jobs,
            "total": len(all_jobs)
        }
```

## 任务状态查询

### 使用调度器查询

```python
from myboot.core.decorators import get, rest_controller
from myboot.core.scheduler import get_scheduler

@rest_controller('/api/jobs')
class JobStatusController:
    """任务状态控制器"""

    def __init__(self):
        self.scheduler = get_scheduler()

    @get('/{job_id}')
    def get_job_status(self, job_id: str):
        """获取任务状态"""
        job = self.scheduler.get_scheduled_job(job_id)

        if not job:
            return {
                "error": "任务不存在"
            }

        return job.get_info()

    @get('/')
    def list_all_jobs(self):
        """列出所有任务"""
        # 获取所有 ScheduledJob 对象
        jobs = self.scheduler.get_all_scheduled_jobs()
        all_jobs = [job.get_info() for job in jobs]
        
        # 计算统计信息
        total = len(all_jobs)
        running = sum(1 for j in all_jobs if j["status"] == "running")
        completed = sum(1 for j in all_jobs if j["status"] == "completed")
        failed = sum(1 for j in all_jobs if j["status"] == "failed")
        
        statistics = {
            "total": total,
            "running": running,
            "completed": completed,
            "failed": failed,
            "success_rate": completed / total if total > 0 else 0
        }

        return {
            "jobs": all_jobs,
            "statistics": statistics
        }

    @get('/statistics')
    def get_statistics(self):
        """获取任务统计信息"""
        # 获取所有 ScheduledJob 对象
        jobs = self.scheduler.get_all_scheduled_jobs()
        all_jobs = [job.get_info() for job in jobs]
        
        total = len(all_jobs)
        running = sum(1 for j in all_jobs if j["status"] == "running")
        completed = sum(1 for j in all_jobs if j["status"] == "completed")
        failed = sum(1 for j in all_jobs if j["status"] == "failed")
        
        return {
            "total": total,
            "running": running,
            "completed": completed,
            "failed": failed,
            "success_rate": completed / total if total > 0 else 0
        }
```

以下是一个完整的示例，展示如何在 REST API 中实现文件上传和异步处理：

```python
from myboot.core.decorators import post, get, rest_controller
from myboot.jobs.scheduled_job import ScheduledJob
from myboot.core.scheduler import get_scheduler
from myboot.utils.async_utils import asyn_run
import time
import uuid

@rest_controller('/api/files')
class FileController:
    """文件处理控制器"""

    def __init__(self):
        self.scheduler = get_scheduler()
        self._file_storage = {}  # 简单的存储，实际应使用数据库

    @post('/upload')
    def upload_file(self, file_path: str, options: dict = None):
        """上传文件并创建处理任务"""
        # 生成任务 ID
        task_id = str(uuid.uuid4())

        # 创建自定义 ScheduledJob
        class FileProcessJob(ScheduledJob):
            def __init__(self, file_path: str, options: dict, task_id: str):
                super().__init__(
                    name=f"处理文件-{task_id}",
                    description=f"处理上传的文件: {file_path}",
                    max_retries=3,
                    timeout=600  # 10分钟超时
                )
                self.file_path = file_path
                self.options = options or {}
            
            def run(self, *args, **kwargs):
                """处理上传的文件"""
                print(f"开始处理文件: {self.file_path}")

                # 模拟文件处理过程
                for i in range(20):
                    time.sleep(0.5)
                    print(f"处理进度: {(i+1)*5}%")

                return {
                    "file_path": self.file_path,
                    "processed": True,
                    "records": 1000,
                    "options": self.options
                }

        # 创建处理任务
        job = FileProcessJob(file_path, options, task_id)

        # 添加到调度器（用于状态跟踪，非定时任务）
        job_id = self.scheduler.add_job_object(job)

        # 保存文件信息
        self._file_storage[task_id] = {
            "job_id": job_id,
            "file_path": file_path,
            "status": "pending",
            "created_at": time.time()
        }

        # 在后台执行任务
        import threading
        thread = threading.Thread(target=self._execute_job, args=(job, task_id))
        thread.daemon = True
        thread.start()

        return {
            "message": "文件上传成功，处理任务已创建",
            "task_id": task_id,
            "job_id": job_id,
            "status": "pending"
        }

    def _execute_job(self, job, task_id: str):
        """执行任务并更新状态"""
        try:
            result = job.execute()
            self._file_storage[task_id]["status"] = "completed"
            self._file_storage[task_id]["result"] = result
        except Exception as e:
            self._file_storage[task_id]["status"] = "failed"
            self._file_storage[task_id]["error"] = str(e)

    @get('/status/{task_id}')
    def get_file_status(self, task_id: str):
        """查询文件处理状态"""
        if task_id not in self._file_storage:
            return {
                "error": "任务不存在"
            }

        file_info = self._file_storage[task_id]
        job = self.scheduler.get_scheduled_job(file_info["job_id"])
        job_info = job.get_info() if job else None

        return {
            "task_id": task_id,
            "file_path": file_info["file_path"],
            "status": file_info.get("status", "unknown"),
            "job_info": job_info,
            "result": file_info.get("result"),
            "error": file_info.get("error")
        }

    @get('/tasks')
    def list_tasks(self):
        """列出所有文件处理任务"""
        return {
            "tasks": list(self._file_storage.values()),
            "total": len(self._file_storage)
        }
```

## 最佳实践

### 1. 选择合适的异步方式

- **简单任务，无需跟踪**：使用 `asyn_run`
- **需要跟踪状态**：使用 `ScheduledJob`（继承并实现 `run` 方法）
- **需要定时执行**：使用 `ScheduledJob` + `Scheduler.add_scheduled_job()`

### 2. 任务超时设置

任务超时功能支持跨平台（Windows、Linux、macOS），使用 `ThreadPoolExecutor` 实现：

```python
class MyTask(ScheduledJob):
    def __init__(self):
        super().__init__(
            name="我的任务",
            timeout=300  # 设置5分钟超时
        )
    
    def run(self, *args, **kwargs):
        # 任务逻辑
        pass
```

**注意**：
- 超时功能在 Windows、Linux 和 macOS 上均可正常工作
- 超时后会抛出 `TimeoutError` 异常
- 由于 Python GIL 的限制，超时后任务线程可能仍在后台运行，但不会再等待其结果

### 3. 错误处理和重试

```python
class MyTask(ScheduledJob):
    def __init__(self):
        super().__init__(
            name="我的任务",
            max_retries=3,
            retry_delay=5.0  # 失败后等待5秒再重试
        )
    
    def run(self, *args, **kwargs):
        # 任务逻辑
        pass
```

### 4. 资源清理

```python
from myboot.utils.async_utils import cleanup_async_executor

# 在应用关闭时清理
@app.add_shutdown_hook
def shutdown_hook():
    cleanup_async_executor()
```

### 5. 任务状态管理

建议使用数据库或 Redis 来持久化任务状态，而不是内存存储。

## 注意事项

1. **线程安全**：`Scheduler` 是线程安全的，可以在多个线程中使用
2. **任务执行**：使用 `threading.Thread` 在后台执行任务，避免阻塞主线程
3. **资源管理**：长时间运行的应用应定期清理已完成的任务
4. **错误处理**：确保任务函数有适当的错误处理，避免任务失败影响系统
5. **ScheduledJob 使用**：对于非定时任务，可以直接创建 `ScheduledJob` 实例并执行，无需添加到调度器

## 相关文档

- [异步工具使用指南](../myboot/utils/async_utils.py)
- [调度器文档](../myboot/core/scheduler.py)
- [ScheduledJob 基类文档](../myboot/jobs/scheduled_job.py)
