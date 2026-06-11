"""
任务调度器模块

提供定时任务调度功能，基于 APScheduler 实现
"""

import uuid
from datetime import datetime
from typing import Optional, Callable, Any, TYPE_CHECKING, Union

from loguru import logger
from dynaconf import Dynaconf
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore

from .config import get_config, get_settings
from ..exceptions import SchedulerError

if TYPE_CHECKING:
    from ..jobs.scheduled_job import ScheduledJob


class Scheduler:
    """应用任务调度器（基于 APScheduler）"""
    
    def __init__(self, config: Optional[Union[str, Dynaconf]] = None, enabled: Optional[bool] = None):
        """
        初始化调度器
        
        Args:
            config: 配置文件路径或配置对象（Dynaconf），如果为 None 则使用默认配置
            enabled: 是否启用调度器，如果为 None 则从配置文件读取
                     多 workers 模式下，默认只在 primary worker 启用
        """
        self._logger = logger.bind(name="scheduler")
        self._scheduled_jobs = {}  # 存储 ScheduledJob 对象
        
        # 获取配置对象
        if isinstance(config, Dynaconf):
            # 如果传入的是配置对象，直接使用
            self._config = config
        else:
            # 如果传入的是配置文件路径或 None，则获取配置对象
            self._config = get_settings(config)
        
        # 从配置对象读取调度器配置，enabled 参数可覆盖配置
        config_enabled = self._config.get('scheduler.enabled', True)
        self._enabled = enabled if enabled is not None else config_enabled
        timezone_str = self._config.get('scheduler.timezone', 'UTC')
        self._timezone = self._parse_timezone(timezone_str)
        max_workers = self._config.get('scheduler.max_workers', 10)
        
        # 配置 APScheduler
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': ThreadPoolExecutor(max_workers=max_workers)
        }
        job_defaults = {
            'coalesce': False,  # 不合并错过的任务
            'max_instances': 3,  # 最大并发实例数
            'misfire_grace_time': 30  # 错过执行的宽限时间（秒）
        }
        
        # 创建 APScheduler 实例
        self._scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=self._timezone
        )
        
        self._logger.debug(f"调度器初始化完成 - enabled: {self._enabled}, timezone: {timezone_str}, max_workers: {max_workers}")
    
    def _parse_timezone(self, timezone_str: str):
        """
        解析时区字符串
        
        APScheduler 会自动使用此时区处理所有任务，无需手动转换
        """
        try:
            import pytz
            return pytz.timezone(timezone_str)
        except ImportError:
            self._logger.warning("未安装 pytz，使用系统时区")
            return None
        except Exception as e:
            self._logger.warning(f"解析时区失败: {e}，使用系统时区")
            return None
    
    def _resolve_job_id(self, job_id: Optional[str], prefix: str, func: Callable) -> str:
        """解析并校验任务 ID

        默认 ID 为 ``{prefix}_{模块名}.{限定名}``（限定名含类名），
        不同类中的同名方法不会冲突（issue #14）。

        注册前显式查重——APScheduler 未启动时不校验 pending 队列中的
        ID 冲突，问题会延迟到 ``start()`` 落库时才爆发，这里提前拦截：
        - 显式传入的 ID 已存在：抛 ``SchedulerError``
        - 自动生成的 ID 已存在（同一函数注册多次）：追加短 uid 消歧并告警
        """
        existing_ids = set(self.list_jobs())

        if job_id is not None:
            if job_id in existing_ids:
                raise SchedulerError(
                    f"任务 ID 已存在: {job_id}，请使用唯一的 job_id",
                    scheduler="scheduler",
                )
            return job_id

        module = getattr(func, '__module__', 'unknown')
        qualname = getattr(func, '__qualname__', getattr(func, '__name__', str(func)))
        auto_id = f"{prefix}_{module}.{qualname}"
        if auto_id in existing_ids:
            suffix = uuid.uuid4().hex[:8]
            self._logger.warning(
                f"自动生成的任务 ID 冲突: {auto_id}（同一函数注册多次），"
                f"追加后缀消歧: {auto_id}_{suffix}"
            )
            auto_id = f"{auto_id}_{suffix}"
        return auto_id

    def add_cron_job(
        self,
        func: Callable,
        cron: str,
        job_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        添加 Cron 任务
        
        Args:
            func: 要执行的函数
            cron: Cron 表达式（标准5位格式：分 时 日 月 周，或使用 CronTrigger 支持的格式）
            job_id: 任务ID，如果为 None 则自动生成
            **kwargs: 其他任务参数
            
        Returns:
            str: 任务ID
        """
        job_id = self._resolve_job_id(job_id, "cron", func)
        
        try:
            # 解析 cron 表达式
            trigger = self._parse_cron(cron)
            
            # 添加到 APScheduler
            self._scheduler.add_job(
                func=func,
                trigger=trigger,
                id=job_id,
                **kwargs
            )
            
            self._logger.info(f"已添加 Cron 任务: {job_id} - {cron}")
            return job_id
            
        except Exception as e:
            self._logger.error(f"添加 Cron 任务失败: {e}", exc_info=True)
            raise
    
    def _parse_cron(self, cron_expr: str) -> CronTrigger:
        """
        解析 Cron 表达式
        
        Args:
            cron_expr: Cron 表达式字符串
            
        Returns:
            CronTrigger 对象（使用调度器配置的时区）
        """
        # 尝试使用 CronTrigger.from_crontab 解析标准格式
        try:
            return CronTrigger.from_crontab(cron_expr, timezone=self._timezone)
        except (ValueError, TypeError):
            # 如果不是标准格式，尝试手动解析
            parts = cron_expr.split()
            if len(parts) == 5:
                # 标准5位格式：分 时 日 月 周
                minute, hour, day, month, day_of_week = parts
                return CronTrigger(
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    timezone=self._timezone
                )
            elif len(parts) == 6:
                # 6位格式：秒 分 时 日 月 周（兼容旧格式）
                second, minute, hour, day, month, day_of_week = parts
                return CronTrigger(
                    second=second,
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    timezone=self._timezone
                )
            else:
                raise ValueError(f"无效的 Cron 表达式格式: {cron_expr}，应为5位或6位")
    
    def add_interval_job(
        self,
        func: Callable,
        interval: int,
        job_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        添加间隔任务
        
        Args:
            func: 要执行的函数
            interval: 间隔时间（秒）
            job_id: 任务ID，如果为 None 则自动生成
            **kwargs: 其他任务参数
            
        Returns:
            str: 任务ID
        """
        job_id = self._resolve_job_id(job_id, "interval", func)
        
        try:
            # 创建间隔触发器（使用调度器配置的时区）
            trigger = IntervalTrigger(seconds=interval, timezone=self._timezone)
            
            # 添加到 APScheduler
            self._scheduler.add_job(
                func=func,
                trigger=trigger,
                id=job_id,
                **kwargs
            )
            
            self._logger.info(f"已添加间隔任务: {job_id} - {interval}秒")
            return job_id
            
        except Exception as e:
            self._logger.error(f"添加间隔任务失败: {e}", exc_info=True)
            raise
    
    def add_date_job(
        self,
        func: Callable,
        run_date: str,
        job_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        添加一次性任务
        
        Args:
            func: 要执行的函数
            run_date: 运行日期时间字符串，格式: 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD HH:MM'
            job_id: 任务ID，如果为 None 则自动生成
            **kwargs: 其他任务参数
            
        Returns:
            str: 任务ID
        """
        job_id = self._resolve_job_id(job_id, "date", func)
        
        try:
            # 解析日期时间（返回 naive datetime）
            run_datetime = self._parse_run_date(run_date)
            
            # 创建日期触发器（使用调度器配置的时区）
            trigger = DateTrigger(run_date=run_datetime, timezone=self._timezone)
            
            # 添加到 APScheduler
            self._scheduler.add_job(
                func=func,
                trigger=trigger,
                id=job_id,
                **kwargs
            )
            
            self._logger.info(f"已添加一次性任务: {job_id} - {run_date}")
            return job_id
            
        except Exception as e:
            self._logger.error(f"添加一次性任务失败: {e}", exc_info=True)
            raise
    
    def _parse_run_date(self, run_date: str) -> datetime:
        """
        解析运行日期时间字符串
        
        Args:
            run_date: 日期时间字符串
            
        Returns:
            naive datetime 对象（APScheduler 会自动应用调度器的全局时区）
        """
        # 尝试解析不同的日期格式
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
        ]
        
        for fmt in formats:
            try:
                # 直接返回 naive datetime，APScheduler 会自动应用全局时区
                return datetime.strptime(run_date, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"无效的日期格式: {run_date}，支持的格式: 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD'")
    
    def remove_job(self, job_id: str) -> bool:
        """
        移除任务
        
        Args:
            job_id: 任务ID
            
        Returns:
            bool: 是否成功移除
        """
        try:
            if self._scheduler.get_job(job_id):
                self._scheduler.remove_job(job_id)
                self._logger.info(f"已移除任务: {job_id}")
                return True
            return False
        except Exception as e:
            self._logger.error(f"移除任务失败: {e}", exc_info=True)
            return False
    
    def get_job(self, job_id: str) -> Optional[Any]:
        """
        获取任务
        
        Args:
            job_id: 任务ID
            
        Returns:
            APScheduler Job 对象，如果不存在则返回 None
        """
        return self._scheduler.get_job(job_id)
    
    def list_jobs(self) -> list:
        """
        列出所有任务ID
        
        Returns:
            list: 任务ID列表
        """
        return [job.id for job in self._scheduler.get_jobs()]
    
    def start(self) -> None:
        """启动调度器"""
        # 检查是否启用
        if not self._enabled:
            self._logger.info("调度器已禁用，不启动")
            return
        
        if not self._scheduler.running:
            try:
                self._scheduler.start()
                self._logger.info(f"任务调度器已启动 (时区: {self._timezone}, 任务数: {len(self._scheduler.get_jobs())})")
            except Exception as e:
                self._logger.error(f"启动调度器失败: {e}", exc_info=True)
                raise
    
    def stop(self) -> None:
        """停止调度器"""
        if self._scheduler.running:
            try:
                self._scheduler.shutdown(wait=True)
                self._logger.info("任务调度器已停止")
            except Exception as e:
                self._logger.error(f"停止调度器失败: {e}", exc_info=True)
    
    def is_running(self) -> bool:
        """检查调度器是否运行中"""
        return self._scheduler.running
    
    def has_jobs(self) -> bool:
        """检查是否有任务"""
        return len(self._scheduler.get_jobs()) > 0
    
    def get_job_info(self, job_id: str) -> Optional[dict]:
        """
        获取任务信息
        
        Args:
            job_id: 任务ID
            
        Returns:
            dict: 任务信息字典
        """
        job = self._scheduler.get_job(job_id)
        if not job:
            return None
        
        # 未启动调度器时 pending Job 对象没有 next_run_time 属性，用 getattr 防御
        next_run_time = getattr(job, 'next_run_time', None)
        info = {
            'job_id': job_id,
            'func_name': job.func.__name__ if hasattr(job.func, '__name__') else str(job.func),
            'next_run_time': next_run_time.isoformat() if next_run_time else None,
        }
        
        # 根据触发器类型添加特定信息
        trigger = job.trigger
        if isinstance(trigger, CronTrigger):
            info['type'] = 'cron'
            info['cron'] = str(trigger)
        elif isinstance(trigger, IntervalTrigger):
            info['type'] = 'interval'
            info['interval'] = trigger.interval.total_seconds()
        elif isinstance(trigger, DateTrigger):
            info['type'] = 'date'
            info['run_date'] = trigger.run_date.isoformat() if trigger.run_date else None
        
        return info
    
    def list_all_jobs(self) -> list:
        """
        列出所有任务的详细信息
        
        Returns:
            list: 任务信息列表
        """
        return [self.get_job_info(job.id) for job in self._scheduler.get_jobs()]
       
    def is_enabled(self) -> bool:
        """检查调度器是否启用"""
        return self._enabled
    
    def get_config(self) -> dict:
        """获取调度器配置"""
        return {
            'enabled': self._enabled,
            'timezone': str(self._timezone) if self._timezone else 'system',
            'running': self._scheduler.running,
            'job_count': len(self._scheduler.get_jobs()),
            'scheduled_job_count': len(self._scheduled_jobs)
        }
    
    def add_scheduled_job(
        self,
        job: 'ScheduledJob',
        job_id: Optional[str] = None
    ) -> str:
        """
        添加 ScheduledJob 对象到调度器
        
        Args:
            job: ScheduledJob 对象
            job_id: 任务ID，如果为 None 则自动生成
            
        Returns:
            str: 任务ID
        """
        from ..jobs.scheduled_job import ScheduledJob as ScheduledJobClass
        
        if not isinstance(job, ScheduledJobClass):
            raise TypeError(f"job 必须是 ScheduledJob 的实例，当前类型: {type(job)}")
        
        if job.trigger is None:
            raise ValueError("ScheduledJob 必须设置 trigger 属性")
        
        # 生成任务ID
        if job_id is None:
            job_id = f"scheduled_{job.name}_{id(job)}"
        
        # 保存 ScheduledJob 对象
        self._scheduled_jobs[job_id] = job
        job.job_id = job_id
        
        # 转换触发器格式并添加到调度器
        trigger = job.trigger
        
        if isinstance(trigger, str):
            # 字符串视为 cron 表达式
            self.add_cron_job(
                func=job.execute,
                cron=trigger,
                job_id=job_id,
                name=job.name
            )
        elif isinstance(trigger, dict):
            trigger_type = trigger.get('type')
            if trigger_type == 'cron':
                self.add_cron_job(
                    func=job.execute,
                    cron=trigger['cron'],
                    job_id=job_id,
                    name=job.name
                )
            elif trigger_type == 'interval':
                interval = trigger.get('seconds', 0) or 0
                interval += (trigger.get('minutes', 0) or 0) * 60
                interval += (trigger.get('hours', 0) or 0) * 3600
                interval += (trigger.get('days', 0) or 0) * 86400
                if interval <= 0:
                    raise ValueError("间隔时间必须大于 0")
                self.add_interval_job(
                    func=job.execute,
                    interval=interval,
                    job_id=job_id,
                    name=job.name
                )
            elif trigger_type == 'date':
                self.add_date_job(
                    func=job.execute,
                    run_date=trigger['run_date'],
                    job_id=job_id,
                    name=job.name
                )
            else:
                raise ValueError(f"不支持的触发器类型: {trigger_type}")
        else:
            raise ValueError(f"不支持的触发器类型: {type(trigger)}")
        
        self._logger.info(f"已添加 ScheduledJob: {job.name} (ID: {job_id})")
        return job_id
    
    def get_scheduled_job(self, job_id: str) -> Optional['ScheduledJob']:
        """
        获取 ScheduledJob 对象
        
        Args:
            job_id: 任务ID
            
        Returns:
            ScheduledJob 对象，如果不存在则返回 None
        """
        return self._scheduled_jobs.get(job_id)
    
    def get_all_scheduled_jobs(self) -> list:
        """
        获取所有 ScheduledJob 对象
        
        Returns:
            ScheduledJob 对象列表
        """
        return list(self._scheduled_jobs.values())
    
    def add_job_object(
        self,
        job: 'ScheduledJob',
        job_id: Optional[str] = None
    ) -> str:
        """
        添加 ScheduledJob 对象到调度器（不设置触发器，用于非定时任务）
        
        此方法用于添加不需要定时执行的任务，仅用于状态跟踪和管理。
        如果需要定时执行，请使用 add_scheduled_job() 并设置 trigger。
        
        Args:
            job: ScheduledJob 对象
            job_id: 任务ID，如果为 None 则自动生成
            
        Returns:
            str: 任务ID
        """
        from ..jobs.scheduled_job import ScheduledJob as ScheduledJobClass
        
        if not isinstance(job, ScheduledJobClass):
            raise TypeError(f"job 必须是 ScheduledJob 的实例，当前类型: {type(job)}")
        
        # 生成任务ID
        if job_id is None:
            job_id = f"job_{job.name}_{id(job)}"
        
        # 保存 ScheduledJob 对象
        self._scheduled_jobs[job_id] = job
        job.job_id = job_id
        
        self._logger.info(f"已添加任务对象: {job.name} (ID: {job_id})")
        return job_id
    
    def remove_scheduled_job(self, job_id: str) -> bool:
        """
        移除 ScheduledJob
        
        Args:
            job_id: 任务ID
            
        Returns:
            是否成功移除
        """
        if job_id in self._scheduled_jobs:
            # 同时从调度器中移除（如果存在）
            self.remove_job(job_id)
            del self._scheduled_jobs[job_id]
            self._logger.info(f"已移除 ScheduledJob: {job_id}")
            return True
        return False


# 全局调度器实例
_scheduler: Optional[Scheduler] = None


def get_scheduler() -> Scheduler:
    """获取调度器实例"""
    global _scheduler
    
    if _scheduler is None:
        _scheduler = Scheduler()
    
    return _scheduler
