"""
调度器特征测试（characterization tests）

固化 myboot.core.scheduler.Scheduler 的「当前实际行为」，作为后续重构的兼容性闸门。
这些测试只断言现状，不代表理想行为；行为有意变更时应同步更新本文件。

注意：
- 全程不调用 scheduler.start()，只测注册层（BackgroundScheduler 未启动时
  任务进入 pending 队列，get_job/get_jobs/remove_job 均可操作 pending 任务）。
- 通过直接构造 Dynaconf 对象传入 Scheduler，避免污染全局配置单例。

环境基线：APScheduler 3.11.x + pytz。
"""

import pytest
from dynaconf import Dynaconf
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from myboot.core.scheduler import Scheduler


def make_scheduler(scheduler_config: dict = None, enabled=None) -> Scheduler:
    """构造一个使用独立 Dynaconf 配置的 Scheduler（不依赖全局配置单例）"""
    if scheduler_config is None:
        config = Dynaconf()
    else:
        config = Dynaconf(scheduler=scheduler_config)
    return Scheduler(config=config, enabled=enabled)


@pytest.fixture
def scheduler():
    return make_scheduler()


def sample_task():
    """用于注册的示例任务函数"""
    pass


# ---------------------------------------------------------------------------
# 1. 构造与配置读取
# ---------------------------------------------------------------------------


class TestSchedulerConstruction:
    def test_default_enabled_is_true(self):
        s = make_scheduler()
        assert s.is_enabled() is True

    def test_enabled_read_from_config(self):
        s = make_scheduler({"enabled": False})
        assert s.is_enabled() is False

    def test_enabled_param_overrides_config(self):
        # application.py 即以 Scheduler(config=..., enabled=...) 方式覆盖
        s = make_scheduler({"enabled": False}, enabled=True)
        assert s.is_enabled() is True

    def test_default_timezone_is_utc(self):
        s = make_scheduler()
        assert str(s._timezone) == "UTC"

    def test_not_running_before_start(self, scheduler):
        assert scheduler.is_running() is False

    def test_get_config_shape(self):
        s = make_scheduler({"timezone": "Asia/Shanghai", "enabled": False})
        cfg = s.get_config()
        assert cfg == {
            "enabled": False,
            "timezone": "Asia/Shanghai",
            "running": False,
            "job_count": 0,
            "scheduled_job_count": 0,
        }


# ---------------------------------------------------------------------------
# 2. 时区解析 _parse_timezone
# ---------------------------------------------------------------------------


class TestParseTimezone:
    def test_valid_timezone_string(self, scheduler):
        tz = scheduler._parse_timezone("Asia/Shanghai")
        assert str(tz) == "Asia/Shanghai"

    def test_invalid_timezone_falls_back_to_none(self, scheduler):
        # 当前行为：解析失败仅记 warning，返回 None（即使用系统时区）
        assert scheduler._parse_timezone("Not/AZone") is None

    def test_invalid_timezone_in_config_reports_system(self):
        s = make_scheduler({"timezone": "Not/AZone"})
        assert s._timezone is None
        assert s.get_config()["timezone"] == "system"

    def test_valid_timezone_in_config_applied(self):
        s = make_scheduler({"timezone": "Asia/Shanghai"})
        assert str(s._timezone) == "Asia/Shanghai"


# ---------------------------------------------------------------------------
# 3. Cron 表达式解析 _parse_cron
# ---------------------------------------------------------------------------


class TestParseCron:
    def test_standard_5_field(self, scheduler):
        trigger = scheduler._parse_cron("0 2 * * *")
        assert isinstance(trigger, CronTrigger)
        text = str(trigger)
        assert "hour='2'" in text
        assert "minute='0'" in text
        # 5 位格式不含秒字段
        assert "second" not in text

    def test_5_field_with_step(self, scheduler):
        trigger = scheduler._parse_cron("*/15 * * * *")
        assert isinstance(trigger, CronTrigger)
        assert "minute='*/15'" in str(trigger)

    def test_6_field_with_seconds(self, scheduler):
        # 6 位格式：秒 分 时 日 月 周（兼容旧格式，走手动解析分支）
        trigger = scheduler._parse_cron("30 0 2 * * *")
        assert isinstance(trigger, CronTrigger)
        text = str(trigger)
        assert "second='30'" in text
        assert "minute='0'" in text
        assert "hour='2'" in text

    def test_trigger_uses_scheduler_timezone(self):
        s = make_scheduler({"timezone": "Asia/Shanghai"})
        trigger = s._parse_cron("0 2 * * *")
        assert str(trigger.timezone) == "Asia/Shanghai"

    def test_too_few_fields_raises_value_error(self, scheduler):
        with pytest.raises(ValueError):
            scheduler._parse_cron("* * *")

    def test_too_many_fields_raises_value_error(self, scheduler):
        with pytest.raises(ValueError):
            scheduler._parse_cron("* * * * * * *")

    def test_out_of_range_field_value_raises_value_error(self, scheduler):
        # from_crontab 与手动构造均拒绝越界值
        with pytest.raises(ValueError):
            scheduler._parse_cron("99 * * * *")


# ---------------------------------------------------------------------------
# 4. add_cron_job / add_interval_job / add_date_job
# ---------------------------------------------------------------------------


class TestAddCronJob:
    def test_default_job_id_format(self, scheduler):
        job_id = scheduler.add_cron_job(sample_task, "0 2 * * *")
        # 当前默认 job_id 格式：cron_{func.__name__}
        assert job_id == "cron_sample_task"

    def test_explicit_job_id_used_verbatim(self, scheduler):
        job_id = scheduler.add_cron_job(sample_task, "0 2 * * *", job_id="my_job")
        assert job_id == "my_job"
        assert scheduler.get_job("my_job") is not None
        # 默认 ID 没有被注册
        assert scheduler.get_job("cron_sample_task") is None

    def test_registered_job_retrievable(self, scheduler):
        job_id = scheduler.add_cron_job(sample_task, "0 2 * * *")
        job = scheduler.get_job(job_id)
        assert job is not None
        assert job.id == job_id
        assert isinstance(job.trigger, CronTrigger)

    def test_invalid_cron_raises_value_error(self, scheduler):
        with pytest.raises(ValueError):
            scheduler.add_cron_job(sample_task, "not a cron")
        # 失败的注册不会留下任务
        assert scheduler.list_jobs() == []


class TestAddIntervalJob:
    def test_default_job_id_format(self, scheduler):
        job_id = scheduler.add_interval_job(sample_task, 60)
        # 当前默认 job_id 格式：interval_{func.__name__}
        assert job_id == "interval_sample_task"

    def test_explicit_job_id_used_verbatim(self, scheduler):
        job_id = scheduler.add_interval_job(sample_task, 60, job_id="tick")
        assert job_id == "tick"

    def test_interval_seconds_applied(self, scheduler):
        job_id = scheduler.add_interval_job(sample_task, 60)
        job = scheduler.get_job(job_id)
        assert isinstance(job.trigger, IntervalTrigger)
        assert job.trigger.interval.total_seconds() == 60.0


class TestAddDateJob:
    def test_default_job_id_format(self, scheduler):
        job_id = scheduler.add_date_job(sample_task, "2099-12-31 23:59:59")
        # 当前默认 job_id 格式：date_{func.__name__}
        assert job_id == "date_sample_task"

    def test_explicit_job_id_used_verbatim(self, scheduler):
        job_id = scheduler.add_date_job(
            sample_task, "2099-12-31 23:59:59", job_id="once"
        )
        assert job_id == "once"

    @pytest.mark.parametrize(
        "run_date",
        ["2099-12-31 23:59:59", "2099-12-31 23:59", "2099-12-31"],
    )
    def test_supported_date_formats(self, scheduler, run_date):
        job_id = scheduler.add_date_job(sample_task, run_date)
        job = scheduler.get_job(job_id)
        assert isinstance(job.trigger, DateTrigger)

    def test_invalid_date_raises_value_error(self, scheduler):
        with pytest.raises(ValueError):
            scheduler.add_date_job(sample_task, "2099-13-01")
        with pytest.raises(ValueError):
            scheduler.add_date_job(sample_task, "31/12/2099")


# ---------------------------------------------------------------------------
# 5. 同名函数重复注册（默认 ID 冲突）的当前行为
# ---------------------------------------------------------------------------


class TestDuplicateRegistration:
    def test_duplicate_default_id_does_not_raise_before_start(self, scheduler):
        """当前行为（特征）：调度器未启动时，APScheduler 把任务放入 pending
        队列且不做 ID 冲突检查——同名函数注册两次不抛异常，两个任务都进入
        pending，list_jobs() 出现重复 ID。
        （ConflictingIdError 只会在 scheduler.start() 真正落库时才抛出。）
        """
        id1 = scheduler.add_cron_job(sample_task, "0 2 * * *")
        id2 = scheduler.add_cron_job(sample_task, "0 3 * * *")
        assert id1 == id2 == "cron_sample_task"
        assert scheduler.list_jobs() == ["cron_sample_task", "cron_sample_task"]

    def test_remove_only_removes_first_pending_duplicate(self, scheduler):
        """当前行为（特征）：remove_job 只移除 pending 队列中第一个匹配项"""
        scheduler.add_cron_job(sample_task, "0 2 * * *")
        scheduler.add_cron_job(sample_task, "0 3 * * *")
        assert scheduler.remove_job("cron_sample_task") is True
        assert scheduler.list_jobs() == ["cron_sample_task"]


# ---------------------------------------------------------------------------
# 6. remove_job / get_job / list_jobs / has_jobs / get_job_info
# ---------------------------------------------------------------------------


class TestJobManagement:
    def test_remove_existing_job_returns_true(self, scheduler):
        job_id = scheduler.add_interval_job(sample_task, 60)
        assert scheduler.remove_job(job_id) is True
        assert scheduler.get_job(job_id) is None
        assert scheduler.list_jobs() == []

    def test_remove_missing_job_returns_false(self, scheduler):
        assert scheduler.remove_job("does_not_exist") is False

    def test_get_missing_job_returns_none(self, scheduler):
        assert scheduler.get_job("does_not_exist") is None

    def test_list_jobs_and_has_jobs(self, scheduler):
        assert scheduler.has_jobs() is False
        scheduler.add_cron_job(sample_task, "0 2 * * *")
        scheduler.add_interval_job(sample_task, 60)
        assert scheduler.has_jobs() is True
        assert sorted(scheduler.list_jobs()) == [
            "cron_sample_task",
            "interval_sample_task",
        ]

    def test_get_job_info_on_pending_job_raises_attribute_error(self, scheduler):
        """当前行为（特征/可疑）：调度器未启动时任务处于 pending 状态，
        Job 对象尚无 next_run_time 属性，get_job_info 访问它会抛
        AttributeError，而不是返回信息字典。
        """
        job_id = scheduler.add_cron_job(sample_task, "0 2 * * *")
        with pytest.raises(AttributeError):
            scheduler.get_job_info(job_id)

    def test_get_job_info_missing_job_returns_none(self, scheduler):
        assert scheduler.get_job_info("does_not_exist") is None
