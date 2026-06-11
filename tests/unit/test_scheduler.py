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
from myboot.exceptions import SchedulerError


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


# issue #14：默认 job_id 为 {prefix}_{模块名}.{限定名}
SAMPLE_TASK_QUALNAME = f"{sample_task.__module__}.{sample_task.__qualname__}"


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
        # issue #14：0.2.0 起默认 job_id 为 cron_{模块名}.{限定名}（含类名）
        assert job_id == f"cron_{SAMPLE_TASK_QUALNAME}"

    def test_explicit_job_id_used_verbatim(self, scheduler):
        job_id = scheduler.add_cron_job(sample_task, "0 2 * * *", job_id="my_job")
        assert job_id == "my_job"
        assert scheduler.get_job("my_job") is not None
        # 默认 ID 没有被注册
        assert scheduler.get_job(f"cron_{SAMPLE_TASK_QUALNAME}") is None

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
        # issue #14：0.2.0 起默认 job_id 为 interval_{模块名}.{限定名}
        assert job_id == f"interval_{SAMPLE_TASK_QUALNAME}"

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
        # issue #14：0.2.0 起默认 job_id 为 date_{模块名}.{限定名}
        assert job_id == f"date_{SAMPLE_TASK_QUALNAME}"

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
    def test_duplicate_auto_id_disambiguated_with_uid_suffix(self, scheduler):
        """0.2.0（issue #14 配套）：注册时显式查重。同一函数注册两次时，
        第二个自动生成的 ID 追加 8 位 uid 后缀消歧（并打 warning），
        不再出现重复 ID 进入 pending 队列、延迟到 start() 才爆发的问题。
        """
        id1 = scheduler.add_cron_job(sample_task, "0 2 * * *")
        id2 = scheduler.add_cron_job(sample_task, "0 3 * * *")
        assert id1 == f"cron_{SAMPLE_TASK_QUALNAME}"
        assert id2.startswith(f"cron_{SAMPLE_TASK_QUALNAME}_")
        assert len(id2) == len(f"cron_{SAMPLE_TASK_QUALNAME}_") + 8
        assert sorted(scheduler.list_jobs()) == sorted([id1, id2])

    def test_duplicate_explicit_id_raises_scheduler_error(self, scheduler):
        """0.2.0（issue #14 配套）：显式传入的 job_id 已存在时抛 SchedulerError"""
        scheduler.add_cron_job(sample_task, "0 2 * * *", job_id="my_job")
        with pytest.raises(SchedulerError, match="my_job"):
            scheduler.add_cron_job(sample_task, "0 3 * * *", job_id="my_job")
        # 失败的注册不留残留
        assert scheduler.list_jobs() == ["my_job"]

    def test_same_method_name_in_different_classes_no_conflict(self, scheduler):
        """issue #14 核心场景：两个类中的同名方法注册互不冲突"""

        class AlphaJobs:
            def job(self):
                pass

        class BetaJobs:
            def job(self):
                pass

        id1 = scheduler.add_cron_job(AlphaJobs().job, "0 2 * * *")
        id2 = scheduler.add_cron_job(BetaJobs().job, "0 3 * * *")
        assert id1 != id2
        assert "AlphaJobs.job" in id1
        assert "BetaJobs.job" in id2

    def test_remove_job_after_disambiguation(self, scheduler):
        """消歧后两个任务可分别独立移除"""
        id1 = scheduler.add_cron_job(sample_task, "0 2 * * *")
        id2 = scheduler.add_cron_job(sample_task, "0 3 * * *")
        assert scheduler.remove_job(id1) is True
        assert scheduler.list_jobs() == [id2]


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
        assert sorted(scheduler.list_jobs()) == sorted([
            f"cron_{SAMPLE_TASK_QUALNAME}",
            f"interval_{SAMPLE_TASK_QUALNAME}",
        ])

    def test_get_job_info_on_pending_job_returns_dict(self, scheduler):
        """0.2.0 修复：调度器未启动时 pending Job 没有 next_run_time 属性，
        get_job_info 现在返回信息字典（next_run_time 为 None）而不是抛
        AttributeError。
        """
        job_id = scheduler.add_cron_job(sample_task, "0 2 * * *")
        info = scheduler.get_job_info(job_id)
        assert info is not None
        assert info["job_id"] == job_id
        assert info["func_name"] == "sample_task"
        assert info["next_run_time"] is None
        assert info["type"] == "cron"

    def test_list_all_jobs_on_pending_jobs(self, scheduler):
        """list_all_jobs 同样不再因 pending 任务崩溃"""
        scheduler.add_cron_job(sample_task, "0 2 * * *")
        scheduler.add_interval_job(sample_task, 60)
        infos = scheduler.list_all_jobs()
        assert len(infos) == 2
        assert all(i is not None for i in infos)

    def test_get_job_info_missing_job_returns_none(self, scheduler):
        assert scheduler.get_job_info("does_not_exist") is None
