"""定时任务组件：每秒记录执行进程 pid

- record_pid: 默认任务，用于断言只在 primary worker 运行
- record_pid_all_workers: all_workers=True 任务，用于断言在每个 worker 都运行
"""

import os

from myboot.core.decorators import component, interval


def _write_tick(prefix: str) -> None:
    job_dir = os.environ.get("MW_JOB_DIR")
    if not job_dir:
        return
    path = os.path.join(job_dir, f"{prefix}_{os.getpid()}.txt")
    with open(path, "a", encoding="utf-8") as f:
        f.write("tick\n")


@component()
class MwJobs:

    @interval(seconds=1)
    def record_pid(self):
        _write_tick("job")

    @interval(seconds=1, all_workers=True)
    def record_pid_all_workers(self):
        _write_tick("awjob")
