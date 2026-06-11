"""定时任务组件：每秒记录执行进程 pid，用于断言任务只在 primary worker 运行"""

import os

from myboot.core.decorators import component, interval


@component()
class MwJobs:

    @interval(seconds=1)
    def record_pid(self):
        job_dir = os.environ.get("MW_JOB_DIR")
        if not job_dir:
            return
        path = os.path.join(job_dir, f"job_{os.getpid()}.txt")
        with open(path, "a", encoding="utf-8") as f:
            f.write("tick\n")
