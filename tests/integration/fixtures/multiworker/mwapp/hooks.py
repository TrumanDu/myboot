"""worker 生命周期钩子：每个 worker 各写一个文件，供测试断言触发次数/进程"""

import os

from myboot.core.application import app as current_app
from myboot.core.decorators import on_worker_start, on_worker_stop


@on_worker_start
def record_worker_start():
    hook_dir = os.environ.get("MW_HOOK_DIR")
    if not hook_dir:
        return
    a = current_app()
    instance_client = a.get_client("instance_client")
    instance_id = instance_client.instance_id if instance_client else ""
    path = os.path.join(hook_dir, f"start_{os.getpid()}.txt")
    # 追加写：若同一 worker 误触发两次，文件会出现两行，测试可检测到
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{os.getpid()},{a.worker_id},{instance_id}\n")


@on_worker_stop
def record_worker_stop():
    # 注意：Windows 多 worker 模式下父进程 terminate() 硬终止 worker，
    # 此钩子可能不触发（测试在 Windows 上跳过 stop 断言）
    hook_dir = os.environ.get("MW_HOOK_DIR")
    if not hook_dir:
        return
    a = current_app()
    path = os.path.join(hook_dir, f"stop_{os.getpid()}.txt")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{os.getpid()},{a.worker_id}\n")
