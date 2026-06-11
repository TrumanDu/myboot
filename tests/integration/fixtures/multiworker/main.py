"""多 worker 集成测试 fixture 应用入口

通过 subprocess 启动：python main.py
环境变量：
    MW_PORT      监听端口（必需）
    MW_WORKERS   worker 数量（默认 2）
    MW_HOOK_DIR  worker 钩子记录目录
    MW_JOB_DIR   调度任务记录目录

注意：本模块在 Windows spawn 模式下会被 multiprocessing 以 __mp_main__
重新导入，模块级代码（路径注入、配置覆盖）会在每个 worker 中重新执行，
这是预期行为；app.run() 受 __main__ 守卫保护只在父进程执行。
"""

import os
import sys
from pathlib import Path

FIXTURE_DIR = Path(__file__).parent.absolute()
REPO_ROOT = FIXTURE_DIR.parent.parent.parent.parent

# 确保能导入 mwapp 包与源码版 myboot
sys.path.insert(0, str(FIXTURE_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from myboot.core import auto_configuration  # noqa: E402
from myboot.core.application import Application  # noqa: E402

app = Application(
    name="multiworker-fixture",
    auto_configuration=True,
    auto_discover_package="mwapp",
)

# 自动配置管理器默认把 myboot 源码仓库根当作 app_root，
# 这里指向 fixture 目录，使 "mwapp" 包能被发现；
# 禁用缓存避免在仓库中残留 .myboot_cache_*.json
auto_configuration._auto_configuration_manager.app_root = str(FIXTURE_DIR)
auto_configuration._auto_configuration_manager.use_cache = False

PORT = int(os.environ.get("MW_PORT", "8765"))
WORKERS = int(os.environ.get("MW_WORKERS", "2"))

# 显式覆盖配置——仓库 conf/config.yaml（server.workers=1 等）的优先级
# 高于 run() 入参，必须在配置层覆盖
app.config.set("server.host", "127.0.0.1")
app.config.set("server.port", PORT)
app.config.set("server.workers", WORKERS)

if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=PORT,
        workers=WORKERS,
        app_path="main:app.get_fastapi_app()",
    )
