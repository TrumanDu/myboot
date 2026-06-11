"""测试客户端：记录实例 id 与创建进程 pid，用于断言每个 worker 独立实例化"""

import os
import uuid

from myboot.core.decorators import client


@client()
class InstanceClient:
    """自动注册为 'instance_client'"""

    def __init__(self):
        self.instance_id = uuid.uuid4().hex
        self.created_pid = os.getpid()
