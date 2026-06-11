"""GET /api/whoami：返回当前 worker 与注入的 client 实例信息"""

import os

from myboot.core.application import app as current_app
from myboot.core.decorators import rest_controller, get

from mwapp.clients import InstanceClient


@rest_controller('/api')
class WhoamiController:

    def __init__(self, instance_client: InstanceClient):
        self.instance_client = instance_client

    @get('/whoami')
    def whoami(self):
        a = current_app()
        return {
            "worker_id": a.worker_id,
            "worker_count": a.worker_count,
            "is_primary": a.is_primary_worker,
            "pid": os.getpid(),
            "client_instance_id": self.instance_client.instance_id,
            "client_created_pid": self.instance_client.created_pid,
        }
