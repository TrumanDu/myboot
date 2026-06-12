# Prometheus Metrics 指南

myboot 0.2.0 起内置 Prometheus 指标支持：零代码获得 `/metrics` 端点与 HTTP
请求指标，多 worker 模式下自动聚合全部进程的指标，业务代码可用轻量 API 记录
自定义指标。

## 1. 开启

```bash
pip install myboot[metrics]      # prometheus-client 为可选依赖
```

```yaml
# conf/config.yaml
metrics:
  enabled: true                  # 默认 false，不开启则一切如旧
```

不需要在 main.py 写任何 metrics 代码——框架自动完成 `/metrics` 挂载、HTTP
中间件注册、多进程聚合配置与进程退出清理。

> `metrics.enabled: true` 但未安装 prometheus-client 时只打 warning，
> 应用照常启动（指标功能禁用）。

## 2. 配置项

| 配置 | 默认 | 说明 |
|---|---|---|
| `metrics.enabled` | `false` | 总开关 |
| `metrics.path` | `/metrics` | 暴露端点路径 |
| `metrics.http_metrics` | `true` | 是否注册内置 HTTP 请求指标中间件 |
| `metrics.multiproc_dir` | 自动（系统临时目录） | 多进程指标共享目录，一般无需配置 |

## 3. 内置 HTTP 指标

开启后自动采集（无需写代码）：

```
# 请求计数（按方法、路由模板、状态码）
myboot_http_requests_total{method="POST", path="/api/items/{id}", status="200"}

# 请求延迟直方图
myboot_http_request_duration_seconds_bucket{method="POST", path="/api/items/{id}", le="0.1"}
```

- `path` 标签使用**路由模板**（`/api/items/{id}`），而非真实 URL
  （`/api/items/123`），避免标签基数爆炸；
- 未匹配任何路由的请求（如 404）统一归入 `path="unmatched"`；
- `/metrics` 端点自身不计入统计。

## 4. 自定义指标 API

```python
from myboot.metrics import get_counter, get_histogram, observe_stage, time_stage

# 计数器（同名重复调用返回同一对象，无需担心重复注册）
cache_miss = get_counter("cache_miss_total", "缓存未命中数", ["source"])
cache_miss.labels(source="redis").inc()

# 直方图
latency = get_histogram("external_api_seconds", "外部接口耗时", ["api"])
latency.labels(api="item-feature").observe(0.123)

# 阶段计时：内置 myboot_stage_duration_seconds{stage} 直方图的便捷封装
with time_stage("sasrec"):
    candidates = model.infer(request)

with time_stage("rerank"):
    result = reranker.score(candidates)

# 或手动上报耗时（秒）
observe_stage("persist", 0.05)
```

重要特性：**未开启 `metrics.enabled` 或未安装 prometheus-client 时，以上调用
全部是静默 no-op**——业务代码不需要任何条件判断，开发环境关掉指标也不会报错。

## 5. 多 Worker 聚合原理

多进程下 Prometheus 官方方案要求：

1. `PROMETHEUS_MULTIPROC_DIR` 必须在 `prometheus_client` 首次 import **之前**设置；
2. 各 worker 把指标值写入该目录的 mmap 文件，`/metrics` 端点用
   `MultiProcessCollector` 聚合读取；
3. worker 退出时调用 `mark_process_dead` 清理 gauge 类残留。

这三步时序极易写错，myboot 已全部内置：环境变量在 `Application.__init__`
阶段设置（fork 子进程继承、spawn 子进程重新执行 `__init__`，两种模式都来得及），
陈旧文件由父进程启动时清理，`mark_process_dead` 挂在 lifespan 关闭末尾。

唯一注意事项：**不要在 `create_app()` 之前 import prometheus_client**
（框架检测到会打 warning）。

### Windows 限制

Windows 多 worker（spawn + terminate）下 multiproc 模式不可靠，框架自动降级为
各进程独立指标：`/metrics` 只反映处理该次请求的 worker。Linux/macOS 生产部署
不受影响；Windows 单 worker 完全正常。

## 6. Prometheus 抓取配置

```yaml
scrape_configs:
  - job_name: my-app
    scrape_interval: 15s
    static_configs:
      - targets: ["my-app:8000"]   # 抓取到的即全 worker 聚合值
```

## 7. 常用查询示例（PromQL）

```promql
# QPS（按路由）
sum(rate(myboot_http_requests_total[1m])) by (path)

# P99 延迟
histogram_quantile(0.99, sum(rate(myboot_http_request_duration_seconds_bucket[5m])) by (path, le))

# 错误率
sum(rate(myboot_http_requests_total{status=~"5.."}[5m]))
  / sum(rate(myboot_http_requests_total[5m]))

# 自定义阶段耗时 P95
histogram_quantile(0.95, sum(rate(myboot_stage_duration_seconds_bucket[5m])) by (stage, le))
```
