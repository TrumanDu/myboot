# 配置管理使用说明

MyBoot 使用 [Dynaconf](https://www.dynaconf.com/) 管理配置，支持 YAML 文件、多文件合并、环境变量覆盖与远程配置文件。实现见 `myboot/core/config.py`。

## 1. 快速开始

在项目根目录或 `conf/` 目录放置 `config.yaml`，应用启动时自动加载：

```yaml
# conf/config.yaml
app:
  name: "MyBoot App"
  version: "0.1.0"

server:
  port: 8000
  reload: false

logging:
  level: "INFO"
```

代码中读取：

```python
from myboot.core.config import get_settings, get_config, get_config_bool

# 点号路径（推荐）
port = get_config("server.port", 8000)
debug = get_config_bool("app.debug", False)

# Dynaconf 对象
settings = get_settings()
name = settings.app.name          # 属性访问
port = settings.get("server.port") # 与 get_config 等价
```

通过 `create_app` 创建应用时，配置挂在 `app.config` 上，与全局 `get_settings()` 为同一实例（首次初始化后）：

```python
from myboot import create_app

app = create_app(name="我的应用", config_file="conf/config.yaml")
port = app.config.get("server.port", 8000)
```

## 2. 配置文件位置与合并

### 2.1 自动发现的文件（按加载顺序）

Dynaconf **按列表顺序加载**，**后加载的文件覆盖先加载的同名字段**：

| 顺序 | 路径                                                                       | 说明                     |
| ---- | -------------------------------------------------------------------------- | ------------------------ |
| 1    | `{项目根}/config.yaml` 或 `config.yml`                                     | 基底配置                 |
| 2    | `{项目根}/conf/config.yaml` 或 `conf/config.yml`                           | 覆盖根目录同名键         |
| 3    | `create_app(config_file=...)` / `get_settings(config_file=...)` 传入的路径 | 覆盖上述文件             |
| 4    | 环境变量 `CONFIG_FILE` 指向的路径或 URL                                    | **文件来源中优先级最高** |

示例：根目录 `server.port: 8000`，`conf/config.yaml` 中 `server.port: 9000`，最终以 **9000** 为准（若未再被 `CONFIG_FILE` 或环境变量键覆盖）。

同一嵌套对象（如 `server.cors`）在多个文件中出现时，还会受全局 `merge_enabled` 与 `dynaconf_merge` 影响，见下文 **第 3 节**。

### 2.2 指定配置文件

```bash
# 本地文件
export CONFIG_FILE=/etc/myboot/production.yaml

# 远程 URL（会下载到系统临时目录缓存，失败时尝试使用缓存）
export CONFIG_FILE=https://example.com/config.yaml
```

```python
from myboot import create_app

app = create_app(config_file="conf/config.prod.yaml")
```

远程配置下载逻辑见 `_download_config`：网络失败且存在缓存时回退到缓存文件。

### 2.3 内置默认值

未在任何 YAML 或环境变量中声明时，使用 `config.py` 中 `default_settings`（节选）：

| 键                               | 默认值         |
| -------------------------------- | -------------- |
| `app.name`                       | `"MyBoot App"` |
| `app.version`                    | `"0.1.0"`      |
| `server.host`                    | `"0.0.0.0"`    |
| `server.port`                    | `8000`         |
| `server.reload`                  | `true`         |
| `server.workers`                 | `1`            |
| `server.response_format.enabled` | `true`         |
| `logging.level`                  | `"INFO"`       |
| `scheduler.enabled`              | `true`         |
| `scheduler.timezone`             | `"UTC"`        |
| `scheduler.max_workers`          | `10`           |

## 3. 字典/列表合并与 `dynaconf_merge`

MyBoot 在 `create_settings()` 中启用了 Dynaconf 全局合并：

```python
# myboot/core/config.py
merge_enabled=True,
```

因此，**多个配置文件**（或带 `@merge` 的环境变量）加载到**同一嵌套字典/列表**时，默认会**深合并**，而不是简单地把后一个文件里的整块 YAML 盖掉前一个。

### 3.1 默认合并行为（`merge_enabled=True`）

| 类型                              | 后加载文件中的同路径值 | 结果                                     |
| --------------------------------- | ---------------------- | ---------------------------------------- |
| 标量（`str` / `int` / `bool` 等） | 新值                   | **覆盖**旧值                             |
| 字典                              | 新键与旧键             | **递归合并**（保留旧文件中未出现的子键） |
| 列表                              | 新元素                 | **拼接合并**（可能产生重复项）           |

**示例**：根目录与 `conf/` 均定义 `server.cors` 时，若后加载文件只改 `allow_origins`，未写 `allow_methods`，合并后仍会保留先前的 `allow_methods`、`allow_headers`。

```yaml
# config.yaml（先加载）
server:
  cors:
    allow_origins: ["*"]
    allow_methods: ["*"]
    allow_headers: ["*"]

# conf/config.yaml（后加载，未使用 dynaconf_merge: false）
server:
  cors:
    allow_origins: ["http://localhost:3000"]
```

合并结果等价于：

```yaml
server:
  cors:
    allow_origins: ["*", "http://localhost:3000"] # 列表被拼接
    allow_methods: ["*"]
    allow_headers: ["*"]
```

### 3.2 `dynaconf_merge: false` 的作用

在需要**整块替换**某个字典（或列表）而不是与旧值合并时，在该字典（或列表）内加上 **`dynaconf_merge: false`**。  
该标记是 Dynaconf 的**合并控制元数据**，不会作为业务配置键出现在 `get_config()` 结果中。

| 写法                          | 作用范围                                             |
| ----------------------------- | ---------------------------------------------------- |
| 写在某个 **dict / list 内部** | 仅该节点：后加载内容**整体替换**先加载的同级对象     |
| 写在 **YAML 文件顶层**        | 整个后加载文件相对先前来源按「替换」策略处理（慎用） |

**示例：用 `false` 完全替换 `server.cors`**

```yaml
# config.yaml
server:
  cors:
    allow_origins: ["*"]
    allow_methods: ["*"]
    allow_headers: ["*"]

# conf/config.prod.yaml（后加载）
server:
  cors:
    allow_origins: ["https://api.example.com"]
    allow_credentials: true
    dynaconf_merge: false   # 整块替换 cors，不保留 allow_methods / allow_headers
```

最终 `server.cors` 仅包含：

```yaml
allow_origins: ["https://api.example.com"]
allow_credentials: true
```

**对比**：若去掉 `dynaconf_merge: false`，后加载文件里未写的 `allow_methods`、`allow_headers` 会**继续保留**，且 `allow_origins` 可能与旧列表**拼接**。

### 3.3 `dynaconf_merge: true`

显式声明「与已有字典/列表合并」。在 `merge_enabled=True` 时，多数嵌套 dict/list **已默认合并**，一般仅在需要强调或配合 `@merge` 环境变量时使用：

```yaml
server:
  cors:
    allow_origins: ["http://localhost:3000"]
    dynaconf_merge: true
```

环境变量也可使用 Dynaconf 的 `@merge` 标记（TOML/JSON 形式），例如：

```bash
export SERVER__CORS='@merge {"allow_origins": ["http://localhost:3000"]}'
```

### 3.4 何时用 `false`、何时用环境变量

| 场景                                                         | 建议                                                                          |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------- |
| 生产配置要**换一整段** `server.cors` / `logging.third_party` | 后加载 YAML 中对应该段加 `dynaconf_merge: false`                              |
| 只改**一两个嵌套字段**                                       | 直接写子键，或 `SERVER__CORS__ALLOW_ORIGINS=...`                              |
| 列表不想与旧值拼接                                           | 对该 list 使用 `dynaconf_merge: false`，或写完整列表并 `false`                |
| 不确定合并结果                                               | 用 `get_settings().server.cors` 或 `get_config("server.cors")` 在本地打印验证 |

### 3.5 注意

1. **`dynaconf_merge` 只影响合并策略**，不改变「后加载文件优先」的顺序（见第 2 节）。
2. **标量字段**始终是覆盖，与 `dynaconf_merge` 无关。
3. 对**深层嵌套**的精细控制，除 `dynaconf_merge: false` 外，也可用环境变量 `__` 逐键覆盖（见第 5 节环境变量）。
4. 更多语法见 [Dynaconf 合并文档](https://www.dynaconf.com/merging/)。

## 4. 优先级总览

从高到低（后者覆盖前者）：

```
环境变量键（如 SERVER__PORT）  >  CONFIG_FILE 指向的文件  >  config_file 参数  >  conf/config.yaml  >  根目录 config.yaml  >  default_settings
```

```mermaid
flowchart LR
  A[default_settings] --> B[根目录 config.yaml]
  B --> C[conf/config.yaml]
  C --> D[config_file 参数]
  D --> E[CONFIG_FILE]
  E --> F[环境变量 SERVER__PORT 等]
```

## 5. 环境变量

### 5.1 规则

| 项           | 说明                                                        |
| ------------ | ----------------------------------------------------------- |
| 前缀         | **无**（`envvar_prefix=False`），变量名即配置路径的大写形式 |
| 嵌套分隔符   | **双下划线 `__`**，对应 YAML 中的层级                       |
| 单下划线 `_` | 仅作为**同一层键名**的一部分（如 `keep_alive_timeout`）     |
| 类型         | `env_parse_values=True`，自动尝试解析布尔、数字等           |
| 未知变量     | `ignore_unknown_envvars=True`，**仅当该键已在配置树中存在时**才接受环境变量覆盖（见下） |

#### 已知键限制（`ignore_unknown_envvars=True`）

MyBoot 在 `config.py` 中启用了 `ignore_unknown_envvars=True`。环境变量（含 `.env` 加载进 `os.environ` 的项）**不会**凭空新增配置项，只能**覆盖**下列来源里**已经出现过**的键路径：

1. 已加载的 YAML（`config.yaml`、`conf/config.yaml`、`CONFIG_FILE` 等）
2. `config.py` 中 `default_settings` 内置项（如 `app.name`、`server.port`、`logging.level`、`scheduler.timezone` 等）

因此：

- ✅ `SERVER__PORT=9000` 有效（`server.port` 在默认配置或 YAML 中已有）
- ❌ `DATABASE__URL=...` **无效**，若 YAML / 默认配置里**没有** `database` 段——变量会进入 `os.environ`，但 **MyBoot 不会读入** `get_config("database.url")`
- ❌ 任意「只在 `.env` 里出现、YAML 从未声明」的键都会被**静默忽略**

**正确做法**：需要靠 `.env` 注入的新配置，先在 YAML 中**声明结构**（值可写占位符），再用环境变量覆盖：

```yaml
# conf/config.yaml — 先声明键结构
database:
  url: ""          # 占位，真实值放在 .env / .local.env
app:
  secret_key: ""   # 敏感项同理
jobs:
  cleanup_task:
    enabled: true
```

```bash
# .local.env — 再覆盖
DATABASE__URL=postgresql://user:pass@127.0.0.1:5432/mydb
APP__SECRET_KEY=your-local-secret-key
JOBS__CLEANUP_TASK__ENABLED=false
```

若希望环境变量**无需**在 YAML 预声明即可生效，需修改 `myboot/core/config.py` 将 `ignore_unknown_envvars` 设为 `False`（当前框架默认未开启）。

### 5.2 示例

| YAML 路径                   | 环境变量                                                  |
| --------------------------- | --------------------------------------------------------- |
| `app.name`                  | `APP__NAME=MyApp`                                         |
| `server.port`               | `SERVER__PORT=9000`                                       |
| `logging.level`             | `LOGGING__LEVEL=DEBUG`                                    |
| `server.keep_alive_timeout` | `SERVER__KEEP_ALIVE_TIMEOUT=60`                           |
| `server.cors.allow_origins` | `SERVER__CORS__ALLOW_ORIGINS='["http://localhost:3000"]'` |

```bash
export APP__NAME="生产应用"
export SERVER__PORT=8080
export LOGGING__LEVEL=WARNING
export SCHEDULER__TIMEZONE=Asia/Shanghai
```

**不要**用单下划线表示嵌套层级，例如 `SERVER_PORT` **不会**映射到 `server.port`（除非你在 YAML 里真有名为 `server_port` 的顶层键）。

### 5.3 布尔值

`get_config_bool` 将字符串视为真：`true`、`1`、`yes`、`on`（不区分大小写）。

### 5.4 MyBoot 与 `.env` 的关系

MyBoot **不会**自动读取项目根目录的 `.env` 文件（`config.py` 未启用 Dynaconf 的 `load_dotenv`）。

`.env` 要生效，必须先把其中的键值加载进 **`os.environ`**（通常用 `python-dotenv`，项目依赖中已包含）。之后 Dynaconf 会像读取普通环境变量一样合并进配置，**优先级高于 YAML**。

同时受 **`ignore_unknown_envvars=True`** 约束：`.env` 里的变量名必须对应 **YAML 或内置默认配置中已声明过的键路径**，否则不会进入 `get_config()`（见上文 **5.1 已知键限制**）。

```mermaid
flowchart LR
  A[".env / .local.env"] --> B["main.py 中 load_dotenv()"]
  B --> C["os.environ"]
  C --> D["Dynaconf / MyBoot"]
  E["config.yaml"] --> D
```

### 5.5 在 `main.py` 中加载.env

在 **`create_app()` 或任何会触发 `get_settings()` 的导入之前** 调用 `load_dotenv`，否则配置单例可能已在未加载 `.env` 时初始化。

**推荐 `main.py` 写法：**

```python
"""
应用入口：先加载 .env，再创建 MyBoot 应用
"""
from pathlib import Path

from dotenv import load_dotenv

# 项目根目录（按你的 main.py 位置调整）
ROOT = Path(__file__).resolve().parent

# 先基础配置，再本地覆盖（.local.env 中同名项覆盖 .env）
load_dotenv(ROOT / ".env")
load_dotenv(ROOT / ".local.env", override=True)

# 以下 import 会触发配置加载，必须放在 load_dotenv 之后
from myboot import create_app

app = create_app(name="我的应用")

if __name__ == "__main__":
    app.run()
```

**注意：**

| 事项                | 说明                                                                      |
| ------------------- | ------------------------------------------------------------------------- |
| 调用顺序            | `load_dotenv` → `from myboot import ...` → `create_app()`                 |
| 模块级 `get_config` | 其他文件若在 import 时调用 `get_config()`，也需保证入口已先 `load_dotenv` |
| 修改 `.env` 后      | 需**重启进程**；不会热更新                                                |
| 版本控制            | `.env`、`.local.env` 加入 `.gitignore`；可提交 `.env.example` 作模板      |
| 键必须已声明        | `.env` 中每一项都须在 YAML（或 `default_settings`）中有对应路径，见 5.1   |

若使用 Uvicorn/Hypercorn 命令行启动（`uvicorn main:app`），只要 **`main` 模块被加载时会执行上述 `load_dotenv`**（写在 `main.py` 顶层即可）。

### 5.6 `.env` 文件如何书写

`.env` 使用 **dotenv 格式**（不是 YAML）：每行 `键=值`，`#` 开头为注释。**不要**写 `export`（那是 shell 脚本写法）。

> **重要**：`.env` 只用于**覆盖**已有配置键，不能代替 YAML 做「首次声明」。未在 YAML / `default_settings` 中出现的键（如仅写 `DATABASE__URL` 而 `config.yaml` 里没有 `database.url`）将被忽略。

#### 命名规则（与 MyBoot 一致）

- 嵌套配置用 **`__`（双下划线）** 对应 YAML 层级。
- 键名一般**大写**（与常见约定一致；写入环境后 Dynaconf 会映射到配置树）。
- 单下划线 `_` 只表示键名的一部分，例如 `KEEP_ALIVE_TIMEOUT`。

#### 示例 `.env`（非敏感、可提交 `.env.example`）

```bash
# 应用
APP__NAME=MyBoot App
APP__VERSION=0.1.0
APP__DEBUG=false

# 服务
SERVER__HOST=0.0.0.0
SERVER__PORT=8000
SERVER__RELOAD=true

# 日志
LOGGING__LEVEL=INFO

# 调度器
SCHEDULER__ENABLED=true
SCHEDULER__TIMEZONE=Asia/Shanghai
```

#### 示例 `.local.env`（敏感信息，勿提交 Git）

以下变量均要求 `conf/config.yaml`（或根目录 `config.yaml`）中**已有同名路径**；示例见上一节 YAML 占位声明。

```bash
# 覆盖 .env 中的端口（server.port 已在 YAML/默认配置中存在）
SERVER__PORT=9000

# 以下键须先在 YAML 中声明 database.url、app.secret_key
DATABASE__URL=postgresql://user:password@127.0.0.1:5432/mydb
APP__SECRET_KEY=your-local-secret-key
```

#### 与 YAML 的对应关系

| `.env` 中的写法                       | 等价 YAML 路径              | 代码读取                                  |
| ------------------------------------- | --------------------------- | ----------------------------------------- |
| `SERVER__PORT=9000`                   | `server.port`               | `get_config("server.port")`               |
| `LOGGING__LEVEL=DEBUG`                | `logging.level`             | `get_config("logging.level")`             |
| `DATABASE__URL=...`                   | `database.url`（**须先在 YAML 声明**） | `get_config("database.url")`              |
| `SERVER__CORS__ALLOW_ORIGINS='["*"]'` | `server.cors.allow_origins` | `get_config("server.cors.allow_origins")` |

#### 值的书写建议

```bash
# 字符串含空格或特殊字符时用引号
APP__NAME="My Boot App"

# 布尔（会被 env_parse_values 解析）
APP__DEBUG=true
SERVER__RELOAD=false

# 数字
SERVER__PORT=8080
SCHEDULER__MAX_WORKERS=20

# 列表 / 字典建议用 JSON 字符串（尤其嵌套较深时）
SERVER__CORS__ALLOW_ORIGINS=["http://localhost:3000","http://127.0.0.1:3000"]
```

**错误示例（不会映射到预期配置）：**

```bash
# 错误：单下划线不能表示 server.port
SERVER_PORT=8000

# 错误：dotenv 行内不要用 export
export SERVER__PORT=8000

# 错误：YAML 缩进语法不能用在 .env
server:
  port: 8000

# 错误：YAML 中未声明 database 段时，此行会被 ignore_unknown_envvars 忽略
DATABASE__URL=postgresql://...
```

**内置默认已存在、可直接用 `.env` 覆盖的键（无需 YAML）** 包括：`app.name`、`app.version`、`server.host`、`server.port`、`server.reload`、`server.workers`、`logging.level`、`scheduler.enabled`、`scheduler.timezone`、`scheduler.max_workers` 等，完整列表见 `myboot/core/config.py` 中 `default_settings`。

#### 验证是否加载成功

```python
# 在 load_dotenv 且 create_app 之后执行
from myboot.core.config import get_config

# server.port 在 YAML/默认配置中存在 → 能读到 .env 覆盖值
print(get_config("server.port"))

# database.url 仅当 YAML 中已声明 database.url 时才能读到 .env 值
print(get_config("database.url", "(YAML 未声明或 .env 未覆盖)"))
```

`echo $DATABASE__URL` 能看到 shell 环境变量，但 **`get_config` 仍可能取不到**——说明键未进入 MyBoot 配置树，请检查 YAML 是否已声明该路径。

或在 shell 中确认环境变量已写入（Windows Git Bash）：

```bash
echo $SERVER__PORT
```

## 6. 读取 API

| 函数 / 对象                           | 用途                                     |
| ------------------------------------- | ---------------------------------------- |
| `get_settings(config_file=None)`      | 获取全局 `Dynaconf` 单例                 |
| `get_config(key, default=None)`       | 按点号路径取值                           |
| `get_config_str(key, default="")`     | 转为字符串                               |
| `get_config_int(key, default=0)`      | 转为整数，失败返回 default               |
| `get_config_bool(key, default=False)` | 转为布尔                                 |
| `reload_config()`                     | 清空单例，下次 `get_settings()` 重新加载 |
| `app.config`                          | 应用持有的同一配置对象                   |

```python
# 运行时修改（仅当前进程内存，不写回文件）
app.config.set("server.port", 9001)
```

### 6.1 单例注意

`get_settings()` 使用模块级单例，**首次调用**时确定加载了哪些文件；之后传入不同的 `config_file` 不会生效，除非先调用 `reload_config()`。

```python
from myboot.core.config import get_settings, reload_config

settings = get_settings()  # 已固定加载结果
reload_config()
settings = get_settings("other.yaml")  # 重新加载
```

## 7. 常用配置项

与框架行为直接相关的键（完整示例可参考 `conf/config.yaml` 与 README）：

### 7.1 `app`

| 键            | 说明                 |
| ------------- | -------------------- |
| `app.name`    | 应用名称             |
| `app.version` | 版本号               |
| `app.debug`   | 调试开关（按需使用） |

### 7.2 `server`

| 键                                     | 说明                                |
| -------------------------------------- | ----------------------------------- |
| `server.host`                          | 监听地址                            |
| `server.port`                          | 端口                                |
| `server.reload`                        | 热重载                              |
| `server.workers`                       | Worker 数量                         |
| `server.keep_alive_timeout`            | Keep-Alive 超时                     |
| `server.graceful_timeout`              | 优雅关闭超时                        |
| `server.cors`                          | CORS 子对象，存在时启用 CORS 中间件 |
| `server.response_format.enabled`       | 是否统一响应格式                    |
| `server.response_format.exclude_paths` | 排除路径列表                        |

### 7.3 `logging`

| 键                    | 说明                 |
| --------------------- | -------------------- |
| `logging.level`       | 日志级别             |
| `logging.format`      | 日志格式             |
| `logging.file`        | 日志文件路径（可选） |
| `logging.third_party` | 第三方库日志级别映射 |

### 7.4 `scheduler`

| 键                         | 说明                               |
| -------------------------- | ---------------------------------- |
| `scheduler.enabled`        | 是否启用调度器                     |
| `scheduler.timezone`       | 时区（建议安装 `pytz`）            |
| `scheduler.max_workers`    | 调度线程池大小                     |
| `scheduler.on_all_workers` | 多进程时是否在每个 worker 运行调度 |

Cron 与任务装饰器详见 [scheduler.md](./scheduler.md)。

### 7.5 `jobs`（可选）

用于在 `@cron` / `@interval` / `@once` 的 `enabled` 参数中按任务开关：

```yaml
jobs:
  cleanup_task:
    enabled: false
```

```python
from myboot.core.config import get_config

@cron("0 2 * * *", enabled=get_config("jobs.cleanup_task.enabled", True))
def cleanup(self):
    ...
```

## 8. 配置与装饰器、组件

- **定时任务**：`enabled=get_config('jobs.xxx.enabled', True)` 在类定义时求值，修改环境变量后需**重启进程**才生效。
- **日志**：`Application` 构造时调用 `setup_logging(self.config)`，之后改 `logging.*` 需自行处理或重启。
- **依赖注入**：`get_config` 可在模块级或组件方法内使用；与 `@component` 无冲突。

## 9. 最佳实践

1. **开发**用 `conf/config.yaml`，**生产**用 `CONFIG_FILE` 或环境变量注入敏感项，避免密钥进仓库。
2. **嵌套键**统一用 `__` 环境变量，避免与 `snake_case` 字段混淆。
3. **显式设置** `logging.level`、`scheduler.timezone`，减少环境差异。
4. 列表、字典类配置在环境变量中优先使用 **JSON 字符串**（如 CORS origins）。
5. 需要切换配置源时调用 `reload_config()`，或保证在任意 `get_settings()` 之前设置好 `CONFIG_FILE`。
6. 多文件拆分时保持「基底 → 环境专用」的加载顺序意识：后加载覆盖先加载。
7. 多文件共用同一嵌套段（如 `server.cors`）且不想**拼接列表、保留旧子键**时，在后加载文件中对应该段添加 **`dynaconf_merge: false`**（见第 3 节）。
8. 本地敏感项用 **`.local.env`** + `main.py` 中 `load_dotenv`（见第 5.5、5.6 节）；敏感值放 `.env`，但**键名须先在 YAML 声明**（`ignore_unknown_envvars=True`），结构可进 `config.yaml`，秘密不进 Git。

## 10. 相关文档与代码

| 资源                                                 | 说明                         |
| ---------------------------------------------------- | ---------------------------- |
| `myboot/core/config.py`                              | 配置加载与便捷函数           |
| `conf/config.yaml`                                   | 项目默认配置示例             |
| [scheduler.md](./scheduler.md)                       | 调度器配置与 Cron            |
| [dependency-injection.md](./dependency-injection.md) | 组件与 `get_config` 结合示例 |
