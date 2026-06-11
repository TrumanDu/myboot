"""
配置系统特征测试（characterization tests）

固化 myboot.core.config 的「当前实际行为」，作为后续重构的兼容性闸门。
所有断言以代码现状为准（已用探针脚本逐项验证），而非文档或「理想行为」。

已知可疑现状（详见各测试注释，重构时务必先确认再改）：

1. ``create_settings()`` 把 ``default_settings={...}`` 作为普通 kwarg 传给
   Dynaconf。Dynaconf 没有这个选项，于是它变成了一个名为
   ``DEFAULT_SETTINGS`` 的普通配置项——内置默认值并 **不会** 出现在
   ``app.name`` / ``server.port`` 等路径上（与 docs/configuration.md 2.3 节相悖）。
2. 由 1 推论：在没有任何 YAML 声明对应键时，``SERVER__PORT`` 等环境变量
   会被 ``ignore_unknown_envvars=True`` 静默忽略（文档 5.6 声称内置默认键
   可直接用 .env 覆盖，实际不行）。
3. 同目录同时存在 ``config.yaml`` 与 ``config.yml`` 时，``.yml`` 后加载，
   **覆盖** ``.yaml`` 的同名键。
4. ``merge_enabled=True`` 下，多文件中同路径的 list 会拼接（可能重复），
   嵌套 dict 深合并；``dynaconf_merge: false`` 则整块替换（issue #13）。
5. ``env_parse_values=True`` 下，``"1"`` 解析为 int 1 而非 bool True。
6. 环境变量提供的 JSON 列表会 **整体替换** YAML 中的列表（不拼接）。

注意：``_find_project_root()`` 以 config.py 所在包向上找 pyproject.toml，
与进程工作目录无关（仓库内运行时恒为仓库根）。因此仅靠
``monkeypatch.chdir(tmp_path)`` 无法隔离根 config.yaml 查找，必须同时
monkeypatch ``_find_project_root``。
"""

import hashlib
import os
import tempfile
import textwrap
import uuid

import pytest

import myboot.core.config as config_module
from myboot.core.config import (
    _get_config_files,
    _is_url,
    create_settings,
    get_config,
    get_config_bool,
    get_config_int,
    get_config_str,
    get_settings,
    reload_config,
)


@pytest.fixture(autouse=True)
def isolate_config(tmp_path, monkeypatch):
    """每个测试独立的「项目根」+ 干净的全局单例 / CONFIG_FILE。

    - monkeypatch _find_project_root：见模块 docstring，仅 chdir 不够。
    - chdir 仍然保留，防止任何依赖 cwd 的退路逻辑读到真实仓库。
    - 前后都清空模块级单例 _settings，避免测试间污染。
    """
    monkeypatch.setattr(config_module, "_find_project_root", lambda: str(tmp_path))
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CONFIG_FILE", raising=False)
    reload_config()
    yield
    reload_config()


def write_yaml(path, content):
    """写入 YAML 文件（自动 dedent / 建父目录），返回 str 路径"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# 1. 多源配置文件优先级
# ---------------------------------------------------------------------------


class TestConfigFilePriority:
    def test_root_config_yaml_loaded(self, tmp_path):
        write_yaml(tmp_path / "config.yaml", """\
            app:
              name: root-app
            """)
        settings = create_settings()
        assert settings.get("app.name") == "root-app"

    def test_conf_dir_overrides_root(self, tmp_path):
        write_yaml(tmp_path / "config.yaml", """\
            app:
              name: root-app
            server:
              port: 1111
            only_root: 1
            """)
        write_yaml(tmp_path / "conf" / "config.yaml", """\
            app:
              name: conf-app
            server:
              port: 2222
            """)
        settings = create_settings()
        # conf/config.yaml 后加载，覆盖根目录同名标量
        assert settings.get("app.name") == "conf-app"
        assert settings.get("server.port") == 2222
        # 仅根目录出现的键仍然保留（merge_enabled=True）
        assert settings.get("only_root") == 1

    def test_param_config_file_overrides_conf(self, tmp_path, tmp_config_file):
        write_yaml(tmp_path / "conf" / "config.yaml", """\
            app:
              name: conf-app
            only_conf: 2
            """)
        param = tmp_config_file(
            """\
            app:
              name: param-app
            """,
            filename="param.yaml",
        )
        settings = create_settings(param)
        assert settings.get("app.name") == "param-app"
        assert settings.get("only_conf") == 2

    def test_config_file_env_var_overrides_param(
        self, tmp_path, tmp_config_file, monkeypatch
    ):
        param = tmp_config_file(
            """\
            app:
              name: param-app
            only_param: 3
            """,
            filename="param.yaml",
        )
        env_file = tmp_config_file(
            """\
            app:
              name: envfile-app
            """,
            filename="envfile.yaml",
        )
        monkeypatch.setenv("CONFIG_FILE", env_file)
        settings = create_settings(param)
        # CONFIG_FILE 指定的文件最后加载，优先级最高（文件来源中）
        assert settings.get("app.name") == "envfile-app"
        assert settings.get("only_param") == 3

    def test_get_config_files_order(self, tmp_path, monkeypatch):
        """固化 _get_config_files 返回的加载顺序：根 → conf → 参数 → CONFIG_FILE"""
        root = write_yaml(tmp_path / "config.yaml", "a: 1\n")
        conf = write_yaml(tmp_path / "conf" / "config.yaml", "a: 2\n")
        param = write_yaml(tmp_path / "param.yaml", "a: 3\n")
        envf = write_yaml(tmp_path / "envfile.yaml", "a: 4\n")
        monkeypatch.setenv("CONFIG_FILE", envf)
        assert _get_config_files(param) == [root, conf, param, envf]

    def test_nonexistent_param_file_silently_dropped(self, tmp_path):
        write_yaml(tmp_path / "config.yaml", "app:\n  name: root-app\n")
        # 不存在的文件路径不报错，静默跳过
        settings = create_settings(str(tmp_path / "no-such-file.yaml"))
        assert settings.get("app.name") == "root-app"

    def test_yml_overrides_yaml_in_same_dir(self, tmp_path):
        """可疑现状 #3：config.yml 排在 config.yaml 之后加载，.yml 覆盖 .yaml。

        直觉上 .yaml 是「主」扩展名，但实际 .yml 同名键胜出。
        """
        write_yaml(tmp_path / "config.yaml", "app:\n  name: from-yaml\n")
        write_yaml(tmp_path / "config.yml", "app:\n  name: from-yml\n")
        settings = create_settings()
        assert settings.get("app.name") == "from-yml"


# ---------------------------------------------------------------------------
# 2. 内置默认值（0.2.0 修复：默认值以大写 kwargs 传入 Dynaconf）
# ---------------------------------------------------------------------------


class TestDefaultSettings:
    def test_builtin_defaults_exposed_at_documented_paths(self):
        """0.2.0 修复：无任何 YAML 时内置默认值在文档路径上可取到。

        旧版把 default_settings={...} 当 kwarg 传给 Dynaconf（无此参数），
        默认值整体失效；现以大写 kwargs（APP=/SERVER=...）注册为默认配置项。
        """
        settings = create_settings()
        assert settings.get("app.name") == "MyBoot App"
        assert settings.get("server.port") == 8000
        assert settings.get("logging.level") == "INFO"
        assert settings.get("scheduler.enabled") is True
        assert settings.get("scheduler.max_workers") == 10

    def test_no_stray_default_settings_key(self):
        """修复后不再产生名为 DEFAULT_SETTINGS 的杂散配置项"""
        settings = create_settings()
        assert "DEFAULT_SETTINGS" not in settings

    def test_yaml_partial_override_deep_merges_with_defaults(self, tmp_path):
        """YAML 只覆盖 server.port 时，server 其余默认键保留（深合并）"""
        write_yaml(tmp_path / "config.yaml", "server:\n  port: 9000\n")
        settings = create_settings()
        assert settings.get("server.port") == 9000
        assert settings.get("server.host") == "0.0.0.0"
        assert settings.get("server.workers") == 1


# ---------------------------------------------------------------------------
# 3. 环境变量 `__` 分隔符覆盖
# ---------------------------------------------------------------------------


class TestEnvVarOverride:
    def test_double_underscore_overrides_declared_key(self, tmp_path, monkeypatch):
        # envvar_prefix=False：变量名即配置路径大写形式，无前缀
        write_yaml(tmp_path / "config.yaml", """\
            app:
              name: yaml-app
            """)
        monkeypatch.setenv("APP__NAME", "env-app")
        settings = create_settings()
        assert settings.get("app.name") == "env-app"

    def test_env_var_beats_config_file_env_source(
        self, tmp_path, tmp_config_file, monkeypatch
    ):
        env_file = tmp_config_file(
            """\
            app:
              name: from-config-file
            """,
            filename="cf.yaml",
        )
        monkeypatch.setenv("CONFIG_FILE", env_file)
        monkeypatch.setenv("APP__NAME", "from-envvar")
        settings = create_settings()
        # 环境变量键 > CONFIG_FILE 文件
        assert settings.get("app.name") == "from-envvar"

    def test_undeclared_env_key_silently_ignored(self, tmp_path, monkeypatch):
        # ignore_unknown_envvars=True：YAML 中没有 database 段时被静默忽略
        write_yaml(tmp_path / "config.yaml", "app:\n  name: yaml-app\n")
        monkeypatch.setenv("DATABASE__URL", "postgresql://ignored")
        settings = create_settings()
        assert settings.get("database.url") is None

    def test_undeclared_leaf_under_declared_parent_also_ignored(
        self, tmp_path, monkeypatch
    ):
        # server 段存在但 server.custom_flag 既未在 YAML 也未在内置默认值中
        # 声明 → SERVER__CUSTOM_FLAG 被 ignore_unknown_envvars 忽略
        # （注意 server.reload 已是内置默认键，不能再用作"未声明"示例）
        write_yaml(tmp_path / "config.yaml", """\
            server:
              port: 8000
            """)
        monkeypatch.setenv("SERVER__CUSTOM_FLAG", "true")
        settings = create_settings()
        assert settings.get("server.custom_flag") is None

    def test_env_var_overrides_builtin_defaults_without_yaml(self, monkeypatch):
        """0.2.0 修复：内置默认键生效后，无 YAML 时也可用环境变量覆盖
        （与 docs/configuration.md 5.6 节一致）"""
        monkeypatch.setenv("SERVER__PORT", "7777")
        monkeypatch.setenv("APP__NAME", "env-only")
        settings = create_settings()
        assert settings.get("server.port") == 7777
        assert settings.get("app.name") == "env-only"


# ---------------------------------------------------------------------------
# 4. 类型自动转换（env_parse_values=True）
# ---------------------------------------------------------------------------


class TestEnvVarTypeConversion:
    @pytest.fixture(autouse=True)
    def base_yaml(self, tmp_path):
        write_yaml(tmp_path / "config.yaml", """\
            app:
              debug: false
            server:
              port: 8000
              reload: false
              cors:
                allow_origins: ["*"]
            """)

    def test_numeric_string_to_int(self, monkeypatch):
        monkeypatch.setenv("SERVER__PORT", "9090")
        settings = create_settings()
        value = settings.get("server.port")
        assert value == 9090
        assert isinstance(value, int)

    def test_true_false_strings_to_bool(self, monkeypatch):
        monkeypatch.setenv("SERVER__RELOAD", "true")
        settings = create_settings()
        assert settings.get("server.reload") is True

        monkeypatch.setenv("SERVER__RELOAD", "false")
        settings = create_settings()
        assert settings.get("server.reload") is False

    def test_string_one_parses_as_int_not_bool(self, monkeypatch):
        """可疑现状 #5："1" 被解析为 int 1，不是 bool True。

        依赖 `settings.get("app.debug") is True` 的调用方会受影响；
        get_config_bool 仍能正确转换（见 TestHelperFunctions）。
        """
        monkeypatch.setenv("APP__DEBUG", "1")
        settings = create_settings()
        value = settings.get("app.debug")
        assert value == 1
        assert not isinstance(value, bool)
        assert isinstance(value, int)

    def test_json_list_string_parsed_and_replaces_yaml_list(self, monkeypatch):
        """可疑现状 #6：env 提供的 JSON 列表整体替换 YAML 列表，
        不与原列表 ["*"] 拼接（与多文件加载时的列表拼接行为不同）。
        """
        monkeypatch.setenv(
            "SERVER__CORS__ALLOW_ORIGINS", '["http://a", "http://b"]'
        )
        settings = create_settings()
        assert list(settings.get("server.cors.allow_origins")) == [
            "http://a",
            "http://b",
        ]


# ---------------------------------------------------------------------------
# 5. 合并行为与 dynaconf_merge: false（issue #13 回归）
# ---------------------------------------------------------------------------


class TestMergeBehavior:
    def _write_base_cors(self, tmp_path):
        write_yaml(tmp_path / "config.yaml", """\
            server:
              cors:
                allow_origins: ["*"]
                allow_methods: ["*"]
                allow_headers: ["*"]
            """)

    def test_nested_dict_deep_merge_keeps_sibling_keys(self, tmp_path):
        # merge_enabled=True：后加载文件只写部分子键时，旧子键保留
        self._write_base_cors(tmp_path)
        write_yaml(tmp_path / "conf" / "config.yaml", """\
            server:
              cors:
                allow_origins: ["http://x"]
            """)
        settings = create_settings()
        cors = settings.get("server.cors")
        assert list(cors["allow_methods"]) == ["*"]
        assert list(cors["allow_headers"]) == ["*"]

    def test_list_values_concatenate_across_files(self, tmp_path):
        """可疑现状 #4：同路径列表跨文件「拼接」而非替换，可能产生意外元素。

        基底 ["*"] + 覆盖文件 ["http://x"] → ["*", "http://x"]，
        通配符 "*" 仍然留在 CORS origins 里。
        """
        self._write_base_cors(tmp_path)
        write_yaml(tmp_path / "conf" / "config.yaml", """\
            server:
              cors:
                allow_origins: ["http://x"]
            """)
        settings = create_settings()
        assert list(settings.get("server.cors.allow_origins")) == ["*", "http://x"]

    def test_dynaconf_merge_false_replaces_whole_dict(self, tmp_path):
        """issue #13 回归：dynaconf_merge: false 时嵌套 dict 整块替换，
        先前的 allow_methods / allow_headers 不再保留。
        """
        self._write_base_cors(tmp_path)
        write_yaml(tmp_path / "conf" / "config.yaml", """\
            server:
              cors:
                allow_origins: ["http://x"]
                dynaconf_merge: false
            """)
        settings = create_settings()
        cors = settings.get("server.cors")
        assert cors.to_dict() == {"allow_origins": ["http://x"]}

    def test_dynaconf_merge_marker_not_exposed_as_config_key(self, tmp_path):
        self._write_base_cors(tmp_path)
        write_yaml(tmp_path / "conf" / "config.yaml", """\
            server:
              cors:
                allow_origins: ["http://x"]
                dynaconf_merge: false
            """)
        settings = create_settings()
        # 合并控制元数据不会作为业务键出现
        assert settings.get("server.cors.dynaconf_merge") is None


# ---------------------------------------------------------------------------
# 6. 缺失键默认值 / 便捷函数 / 大小写
# ---------------------------------------------------------------------------


class TestHelperFunctions:
    @pytest.fixture(autouse=True)
    def base_yaml(self, tmp_path):
        write_yaml(tmp_path / "config.yaml", """\
            app:
              name: helper-app
            server:
              port: 8000
            flags:
              str_yes: "yes"
              str_other: "definitely"
            items:
              - a
              - b
            """)
        reload_config()  # 便捷函数走全局单例，确保读到本测试的 YAML

    def test_get_missing_key_returns_default(self):
        assert get_config("no.such.key", "fallback") == "fallback"

    def test_get_missing_key_default_none(self):
        assert get_config("no.such.key") is None

    def test_get_config_str_converts(self):
        assert get_config_str("server.port") == "8000"
        assert get_config_str("no.such.key", "dft") == "dft"

    def test_get_config_int_converts_and_falls_back(self):
        assert get_config_int("server.port") == 8000
        # 列表无法转 int → 返回 default（当前实现吞掉 TypeError）
        assert get_config_int("items", 42) == 42
        assert get_config_int("no.such.key", 7) == 7

    def test_get_config_bool_string_semantics(self):
        # 字符串仅 'true'/'1'/'yes'/'on'（不区分大小写）为真
        assert get_config_bool("flags.str_yes") is True
        assert get_config_bool("flags.str_other") is False
        assert get_config_bool("no.such.key", True) is True

    def test_get_is_case_insensitive(self):
        settings = get_settings()
        assert settings.get("APP.NAME") == "helper-app"
        assert settings.get("app.name") == "helper-app"


class TestSingletonBehavior:
    def test_get_settings_ignores_config_file_after_first_call(
        self, tmp_path, tmp_config_file
    ):
        write_yaml(tmp_path / "config.yaml", "app:\n  name: first\n")
        other = tmp_config_file("app:\n  name: second\n", filename="other.yaml")

        first = get_settings()
        assert first.get("app.name") == "first"
        # 单例已固定，后续传入不同 config_file 不生效
        second = get_settings(other)
        assert second is first
        assert second.get("app.name") == "first"

    def test_reload_config_resets_singleton(self, tmp_path, tmp_config_file):
        write_yaml(tmp_path / "config.yaml", "app:\n  name: first\n")
        other = tmp_config_file("app:\n  name: second\n", filename="other.yaml")

        first = get_settings()
        reload_config()
        second = get_settings(other)
        assert second is not first
        assert second.get("app.name") == "second"


# ---------------------------------------------------------------------------
# 7. 远程配置（HTTP URL，mock requests）
# ---------------------------------------------------------------------------


def _cache_path_for(url):
    cache_dir = os.path.join(tempfile.gettempdir(), "myboot_config_cache")
    return os.path.join(cache_dir, hashlib.md5(url.encode()).hexdigest() + ".yaml")


@pytest.fixture
def remote_url():
    """每个测试唯一的 URL，避免共享缓存目录（系统临时目录）相互污染"""
    url = f"https://config.example.invalid/{uuid.uuid4().hex}.yaml"
    yield url
    cache = _cache_path_for(url)
    if os.path.exists(cache):
        os.remove(cache)


class TestRemoteConfig:
    def test_is_url_detection(self):
        assert _is_url("http://example.com/c.yaml")
        assert _is_url("https://example.com/c.yaml")
        assert not _is_url("/etc/myboot/config.yaml")
        assert not _is_url("ftp://example.com/c.yaml")
        # 空字符串走 `path and ...` 短路，返回原值（falsy），不是 False
        assert not _is_url("")

    def test_remote_config_downloaded_cached_and_loaded(
        self, monkeypatch, mocker, remote_url
    ):
        response = mocker.Mock()
        response.text = "app:\n  name: remote-app\nremote_only: 42\n"
        response.raise_for_status = mocker.Mock()
        mock_get = mocker.patch.object(
            config_module.requests, "get", return_value=response
        )

        monkeypatch.setenv("CONFIG_FILE", remote_url)
        settings = create_settings()

        mock_get.assert_called_once_with(remote_url, timeout=30)
        # 下载内容写入系统临时目录缓存（md5(url).yaml）
        cache = _cache_path_for(remote_url)
        assert os.path.exists(cache)
        assert "remote-app" in open(cache, encoding="utf-8").read()
        # 远程配置作为 CONFIG_FILE 来源参与合并
        assert settings.get("app.name") == "remote-app"
        assert settings.get("remote_only") == 42

    def test_download_failure_falls_back_to_existing_cache(
        self, monkeypatch, mocker, remote_url
    ):
        cache = _cache_path_for(remote_url)
        os.makedirs(os.path.dirname(cache), exist_ok=True)
        with open(cache, "w", encoding="utf-8") as f:
            f.write("app:\n  name: cached-app\n")

        mocker.patch.object(
            config_module.requests,
            "get",
            side_effect=ConnectionError("network down"),
        )
        monkeypatch.setenv("CONFIG_FILE", remote_url)
        settings = create_settings()
        assert settings.get("app.name") == "cached-app"

    def test_download_failure_without_cache_raises(
        self, monkeypatch, mocker, remote_url
    ):
        # 无缓存且下载失败 → 原始异常向上抛出（create_settings 直接失败）
        mocker.patch.object(
            config_module.requests,
            "get",
            side_effect=ValueError("network down"),
        )
        monkeypatch.setenv("CONFIG_FILE", remote_url)
        with pytest.raises(ValueError, match="network down"):
            create_settings()

    def test_remote_param_config_file_also_supported(self, mocker, remote_url):
        # config_file 参数同样支持 URL（不只 CONFIG_FILE 环境变量）
        response = mocker.Mock()
        response.text = "app:\n  name: remote-param\n"
        response.raise_for_status = mocker.Mock()
        mocker.patch.object(config_module.requests, "get", return_value=response)

        settings = create_settings(remote_url)
        assert settings.get("app.name") == "remote-param"
