"""
日志系统特征测试（characterization tests）

固化 myboot/core/logger.py 的「当前实际行为」，作为后续重构的兼容性闸门。
失败时优先修改测试以匹配源码行为（除非源码行为本身是回归）。

注意:
- loguru 是全局单例，每个测试添加的 sink 必须移除（见 _loguru_isolation fixture）。
- setup_logging / setup_worker_logging 会调用 loguru_logger.remove() 清掉所有
  handler（包括测试 sink），因此捕获 sink 必须在 setup 调用之后添加，
  或者通过 capsys 捕获 stdout。
- 始终显式传入 Dynaconf 配置对象，避免 get_settings() 全局单例读取
  项目根目录的 conf/config.yaml 造成测试间/环境间耦合。
"""

import inspect
import json
import logging

import pytest
from dynaconf import Dynaconf
from loguru import logger as loguru_logger

from myboot.core.logger import (
    Logger,
    _configure_third_party_loggers,
    _convert_logging_format_to_loguru,
    _get_worker_info,
    _parse_json_config,
    configure_worker_logger,
    get_logger,
    setup_logging,
    setup_worker_logging,
)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _loguru_isolation():
    """保证测试顺序无关性。

    loguru 是全局单例；setup_logging/setup_worker_logging 会 remove() 所有
    handler 并 configure() 全局 extra。每个测试结束后清掉所有 handler 并
    重置 extra，使下一个测试从确定状态开始（无 handler、空 extra）。
    """
    yield
    loguru_logger.remove()
    loguru_logger.configure(extra={})


@pytest.fixture
def capture_records():
    """添加内存 sink 捕获 loguru record，返回 (records, add) 工具。

    add() 在调用时刻挂 sink（便于在 setup_* 调用之后再挂，避免被
    loguru_logger.remove() 清掉），finally 由 _loguru_isolation 兜底移除。
    """
    records = []
    handler_ids = []

    def _add(level="DEBUG"):
        hid = loguru_logger.add(
            lambda message: records.append(message),
            format="{message}",
            level=level,
        )
        handler_ids.append(hid)
        return hid

    try:
        yield records, _add
    finally:
        for hid in handler_ids:
            try:
                loguru_logger.remove(hid)
            except ValueError:
                # 已被 setup_* 的 remove() 清掉
                pass


def _make_config(tmp_path, yaml_content: str) -> Dynaconf:
    """从 YAML 字符串创建独立的 Dynaconf 配置对象（不经过全局 get_settings）"""
    config_path = tmp_path / "logger_test_config.yaml"
    config_path.write_text(yaml_content, encoding="utf-8")
    return Dynaconf(settings_files=[str(config_path)])


def _messages(records):
    """提取捕获到的纯文本消息列表（去掉末尾换行）"""
    return [str(m).rstrip("\n") for m in records]


# ---------------------------------------------------------------------------
# 1. issue #5 回归：setup_worker_logging 应遵循配置的日志级别（已修复）
# ---------------------------------------------------------------------------


class TestSetupWorkerLoggingLevel:
    def test_signature_accepts_config(self):
        """修复后的签名：setup_worker_logging(worker_id, total_workers, config=None)"""
        params = inspect.signature(setup_worker_logging).parameters
        assert list(params) == ["worker_id", "total_workers", "config"]
        assert params["config"].default is None

    def test_respects_configured_warning_level(self, tmp_path, capsys):
        """配置 WARNING 级别时，DEBUG/INFO 被抑制（不再硬编码 DEBUG）"""
        cfg = _make_config(tmp_path, "logging:\n  level: WARNING\n")
        setup_worker_logging(worker_id=1, total_workers=2, config=cfg)

        loguru_logger.debug("debug-should-not-appear-xyz")
        loguru_logger.info("info-should-not-appear-xyz")
        loguru_logger.warning("warning-should-appear-xyz")

        out = capsys.readouterr().out
        assert "debug-should-not-appear-xyz" not in out
        assert "info-should-not-appear-xyz" not in out
        assert "warning-should-appear-xyz" in out

    def test_respects_configured_debug_level(self, tmp_path, capsys):
        """配置 DEBUG 级别时，DEBUG 消息正常输出"""
        cfg = _make_config(tmp_path, "logging:\n  level: DEBUG\n")
        setup_worker_logging(worker_id=1, total_workers=2, config=cfg)

        loguru_logger.debug("debug-should-appear-xyz")
        out = capsys.readouterr().out
        assert "debug-should-appear-xyz" in out

    def test_default_level_is_info_when_config_missing_key(self, tmp_path, capsys):
        """配置中无 logging.level 时，当前默认级别为 INFO"""
        cfg = _make_config(tmp_path, "app:\n  name: demo\n")
        setup_worker_logging(worker_id=1, total_workers=2, config=cfg)

        loguru_logger.debug("debug-hidden-by-default-xyz")
        loguru_logger.info("info-shown-by-default-xyz")
        out = capsys.readouterr().out
        assert "debug-hidden-by-default-xyz" not in out
        assert "info-shown-by-default-xyz" in out

    def test_output_contains_worker_tag(self, tmp_path, capsys):
        """worker 日志格式包含 Worker-{id}/{total} 标识"""
        cfg = _make_config(tmp_path, "logging:\n  level: INFO\n")
        setup_worker_logging(worker_id=3, total_workers=8, config=cfg)

        loguru_logger.info("hello-from-worker-xyz")
        out = capsys.readouterr().out
        assert "Worker-3/8" in out
        assert "hello-from-worker-xyz" in out


# ---------------------------------------------------------------------------
# 2. Logger 兼容类
# ---------------------------------------------------------------------------


class TestLoggerCompatClass:
    def test_instantiation_stores_name(self):
        log = Logger("svc")
        assert log.name == "svc"

    def test_default_name_is_app(self):
        assert Logger().name == "app"

    def test_all_level_methods_callable_without_error(self, capture_records):
        records, add = capture_records
        add(level="DEBUG")
        log = Logger("compat")
        log.debug("d-msg")
        log.info("i-msg")
        log.warning("w-msg")
        log.error("e-msg")
        log.critical("c-msg")
        # exception() 在无活动异常时也不抛错（loguru 行为）
        log.exception("x-msg")
        msgs = _messages(records)
        assert msgs[:5] == ["d-msg", "i-msg", "w-msg", "e-msg", "c-msg"]
        # 可疑现状：无活动异常时 exception() 仍输出异常段，
        # 格式化文本末尾附加 "NoneType: None"
        assert msgs[5] == "x-msg\nNoneType: None"

    def test_bind_name_in_extra(self):
        """Logger 内部 _logger 是 bind(name=...) 后的 loguru logger，
        name 进入 record["extra"]["name"]（不影响 record["name"] 即模块名）"""
        records = []
        hid = loguru_logger.add(
            lambda m: records.append(m.record), format="{message}", level="DEBUG"
        )
        try:
            Logger("my-component").info("bound message")
        finally:
            loguru_logger.remove(hid)

        assert len(records) == 1
        assert records[0]["extra"]["name"] == "my-component"
        # 可疑现状：record["name"]（loguru 内置，模块路径）不受 bind 影响，
        # 仍为调用方模块名，而非传入的 name —— 文本格式 {name} 显示的是模块名
        assert records[0]["name"] != "my-component"


# ---------------------------------------------------------------------------
# 3. get_logger
# ---------------------------------------------------------------------------


class TestGetLogger:
    def test_returns_logger_bound_with_name(self):
        records = []
        hid = loguru_logger.add(
            lambda m: records.append(m.record), format="{message}", level="DEBUG"
        )
        try:
            log = get_logger("api")
            log.info("from get_logger")
        finally:
            loguru_logger.remove(hid)

        assert len(records) == 1
        assert records[0]["extra"]["name"] == "api"

    def test_default_name_is_app(self):
        records = []
        hid = loguru_logger.add(
            lambda m: records.append(m.record), format="{message}", level="DEBUG"
        )
        try:
            get_logger().info("default name")
        finally:
            loguru_logger.remove(hid)

        assert records[0]["extra"]["name"] == "app"


# ---------------------------------------------------------------------------
# 4. loguru {} 占位符格式化
# ---------------------------------------------------------------------------


class TestBracePlaceholderFormatting:
    def test_positional_brace_formatting(self, capture_records):
        records, add = capture_records
        add()
        loguru_logger.info("hello {}", "world")
        assert _messages(records) == ["hello world"]

    def test_named_brace_formatting_via_kwargs(self, capture_records):
        records, add = capture_records
        add()
        loguru_logger.info("hello {who}", who="myboot")
        assert _messages(records) == ["hello myboot"]

    def test_logger_compat_class_supports_brace_args(self, capture_records):
        """Logger 兼容类透传 *args，因此 {} 占位符同样可用"""
        records, add = capture_records
        add()
        Logger("x").info("count={}", 42)
        assert _messages(records) == ["count=42"]


# ---------------------------------------------------------------------------
# 5. %s 风格调用（issue #12：Logger 兼容类同时支持 % 与 {} 占位符）
# ---------------------------------------------------------------------------


class TestPercentStyleSupport:
    def test_percent_s_is_formatted(self, capture_records):
        """issue #12：原行为是 %s 字面量输出、参数静默丢弃；
        修复后 Logger 兼容类对 % 占位符做预格式化"""
        records, add = capture_records
        add()
        Logger("x").info("hello %s", "world")
        assert _messages(records) == ["hello world"]

    def test_percent_multiple_args_and_types(self, capture_records):
        """issue #12：%s/%d 多参数预格式化"""
        records, add = capture_records
        add()
        Logger("x").info("user=%s count=%d", "alice", 3)
        assert _messages(records) == ["user=alice count=3"]

    def test_percent_works_on_all_level_methods(self, capture_records):
        """issue #12：六个级别方法统一支持 % 占位符"""
        records, add = capture_records
        add(level="DEBUG")
        log = Logger("x")
        log.debug("d=%s", 1)
        log.info("i=%s", 2)
        log.warning("w=%s", 3)
        log.error("e=%s", 4)
        log.critical("c=%s", 5)
        log.exception("x=%s", 6)
        msgs = _messages(records)
        assert msgs[:5] == ["d=1", "i=2", "w=3", "e=4", "c=5"]
        # exception() 无活动异常时附加 "NoneType: None"（既有 loguru 行为）
        assert msgs[5] == "x=6\nNoneType: None"

    def test_double_percent_escape(self, capture_records):
        """issue #12：%% 是转义不是占位符，% 预格式化后输出单个 %"""
        records, add = capture_records
        add()
        Logger("x").info("progress 100%% by %s", "worker")
        assert _messages(records) == ["progress 100% by worker"]

    def test_double_percent_only_without_real_placeholder(self, capture_records):
        """issue #12：消息只含 %%（无真正 % 占位符）且带参数时，
        不做 % 预格式化，参数交给 loguru（无 {} 则被忽略），%% 原样保留"""
        records, add = capture_records
        add()
        Logger("x").info("progress 100%%", "ignored")
        assert _messages(records) == ["progress 100%%"]

    def test_mixed_percent_and_brace_prefers_brace(self, capture_records):
        """issue #12 设计决策：消息同时含 % 和 {} 占位符时，{} 优先，
        原样交给 loguru 的 str.format（保持 loguru 原生语义），% 保留字面量"""
        records, add = capture_records
        add()
        Logger("x").info("pct=%s val={}", 42)
        assert _messages(records) == ["pct=%s val=42"]

    def test_percent_args_mismatch_falls_back_without_raising(self, capture_records):
        """issue #12：% 预格式化参数不匹配时回退为原样输出，不向调用方抛错"""
        records, add = capture_records
        add()
        Logger("x").info("a=%s b=%s", "only-one")
        assert _messages(records) == ["a=%s b=%s"]

    def test_lone_brace_with_args_does_not_raise(self, capture_records):
        """issue #12：原行为是孤立 { 带参数时 loguru 的 str.format 异常
        直接传播；修复后走 % 预格式化路径（无完整 {} 占位符），不抛错"""
        records, add = capture_records
        add()
        Logger("x").info("ratio %s {bad", "x")
        assert _messages(records) == ["ratio x {bad"]

    def test_lone_brace_without_percent_degrades_safely(self, capture_records):
        """issue #12：孤立 { 且无 % 占位符但带参数时，loguru 内部
        str.format 抛错被捕获，降级为无参输出消息本体，不抛错"""
        records, add = capture_records
        add()
        Logger("x").info("oops {bad", "x")
        assert _messages(records) == ["oops {bad"]

    def test_brace_format_failure_degrades_safely(self, capture_records):
        """issue #12：{} 路径下 str.format 仍可能失败（如孤立 { 与 {} 共存），
        捕获后降级输出消息本体，不抛错"""
        records, add = capture_records
        add()
        Logger("x").info("{bad and {}", "x")
        assert _messages(records) == ["{bad and {}"]

    def test_percent_s_on_raw_loguru_logger_unchanged(self, capture_records):
        """裸 loguru logger 不受 issue #12 影响：仍原样输出 %s，参数被丢弃"""
        records, add = capture_records
        add()
        loguru_logger.info("value=%s", 123)
        assert _messages(records) == ["value=%s"]

    def test_raw_loguru_brace_chars_still_raise(self, capture_records):
        """裸 loguru logger 不受 issue #12 影响：含未转义 { } 且带参数时
        str.format() 异常仍直接传播"""
        records, add = capture_records
        add()
        with pytest.raises((ValueError, KeyError, IndexError)):
            loguru_logger.info("ratio %s {bad", "x")

    def test_message_without_args_not_formatted(self, capture_records):
        """无参数时不做任何格式化，含 % 或 {} 的消息原样输出（Logger 类与裸 loguru 一致）"""
        records, add = capture_records
        add()
        loguru_logger.info("progress 100% {not-a-placeholder}")
        Logger("x").info("progress 100% {not-a-placeholder}")
        assert _messages(records) == [
            "progress 100% {not-a-placeholder}",
            "progress 100% {not-a-placeholder}",
        ]


class TestBracePathNoRegression:
    """issue #12 回归保护：{} 路径与命名 kwargs 用法保持不变"""

    def test_logger_brace_positional_not_regressed(self, capture_records):
        records, add = capture_records
        add()
        Logger("x").info("count={} name={}", 42, "svc")
        assert _messages(records) == ["count=42 name=svc"]

    def test_logger_named_kwargs_not_regressed(self, capture_records):
        """命名 {name} 占位符通过 kwargs 透传给 loguru"""
        records, add = capture_records
        add()
        Logger("x").info("hello {who}", who="myboot")
        assert _messages(records) == ["hello myboot"]

    def test_logger_named_kwargs_with_positional_args(self, capture_records):
        """位置 {} 与命名 {name} 混用时 args/kwargs 均透传"""
        records, add = capture_records
        add()
        Logger("x").info("{} meets {who}", "alice", who="bob")
        assert _messages(records) == ["alice meets bob"]


# ---------------------------------------------------------------------------
# 6. JSON 日志格式
# ---------------------------------------------------------------------------


class TestJsonLogging:
    def test_setup_logging_json_outputs_valid_json(self, tmp_path, capsys, clean_myboot_env):
        cfg = _make_config(tmp_path, "logging:\n  level: INFO\n  json: true\n")
        setup_logging(config=cfg)

        loguru_logger.info("json hello")
        out = capsys.readouterr().out.strip()

        parsed = json.loads(out)
        assert parsed["record"]["message"] == "json hello"
        assert parsed["record"]["level"]["name"] == "INFO"
        # setup_logging 配置了全局 extra worker（单进程为 "Main"）
        assert parsed["record"]["extra"]["worker"] == "Main"
        # serialize=True 时同时包含人类可读的 text 字段
        assert "json hello" in parsed["text"]

    def test_json_config_accepts_string_true(self, tmp_path, capsys, clean_myboot_env):
        """logging.json 为字符串 "true" 时同样启用 JSON（_parse_json_config）"""
        cfg = _make_config(tmp_path, 'logging:\n  level: INFO\n  json: "true"\n')
        setup_logging(config=cfg)

        loguru_logger.info("string-true json")
        out = capsys.readouterr().out.strip()
        assert json.loads(out)["record"]["message"] == "string-true json"

    def test_parse_json_config_truth_table(self):
        """固化 _parse_json_config 的当前真值表"""
        assert _parse_json_config(True) is True
        assert _parse_json_config(False) is False
        assert _parse_json_config("true") is True
        assert _parse_json_config("TRUE") is True
        assert _parse_json_config("1") is True
        assert _parse_json_config("yes") is True
        assert _parse_json_config("on") is True
        assert _parse_json_config("false") is False
        assert _parse_json_config("off") is False
        # 可疑现状：任意其他字符串（如 "enabled"）一律为 False
        assert _parse_json_config("enabled") is False
        # 可疑现状：非 bool/str 走 bool() 转换，整数 1 为 True
        assert _parse_json_config(1) is True
        assert _parse_json_config(0) is False
        assert _parse_json_config(None) is False


# ---------------------------------------------------------------------------
# 7. 第三方库日志级别控制
# ---------------------------------------------------------------------------


class TestThirdPartyLoggerControl:
    def test_configure_third_party_loggers_sets_std_logging_level(self):
        name = "myboot_test_3rdparty_a"
        try:
            _configure_third_party_loggers({name: "WARNING"})
            assert logging.getLogger(name).level == logging.WARNING

            _configure_third_party_loggers({name: "error"})  # 大小写不敏感
            assert logging.getLogger(name).level == logging.ERROR
        finally:
            logging.getLogger(name).setLevel(logging.NOTSET)

    def test_unknown_level_falls_back_to_info(self):
        """可疑现状：无效级别名静默回退为 INFO（getattr 默认值），不报错"""
        name = "myboot_test_3rdparty_b"
        try:
            _configure_third_party_loggers({name: "NOT_A_LEVEL"})
            assert logging.getLogger(name).level == logging.INFO
        finally:
            logging.getLogger(name).setLevel(logging.NOTSET)

    def test_non_dict_and_non_str_values_ignored(self):
        name = "myboot_test_3rdparty_c"
        try:
            # 非 dict 输入：直接 return，不抛错
            _configure_third_party_loggers("not-a-dict")
            _configure_third_party_loggers(None)
            # 可疑现状：级别值非字符串（如 int 30）被静默忽略
            _configure_third_party_loggers({name: 30})
            assert logging.getLogger(name).level == logging.NOTSET
        finally:
            logging.getLogger(name).setLevel(logging.NOTSET)

    def test_setup_logging_applies_third_party_config(self, tmp_path, capsys, clean_myboot_env):
        name = "myboot_test_3rdparty_d"
        cfg = _make_config(
            tmp_path,
            f"logging:\n  level: INFO\n  third_party:\n    {name}: ERROR\n",
        )
        try:
            setup_logging(config=cfg)
            assert logging.getLogger(name).level == logging.ERROR
        finally:
            logging.getLogger(name).setLevel(logging.NOTSET)


# ---------------------------------------------------------------------------
# setup_logging 整体行为
# ---------------------------------------------------------------------------


class TestSetupLogging:
    def test_respects_configured_level(self, tmp_path, capsys, clean_myboot_env):
        cfg = _make_config(tmp_path, "logging:\n  level: ERROR\n")
        setup_logging(config=cfg)

        loguru_logger.info("info-suppressed-xyz")
        loguru_logger.error("error-shown-xyz")
        out = capsys.readouterr().out
        assert "info-suppressed-xyz" not in out
        assert "error-shown-xyz" in out

    def test_removes_preexisting_handlers(self, tmp_path, capsys, clean_myboot_env):
        """setup_logging 调用 loguru_logger.remove() 清掉之前的所有 sink"""
        before = []
        loguru_logger.add(lambda m: before.append(m), format="{message}")

        cfg = _make_config(tmp_path, "logging:\n  level: INFO\n")
        setup_logging(config=cfg)

        loguru_logger.info("after-setup-xyz")
        assert before == []  # 之前的 sink 已被移除，收不到任何消息
        assert "after-setup-xyz" in capsys.readouterr().out

    def test_single_worker_uses_simple_format_without_worker_tag(
        self, tmp_path, capsys, clean_myboot_env
    ):
        """无 MYBOOT_WORKER_COUNT 环境变量时使用简单格式，不含 worker 标识"""
        cfg = _make_config(tmp_path, "logging:\n  level: INFO\n")
        setup_logging(config=cfg)

        loguru_logger.info("simple-format-xyz")
        out = capsys.readouterr().out
        assert "simple-format-xyz" in out
        assert "Main" not in out
        assert "Worker-" not in out

    def test_multi_worker_env_enables_worker_tag(self, tmp_path, capsys, clean_myboot_env):
        clean_myboot_env.setenv("MYBOOT_WORKER_COUNT", "4")
        clean_myboot_env.setenv("MYBOOT_WORKER_ID", "2")
        cfg = _make_config(tmp_path, "logging:\n  level: INFO\n")
        setup_logging(config=cfg)

        loguru_logger.info("tagged-xyz")
        out = capsys.readouterr().out
        assert "Worker-2/4" in out
        assert "tagged-xyz" in out

    def test_enable_worker_info_false_suppresses_worker_tag(
        self, tmp_path, capsys, clean_myboot_env
    ):
        clean_myboot_env.setenv("MYBOOT_WORKER_COUNT", "4")
        clean_myboot_env.setenv("MYBOOT_WORKER_ID", "2")
        cfg = _make_config(tmp_path, "logging:\n  level: INFO\n")
        setup_logging(config=cfg, enable_worker_info=False)

        loguru_logger.info("untagged-xyz")
        out = capsys.readouterr().out
        assert "Worker-2/4" not in out
        assert "untagged-xyz" in out

    def test_custom_logging_format_converted_to_loguru(self, tmp_path, capsys, clean_myboot_env):
        """logging.format 支持标准 logging 风格 %(...)s 占位符并被转换"""
        cfg = _make_config(
            tmp_path,
            'logging:\n  level: INFO\n  format: "%(levelname)s | %(message)s"\n',
        )
        setup_logging(config=cfg)

        loguru_logger.info("converted-format-xyz")
        out = capsys.readouterr().out
        assert "INFO" in out
        assert "converted-format-xyz" in out

    def test_file_handler_writes_to_configured_path(self, tmp_path, capsys, clean_myboot_env):
        log_file = tmp_path / "logs" / "app.log"
        cfg = _make_config(
            tmp_path,
            f"logging:\n  level: INFO\n  file: {json.dumps(str(log_file))}\n",
        )
        setup_logging(config=cfg)

        loguru_logger.info("written-to-file-xyz")
        # 移除 handler 以确保文件被 flush/关闭后再读取（Windows）
        loguru_logger.remove()

        assert log_file.exists()  # 父目录由 _add_file_handler 自动创建
        content = log_file.read_text(encoding="utf-8")
        assert "written-to-file-xyz" in content


# ---------------------------------------------------------------------------
# configure_worker_logger 与辅助函数
# ---------------------------------------------------------------------------


class TestWorkerContextHelpers:
    def test_configure_worker_logger_sets_extra_worker(self):
        configure_worker_logger(worker_id=3, total_workers=8)

        records = []
        hid = loguru_logger.add(
            lambda m: records.append(m.record), format="{message}", level="DEBUG"
        )
        try:
            loguru_logger.info("ctx check")
        finally:
            loguru_logger.remove(hid)

        assert records[0]["extra"]["worker"] == "Worker-3/8"

    def test_get_worker_info_main_without_env(self, clean_myboot_env):
        assert _get_worker_info() == "Main"

    def test_get_worker_info_with_env(self, clean_myboot_env):
        clean_myboot_env.setenv("MYBOOT_WORKER_ID", "2")
        clean_myboot_env.setenv("MYBOOT_WORKER_COUNT", "4")
        assert _get_worker_info() == "Worker-2/4"

    def test_get_worker_info_requires_both_env_vars(self, clean_myboot_env):
        """可疑现状：只设置 WORKER_ID 而无 WORKER_COUNT 时返回 "Main" """
        clean_myboot_env.setenv("MYBOOT_WORKER_ID", "2")
        assert _get_worker_info() == "Main"


class TestFormatConversion:
    def test_standard_logging_placeholders_converted(self):
        converted = _convert_logging_format_to_loguru(
            "%(asctime)s %(name)s %(levelname)s %(message)s "
            "%(filename)s %(funcName)s %(lineno)d"
        )
        assert converted == (
            "{time:YYYY-MM-DD HH:mm:ss} {name} {level: <8} {message} "
            "{file.name} {function} {line}"
        )

    def test_unknown_placeholders_left_untouched(self):
        """可疑现状：映射表之外的占位符（如 %(thread)d）原样保留，
        进入 loguru format 后会原样输出在每条日志中"""
        assert _convert_logging_format_to_loguru("%(thread)d %(message)s") == (
            "%(thread)d {message}"
        )
