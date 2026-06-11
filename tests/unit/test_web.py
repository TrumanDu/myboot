"""
Web 层特征测试（characterization tests）

固化 myboot web 层的「当前实际行为」，作为后续重构的兼容性闸门。
这些测试不评判行为是否正确——可疑之处照实断言并以注释标记，
重构时若行为有意变更，应同步更新对应测试。

覆盖范围：
- myboot/web/decorators.py 路由装饰器元数据
- myboot/core/decorators.py 路由装饰器与 @rest_controller（auto_configuration 实际消费的那套）
- myboot/core/auto_configuration.py 的 AST 扫描 route_type 前缀（issue #8 回归）与路由注册分支
- myboot/web/models.py BaseResponse 系列模型
- myboot/web/response.py ResponseWrapper / ApiResponse
- myboot/web/exceptions.py HTTP 异常体系
- myboot/exceptions.py 与 myboot/web/exceptions.py 的同名异常类二义性
"""

import asyncio
import textwrap
from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from myboot.core.auto_configuration import (
    AutoConfigurationError,
    AutoConfigurationManager,
)
from myboot.core.decorators import delete as core_delete
from myboot.core.decorators import get as core_get
from myboot.core.decorators import patch as core_patch
from myboot.core.decorators import post as core_post
from myboot.core.decorators import put as core_put
from myboot.core.decorators import rest_controller
from myboot.core.decorators import route as core_route
from myboot.web.decorators import (
    async_route,
    delete,
    get,
    patch,
    post,
    put,
    route,
)
from myboot.web.exceptions import (
    HTTP_STATUS_EXCEPTIONS,
    BadRequestError,
    ConflictError,
    ForbiddenError,
    HTTPException,
    InternalServerError,
    MethodNotAllowedError,
    NotFoundError,
    ServiceUnavailableError,
    TooManyRequestsError,
    UnauthorizedError,
    UnprocessableEntityError,
    create_http_exception,
)
from myboot.web.models import (
    BaseResponse,
    CreatedResponse,
    DeletedResponse,
    ErrorResponse,
    SuccessResponse,
    UpdatedResponse,
    error_response,
    success_response,
)
from myboot.web.response import ApiResponse, ResponseWrapper


def _make_manager() -> AutoConfigurationManager:
    """每个测试创建独立的 manager，避免共享 discovered_components 状态。

    AutoConfigurationManager 的组件元数据是实例级状态（非模块级全局
    registry），use_cache=False 确保不读写磁盘缓存。
    """
    return AutoConfigurationManager(app_root=".", use_cache=False)


# ---------------------------------------------------------------------------
# 1. myboot/web/decorators.py 路由装饰器元数据
# ---------------------------------------------------------------------------


class TestWebRouteDecorators:
    """myboot.web.decorators 装饰器把元数据写入 func._route_metadata"""

    def test_get_decorator_metadata(self):
        @get("/users", tags=["user"])
        def handler():
            """获取用户列表"""
            return []

        meta = handler._route_metadata
        assert meta["path"] == "/users"
        assert meta["methods"] == ["GET"]
        assert meta["status_code"] == 200
        assert meta["tags"] == ["user"]
        # summary/description 未显式提供时回退到 __doc__
        assert meta["summary"] == "获取用户列表"
        assert meta["description"] == "获取用户列表"
        assert meta["response_model"] is None

    def test_default_status_codes_per_method(self):
        # 当前行为：post 默认 201，delete 默认 204，get/put/patch 默认 200
        cases = [
            (get, ["GET"], 200),
            (post, ["POST"], 201),
            (put, ["PUT"], 200),
            (delete, ["DELETE"], 204),
            (patch, ["PATCH"], 200),
        ]
        for decorator, methods, status_code in cases:
            @decorator("/x")
            def handler():
                return None

            meta = handler._route_metadata
            assert meta["methods"] == methods
            assert meta["status_code"] == status_code

    def test_route_decorator_defaults_and_extra_kwargs(self):
        @route("/multi", methods=["GET", "POST"], deprecated=True)
        def handler():
            return None

        meta = handler._route_metadata
        assert meta["methods"] == ["GET", "POST"]
        # 额外 kwargs 被直接展开合并进元数据字典
        assert meta["deprecated"] is True

        @route("/default")
        def handler2():
            return None

        # methods 缺省为 ["GET"]
        assert handler2._route_metadata["methods"] == ["GET"]

    def test_summary_falls_back_to_empty_string_without_docstring(self):
        @get("/nodoc")
        def handler():
            return None

        meta = handler._route_metadata
        assert meta["summary"] == ""
        assert meta["description"] == ""
        assert meta["tags"] == []

    def test_decorator_returns_wrapper_that_still_works(self):
        # 当前行为：装饰器返回 @wraps 包装函数（非原函数），
        # functools.wraps 复制 __dict__，因此包装函数上也带 _route_metadata
        @get("/calc")
        def add(a, b):
            return a + b

        assert add(1, 2) == 3
        assert add.__name__ == "add"
        assert add._route_metadata["path"] == "/calc"

    @pytest.mark.asyncio
    async def test_async_route_metadata_and_await(self):
        @async_route("/async", methods=["POST"])
        async def handler(value):
            return value * 2

        meta = handler._route_metadata
        assert meta["path"] == "/async"
        assert meta["methods"] == ["POST"]
        # async_route 额外写入 'async': True 标记
        assert meta["async"] is True
        assert await handler(21) == 42
        assert asyncio.iscoroutinefunction(handler)


# ---------------------------------------------------------------------------
# 1b. myboot/core/decorators.py 路由装饰器（auto_configuration 消费的元数据）
# ---------------------------------------------------------------------------


class TestCoreRouteDecorators:
    """myboot.core.decorators 装饰器把元数据写入 func.__myboot_route__"""

    def test_core_get_sets_myboot_route_and_returns_original(self):
        def raw(a):
            return a

        decorated = core_get("/ping", tags=["t"])(raw)
        # 当前行为：core 装饰器不包装，直接返回原函数对象
        assert decorated is raw
        assert raw.__myboot_route__ == {
            "path": "/ping",
            "methods": ["GET"],
            "kwargs": {"tags": ["t"]},
        }

    def test_core_http_method_decorators(self):
        cases = [
            (core_get, ["GET"]),
            (core_post, ["POST"]),
            (core_put, ["PUT"]),
            (core_delete, ["DELETE"]),
            (core_patch, ["PATCH"]),
        ]
        for decorator, methods in cases:
            @decorator("/x")
            def handler():
                return None

            assert handler.__myboot_route__["methods"] == methods

    def test_core_route_default_methods(self):
        @core_route("/r")
        def handler():
            return None

        assert handler.__myboot_route__["methods"] == ["GET"]
        assert handler.__myboot_route__["kwargs"] == {}

    def test_rest_controller_sets_class_attribute(self):
        @rest_controller("/api/items/", version=1)
        class ItemController:
            pass

        config = ItemController.__myboot_rest_controller__
        # base_path 尾部 / 被 rstrip 去掉
        assert config["base_path"] == "/api/items"
        assert config["kwargs"] == {"version": 1}


# ---------------------------------------------------------------------------
# 2. route_type 前缀格式（issue #8 回归）与路由注册分支
# ---------------------------------------------------------------------------


class TestRouteTypeDiscovery:
    """AST 扫描产生的 type 字段格式：f'function_{dec}' / f'class_{dec}'"""

    def test_ast_scan_route_type_prefixes(self, tmp_path):
        # issue #8 回归：发现阶段的 type 必须以 function_ / class_ 开头
        source = textwrap.dedent(
            """
            from myboot.core.decorators import rest_controller, get, route

            @rest_controller('/api/x')
            class XController:
                pass

            @get('/ping')
            def ping():
                return 'pong'

            @route('/r')
            def plain_route():
                return 'r'
            """
        )
        mod_file = tmp_path / "scanned_mod.py"
        mod_file.write_text(source, encoding="utf-8")

        manager = _make_manager()
        manager._scan_file_ast(mod_file, "scanned_mod")

        controllers = manager._component_metadata["rest_controllers"]
        assert controllers == [
            {
                "module": "scanned_mod",
                "class_name": "XController",
                "type": "class_rest_controller",
            }
        ]

        routes = manager._component_metadata["routes"]
        types = {item["func_name"]: item["type"] for item in routes}
        assert types == {
            "ping": "function_get",
            "plain_route": "function_route",
        }
        for item in routes:
            assert item["type"].startswith("function_") or item["type"].startswith(
                "class_"
            )

    def test_function_route_type_is_registered(self):
        @core_route("/fr", methods=["GET"])
        def fr():
            return "x"

        calls = []

        class FakeApp:
            def add_route(self, **kwargs):
                calls.append(kwargs)

        manager = _make_manager()
        manager.discovered_components["routes"] = [
            {"type": "function_route", "function": fr, "module": "m"}
        ]
        manager._auto_register_routes(FakeApp())

        assert len(calls) == 1
        assert calls[0]["path"] == "/fr"
        assert calls[0]["handler"] is fr
        assert calls[0]["methods"] == ["GET"]

    def test_function_get_route_type_registers(self):
        # 0.2.0 修复（issue #8 残留）：_auto_register_routes 现以前缀匹配
        # route_type，AST 扫描产生的 'function_get'/'function_post' 等
        # 模块级函数路由不再被静默跳过。
        @core_get("/fn")
        def fn():
            return "x"

        calls = []

        class FakeApp:
            def add_route(self, **kwargs):
                calls.append(kwargs)

        manager = _make_manager()
        manager.discovered_components["routes"] = [
            {"type": "function_get", "function": fn, "module": "m"}
        ]
        manager._auto_register_routes(FakeApp())

        assert len(calls) == 1
        assert calls[0]["path"] == "/fn"
        assert calls[0]["methods"] == ["GET"]

    def test_class_route_type_registers_methods_without_error(self):
        # 0.2.0 修复：class 类型条目没有 'function' 键，旧版收尾 debug 日志
        # 访问 route_info['function'] 抛 KeyError 导致注册必然失败；
        # 现在方法路由注册成功且不再抛 AutoConfigurationError。
        @core_route("/cls")
        class ClassController:
            @core_get("/m")
            def m(self):
                return 1

        calls = []

        class FakeApp:
            def add_route(self, **kwargs):
                calls.append(kwargs)

        manager = _make_manager()
        manager.discovered_components["routes"] = [
            {"type": "class_route", "class": ClassController, "module": "m"}
        ]
        manager._auto_register_routes(FakeApp())

        assert len(calls) == 1
        assert calls[0]["path"] == "/m"


class TestRestControllerEndToEnd:
    """用最小 FastAPI app + TestClient 验证 @rest_controller 注册后端点可访问

    myboot 的注册逻辑只依赖 app.add_route 和 app.di_container，
    用桩对象把 add_route 转接到 FastAPI 即可隔离全局 Application 状态。
    """

    def _build_client(self):
        @rest_controller("/api/items")
        class ItemController:
            def __init__(self):
                pass

            @core_get("/list")
            def list_items(self):
                return {"items": [1, 2]}

            @core_get("//absolute")
            def absolute(self):
                return {"abs": True}

            @core_get("relative")
            def rel(self):
                return {"rel": True}

        fastapi_app = FastAPI()

        class FakeApp:
            def __init__(self):
                self.di_container = SimpleNamespace(has_service=lambda name: False)
                self.components = {}
                self.clients = {}

            def add_route(self, path, handler, methods, **kwargs):
                fastapi_app.add_api_route(path, handler, methods=methods)

        manager = _make_manager()
        manager.discovered_components["rest_controllers"] = [
            {
                "class": ItemController,
                "module": "tests.unit.test_web",
                "type": "class_rest_controller",
            }
        ]
        manager._auto_register_rest_controllers(FakeApp())
        return TestClient(fastapi_app)

    def test_method_path_appended_to_base_path(self):
        client = self._build_client()
        resp = client.get("/api/items/list")
        assert resp.status_code == 200
        assert resp.json() == {"items": [1, 2]}

    def test_double_slash_prefix_means_absolute_path(self):
        # 路径合并规则：方法路径以 // 开头时作为绝对路径（去掉一个 /），
        # 不拼接 base_path
        client = self._build_client()
        resp = client.get("/absolute")
        assert resp.status_code == 200
        assert resp.json() == {"abs": True}
        # base_path 下不存在该路由
        assert client.get("/api/items/absolute").status_code == 404

    def test_relative_path_appended_with_slash(self):
        client = self._build_client()
        resp = client.get("/api/items/relative")
        assert resp.status_code == 200
        assert resp.json() == {"rel": True}


# ---------------------------------------------------------------------------
# 3. BaseResponse 响应结构
# ---------------------------------------------------------------------------


class TestBaseResponse:
    def test_defaults(self):
        r = BaseResponse()
        assert r.success is True
        assert r.message == "操作成功"
        assert r.data is None
        assert isinstance(r.timestamp, datetime)

    def test_model_dump_field_names(self):
        dumped = BaseResponse().model_dump()
        # 字段名固定为这 4 个
        assert sorted(dumped.keys()) == ["data", "message", "success", "timestamp"]
        assert dumped["success"] is True
        assert dumped["message"] == "操作成功"
        # 当前行为：data=None 也会出现在 model_dump 结果中
        assert dumped["data"] is None
        assert isinstance(dumped["timestamp"], datetime)

    def test_error_response_defaults(self):
        r = ErrorResponse()
        assert r.success is False
        assert r.message == "操作失败"
        assert r.error_code is None
        assert r.details is None
        dumped = r.model_dump()
        assert sorted(dumped.keys()) == [
            "data",
            "details",
            "error_code",
            "message",
            "success",
            "timestamp",
        ]

    def test_crud_response_defaults(self):
        assert SuccessResponse().message == "操作成功"

        created = CreatedResponse()
        assert created.message == "创建成功"
        # 当前行为：status_code 是模型字段，会出现在序列化结果里
        assert created.status_code == 201
        assert created.model_dump()["status_code"] == 201

        assert UpdatedResponse().status_code == 200
        assert UpdatedResponse().message == "更新成功"

        deleted = DeletedResponse()
        assert deleted.status_code == 204
        assert deleted.message == "删除成功"

    def test_helper_functions(self):
        ok = success_response(data={"id": 1}, message="done")
        assert isinstance(ok, SuccessResponse)
        assert ok.data == {"id": 1}
        assert ok.message == "done"

        err = error_response(message="bad", error_code="E1", details={"k": "v"})
        assert isinstance(err, ErrorResponse)
        assert err.success is False
        assert err.error_code == "E1"
        assert err.details == {"k": "v"}


# ---------------------------------------------------------------------------
# 4. ResponseWrapper 包装行为
# ---------------------------------------------------------------------------


class TestResponseWrapper:
    def test_success_minimal_omits_none_fields(self):
        # message/data 为 None 时根本不出现在返回字典中
        assert ResponseWrapper.success() == {"success": True, "code": 200}

    def test_success_with_data_and_message(self):
        result = ResponseWrapper.success(data=[1], message="ok", code=200)
        assert result == {"success": True, "code": 200, "message": "ok", "data": [1]}

    def test_error_defaults_to_500(self):
        assert ResponseWrapper.error() == {"success": False, "code": 500}
        result = ResponseWrapper.error(message="boom", code=400, data={"f": 1})
        assert result == {
            "success": False,
            "code": 400,
            "message": "boom",
            "data": {"f": 1},
        }

    def test_created_updated_deleted_no_content_codes(self):
        assert ResponseWrapper.created(data={"id": 1}) == {
            "success": True,
            "code": 201,
            "data": {"id": 1},
        }
        assert ResponseWrapper.updated() == {"success": True, "code": 200}
        # 可疑现状：deleted() 的 code 是 200（文档注释也写明 200），
        # 而 DeletedResponse 模型的 status_code 默认 204，两处不一致
        assert ResponseWrapper.deleted() == {"success": True, "code": 200}
        # no_content() 标记 204 但仍返回非空响应体字典
        assert ResponseWrapper.no_content() == {"success": True, "code": 204}

    def test_pagination_structure(self):
        result = ResponseWrapper.pagination(data=[1, 2], total=5, page=1, size=2)
        assert result["success"] is True
        assert result["code"] == 200
        # 分页字段为 camelCase（totalPages/hasNext/hasPrev），
        # 与 PaginationResponse 模型的 snake_case（total_pages 等）不一致
        assert result["data"] == {
            "list": [1, 2],
            "pagination": {
                "total": 5,
                "page": 1,
                "size": 2,
                "totalPages": 3,
                "hasNext": True,
                "hasPrev": False,
            },
        }

    def test_pagination_size_zero_total_pages_zero(self):
        result = ResponseWrapper.pagination(data=[], total=0, page=1, size=0)
        assert result["data"]["pagination"]["totalPages"] == 0

    def test_wrap_plain_data(self):
        assert ResponseWrapper.wrap({"id": 1}) == {
            "success": True,
            "code": 200,
            "data": {"id": 1},
        }
        assert ResponseWrapper.wrap("text", message="m", code=201) == {
            "success": True,
            "code": 201,
            "message": "m",
            "data": "text",
        }

    def test_wrap_passthrough_when_keys_present(self):
        # 当前行为：只要 dict 同时含 "success" 和 "code" 键就原样透传，
        # 即使是手工构造、值类型任意的字典也不会被再包装
        already = {"success": False, "code": 500, "message": "x"}
        assert ResponseWrapper.wrap(already) is already
        fake = {"success": "yes", "code": "200", "extra": 1}
        assert ResponseWrapper.wrap(fake) is fake

    def test_api_response_model_dump_includes_none(self):
        # ConfigDict(exclude_none=True) 在 FastAPI JSON 响应中生效；
        # model_dump() 默认仍包含 None 字段，需显式 exclude_none=True 才会排除
        r = ApiResponse(success=True, code=200)
        assert r.model_dump() == {
            "success": True,
            "code": 200,
            "message": None,
            "data": None,
        }
        assert r.model_dump(exclude_none=True) == {"success": True, "code": 200}


# ---------------------------------------------------------------------------
# 5. HTTP 异常体系
# ---------------------------------------------------------------------------


class TestHTTPExceptions:
    def test_http_status_exceptions_mapping_complete(self):
        # 10 个状态码齐全，映射到对应异常类
        assert HTTP_STATUS_EXCEPTIONS == {
            400: BadRequestError,
            401: UnauthorizedError,
            403: ForbiddenError,
            404: NotFoundError,
            405: MethodNotAllowedError,
            409: ConflictError,
            422: UnprocessableEntityError,
            429: TooManyRequestsError,
            500: InternalServerError,
            503: ServiceUnavailableError,
        }
        assert len(HTTP_STATUS_EXCEPTIONS) == 10

    def test_each_exception_default_status_code_and_message(self):
        expected = {
            BadRequestError: (400, "错误请求"),
            UnauthorizedError: (401, "未授权"),
            ForbiddenError: (403, "禁止访问"),
            NotFoundError: (404, "未找到"),
            MethodNotAllowedError: (405, "方法不允许"),
            ConflictError: (409, "资源冲突"),
            UnprocessableEntityError: (422, "无法处理的实体"),
            TooManyRequestsError: (429, "请求过多"),
            InternalServerError: (500, "内部服务器错误"),
            ServiceUnavailableError: (503, "服务不可用"),
        }
        for exc_class, (status_code, message) in expected.items():
            exc = exc_class()
            assert exc.status_code == status_code
            assert exc.message == message
            # details 缺省为 {}（非 None）
            assert exc.details == {}
            assert isinstance(exc, HTTPException)
            assert str(exc) == message

    def test_exception_with_custom_message_and_details(self):
        exc = NotFoundError(message="用户不存在", details={"user_id": 1})
        assert exc.status_code == 404
        assert exc.message == "用户不存在"
        assert exc.details == {"user_id": 1}

    def test_create_http_exception_unknown_status_returns_base(self):
        exc = create_http_exception(418, "teapot", {"hint": "rfc2324"})
        assert type(exc) is HTTPException
        assert exc.status_code == 418
        assert exc.message == "teapot"
        assert exc.details == {"hint": "rfc2324"}

    def test_create_http_exception_known_status_returns_mapped_subclass(self):
        # 0.2.0 修复：旧版对已知状态码用三个位置参数调用 (message, details)
        # 签名的子类，对全部 10 个已知状态码都抛 TypeError；
        # 现在已知状态码返回对应子类实例
        for status_code, exc_class in HTTP_STATUS_EXCEPTIONS.items():
            exc = create_http_exception(status_code, "msg", {"k": "v"})
            assert isinstance(exc, exc_class)
            assert exc.status_code == status_code
            assert exc.message == "msg"
            assert exc.details == {"k": "v"}


# ---------------------------------------------------------------------------
# 6. myboot/exceptions.py 与 myboot/web/exceptions.py 同名类二义性
# ---------------------------------------------------------------------------


class TestExceptionClassUnification:
    """0.2.0 起两套同名异常收敛为同一个类（以 myboot.exceptions 为准），
    web 路径保留 re-export，import 兼容但身份唯一"""

    def test_validation_error_is_same_class(self):
        from myboot.exceptions import MyBootException
        from myboot.exceptions import ValidationError as CoreValidationError
        from myboot.web.exceptions import ValidationError as WebValidationError

        # 0.2.0：同一个类对象，except 任一路径都能捕获
        assert CoreValidationError is WebValidationError

        exc = WebValidationError(field="name", value="x")
        assert isinstance(exc, MyBootException)
        assert exc.code == "VALIDATION_ERROR"
        assert exc.message == "验证失败"
        assert exc.field == "name"
        assert exc.value == "x"
        # 旧 web 版独有的 error_type 参数已移除（以核心版签名为准）
        assert not hasattr(exc, "error_type")

    def test_configuration_error_is_same_class(self):
        from myboot.exceptions import ConfigurationError as CoreConfigurationError
        from myboot.exceptions import MyBootException
        from myboot.web.exceptions import (
            ConfigurationError as WebConfigurationError,
        )

        assert CoreConfigurationError is WebConfigurationError

        exc = WebConfigurationError(config_key="app.name")
        assert isinstance(exc, MyBootException)
        assert exc.code == "CONFIGURATION_ERROR"
        assert exc.config_key == "app.name"
        assert exc.details == {}
