# -*- coding: utf-8 -*-
"""
依赖注入子系统特征测试（characterization tests）

固化 myboot.core.di（container/registry/providers/decorators）与
myboot.core.decorators 中 @service/@client 的「当前实际行为」，
作为后续重构的兼容性闸门。

绝对规则：本文件只断言现状，不修改任何源码。
可疑现状行为均在对应用例中以注释标注（搜索 "可疑现状"）。
"""

import inspect
from typing import Optional

import pytest

from myboot.core.decorators import _camel_to_snake, client, service
from myboot.core.di.container import DependencyContainer
from myboot.core.di.decorators import Provide, get_injectable_params, inject
from myboot.core.di.providers import ServiceProvider
from myboot.core.di.registry import ServiceRegistry

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# 辅助类（前缀避开 Test，防止被 pytest 收集）
# ---------------------------------------------------------------------------

class AlphaService:
    """无依赖的服务"""

    def __init__(self):
        self.tag = "alpha"


class BetaService:
    """通过类型注解依赖 AlphaService"""

    def __init__(self, alpha_service: AlphaService):
        self.alpha_service = alpha_service


class GammaService:
    """通过字符串形式的 Provide 注解依赖 alpha_service"""

    def __init__(self, dep: "Provide['alpha_service']"):
        self.dep = dep


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------
# 说明：DI 子系统没有模块级单例——DependencyContainer / ServiceRegistry 都是
# 普通类，每个测试构造新实例即可天然隔离。container fixture 在 teardown 时
# 仍调用 clear() 以释放 dependency_injector 的 DynamicContainer 状态。


@pytest.fixture
def registry():
    return ServiceRegistry()


@pytest.fixture
def container():
    c = DependencyContainer()
    yield c
    c.clear()


# ---------------------------------------------------------------------------
# 1. 服务注册：registry 中 dependencies / dependents 数据结构
# ---------------------------------------------------------------------------

class TestServiceRegistration:
    def test_register_service_records_class_and_config(self, registry):
        registry.register_service(AlphaService, "alpha_service", {"k": "v"})

        assert registry.services == {"alpha_service": AlphaService}
        assert registry.service_configs == {"alpha_service": {"k": "v"}}
        assert registry.has_service("alpha_service") is True
        assert registry.get_service_class("alpha_service") is AlphaService
        assert registry.get_service_config("alpha_service") == {"k": "v"}

    def test_register_service_default_config_is_empty_dict(self, registry):
        registry.register_service(AlphaService, "alpha_service")
        assert registry.get_service_config("alpha_service") == {}

    def test_no_dependency_service_has_empty_sets(self, registry):
        registry.register_service(AlphaService, "alpha_service")

        assert registry.dependencies == {"alpha_service": set()}
        # register_service 通过 setdefault 为自身建立空 dependents 条目
        assert registry.dependents == {"alpha_service": set()}

    def test_dependency_recorded_in_both_directions(self, registry):
        registry.register_service(AlphaService, "alpha_service")
        registry.register_service(BetaService, "beta_service")

        assert registry.dependencies["beta_service"] == {"alpha_service"}
        assert registry.dependencies["alpha_service"] == set()
        assert registry.dependents["alpha_service"] == {"beta_service"}
        assert registry.dependents["beta_service"] == set()

        assert registry.get_dependencies("beta_service") == {"alpha_service"}
        assert registry.get_dependents("alpha_service") == {"beta_service"}

    def test_dependent_registered_first_creates_dependents_entry_for_unregistered_dep(
        self, registry
    ):
        """先注册依赖方时，被依赖方（尚未注册）的 dependents 条目已被预先写入"""
        registry.register_service(BetaService, "beta_service")

        assert "alpha_service" not in registry.services
        assert registry.dependents["alpha_service"] == {"beta_service"}

    def test_get_dependencies_of_unknown_service_returns_empty_set(self, registry):
        assert registry.get_dependencies("nonexistent") == set()
        assert registry.get_dependents("nonexistent") == set()

    def test_reregister_resets_then_reanalyzes_dependencies(self, registry):
        """可疑现状：register_service 无条件重置 dependencies[service_name]，
        随后重新分析；对 dependents 中旧条目不做清理（只增不减）。"""
        registry.register_service(BetaService, "beta_service")
        registry.register_service(BetaService, "beta_service")

        assert registry.dependencies["beta_service"] == {"alpha_service"}
        assert registry.dependents["alpha_service"] == {"beta_service"}


# ---------------------------------------------------------------------------
# 2. issue #9 回归测试：注册不得清空已记录的反向依赖（dependents）
# ---------------------------------------------------------------------------

class TestIssue9DependentsNotOverwritten:
    """issue #9：register_service 曾无条件 `self.dependents[name] = set()`，
    后注册的服务会清掉先前分析出的 dependents。
    当前代码（registry.py:40）已改为 setdefault —— bug 已修复，以下断言修复行为。
    """

    def test_registering_dependency_after_dependent_preserves_dependents(
        self, registry
    ):
        # 先注册 B（依赖 A）：此时分析出 A 的 dependents 含 B
        registry.register_service(BetaService, "beta_service")
        assert registry.dependents["alpha_service"] == {"beta_service"}

        # 再注册 A：A 的 dependents 中 B 必须仍然存在（修复后行为）
        registry.register_service(AlphaService, "alpha_service")
        assert "beta_service" in registry.dependents["alpha_service"], (
            "issue #9 回归：注册 alpha_service 清空了已记录的 dependents"
        )
        assert registry.dependents["alpha_service"] == {"beta_service"}

    def test_initialization_order_correct_when_dependent_registered_first(
        self, registry
    ):
        """dependents 保留后，拓扑排序能把 A 排在 B 之前"""
        registry.register_service(BetaService, "beta_service")
        registry.register_service(AlphaService, "alpha_service")

        order = registry.get_initialization_order()
        assert order == ["alpha_service", "beta_service"]

    def test_injection_works_when_dependent_registered_first(self, container):
        """端到端：先注册 B 再注册 A，构建容器后 B 仍能注入 A"""
        container.register_service(BetaService, "beta_service")
        container.register_service(AlphaService, "alpha_service")
        container.build_container()

        beta = container.get_service("beta_service")
        alpha = container.get_service("alpha_service")
        assert isinstance(beta.alpha_service, AlphaService)
        assert beta.alpha_service is alpha


# ---------------------------------------------------------------------------
# 3. 循环依赖检测
# ---------------------------------------------------------------------------

class CycleServiceA:
    def __init__(self, dep: "Provide['cycle_b']"):
        self.dep = dep


class CycleServiceB:
    def __init__(self, dep: "Provide['cycle_a']"):
        self.dep = dep


class TestCircularDependencyDetection:
    def _register_cycle(self, registry):
        registry.register_service(CycleServiceA, "cycle_a")
        registry.register_service(CycleServiceB, "cycle_b")

    def test_detect_circular_dependencies_returns_cycle_chain(self, registry):
        self._register_cycle(registry)
        cycles = registry.detect_circular_dependencies()

        # DFS 按注册顺序从 cycle_a 出发，环以起点收尾
        assert cycles == [["cycle_a", "cycle_b", "cycle_a"]]

    def test_detect_returns_empty_list_when_no_cycle(self, registry):
        registry.register_service(AlphaService, "alpha_service")
        registry.register_service(BetaService, "beta_service")
        assert registry.detect_circular_dependencies() == []

    def test_register_alone_does_not_raise_on_cycle(self, registry):
        """注册阶段不检测循环——只有取初始化顺序/构建容器时才报"""
        self._register_cycle(registry)  # 不抛异常即通过

    def test_get_initialization_order_raises_value_error_with_cycle_chain(
        self, registry
    ):
        self._register_cycle(registry)
        with pytest.raises(ValueError) as exc_info:
            registry.get_initialization_order()

        msg = str(exc_info.value)
        assert "检测到循环依赖" in msg
        assert "cycle_a -> cycle_b -> cycle_a" in msg

    def test_build_container_propagates_cycle_error(self):
        container = DependencyContainer()
        try:
            container.register_service(CycleServiceA, "cycle_a")
            container.register_service(CycleServiceB, "cycle_b")
            with pytest.raises(ValueError, match="检测到循环依赖"):
                container.build_container()
        finally:
            container.clear()

    def test_dependency_graph_is_cached_and_goes_stale(self, registry):
        """可疑现状：build_dependency_graph 结果被缓存，且之后的
        register_service 不会使缓存失效——后注册的服务不会出现在图中，
        只有 clear() 才重置缓存。"""
        registry.register_service(AlphaService, "alpha_service")
        graph1 = registry.build_dependency_graph()
        assert graph1 == {"alpha_service": set()}

        registry.register_service(BetaService, "beta_service")
        graph2 = registry.build_dependency_graph()
        assert "beta_service" not in graph2  # 缓存陈旧（现状如此）
        assert graph2 is graph1


# ---------------------------------------------------------------------------
# 4. 单例语义
# ---------------------------------------------------------------------------

class TestSingletonSemantics:
    def test_singleton_get_twice_returns_same_instance(self, container):
        container.register_service(AlphaService, "alpha_service")
        container.build_container()

        first = container.get_service("alpha_service")
        second = container.get_service("alpha_service")
        assert first is second
        # 单例实例被缓存在 service_instances
        assert container.service_instances["alpha_service"] is first

    def test_default_scope_is_singleton(self, container):
        container.register_service(AlphaService, "alpha_service")
        assert (
            container.service_providers["alpha_service"].scope
            == ServiceProvider.SINGLETON
        )

    def test_factory_scope_returns_new_instance_each_time(self, container):
        container.register_service(
            AlphaService, "alpha_service", scope=ServiceProvider.FACTORY
        )
        container.build_container()

        first = container.get_service("alpha_service")
        second = container.get_service("alpha_service")
        assert first is not second
        # 工厂模式不写入 service_instances 缓存
        assert "alpha_service" not in container.service_instances

    def test_singleton_dependency_shared_across_consumers(self, container):
        container.register_service(AlphaService, "alpha_service")
        container.register_service(BetaService, "beta_service")
        container.register_service(GammaService, "gamma_service")
        container.build_container()

        beta = container.get_service("beta_service")
        gamma = container.get_service("gamma_service")
        alpha = container.get_service("alpha_service")
        assert beta.alpha_service is alpha
        assert gamma.dep is alpha

    def test_get_unregistered_service_raises_key_error(self, container):
        with pytest.raises(KeyError, match="未注册"):
            container.get_service("nope")

    def test_get_registered_but_unbuilt_singleton_raises_runtime_error(
        self, container
    ):
        """可疑现状：注册后未 build_container 就 get，报 RuntimeError
        （提供者未配置），而不是更友好的提示。"""
        container.register_service(AlphaService, "alpha_service")
        with pytest.raises(RuntimeError, match="提供者未正确配置"):
            container.get_service("alpha_service")

    def test_register_instance_returns_exact_object(self, container):
        sentinel = AlphaService()
        container.register_instance("external_alpha", sentinel)

        assert container.get_service("external_alpha") is sentinel
        assert container.has_service("external_alpha") is True
        assert "external_alpha" in container.registry.known_instances


# ---------------------------------------------------------------------------
# 5. @service / @client 装饰器元数据
# ---------------------------------------------------------------------------

class TestServiceClientDecorators:
    def test_service_decorator_sets_metadata_with_snake_case_name(self):
        @service()
        class OrderService:
            pass

        assert OrderService.__myboot_service__ == {
            "name": "order_service",
            "kwargs": {},
        }

    def test_service_decorator_explicit_name_and_kwargs(self):
        @service("custom_name", lazy=True)
        class OrderService:
            pass

        assert OrderService.__myboot_service__ == {
            "name": "custom_name",
            "kwargs": {"lazy": True},
        }

    def test_service_decorator_returns_same_class(self):
        class PlainService:
            pass

        decorated = service()(PlainService)
        assert decorated is PlainService

    def test_client_decorator_sets_metadata(self):
        @client()
        class RedisClient:
            pass

        assert RedisClient.__myboot_client__ == {
            "name": "redis_client",
            "kwargs": {},
        }

    def test_client_decorator_explicit_name(self):
        @client("db", pool_size=10)
        class DatabaseClient:
            pass

        assert DatabaseClient.__myboot_client__ == {
            "name": "db",
            "kwargs": {"pool_size": 10},
        }

    def test_camel_to_snake_conversion(self):
        assert _camel_to_snake("UserService") == "user_service"
        assert _camel_to_snake("EmailService") == "email_service"
        assert _camel_to_snake("HTTPClient") == "http_client"
        assert _camel_to_snake("DatabaseClient") == "database_client"

    def test_inject_decorator_marks_function(self):
        @inject
        def init(self, dep):
            return dep

        # @wraps 把原函数上的 __myboot_inject__ 拷贝到 wrapper
        assert init.__myboot_inject__ is True
        assert init(None, "x") == "x"

    def test_provide_class_getitem_returns_plain_string(self):
        """Provide['name'] 求值结果就是普通字符串 'name'"""
        assert Provide["user_service"] == "user_service"
        assert isinstance(Provide["user_service"], str)


# ---------------------------------------------------------------------------
# 6. 依赖分析：get_injectable_params 对构造参数/注解的解析
# ---------------------------------------------------------------------------

class TestDependencyAnalysis:
    def test_self_is_skipped(self):
        params = get_injectable_params(AlphaService.__init__)
        assert "self" not in params
        assert params == {}

    def test_class_type_annotation_maps_to_snake_case_service_name(self):
        params = get_injectable_params(BetaService.__init__)
        info = params["alpha_service"]
        assert info["type"] is AlphaService
        assert info["service_name"] == "alpha_service"
        assert info["is_optional"] is False
        assert info["default"] is None

    def test_optional_annotation_unwraps_type_and_marks_optional(self):
        class Consumer:
            def __init__(self, dep: Optional[AlphaService] = None):
                self.dep = dep

        info = get_injectable_params(Consumer.__init__)["dep"]
        assert info["type"] is AlphaService
        assert info["service_name"] == "alpha_service"
        assert info["is_optional"] is True
        assert info["default"] is None

    def test_string_provide_annotation_extracts_service_name(self):
        """注解写成字符串 "Provide['x']" 时按字面解析出服务名"""
        info = get_injectable_params(GammaService.__init__)["dep"]
        assert info["service_name"] == "alpha_service"
        assert info["type"] is None
        assert info["is_optional"] is False

    def test_actual_provide_subscript_does_not_resolve_service_name(self):
        """可疑现状：直接写 Provide['alpha_service']（不加引号，如
        di/decorators.py 文档示例所写）时，注解在类定义时即被求值为
        普通字符串 'alpha_service'，不匹配 "Provide['..." 前缀，
        最终 service_name 为 None（即文档示例写法实际不生效）。"""

        class Consumer:
            def __init__(self, dep: Provide["alpha_service"]):
                self.dep = dep

        info = get_injectable_params(Consumer.__init__)["dep"]
        assert info["service_name"] is None
        assert info["type"] == "alpha_service"  # 残留为字符串

    def test_forward_reference_string_annotation_not_resolved(self):
        """可疑现状：普通字符串前向引用注解（'AlphaService'）不会被解析
        为服务名——字符串既不匹配 Provide 前缀也没有 __name__。"""

        class Consumer:
            def __init__(self, dep: "AlphaService"):
                self.dep = dep

        info = get_injectable_params(Consumer.__init__)["dep"]
        assert info["service_name"] is None
        assert info["type"] == "AlphaService"

    def test_unannotated_param_yields_no_service_name(self):
        class Consumer:
            def __init__(self, plain):
                self.plain = plain

        info = get_injectable_params(Consumer.__init__)["plain"]
        assert info["service_name"] is None
        assert info["type"] is inspect.Parameter.empty
        assert info["is_optional"] is False
        assert info["default"] is None

    def test_default_value_marks_optional_and_keeps_default(self):
        class Consumer:
            def __init__(self, count=5):
                self.count = count

        info = get_injectable_params(Consumer.__init__)["count"]
        assert info["is_optional"] is True
        assert info["default"] == 5

    def test_registry_analysis_uses_type_annotations(self, registry):
        """registry 的依赖分析与 get_injectable_params 一致：
        类型注解 -> snake_case 服务名"""

        class ReportService:
            def __init__(self, alpha_service: AlphaService, untyped=None):
                pass

        registry.register_service(ReportService, "report_service")
        # 只有可解析出服务名的参数才进入依赖集合
        assert registry.dependencies["report_service"] == {"alpha_service"}

    def test_registry_analysis_with_string_provide(self, registry):
        registry.register_service(GammaService, "gamma_service")
        assert registry.dependencies["gamma_service"] == {"alpha_service"}

    def test_unregistered_dependency_only_warns_not_raises(self, registry):
        """依赖的服务从未注册时，build/排序只告警不报错，缺失服务被
        当作图外节点（现状行为）"""
        registry.register_service(BetaService, "beta_service")
        order = registry.get_initialization_order()
        assert order == ["beta_service"]
