# 贡献指南

感谢你对 MyBoot 的关注！欢迎通过 Issue 和 Pull Request 参与贡献。

## 开发环境

- Python 3.9+（CI 覆盖 3.9–3.13）
- [uv](https://github.com/astral-sh/uv) 作为包管理器

```bash
git clone https://github.com/TrumanDu/myboot.git
cd myboot
uv sync --extra dev --extra test
```

## 运行测试

```bash
# 全量单元测试
uv run pytest tests/unit -q

# 含覆盖率
uv run pytest tests/unit -q --cov=myboot --cov-report=term-missing

# 集成测试（涉及真实多进程，较慢）
uv run pytest tests/integration -q -m integration
```

提交 PR 前请确保 `tests/unit` 全部通过。CI 会在 ubuntu/windows × Python 3.9–3.13 矩阵上运行。

## 测试约定

- `tests/unit/` 中包含**特征测试**（characterization tests）：固化框架公开行为，作为兼容性闸门。如果你的改动有意变更某个公开行为，请同步更新对应测试并在注释中说明原因（参考现有 `issue #14` 风格的注释）。
- 修复 bug 时请附带回归测试。

## 代码风格

- 遵循现有代码风格（中文 docstring、loguru 日志）
- 格式化：`uv run black myboot tests`，import 排序：`uv run isort myboot tests`
- 公开 API 需带类型注解

## 提交规范

- 提交信息使用 `type: subject` 格式，type 取 `feat` / `fix` / `test` / `docs` / `refactor` / `chore`
- 一个 PR 聚焦一件事；破坏性变更需在 PR 描述中明确标注

## 报告问题

提 Issue 时请附上：

- myboot 版本、Python 版本、操作系统
- 最小复现代码
- 完整报错堆栈（启动时加 `logging.level: DEBUG` 获取更多信息）
