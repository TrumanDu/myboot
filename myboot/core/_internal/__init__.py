"""
auto_configuration 内部实现包

将 auto_configuration.py 的可分离职责拆分到此包，便于维护与测试：
- ast_analyzer: AST 静态分析（解析装饰器、扫描文件/包元数据）
- component_scanner: 模块发现/导入逻辑
- scan_cache: 缓存读写与缓存文件路径计算

注意：本包内的模块不应反向导入 auto_configuration，以避免循环导入。
"""
