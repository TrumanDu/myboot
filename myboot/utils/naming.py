"""
命名转换工具
"""

import re


def camel_to_snake(name: str) -> str:
    """
    将驼峰命名转换为下划线分隔的小写形式

    Args:
        name: 类名（驼峰命名）

    Returns:
        下划线分隔的小写字符串

    Examples:
        UserService -> user_service
        EmailService -> email_service
        DatabaseClient -> database_client
        HTTPClient -> http_client
    """
    # 在大写字母前插入下划线（除了第一个字符）
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # 处理连续大写字母的情况（如 HTTPClient）
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    # 转换为小写
    return s2.lower()
