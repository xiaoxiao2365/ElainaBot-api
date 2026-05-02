"""API 调用插件 — 可视化配置、链式请求、条件响应"""

__plugin_meta__ = {
    'name': 'API调用器',
    'author': '洛',
    'description': '可视化API调用插件，支持指令触发、链式请求、条件响应解析',
    'version': '1.0.0',
    'github': 'https://github.com/xiaoxiao2365/ElainaBot-api',
}

from core.plugin.decorators import on_load, on_unload
from core.base.logger import get_logger, PLUGIN

from plugins.api_caller.app import handler as _handler      # noqa: F401
from plugins.api_caller.app import web_page as _web_page    # noqa: F401

log = get_logger(PLUGIN, "API调用器")


@on_load
def _on_load():
    log.info("✅ API调用器插件已加载")


@on_unload
def _on_unload():
    log.info("API调用器插件已卸载")
