"""API调用器 Web 面板页面 — 可视化管理API配置"""

import os
from core.plugin.decorators import on_unload
from core.plugin.web_pages import register_page, unregister_page

_PAGE_KEY = 'api-caller'
_HTML_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'page.html')

register_page(
    key=_PAGE_KEY,
    label='API调用器',
    source='plugin',
    source_name='api_caller',
    html_file=_HTML_FILE,
    icon='🔌',
)


@on_unload
def _unload():
    unregister_page(_PAGE_KEY)
