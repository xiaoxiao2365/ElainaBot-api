"""API 调用核心逻辑 — 指令触发、链式请求、条件响应"""

import os
import re
import ssl
import json
import time
import asyncio
from datetime import datetime
import aiohttp
from core.plugin.decorators import handler
from core.plugin.context import ctx
from core.base.logger import get_logger, PLUGIN

log = get_logger(PLUGIN, "API调用器")

# ==================== 配置管理 ====================

_DEFAULT_CONFIG = {
    'apis': {
        '示例_IP查询': {
            'trigger': r'查ip\s+(.+)',
            'desc': '查询IP地址的地理位置信息',
            'enabled': True,
            'steps': [
                {
                    'name': '查询IP信息',
                    'url': 'http://ip-api.com/json/{input}',
                    'method': 'GET',
                    'headers': {},
                    'params': {},
                    'body': {},
                    'extract': {
                        'country': 'country',
                        'region': 'regionName',
                        'city': 'city',
                        'isp': 'isp',
                        'query_ip': 'query',
                    },
                },
            ],
            'rules': [
                {
                    'condition': 'status == "success"',
                    'field': 'status',
                    'template': 'IP查询结果:\nIP: {query_ip}\n国家: {country}\n地区: {region}\n城市: {city}\nISP: {isp}',
                },
                {
                    'condition': 'status == "fail"',
                    'field': 'status',
                    'template': '查询失败: {message}',
                },
            ],
            'error_reply': '请求出错，请稍后再试',
        },
        '示例_链式请求': {
            'trigger': r'天气\s+(.+)',
            'desc': '链式请求示例（需替换为真实API Key）',
            'enabled': False,
            'steps': [
                {
                    'name': '地理编码',
                    'url': 'https://restapi.amap.com/v3/geocode/geo',
                    'method': 'GET',
                    'headers': {},
                    'params': {
                        'key': '你的高德API_KEY',
                        'address': '{input}',
                    },
                    'body': {},
                    'extract': {
                        'adcode': 'geocodes.0.adcode',
                    },
                },
                {
                    'name': '查询天气',
                    'url': 'https://restapi.amap.com/v3/weather/weatherInfo',
                    'method': 'GET',
                    'headers': {},
                    'params': {
                        'key': '你的高德API_KEY',
                        'city': '{adcode}',
                    },
                    'body': {},
                    'extract': {
                        'weather': 'lives.0.weather',
                        'temperature': 'lives.0.temperature',
                        'winddirection': 'lives.0.winddirection',
                        'windpower': 'lives.0.windpower',
                        'humidity': 'lives.0.humidity',
                        'city_name': 'lives.0.city',
                    },
                },
            ],
            'rules': [
                {
                    'condition': 'http_code == 200',
                    'field': '_http_code',
                    'template': '{city_name} 天气:\n天气: {weather}\n温度: {temperature}°C\n风向: {winddirection}\n风力: {windpower}级\n湿度: {humidity}%',
                },
                {
                    'condition': 'http_code != 200',
                    'field': '_http_code',
                    'template': '天气查询失败',
                },
            ],
            'error_reply': '天气查询出错，请稍后再试',
        },
    },
}

def _config_path():
    """配置文件路径"""
    return ctx.get_data_path('config.json')


def _load_config():
    """加载API配置 (JSON格式)"""
    path = _config_path()
    if not os.path.isfile(path):
        _save_config(_DEFAULT_CONFIG)
        return dict(_DEFAULT_CONFIG)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"加载配置失败: {e}")
        return dict(_DEFAULT_CONFIG)


def _save_config(cfg):
    """保存完整配置"""
    path = _config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def _save_apis(apis):
    """保存API配置"""
    cfg = _load_config()
    cfg['apis'] = apis
    _save_config(cfg)


def _get_apis():
    """获取所有API配置"""
    cfg = _load_config()
    return cfg.get('apis', {})


# ==================== 调用日志 ====================

_MAX_LOG_ENTRIES = 200

def _log_path():
    return ctx.get_data_path('call_logs.json')


def _load_logs():
    path = _log_path()
    if not os.path.isfile(path):
        return []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []


def _save_logs(logs):
    path = _log_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(logs, f, ensure_ascii=False, indent=2)


def _append_log(entry):
    """追加一条调用日志，自动裁剪到最近 _MAX_LOG_ENTRIES 条"""
    logs = _load_logs()
    logs.insert(0, entry)
    if len(logs) > _MAX_LOG_ENTRIES:
        logs = logs[:_MAX_LOG_ENTRIES]
    _save_logs(logs)


# ==================== 数据提取工具 ====================

def _resolve_path(data, path):
    """从字典/列表中按点号路径提取值
    支持: data.key, data.0.key, data.list.0.name
    """
    if not path or data is None:
        return None
    keys = path.split('.')
    current = data
    for key in keys:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, (list, tuple)):
            try:
                current = current[int(key)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def _fill_template(template, variables):
    """用变量字典填充模板中的 {key} 占位符"""
    result = template
    for key, value in variables.items():
        result = result.replace('{' + key + '}', str(value) if value is not None else '')
    return result


# ==================== HTTP 请求执行 ====================

async def _execute_request(step, variables):
    """执行单个HTTP请求步骤，返回 (http_code, response_data, error)"""
    url = _fill_template(step.get('url', ''), variables)
    method = step.get('method', 'GET').upper()
    headers = {}
    for k, v in step.get('headers', {}).items():
        headers[k] = _fill_template(str(v), variables)

    params = {}
    for k, v in step.get('params', {}).items():
        params[k] = _fill_template(str(v), variables)

    body = step.get('body', {})
    if body:
        body_str = _fill_template(json.dumps(body, ensure_ascii=False), variables)
        try:
            body = json.loads(body_str)
        except json.JSONDecodeError:
            body = {}

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    timeout = aiohttp.ClientTimeout(total=15)
    conn = aiohttp.TCPConnector(ssl=ssl_ctx)

    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
            kwargs = {'headers': headers}
            if method == 'GET':
                kwargs['params'] = params
            elif method in ('POST', 'PUT', 'PATCH'):
                if body:
                    kwargs['json'] = body
                elif params:
                    kwargs['json'] = params
            elif method == 'DELETE':
                kwargs['params'] = params

            async with session.request(method, url, **kwargs) as resp:
                http_code = resp.status
                content_type = resp.content_type or ''
                if 'json' in content_type or 'text' in content_type:
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        text = await resp.text()
                        try:
                            data = json.loads(text)
                        except Exception:
                            data = {'_raw_text': text}
                else:
                    data = {'_raw_text': await resp.text()}

                return http_code, data, None
    except asyncio.TimeoutError:
        return 0, {}, '请求超时'
    except Exception as e:
        return 0, {}, str(e)


# ==================== 链式请求执行 ====================

def _build_system_vars(event=None):
    """构建系统内置变量（事件信息 + 时间）"""
    now = datetime.now()
    sys_vars = {
        'user_id': '',
        'group_id': '',
        'channel_id': '',
        'guild_id': '',
        'username': '',
        'appid': '',
        'message_id': '',
        'timestamp': str(int(time.time())),
        'datetime': now.strftime('%Y-%m-%d %H:%M:%S'),
        'date': now.strftime('%Y-%m-%d'),
        'time': now.strftime('%H:%M:%S'),
    }
    if event:
        sys_vars['user_id'] = event.user_id or ''
        sys_vars['group_id'] = event.group_id or ''
        sys_vars['channel_id'] = event.channel_id or ''
        sys_vars['guild_id'] = event.guild_id or ''
        sys_vars['username'] = event.username or ''
        sys_vars['appid'] = event.appid or ''
        sys_vars['message_id'] = event.message_id or ''
    return sys_vars


async def _execute_api(api_cfg, user_input='', extra_vars=None, event=None):
    """执行完整的API调用流程（含链式步骤）
    返回 (success, reply_text, variables, steps_log)
    extra_vars: 额外变量 (如正则捕获组 {g1},{g2}...)
    event: 消息事件对象，用于注入系统变量
    """
    variables = _build_system_vars(event)
    variables['input'] = user_input
    if extra_vars:
        variables.update(extra_vars)
    steps = api_cfg.get('steps', [])
    steps_log = []
    data = {}

    for i, step in enumerate(steps):
        step_name = step.get('name', f'步骤{i + 1}')
        log.info(f"[API调用] 执行: {step_name}")

        http_code, data, error = await _execute_request(step, variables)

        step_info = {
            'name': step_name,
            'url': _fill_template(step.get('url', ''), variables),
            'method': step.get('method', 'GET'),
            'http_code': http_code,
            'response': data,
            'error': error,
        }
        steps_log.append(step_info)

        if error:
            log.warning(f"[API调用] {step_name} 失败: {error}")
            return False, api_cfg.get('error_reply', '请求出错'), variables, steps_log

        # 提取数据到变量池
        variables['_http_code'] = http_code
        variables['_response'] = data
        extract = step.get('extract', {})
        for var_name, json_path in extract.items():
            variables[var_name] = _resolve_path(data, json_path)

        log.info(f"[API调用] {step_name} 完成, HTTP {http_code}, 提取变量: {list(extract.keys())}")

    # 应用响应规则
    rules = api_cfg.get('rules', [])
    for rule in rules:
        if _evaluate_rule(rule, variables, data):
            template = rule.get('template', '')
            if rule.get('ignore', False):
                return True, None, variables, steps_log  # 不回复
            reply = _fill_template(template, variables) if template else rule.get('reply', '')
            return True, reply, variables, steps_log

    # 无规则匹配时，返回原始数据摘要
    summary = json.dumps(data, ensure_ascii=False, indent=2)
    if len(summary) > 500:
        summary = summary[:500] + '\n...(数据过长已截断)'
    return True, f"API返回:\n{summary}", variables, steps_log


def _evaluate_rule(rule, variables, last_response):
    """评估响应规则条件"""
    condition = rule.get('condition', '')
    if not condition:
        return True  # 无条件 = 默认匹配

    field = rule.get('field', '')
    try:
        # 构建安全的评估上下文
        eval_ctx = {
            'http_code': variables.get('_http_code', 0),
            'status': _resolve_path(last_response, 'status') if last_response else None,
            'code': _resolve_path(last_response, 'code') if last_response else None,
            'data': last_response or {},
        }
        # 将所有提取的变量也加入上下文
        eval_ctx.update(variables)

        result = eval(condition, {"__builtins__": {}}, eval_ctx)
        return bool(result)
    except Exception as e:
        log.debug(f"[API调用] 规则评估失败: {condition} -> {e}")
        return False


# ==================== 正则规范化 ====================

def _normalize_trigger(trigger):
    """自动为触发正则补全 ^ 和 $，用户只需写核心部分
    如用户写 '天气\s+(.+)' -> '^天气\s+(.+)$'
    已有 ^ 或 $ 的不重复添加
    """
    t = trigger.strip()
    if not t:
        return t
    if not t.startswith('^'):
        t = '^' + t
    if not t.endswith('$'):
        t = t + '$'
    return t


def _extract_groups(m, content):
    """从正则匹配结果中提取所有变量
    返回 (user_input, extra_vars)
    - {input} = 最后一个捕获组的值（无捕获组则为整条消息）
    - {g1}~{gN} = 按序号访问每个捕获组
    - 命名捕获组 (?P<name>...) 可通过 {name} 直接引用
    """
    extra_vars = {}
    # 按序号注入 {g1},{g2}...
    if m.lastindex:
        for gi in range(1, m.lastindex + 1):
            extra_vars[f'g{gi}'] = m.group(gi) or ''
    # 命名捕获组直接注入 {name}
    if m.groupdict():
        for k, v in m.groupdict().items():
            extra_vars[k] = v or ''
    # {input} = 最后一个捕获组
    user_input = m.group(m.lastindex) if m.lastindex and m.lastindex >= 1 else content
    return user_input, extra_vars


# ==================== 动态触发匹配 ====================
# 使用通用拦截式 handler, 优先级较低, 匹配用户配置的触发指令

@handler(r'^[\s\S]+$', name='API动态触发', desc='匹配用户配置的API触发指令', priority=-100)
async def dynamic_trigger(event, match):
    content = (event.content or '').strip()
    if not content:
        return

    apis = _get_apis()
    for name, cfg in apis.items():
        if not cfg.get('enabled', True):
            continue
        trigger = cfg.get('trigger', '')
        if not trigger:
            continue
        trigger = _normalize_trigger(trigger)
        try:
            m = re.match(trigger, content)
        except re.error:
            continue
        if not m:
            continue

        user_input, extra_vars = _extract_groups(m, content)

        log.info(f"[API动态触发] 匹配到 [{name}], 输入: {user_input}, 捕获组: {extra_vars}")
        success, reply, variables, steps_log = await _execute_api(cfg, user_input, extra_vars=extra_vars, event=event)

        # 记录调用日志
        now = datetime.now()
        log_entry = {
            'time': now.strftime('%Y-%m-%d %H:%M:%S'),
            'ts': int(time.time()),
            'api_name': name,
            'user_input': user_input,
            'user_id': event.user_id or '',
            'group_id': event.group_id or '',
            'username': event.username or '',
            'success': success,
            'reply': (reply or '')[:500],
            'steps': [],
        }
        for s in steps_log:
            resp_preview = json.dumps(s.get('response', {}), ensure_ascii=False)
            if len(resp_preview) > 300:
                resp_preview = resp_preview[:300] + '...'
            log_entry['steps'].append({
                'name': s.get('name', ''),
                'method': s.get('method', ''),
                'url': s.get('url', ''),
                'http_code': s.get('http_code', 0),
                'error': s.get('error'),
                'response': resp_preview,
            })
        _append_log(log_entry)

        if reply:
            await event.reply(reply)
        return  # 匹配到第一个即停止
