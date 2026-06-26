#!/usr/bin/env python3
"""Daily data monitor report sent through DingTalk webhook."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from typing import Any


DEFAULT_MCP_URL = "http://54.226.190.74:8000/mcp"
DEFAULT_DATABASE_ID = 1
DEFAULT_DINGTALK_WEBHOOK = ""


APP_DAU_SQL = """
select
  sum(app_day_user_cn)
from
  ads_app_device_act_aggr_df
where busi_date = '{date}'
""".strip()


EVENT_LOG_SQL = """
select data_source, count(1)
from dwd_tp_app_log_di
where dt = '{date}'
group by data_source
""".strip()


class MonitorError(RuntimeError):
    pass


class McpSqlClient:
    """Small JSON-RPC client for an HTTP MCP endpoint."""

    def __init__(
        self,
        url: str,
        database_id: int,
        tool_name: str | None = None,
        timeout: int = 60,
    ) -> None:
        self.url = url
        self.database_id = database_id
        self.tool_name = tool_name
        self.timeout = timeout
        self._request_id = int(time.time())
        self._session_id: str | None = None
        self._initialized = False

    def query(self, sql: str) -> Any:
        self._ensure_initialized()
        if not self.tool_name:
            self.tool_name = self._discover_sql_tool()

        last_error: Exception | None = None
        for arguments in self._argument_variants(sql):
            try:
                return self._call_tool(self.tool_name, arguments)
            except Exception as exc:  # noqa: BLE001 - keep trying compatible MCP schemas.
                last_error = exc
                logging.debug("MCP tool call failed with arguments %s: %s", arguments, exc)

        raise MonitorError(f"MCP SQL 查询失败: {last_error}") from last_error

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return

        self._rpc(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "data-monitor",
                    "version": "1.0.0",
                },
            },
            allow_without_session=True,
        )
        try:
            self._notify("notifications/initialized", {})
        except Exception as exc:  # noqa: BLE001 - some HTTP MCP servers do not require it.
            logging.debug("MCP initialized notification failed: %s", exc)
        self._initialized = True

    def _discover_sql_tool(self) -> str:
        result = self._rpc("tools/list", {})
        tools = result.get("tools") if isinstance(result, dict) else None
        if not isinstance(tools, list) or not tools:
            raise MonitorError("MCP 端点没有返回可用 tools，请设置 MCP_TOOL_NAME")

        preferred_words = ("query", "sql", "execute", "database", "db")
        scored_tools: list[tuple[int, str]] = []
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            name = str(tool.get("name") or "")
            description = str(tool.get("description") or "")
            haystack = f"{name} {description}".lower()
            score = sum(1 for word in preferred_words if word in haystack)
            if name:
                scored_tools.append((score, name))

        if not scored_tools:
            raise MonitorError("MCP tools/list 返回格式异常，请设置 MCP_TOOL_NAME")

        scored_tools.sort(reverse=True)
        selected = scored_tools[0][1]
        logging.info("Auto selected MCP tool: %s", selected)
        return selected

    def _call_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        result = self._rpc(
            "tools/call",
            {
                "name": tool_name,
                "arguments": arguments,
            },
        )
        if isinstance(result, dict) and result.get("isError"):
            raise MonitorError(f"MCP tool returned error: {result}")
        return result

    def _rpc(
        self,
        method: str,
        params: dict[str, Any],
        allow_without_session: bool = False,
    ) -> Any:
        self._request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self._request_id,
            "method": method,
            "params": params,
        }
        raw = self._post_json(payload, allow_without_session=allow_without_session)
        message = self._parse_json_or_sse(raw)
        if "error" in message:
            raise MonitorError(f"MCP JSON-RPC error: {message['error']}")
        return message.get("result")

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }
        raw = self._post_json(payload)
        if raw.strip():
            message = self._parse_json_or_sse(raw)
            if "error" in message:
                raise MonitorError(f"MCP JSON-RPC error: {message['error']}")

    def _post_json(self, payload: dict[str, Any], allow_without_session: bool = False) -> str:
        data = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        elif not allow_without_session:
            raise MonitorError("MCP session 尚未初始化")

        request = urllib.request.Request(
            self.url,
            data=data,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                session_id = response.headers.get("Mcp-Session-Id")
                if session_id:
                    self._session_id = session_id
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise MonitorError(f"MCP HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise MonitorError(f"MCP 连接失败: {exc}") from exc

        return raw

    def _parse_json_or_sse(self, raw: str) -> dict[str, Any]:
        raw = raw.strip()
        if not raw:
            raise MonitorError("MCP 返回为空")

        if raw.startswith("{"):
            return json.loads(raw)

        data_lines: list[str] = []
        for line in raw.splitlines():
            if line.startswith("data:"):
                chunk = line.split(":", 1)[1].strip()
                if chunk and chunk != "[DONE]":
                    data_lines.append(chunk)
        if data_lines:
            return json.loads(data_lines[-1])

        raise MonitorError(f"无法解析 MCP 返回: {raw[:200]}")

    def _argument_variants(self, sql: str) -> list[dict[str, Any]]:
        return [
            {"database_id": self.database_id, "sql": sql},
            {"db_id": self.database_id, "sql": sql},
            {"databaseId": self.database_id, "sql": sql},
            {"database": self.database_id, "query": sql},
            {"datasource_id": self.database_id, "query": sql},
            {"sql": sql, "database_id": self.database_id},
            {"query": sql},
            {"sql": sql},
        ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily metrics report.")
    parser.add_argument(
        "--date",
        help="业务日期，格式 YYYY-MM-DD；默认取昨天。",
    )
    parser.add_argument(
        "--send-dingtalk",
        choices=("true", "false"),
        default=os.getenv("SEND_DINGTALK", "false").lower(),
        help="是否推送到钉钉，默认读取 SEND_DINGTALK，未设置时为 false。",
    )
    parser.add_argument("--mcp-url", default=os.getenv("MCP_URL", DEFAULT_MCP_URL))
    parser.add_argument(
        "--database-id",
        type=int,
        default=int(os.getenv("DATABASE_ID", str(DEFAULT_DATABASE_ID))),
    )
    parser.add_argument("--mcp-tool-name", default=os.getenv("MCP_TOOL_NAME"))
    parser.add_argument(
        "--dingtalk-webhook",
        default=os.getenv("DINGTALK_WEBHOOK", DEFAULT_DINGTALK_WEBHOOK),
        help="钉钉机器人 webhook；建议通过 DINGTALK_WEBHOOK 环境变量配置。",
    )
    parser.add_argument("--timeout", type=int, default=int(os.getenv("HTTP_TIMEOUT", "60")))
    return parser.parse_args()


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )


def target_dates(date_text: str | None) -> tuple[str, str]:
    if date_text:
        target = dt.datetime.strptime(date_text, "%Y-%m-%d").date()
    else:
        target = dt.date.today() - dt.timedelta(days=1)
    previous = target - dt.timedelta(days=1)
    return target.isoformat(), previous.isoformat()


def unwrap_mcp_result(result: Any) -> Any:
    if isinstance(result, dict) and "content" in result and isinstance(result["content"], list):
        texts = []
        for item in result["content"]:
            if isinstance(item, dict):
                if item.get("type") == "text" and item.get("text") is not None:
                    texts.append(str(item["text"]))
                elif item.get("text") is not None:
                    texts.append(str(item["text"]))
        if texts:
            return parse_possible_json("\n".join(texts))
    return result


def parse_possible_json(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return None

    for candidate in (stripped, extract_json_fragment(stripped)):
        if not candidate:
            continue
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return stripped


def extract_json_fragment(text: str) -> str | None:
    match = re.search(r"(\{.*\}|\[.*\])", text, flags=re.S)
    return match.group(1) if match else None


def rows_from_result(result: Any) -> list[Any]:
    value = unwrap_mcp_result(result)
    if isinstance(value, dict):
        for key in ("rows", "data", "result", "records", "items"):
            nested = value.get(key)
            if isinstance(nested, list):
                return nested
            if isinstance(nested, dict):
                return rows_from_result(nested)
        return [value]
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = parse_simple_table(value)
        if parsed:
            return parsed
    return [value]


def parse_simple_table(text: str) -> list[Any]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return []

    json_lines = []
    for line in lines:
        try:
            json_lines.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    if json_lines:
        return json_lines

    rows: list[list[str]] = []
    for line in lines:
        if "|" in line:
            cells = [cell.strip() for cell in line.strip("|").split("|")]
            if cells and not all(set(cell) <= {"-", ":"} for cell in cells):
                rows.append(cells)
    return rows


def first_number(result: Any) -> int:
    rows = rows_from_result(result)
    for row in rows:
        numbers = numbers_from_value(row)
        if numbers:
            return int(numbers[0])
    return 0


def event_counts(result: Any) -> dict[str, int]:
    rows = rows_from_result(result)
    counts: dict[str, int] = {}
    for index, row in enumerate(rows):
        if isinstance(row, dict):
            source = first_present(row, ("data_source", "DATA_SOURCE", "source", "数据源"))
            count = first_present(row, ("count", "count(1)", "COUNT(1)", "cnt", "COUNT", "数量"))
            if source is None or count is None:
                values = list(row.values())
                if len(values) >= 2:
                    source, count = values[0], values[1]
            if source is not None and count is not None:
                counts[str(source)] = int(float(str(count).replace(",", "")))
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            if index == 0 and str(row[1]).lower() in {"count", "count(1)", "cnt", "数量"}:
                continue
            counts[str(row[0])] = int(float(str(row[1]).replace(",", "")))
    return counts


def first_present(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def numbers_from_value(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, (int, float)):
        return [float(value)]
    if isinstance(value, dict):
        numbers: list[float] = []
        for item in value.values():
            numbers.extend(numbers_from_value(item))
        return numbers
    if isinstance(value, (list, tuple)):
        numbers = []
        for item in value:
            numbers.extend(numbers_from_value(item))
        return numbers
    matches = re.findall(r"-?\d+(?:\.\d+)?", str(value).replace(",", ""))
    return [float(match) for match in matches]


def change_text(current: int, previous: int) -> str:
    delta = current - previous
    if previous == 0:
        if current == 0:
            return color_text("→ 0 (0.00%)", "gray")
        return color_text(f"▲ +{delta:,} (前日为0)", "red")
    percent = delta / previous * 100
    if delta > 0:
        return color_text(f"▲ +{delta:,} (+{percent:.2f}%)", "red")
    if delta < 0:
        return color_text(f"▼ {delta:,} ({percent:.2f}%)", "green")
    return color_text("→ 0 (0.00%)", "gray")


def color_text(text: str, color: str) -> str:
    colors = {
        "red": "#d93025",
        "green": "#188038",
        "gray": "#5f6368",
    }
    return f"<font color=\"{colors[color]}\">{text}</font>"


def build_report(
    report_date: str,
    previous_date: str,
    app_dau: int,
    previous_app_dau: int,
    event_by_source: dict[str, int],
    previous_event_by_source: dict[str, int],
) -> str:
    event_total = sum(event_by_source.values())
    previous_event_total = sum(previous_event_by_source.values())
    all_sources = sorted(set(event_by_source) | set(previous_event_by_source))

    lines = [
        f"## 数据监控日报（{report_date}）",
        "",
        "### 核心指标",
        "| 指标 | 维度 | 波动 | 昨日 | 前日 |",
        "| --- | --- | ---: | ---: | ---: |",
        (
            f"| APP日活 | 总量 | {change_text(app_dau, previous_app_dau)} | "
            f"{app_dau:,} | {previous_app_dau:,} |"
        ),
        (
            f"| 行为埋点 | 总量 | {change_text(event_total, previous_event_total)} | "
            f"{event_total:,} | {previous_event_total:,} |"
        ),
    ]

    if all_sources:
        for source in all_sources:
            current = event_by_source.get(source, 0)
            previous = previous_event_by_source.get(source, 0)
            lines.append(
                (
                    f"| 行为埋点 | {source} | {change_text(current, previous)} | "
                    f"{current:,} | {previous:,} |"
                )
            )

    return "\n".join(lines)


def send_dingtalk(webhook: str, markdown: str, timeout: int) -> None:
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": "数据监控日报",
            "text": markdown,
        },
    }
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        webhook,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise MonitorError(f"钉钉 webhook HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise MonitorError(f"钉钉 webhook 连接失败: {exc}") from exc

    try:
        result = json.loads(body)
    except json.JSONDecodeError:
        result = {"raw": body}
    if result.get("errcode") not in (None, 0):
        raise MonitorError(f"钉钉 webhook 返回异常: {result}")
    logging.info("DingTalk webhook response: %s", result)


def main() -> int:
    setup_logging()
    args = parse_args()
    report_date, previous_date = target_dates(args.date)

    client = McpSqlClient(
        url=args.mcp_url,
        database_id=args.database_id,
        tool_name=args.mcp_tool_name,
        timeout=args.timeout,
    )

    logging.info("Start monitoring report_date=%s previous_date=%s", report_date, previous_date)
    app_dau = first_number(client.query(APP_DAU_SQL.format(date=report_date)))
    previous_app_dau = first_number(client.query(APP_DAU_SQL.format(date=previous_date)))
    event_by_source = event_counts(client.query(EVENT_LOG_SQL.format(date=report_date)))
    previous_event_by_source = event_counts(client.query(EVENT_LOG_SQL.format(date=previous_date)))

    report = build_report(
        report_date=report_date,
        previous_date=previous_date,
        app_dau=app_dau,
        previous_app_dau=previous_app_dau,
        event_by_source=event_by_source,
        previous_event_by_source=previous_event_by_source,
    )
    logging.info("Daily report:\n%s", report)

    if args.send_dingtalk == "true":
        if not args.dingtalk_webhook:
            raise MonitorError("SEND_DINGTALK=true 时必须配置 DINGTALK_WEBHOOK")
        send_dingtalk(args.dingtalk_webhook, report, args.timeout)
    else:
        logging.info("SEND_DINGTALK=false，跳过钉钉推送。")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 - cron needs a clear non-zero failure.
        logging.exception("数据监控任务失败: %s", exc)
        raise SystemExit(1)
