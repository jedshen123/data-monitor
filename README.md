# Data Monitor

每天查询核心数据指标，生成日报，并可选择推送到钉钉群。

当前日报包含：

- APP日活
- 总用户数
- 90天绑定设备用户数
- 90天绑定设备当天活跃数
- 行为埋点总量
- 行为埋点按 `data_source` 拆分后的各来源指标

## 文件

- `data_monitor.py`：Python 监控主程序
- `run_data_monitor.sh`：服务器/cron 调用脚本

## 运行

默认统计昨天数据，并和前天比较：

```bash
./run_data_monitor.sh
```

指定业务日期：

```bash
./run_data_monitor.sh --date 2026-06-25
```

开启钉钉推送：

```bash
DINGTALK_WEBHOOK='https://oapi.dingtalk.com/robot/send?access_token=你的token' \
SEND_DINGTALK=true \
./run_data_monitor.sh
```

如果 MCP 服务的 SQL 工具名无法自动识别，可以显式指定：

```bash
MCP_TOOL_NAME=execute_sql ./run_data_monitor.sh
```

## 常用环境变量

- `MCP_URL`：MCP 地址，默认 `http://127.0.0.1:8000/mcp`
- `DATABASE_ID`：数据库 ID，默认 `1`
- `MCP_TOOL_NAME`：MCP SQL 工具名，默认自动发现
- `SEND_DINGTALK`：是否推送钉钉，`true` 或 `false`，默认 `false`
- `DINGTALK_WEBHOOK`：钉钉机器人 webhook
- `LOG_LEVEL`：日志级别，默认 `INFO`

## crontab 示例

每天 09:00 运行并推送钉钉：

```cron
0 9 * * * cd /path/to/data-monitor && SEND_DINGTALK=true ./run_data_monitor.sh >> /var/log/data_monitor.log 2>&1
```
