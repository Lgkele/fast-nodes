# fast-nodes

Daily updated fast proxy node subscriptions, tested and sorted by latency.

每日自动更新的高速节点订阅库：从多个公开免费节点源拉取、去重、真实测速后，取最快的 150 个节点发布。

## Subscriptions / 订阅地址

| Format | URL |
| --- | --- |
| V2Ray Base64 (Shadowrocket / v2rayN) | https://raw.githubusercontent.com/Lgkele/fast-nodes/main/sub/speed-base64.txt |
| Plain text links | https://raw.githubusercontent.com/Lgkele/fast-nodes/main/sub/speed.txt |
| Clash / Mihomo | https://raw.githubusercontent.com/Lgkele/fast-nodes/main/sub/speed-clash.yaml |
| sing-box | https://raw.githubusercontent.com/Lgkele/fast-nodes/main/sub/speed-sing-box.json |
| Surge | https://raw.githubusercontent.com/Lgkele/fast-nodes/main/sub/speed-surge.conf |
| Statistics (JSON) | https://raw.githubusercontent.com/Lgkele/fast-nodes/main/sub/speed-stats.json |

## Update schedule / 更新时间

- GitHub Actions runs daily at **02:00 UTC** (10:00 Beijing time, CST), and can also be triggered manually (`workflow_dispatch`).
- 每天 UTC 02:00（北京时间 10:00）自动更新一次，也支持手动触发。
- Up to the fastest **150** nodes are published. If fewer than **20** nodes pass the test, the run fails and nothing is committed, so the previous subscription stays in place.
- 每次最多发布最快的 150 个节点；如果通过测试的节点少于 20 个，本次运行失败且不提交，订阅保持上一版。

## Sources / 节点来源

All sources are public and free:

- https://github.com/Au1rxx/free-vpn-subscriptions/raw/main/output/v2ray-base64.txt
- https://raw.githubusercontent.com/mahdibland/ShadowsocksAggregator/master/Eternity.txt
- https://raw.githubusercontent.com/3000S-HTTP/sNSub/main/output/best59_base64.txt
- https://radarhub.top/v2ray.txt
- https://raw.githubusercontent.com/free18/v2ray/main/v.txt
- https://raw.githubusercontent.com/Lgkele/node-sub-zixf/main/sub/base64.txt
- https://raw.githubusercontent.com/Lgkele/node-sub-zixf/main/sub/v2cross-base64.txt

A single source failing does not fail the whole run; the other sources are still used.
单个来源失败不会导致整个任务失败，其余来源仍会继续使用。

## How it works / 处理流程

1. Fetch all sources above.
2. Parse `vless://`, `vmess://`, `trojan://`, `ss://`, `hysteria2://` and `tuic://` links, including base64-encoded subscriptions.
3. Deduplicate by node address + port + UUID/password.
4. Test and sort by latency, keep the fastest 150 (see below).
5. Write the six output files under `sub/` and commit them.

## Speed test method / 测速方法

The test is two-stage, and the exact method actually used in each run is recorded in `sub/speed-stats.json` (`test_method`):

1. **TCP latency probe:** a TCP connection is opened to each deduplicated node's server and port (3 s timeout, concurrent). Unreachable nodes are discarded.
2. **Real proxy test with sing-box:** for the best TCP candidates, sing-box is started with that node as the outbound, and a real HTTP request is sent through the node to `https://www.gstatic.com/generate_204`. Only nodes that return HTTP 204/200 within 8 s pass. Passing nodes are sorted by the measured HTTP latency through the proxy, and the fastest 150 are published.
3. **Fallback:** if sing-box cannot be downloaded or started in the Actions environment, the run falls back to TCP connect latency sorting only. In that case `speed-stats.json` records the fallback method honestly, and this README's description above still applies to the sing-box path when it runs.

测速分两步，实际使用的方法以每次运行生成的 `sub/speed-stats.json` 中的 `test_method` 为准：

1. **TCP 延迟探测：** 对去重后每个节点的服务器地址和端口建立 TCP 连接（3 秒超时，并发），不可达的节点直接丢弃。
2. **sing-box 真实代理测试：** 对 TCP 延迟最好的候选节点，用 sing-box 把该节点作为出口，真实 HTTP 请求经节点访问 `https://www.gstatic.com/generate_204`，8 秒内返回 204/200 才算通过，按经代理实测的 HTTP 延迟排序，取最快 150 个。
3. **退化方案：** 如果 Actions 环境里无法下载或启动 sing-box，则退化为仅按 TCP 连接延迟排序，此时 `speed-stats.json` 会如实记录退化后的方法。

## Output files / 输出文件

| File | Description |
| --- | --- |
| `sub/speed-base64.txt` | V2Ray base64 subscription |
| `sub/speed.txt` | Plain-text links, one per line |
| `sub/speed-clash.yaml` | Clash / Mihomo config with a `url-test` auto-speed-test group |
| `sub/speed-sing-box.json` | sing-box config |
| `sub/speed-surge.conf` | Surge config |
| `sub/speed-stats.json` | Stats: update time, total nodes, per-protocol counts, test method, sources |

## Limitations / 限制

1. **Latency is measured in GitHub Actions' US data centers.** It does not represent the speed on your own network. A node that is fast from the Actions runner may be slow from your location, and vice versa.
   **测速是在 GitHub Actions 的美国数据中心测的延迟，不代表用户自己网络上的速度。** 从 Actions 跑测快的节点，在你自己的网络上未必快，反之亦然。
2. **Free public nodes are operated by third parties and are not trusted.** Do not use them to log in to banks or important accounts, and do not send sensitive information through them.
   **免费公开节点由第三方运营，不可信，不要用来登录银行或重要账号**，也不要通过它们传输敏感信息。

## Disclaimer / 免责声明

This repository only aggregates and tests publicly available free node subscriptions for learning and testing. Node availability, speed and security are not guaranteed. Use at your own risk.
本仓库仅聚合与测试公开免费节点订阅，供学习测试使用，不保证节点的可用性、速度与安全性，使用风险自担。
