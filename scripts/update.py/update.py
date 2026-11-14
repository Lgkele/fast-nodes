#!/usr/bin/env python3
"""Fetch public node subscriptions, deduplicate, test with sing-box
(real HTTP through the proxy) with a TCP-latency fallback, and publish
the fastest 150 nodes in several formats under sub/."""
import base64, json, os, re, shutil, socket, subprocess, sys, tempfile, threading, time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import parse_qs, unquote, urlparse
from urllib.request import Request, urlopen

SOURCES = [
    "https://github.com/Au1rxx/free-vpn-subscriptions/raw/main/output/v2ray-base64.txt",
    "https://raw.githubusercontent.com/mahdibland/ShadowsocksAggregator/master/Eternity.txt",
    "https://raw.githubusercontent.com/3000S-HTTP/sNSub/main/output/best59_base64.txt",
    "https://radarhub.top/v2ray.txt",
    "https://raw.githubusercontent.com/free18/v2ray/main/v.txt",
    "https://raw.githubusercontent.com/Lgkele/node-sub-zixf/main/sub/base64.txt",
    "https://raw.githubusercontent.com/Lgkele/node-sub-zixf/main/sub/v2cross-base64.txt",
]
MAX_NODES = 150
MIN_NODES = 20
TEST_URL = "https://www.gstatic.com/generate_204"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) fast-nodes-updater/1.0"}

def b64decode(s):
    if not s:
        return None
    s = re.sub(r"\s+", "", s.strip())
    s += "=" * (-len(s) % 4)
    try:
        return base64.urlsafe_b64decode(s).decode("utf-8", "ignore")
    except Exception:
        return None

def decode_text(text):
    t = (text or "").strip()
    if not t:
        return []
    if "://" in t:
        return [l.strip() for l in t.splitlines() if l.strip()]
    d = b64decode(t)
    if d and "://" in d:
        return [l.strip() for l in d.splitlines() if l.strip()]
    return [l.strip() for l in t.splitlines() if l.strip()]

def fetch(url):
    try:
        req = Request(url, headers=UA)
        with urlopen(req, timeout=25) as r:
            return r.read().decode("utf-8", "ignore")
    except Exception as e:
        print(f"source failed: {url}: {e}", flush=True)
        return ""

def q1(q, *names):
    for n in names:
        if n in q and q[n]:
            return q[n][0]
    return ""

def parse_link(line):
    line = (line or "").strip()
    if not line or line.startswith("#"):
        return None
    if "://" not in line:
        d = b64decode(line)
        if d and "://" in d:
            return parse_link(d.strip().splitlines()[0])
        return None
    scheme = line.split("://", 1)[0].lower()
    if scheme in ("shadowsocks",):
        scheme = "ss"
    if scheme in ("hy2",):
        scheme = "hysteria2"
    if scheme not in ("vless", "vmess", "trojan", "ss", "hysteria2", "tuic"):
        return None
    try:
        if scheme == "vmess":
            raw = line[8:].split("#", 1)[0]
            data = json.loads(b64decode(raw) or "{}")
            server = str(data.get("add", "")).strip()
            port = int(data.get("port", 0) or 0)
            if not server or not port:
                return None
            return {"proto": "vmess", "server": server.lower(), "port": port,
                    "uuid": str(data.get("id", "")), "password": "", "method": str(data.get("scy", "auto") or "auto"),
                    "tls": str(data.get("tls", "")) == "tls", "sni": str(data.get("sni", "") or data.get("host", "") or ""),
                    "network": str(data.get("net", "tcp") or "tcp"), "path": str(data.get("path", "/") or "/"),
                    "ws_host": str(data.get("host", "") or ""), "service_name": str(data.get("serviceName", "") or ""),
                    "flow": "", "name": str(data.get("ps", "") or ""), "raw": line}
        if scheme == "ss":
            body = line[5:].split("#", 1)[0]
            name = unquote(line.split("#", 1)[1]) if "#" in line else ""
            if "@" in body:
                userinfo, hostport = body.rsplit("@", 1)
                ui = unquote(userinfo)
                if ":" not in ui:
                    ui = b64decode(ui) or ui
                method, _, password = ui.partition(":")
            else:
                dec = b64decode(body) or ""
                if "@" not in dec:
                    return None
                ui, hostport = dec.rsplit("@", 1)
                method, _, password = ui.partition(":")
            if "@" in hostport:
                hostport = hostport.rsplit("@", 1)[-1]
            host, _, port_s = hostport.rpartition(":")
            host = host.strip("[]")
            port = int(port_s or 0)
            if not host or not port or not method:
                return None
            return {"proto": "ss", "server": host.lower(), "port": port, "uuid": "", "password": password,
                    "method": method, "tls": False, "sni": "", "network": "tcp", "path": "/",
                    "ws_host": "", "service_name": "", "flow": "", "name": name, "raw": line}
        u = urlparse(line)
        host = (u.hostname or "").lower()
        port = u.port or 0
        if not host or not port:
            return None
        q = parse_qs(u.query)
        user = unquote(u.username or "")
        pwd = unquote(u.password or "")
        sec = q1(q, "security", "tls").lower()
        tls = sec in ("tls", "reality") or q1(q, "tls") == "1" or scheme in ("trojan", "hysteria2", "tuic")
        return {"proto": scheme, "server": host, "port": port,
                "uuid": user if scheme in ("vless", "vmess", "tuic") else "",
                "password": pwd or (user if scheme in ("trojan", "hysteria2") else "") or q1(q, "password"),
                "method": "", "tls": tls, "sni": q1(q, "sni", "peer", "serverName"),
                "network": q1(q, "type", "net") or "tcp", "path": unquote(q1(q, "path")) or "/",
                "ws_host": q1(q, "host", "wsHost"), "service_name": q1(q, "serviceName"),
                "flow": q1(q, "flow"), "name": unquote(u.fragment or ""), "raw": line}
    except Exception as e:
        print(f"parse failed: {line[:60]}: {e}", flush=True)
        return None

def dedup_key(n):
    return (n["server"], n["port"], (n["uuid"] or n["password"] or "").lower())

def tcp_probe(n):
    t = time.time()
    try:
        with socket.create_connection((n["server"], n["port"]), timeout=3):
            return (time.time() - t) * 1000.0
    except Exception:
        return None

def sb_outbound(n, tag):
    tls = {"enabled": True, "server_name": n["sni"] or n["server"], "insecure": True} if n["tls"] else None
    transport = None
    if n["network"] == "ws":
        transport = {"type": "ws", "path": n["path"] or "/", "headers": {"Host": n["ws_host"] or n["sni"] or n["server"]}}
    elif n["network"] == "grpc":
        transport = {"type": "grpc", "service_name": n["service_name"] or ""}
    p = n["proto"]
    if p == "vless":
        o = {"type": "vless", "tag": tag, "server": n["server"], "server_port": n["port"], "uuid": n["uuid"]}
        if n["flow"]:
            o["flow"] = n["flow"]
    elif p == "vmess":
        o = {"type": "vmess", "tag": tag, "server": n["server"], "server_port": n["port"], "uuid": n["uuid"], "security": n["method"] or "auto", "alter_id": 0}
    elif p == "trojan":
        o = {"type": "trojan", "tag": tag, "server": n["server"], "server_port": n["port"], "password": n["password"]}
    elif p == "ss":
        return {"type": "shadowsocks", "tag": tag, "server": n["server"], "server_port": n["port"], "method": n["method"] or "aes-256-gcm", "password": n["password"]}
    elif p == "hysteria2":
        o = {"type": "hysteria2", "tag": tag, "server": n["server"], "server_port": n["port"], "password": n["password"]}
        tls = tls or {"enabled": True, "server_name": n["sni"] or n["server"], "insecure": True}
    elif p == "tuic":
        o = {"type": "tuic", "tag": tag, "server": n["server"], "server_port": n["port"], "uuid": n["uuid"], "password": n["password"]}
        tls = tls or {"enabled": True, "server_name": n["sni"] or n["server"], "insecure": True}
    else:
        return None
    if tls:
        o["tls"] = tls
    if transport:
        o["transport"] = transport
    return o

_port_lock = threading.Lock()
_next_port = [22000]

def sb_test(n, binpath):
    out = sb_outbound(n, "proxy")
    if not out:
        return None
    with _port_lock:
        port = _next_port[0]
        _next_port[0] += 1
    cfg = {"log": {"level": "error"}, "inbounds": [{"type": "mixed", "tag": "in", "listen": "127.0.0.1", "listen_port": port}], "outbounds": [out, {"type": "direct", "tag": "direct"}]}
    proc = None
    try:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = os.path.join(td, "config.json")
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f)
            proc = subprocess.Popen([binpath, "run", "-c", cfg_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1.0)
            if proc.poll() is not None:
                return None
            t = time.time()
            r = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "8", "-x", f"http://127.0.0.1:{port}", TEST_URL], capture_output=True, text=True, timeout=12)
            ms = (time.time() - t) * 1000.0
            return ms if r.stdout.strip() in ("204", "200") else None
    except Exception:
        return None
    finally:
        if proc is not None:
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

def yq(v):
    return json.dumps(str(v), ensure_ascii=False)

def clash_proxy(n, name):
    L = [f"  - name: {yq(name)}", f"    type: {n['proto']}", f"    server: {yq(n['server'])}", f"    port: {n['port']}"]
    p = n["proto"]
    if p in ("vless", "vmess", "tuic"):
        L.append(f"    uuid: {yq(n['uuid'])}")
    if p == "vmess":
        L.append(f"    alterId: 0")
        L.append(f"    cipher: {yq(n['method'] or 'auto')}")
    if p in ("trojan", "hysteria2", "tuic"):
        L.append(f"    password: {yq(n['password'])}")
    if p == "ss":
        L.append(f"    cipher: {yq(n['method'] or 'aes-256-gcm')}")
        L.append(f"    password: {yq(n['password'])}")
    if p == "vless" and n["flow"]:
        L.append(f"    flow: {yq(n['flow'])}")
    if n["tls"]:
        L.append("    tls: true")
        if n["sni"]:
            L.append(f"    servername: {yq(n['sni'])}")
    if n["network"] == "ws":
        L.append("    network: ws")
        L.append("    ws-opts:")
        L.append(f"      path: {yq(n['path'] or '/')}")
        if n["ws_host"] or n["sni"]:
            L.append("      headers:")
            L.append(f"        Host: {yq(n['ws_host'] or n['sni'])}")
    elif n["network"] == "grpc":
        L.append("    network: grpc")
        L.append("    grpc-opts:")
        L.append(f"      grpc-service-name: {yq(n['service_name'] or '')}")
    return "\n".join(L)

def surge_proxy(n, name):
    p = n["proto"]
    s, port = n["server"], n["port"]
    tls = ", tls=true" if n["tls"] else ""
    sni = f", sni={n['sni']}" if n["sni"] else ""
    if p == "vless":
        v = f"{name} = vless, {s}, {port}, username={n['uuid']}{tls}{sni}"
        if n["network"] == "ws":
            v += f", ws=true, ws-path={n['path'] or '/'}"
        return v
    if p == "vmess":
        return f"{name} = vmess, {s}, {port}, username={n['uuid']}{tls}{sni}"
    if p == "trojan":
        return f"{name} = trojan, {s}, {port}, password={n['password']}{tls}{sni}"
    if p == "ss":
        return f"{name} = ss, {s}, {port}, encrypt-method={n['method'] or 'aes-256-gcm'}, password={n['password']}"
    if p == "hysteria2":
        return f"{name} = hysteria2, {s}, {port}, password={n['password']}{sni}"
    if p == "tuic":
        return f"{name} = tuic, {s}, {port}, uuid={n['uuid']}, password={n['password']}{sni}"
    return None

def main():
    all_nodes = []
    seen = set()
    for url in SOURCES:
        text = fetch(url)
        if not text:
            continue
        for line in decode_text(text):
            n = parse_link(line)
            if not n:
                continue
            k = dedup_key(n)
            if k in seen:
                continue
            seen.add(k)
            all_nodes.append(n)
    print(f"fetched {len(all_nodes)} unique nodes", flush=True)
    if not all_nodes:
        print("ERROR: no nodes fetched", flush=True)
        return 1
    tcp_results = {}
    with ThreadPoolExecutor(max_workers=100) as ex:
        futs = {ex.submit(tcp_probe, n): n for n in all_nodes}
        for fut in as_completed(futs):
            ms = fut.result()
            if ms is not None:
                tcp_results[id(futs[fut])] = ms
    tcp_ok = [n for n in all_nodes if id(n) in tcp_results]
    tcp_ok.sort(key=lambda n: tcp_results[id(n)])
    print(f"TCP reachable: {len(tcp_ok)}", flush=True)
    if len(tcp_ok) < MIN_NODES:
        print(f"ERROR: only {len(tcp_ok)} nodes passed TCP test (< {MIN_NODES}), not committing", flush=True)
        return 1
    binpath = shutil.which("sing-box") or ("./sing-box" if os.path.exists("./sing-box") else None)
    method = ""
    final = []
    sb_passed = 0
    if binpath:
        candidates = tcp_ok[:200]
        sb_results = {}
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(sb_test, n, binpath): n for n in candidates}
            for fut in as_completed(futs):
                ms = fut.result()
                if ms is not None:
                    sb_results[id(futs[fut])] = ms
        sb_ok = [n for n in candidates if id(n) in sb_results]
        sb_ok.sort(key=lambda n: sb_results[id(n)])
        sb_passed = len(sb_ok)
        print(f"sing-box real-proxy passed: {sb_passed}", flush=True)
        if sb_passed >= MIN_NODES:
            final = sb_ok[:MAX_NODES]
            for n in final:
                n["latency_ms"] = round(sb_results[id(n)], 1)
            method = "sing-box real HTTP proxy test (request to https://www.gstatic.com/generate_204 through the node, 8s timeout) after TCP latency probe; sorted by measured HTTP latency through the proxy"
        else:
            print("sing-box passed too few nodes, falling back to TCP latency sorting", flush=True)
            final = tcp_ok[:MAX_NODES]
            for n in final:
                n["latency_ms"] = round(tcp_results[id(n)], 1)
            method = f"TCP connect latency sorting only (fallback: sing-box real-proxy test passed only {sb_passed} nodes); TCP probe 3s timeout"
    else:
        print("sing-box not available, falling back to TCP latency sorting", flush=True)
        final = tcp_ok[:MAX_NODES]
        for n in final:
            n["latency_ms"] = round(tcp_results[id(n)], 1)
        method = "TCP connect latency sorting only (fallback: sing-box binary not available); TCP probe 3s timeout"
    if len(final) < MIN_NODES:
        print(f"ERROR: only {len(final)} nodes passed (< {MIN_NODES}), not committing", flush=True)
        return 1
    names = []
    for i, n in enumerate(final):
        n["out_name"] = f"fast-{i+1:03d}"
        names.append(n["out_name"])
    os.makedirs("sub", exist_ok=True)
    plain = "\n".join(n["raw"] for n in final) + "\n"
    with open("sub/speed.txt", "w", encoding="utf-8") as f:
        f.write(plain)
    with open("sub/speed-base64.txt", "w", encoding="utf-8") as f:
        f.write(base64.b64encode(plain.encode("utf-8")).decode("ascii") + "\n")
    clash = ["port: 7890", "socks-port: 7891", "allow-lan: false", "mode: rule", "log-level: info", "external-controller: 127.0.0.1:9090", "proxies:"]
    clash += [clash_proxy(n, n["out_name"]) for n in final]
    clash += ["proxy-groups:", "  - name: Auto-Fast", "    type: url-test", "    proxies:"]
    clash += [f"      - {yq(x)}" for x in names]
    clash += ["    url: http://www.gstatic.com/generate_204", "    interval: 300", "  - name: PROXY", "    type: select", "    proxies:", "      - Auto-Fast", "      - DIRECT", "rules:", "  - MATCH,Auto-Fast"]
    with open("sub/speed-clash.yaml", "w", encoding="utf-8") as f:
        f.write("\n".join(clash) + "\n")
    sb_outs = []
    for n in final:
        o = sb_outbound(n, n["out_name"])
        if o:
            sb_outs.append(o)
    sb_cfg = {"log": {"level": "info"}, "inbounds": [{"type": "mixed", "tag": "mixed-in", "listen": "127.0.0.1", "listen_port": 1080}], "outbounds": sb_outs + [{"type": "urltest", "tag": "auto-fast", "outbounds": [o["tag"] for o in sb_outs], "url": TEST_URL, "interval": "5m"}, {"type": "direct", "tag": "direct"}, {"type": "block", "tag": "block"}], "route": {"final": "auto-fast"}}
    with open("sub/speed-sing-box.json", "w", encoding="utf-8") as f:
        json.dump(sb_cfg, f, ensure_ascii=False, indent=2)
    surge = ["[General]", "loglevel = notify", "", "[Proxy]"]
    surge += [x for n in final if (x := surge_proxy(n, n["out_name"]))]
    surge += ["", "[Proxy Group]", "Auto-Fast = url-test, " + ", ".join(names) + ", url = http://www.gstatic.com/generate_204, interval = 300", "PROXY = select, Auto-Fast, DIRECT", "", "[Rule]", "FINAL,Auto-Fast"]
    with open("sub/speed-surge.conf", "w", encoding="utf-8") as f:
        f.write("\n".join(surge) + "\n")
    protocols = dict(sorted(Counter(n["proto"] for n in final).items()))
    stats = {"updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "total_nodes": len(final), "protocols": protocols, "test_method": method, "sources": SOURCES, "tcp_reachable": len(tcp_ok), "sing_box_passed": sb_passed, "latency_ms_top": final[0].get("latency_ms") if final else None}
    with open("sub/speed-stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"DONE: {len(final)} nodes, protocols={protocols}, method={method}", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
