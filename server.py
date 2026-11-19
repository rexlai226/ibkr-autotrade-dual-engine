#!/usr/bin/env python3
"""
server.py
輕量級 HTTP 控制與儀表板伺服器 (Lightweight HTTP Control & Dashboard Server)
支援靜態檔案存取及前端 POST /api/control 交易啟停與一鍵平倉控制
"""

import os
import sys
import time
import json
import math
import socket
import logging
import subprocess
import threading
import urllib.request
from http.server import HTTPServer, ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, quote
from datetime import datetime, date, timedelta

from report_and_incident_manager import IncidentManager, DailyReportGenerator

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONTROL_FILE = os.path.join(CURRENT_DIR, "trade_control.json")
TUNNEL_INFO_FILE = os.path.join(CURRENT_DIR, "data", "tunnel_info.json")

_AUDIT_JOB_LOCK = threading.Lock()
_AUDIT_JOB_STATE = {
    "status": "idle",
    "running": False,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "result": None,
}

_VERSION_CACHE_LOCK = threading.Lock()
_VERSION_REMOTE_CACHE = {
    "fetched_at": 0.0,
    "latest_version": None,
    "latest_tag": None,
    "release_url": "https://github.com/larrywong69-art/ibkr-autotrade-dual-engine/releases",
    "error": None,
}


def _parse_semver(ver_str: str):
    cleaned = (ver_str or "").strip().lstrip("vV")
    parts = []
    for p in cleaned.split(".")[:3]:
        digits = "".join(ch for ch in p if ch.isdigit())
        parts.append(int(digits) if digits else 0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


def get_version_info(base_dir=CURRENT_DIR, check_remote: bool = False, remote_fetcher=None):
    """回傳目前本機版本、Git 提交與可選的遠端最新版本檢查結果。"""
    version_file = os.path.join(base_dir, "VERSION")
    current_version = "9.3.3"
    if os.path.exists(version_file):
        try:
            with open(version_file, "r", encoding="utf-8") as vf:
                raw = vf.read().strip().lstrip("vV")
                if raw:
                    current_version = raw
        except Exception:
            pass

    commit = ""
    if os.path.isdir(os.path.join(base_dir, ".git")):
        try:
            proc = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=base_dir,
                capture_output=True,
                text=True,
                timeout=2,
            )
            if proc.returncode == 0:
                commit = proc.stdout.strip()
        except Exception:
            pass

    repo_url = "https://github.com/larrywong69-art/ibkr-autotrade-dual-engine"
    latest_version = None
    latest_tag = None
    release_url = f"{repo_url}/releases"
    remote_error = None

    if check_remote:
        now = time.time()
        with _VERSION_CACHE_LOCK:
            cached_fresh = (now - _VERSION_REMOTE_CACHE["fetched_at"] < 300) and _VERSION_REMOTE_CACHE["latest_version"]
            if cached_fresh and remote_fetcher is None:
                latest_version = _VERSION_REMOTE_CACHE["latest_version"]
                latest_tag = _VERSION_REMOTE_CACHE["latest_tag"]
                release_url = _VERSION_REMOTE_CACHE["release_url"]
                remote_error = _VERSION_REMOTE_CACHE["error"]

        if latest_version is None:
            try:
                if remote_fetcher is not None:
                    remote_raw = str(remote_fetcher()).strip().lstrip("vV")
                    latest_version = remote_raw or None
                    latest_tag = f"v{latest_version}" if latest_version else None
                else:
                    ver_url = "https://raw.githubusercontent.com/larrywong69-art/ibkr-autotrade-dual-engine/main/VERSION"
                    req = urllib.request.Request(ver_url, headers={"User-Agent": "AutoTrade-VersionCheck"})
                    with urllib.request.urlopen(req, timeout=4) as resp:
                        remote_raw = resp.read().decode("utf-8").strip().lstrip("vV")
                        if remote_raw:
                            latest_version = remote_raw
                            latest_tag = f"v{remote_raw}"
                            release_url = f"{repo_url}/releases/tag/{latest_tag}"
                if remote_fetcher is None:
                    with _VERSION_CACHE_LOCK:
                        _VERSION_REMOTE_CACHE.update({
                            "fetched_at": now,
                            "latest_version": latest_version,
                            "latest_tag": latest_tag,
                            "release_url": release_url,
                            "error": None,
                        })
            except Exception as exc:
                remote_error = str(exc)

    update_available = bool(
        latest_version and _parse_semver(latest_version) > _parse_semver(current_version)
    )

    return {
        "status": "ok",
        "version": current_version,
        "tag": f"v{current_version}",
        "commit": commit,
        "latest_version": latest_version,
        "latest_tag": latest_tag,
        "update_available": update_available,
        "repo_url": repo_url,
        "release_url": release_url,
        "update_command": "./update.sh --restart",
        "curl_update_command": "curl -fsSL https://raw.githubusercontent.com/larrywong69-art/ibkr-autotrade-dual-engine/main/update.sh | bash -s -- --restart",
        "remote_error": remote_error,
    }


def sanitize_json_value(value):
    """將 NaN / Infinity 轉為 None，避免瀏覽器 JSON.parse 拋出格式錯誤。"""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(k): sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_json_value(v) for v in value]
    return value


def start_audit_job(params):
    """啟動背景雙層閉環巡檢任務，避免 Cloudflare Tunnel 長連線逾時返回 HTML。"""
    with _AUDIT_JOB_LOCK:
        if _AUDIT_JOB_STATE["running"]:
            return {
                "status": "running",
                "running": True,
                "already_running": True,
                "started_at": _AUDIT_JOB_STATE["started_at"],
                "message": "雙層閉環巡檢正在背景分析中，請稍候...",
            }
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _AUDIT_JOB_STATE.update({
            "status": "running",
            "running": True,
            "started_at": started_at,
            "finished_at": None,
            "error": None,
            "result": None,
        })

    def _worker():
        try:
            from daily_auto_audit_fixer import DailyAutoAuditFixer
            res = DailyAutoAuditFixer().run_daily_closed_loop(
                hours=int(params.get("hours", 24)),
                upload_gdrive=bool(params.get("upload_gdrive", True)),
                enable_ai_fix=bool(params.get("enable_ai_fix", True)),
                force_ai=bool(params.get("force_ai", False)),
                push_branch=bool(params.get("push_branch", True)),
                notify_telegram=bool(params.get("notify_telegram", True)),
            )
            with _AUDIT_JOB_LOCK:
                _AUDIT_JOB_STATE.update({
                    "status": res.get("status", "ok"),
                    "running": False,
                    "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "error": None,
                    "result": res,
                })
        except Exception as exc:
            with _AUDIT_JOB_LOCK:
                _AUDIT_JOB_STATE.update({
                    "status": "error",
                    "running": False,
                    "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "error": str(exc),
                    "result": None,
                })

    threading.Thread(target=_worker, name="ClosedLoopAuditWorker", daemon=True).start()
    return {
        "status": "running",
        "running": True,
        "already_running": False,
        "started_at": started_at,
        "message": "已啟動雙層閉環巡檢背景分析",
    }


def get_audit_job_status():
    """取得背景雙層閉環巡檢目前狀態或最近結果。"""
    with _AUDIT_JOB_LOCK:
        snapshot = dict(_AUDIT_JOB_STATE)
    if snapshot["status"] == "idle" and snapshot["result"] is None:
        try:
            from daily_auto_audit_fixer import DailyAutoAuditFixer
            latest = DailyAutoAuditFixer().get_latest_audit()
            if latest:
                snapshot["latest"] = latest
        except Exception:
            pass
    return snapshot


def _load_deduped_executions(base_dir: str = CURRENT_DIR):
    """載入並合併 trades.jsonl 與 trade_status.json 之去重成交紀錄與最新狀態快照。"""
    trades = []
    seen_ids = set()
    status_snapshot = {}

    trades_file = os.path.join(base_dir, "data", "trades.jsonl")
    if os.path.exists(trades_file):
        try:
            with open(trades_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    item = json.loads(line)
                    eid = item.get("exec_id") or item.get("position_id")
                    if eid:
                        seen_ids.add(eid)
                    trades.append(item)
        except Exception:
            pass

    status_file = os.path.join(base_dir, "trade_status.json")
    if os.path.exists(status_file):
        try:
            with open(status_file, "r", encoding="utf-8") as sf:
                status_snapshot = json.load(sf)
                for r in status_snapshot.get("recent_trades", []):
                    eid = r.get("exec_id") or r.get("position_id")
                    if eid and eid not in seen_ids:
                        seen_ids.add(eid)
                        trades.append(r)
        except Exception:
            pass

    return trades, status_snapshot


def build_daily_pnl_calendar(month_str: str = None, base_dir: str = CURRENT_DIR):
    """彙整歷史報告、成交流水配對與即時狀態為日曆式每日 HKD 盈虧數據。"""
    from live_ibkr_dual_engine import pair_executions_fifo

    target_month = (month_str or "").strip()
    if len(target_month) != 7 or "-" not in target_month:
        target_month = datetime.now().strftime("%Y-%m")

    all_trades, status_snapshot = _load_deduped_executions(base_dir=base_dir)
    fx_rate = float(status_snapshot.get("fx_rate_usd_hkd") or 7.78)
    paired_groups = pair_executions_fifo(all_trades, fx_rate=fx_rate)

    days_map = {}
    available_months = set([datetime.now().strftime("%Y-%m"), target_month])

    def _ensure_day(d_str: str):
        if d_str not in days_map:
            days_map[d_str] = {
                "date": d_str,
                "pnl_usd": 0.0,
                "pnl_hkd": 0.0,
                "mes_pnl_hkd": 0.0,
                "mnq_pnl_hkd": 0.0,
                "closed_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "open_count": 0,
                "source": "executions",
                "trades": [],
            }
        return days_map[d_str]

    # 1. 由 FIFO 配對成交組彙整每日平倉損益（依平倉日期入帳，已含雙邊手續費）
    for grp in paired_groups:
        close_orders = grp.get("close_orders") or []
        sym = str(grp.get("symbol") or "MES").upper()
        if not close_orders:
            open_date = str(grp.get("open_timestamp") or "")[:10]
            if len(open_date) == 10:
                available_months.add(open_date[:7])
                if open_date.startswith(target_month):
                    entry = _ensure_day(open_date)
                    entry["open_count"] += 1
                    entry["trades"].append({
                        "symbol": sym,
                        "direction": grp.get("direction", "LONG"),
                        "status": grp.get("status", "OPEN"),
                        "contracts": grp.get("contracts", 1),
                        "open_price": grp.get("open_price"),
                        "close_price": None,
                        "pnl_usd": 0.0,
                        "pnl_hkd": 0.0,
                        "timestamp": grp.get("open_timestamp", ""),
                    })
            continue

        # 將同一開倉單下的各平倉子單依其平倉日期彙整
        for co in close_orders:
            close_date = str(co.get("timestamp") or grp.get("open_timestamp") or "")[:10]
            if len(close_date) != 10:
                continue
            available_months.add(close_date[:7])
            if not close_date.startswith(target_month):
                continue
            entry = _ensure_day(close_date)
            pnl_u = float(co.get("pnl") or 0.0)
            pnl_h = float(co.get("pnl_hkd") if co.get("pnl_hkd") is not None else round(pnl_u * fx_rate, 2))
            entry["pnl_usd"] = round(entry["pnl_usd"] + pnl_u, 2)
            entry["pnl_hkd"] = round(entry["pnl_hkd"] + pnl_h, 2)
            if "MNQ" in sym:
                entry["mnq_pnl_hkd"] = round(entry["mnq_pnl_hkd"] + pnl_h, 2)
            else:
                entry["mes_pnl_hkd"] = round(entry["mes_pnl_hkd"] + pnl_h, 2)
            entry["closed_count"] += 1
            if pnl_u > 0:
                entry["win_count"] += 1
            elif pnl_u < 0:
                entry["loss_count"] += 1
            entry["trades"].append({
                "symbol": sym,
                "direction": grp.get("direction", "LONG"),
                "status": grp.get("status", "CLOSED"),
                "contracts": co.get("contracts", grp.get("contracts", 1)),
                "open_price": grp.get("open_price"),
                "close_price": co.get("close_price"),
                "pnl_usd": round(pnl_u, 2),
                "pnl_hkd": round(pnl_h, 2),
                "timestamp": co.get("timestamp", ""),
            })

    # 2. 若歷史某日已有每日結算報告但不在 trades.jsonl 中，補齊該日損益
    reports_dir = os.path.join(base_dir, "reports")
    if os.path.isdir(reports_dir):
        for fn in sorted(os.listdir(reports_dir)):
            if not (fn.startswith("daily_report_") and fn.endswith(".json")):
                continue
            d_str = fn[len("daily_report_"):-len(".json")]
            if len(d_str) != 10:
                continue
            available_months.add(d_str[:7])
            if not d_str.startswith(target_month) or d_str in days_map:
                continue
            try:
                with open(os.path.join(reports_dir, fn), "r", encoding="utf-8") as rf:
                    rep = json.load(rf)
                pnl_perf = rep.get("pnl_performance", {})
                sb = rep.get("strategy_breakdown", {})
                # total_realized_pnl 來自 IBKR commissionReport，已含雙邊手續費
                raw_usd = float(pnl_perf.get("total_realized_pnl") or pnl_perf.get("net_pnl") or 0.0)
                mes_usd = float((sb.get("v43_mes") or {}).get("realized_pnl") or 0.0)
                mnq_usd = float((sb.get("v50_mnq") or {}).get("realized_pnl") or 0.0)
                total_t = int(pnl_perf.get("total_trades") or 0)
                wins = int(pnl_perf.get("win_count") or 0)
                losses = int(pnl_perf.get("loss_count") or 0)
                if total_t > 0 or abs(raw_usd) > 1e-6:
                    entry = _ensure_day(d_str)
                    entry["pnl_usd"] = round(raw_usd, 2)
                    entry["pnl_hkd"] = round(raw_usd * fx_rate, 2)
                    entry["mes_pnl_hkd"] = round(mes_usd * fx_rate, 2)
                    entry["mnq_pnl_hkd"] = round(mnq_usd * fx_rate, 2)
                    entry["closed_count"] = max(total_t, wins + losses)
                    entry["win_count"] = wins
                    entry["loss_count"] = losses
                    entry["source"] = "daily_report"
            except Exception:
                pass

    # 3. 若當日尚無配對成交紀錄，但 trade_status.json 已有即時已實現損益 (HKD)，補齊當日卡片
    status_date = str(status_snapshot.get("last_update") or "")[:10]
    if len(status_date) == 10 and status_date.startswith(target_month):
        live_r_hkd = float(status_snapshot.get("realized_pnl_hkd") if status_snapshot.get("realized_pnl_hkd") is not None else (status_snapshot.get("realized_pnl") or 0.0))
        if abs(live_r_hkd) > 1e-6 and status_date not in days_map:
            entry = _ensure_day(status_date)
            entry["pnl_hkd"] = round(live_r_hkd, 2)
            entry["pnl_usd"] = round(live_r_hkd / fx_rate, 2)
            entry["win_count"] = 1 if live_r_hkd > 0 else 0
            entry["loss_count"] = 1 if live_r_hkd < 0 else 0
            entry["source"] = "live_status"

    net_pnl_hkd = round(sum(d["pnl_hkd"] for d in days_map.values()), 2)
    net_pnl_usd = round(sum(d["pnl_usd"] for d in days_map.values()), 2)
    win_days = sum(1 for d in days_map.values() if d["pnl_hkd"] > 0)
    loss_days = sum(1 for d in days_map.values() if d["pnl_hkd"] < 0)
    total_closed = sum(d["closed_count"] for d in days_map.values())

    return {
        "status": "ok",
        "month": target_month,
        "fx_rate_usd_hkd": round(fx_rate, 4),
        "available_months": sorted(available_months, reverse=True),
        "monthly_summary": {
            "net_pnl_hkd": net_pnl_hkd,
            "net_pnl_usd": net_pnl_usd,
            "win_days": win_days,
            "loss_days": loss_days,
            "trading_days": win_days + loss_days,
            "total_closed_orders": total_closed,
        },
        "days": days_map,
    }

def get_access_pin():
    """取得設定的安全 PIN 碼 (公網遠端防護，支援 .env 即時熱載入)"""
    env_path = os.path.join(CURRENT_DIR, ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("WEB_ACCESS_PIN="):
                        val = line.split("=", 1)[1].strip()
                        if val:
                            return val
        except Exception:
            pass
    return os.getenv("WEB_ACCESS_PIN", "").strip() or "888888"

def get_tunnel_info():
    """取得當前 Cloudflare 穿透狀態 (隱私脫敏)"""
    if os.path.exists(TUNNEL_INFO_FILE):
        try:
            with open(TUNNEL_INFO_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                data.pop("pin", None)
                return data
        except Exception:
            pass
    return {"status": "OFFLINE", "public_url": None, "mobile_url": None}

def get_lan_ip():
    """獲取本機在區域網絡 (Wi-Fi/LAN) 的 IP 地址，供手機在同一個 Wi-Fi 下連線"""
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        return ip
    except Exception:
        return "127.0.0.1"
    finally:
        if s:
            s.close()

# 初始化控制狀態檔案
def get_control_state():
    if os.path.exists(CONTROL_FILE):
        try:
            with open(CONTROL_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"trading_enabled": True, "emergency_flat": False, "last_action": "start"}

def save_control_state(state):
    try:
        with open(CONTROL_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving control state: {e}")

class ControlDashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=CURRENT_DIR, **kwargs)

    def is_authorized(self, payload=None) -> bool:
        """驗證遠端或公網操作者的安全 PIN 碼"""
        expected_pin = get_access_pin()
        if not expected_pin:
            return True

        # 1. 檢查 Header: X-Access-PIN
        req_pin = self.headers.get("X-Access-PIN")
        if req_pin and req_pin.strip() == expected_pin:
            return True

        # 2. 檢查 Header: Authorization: Bearer <pin>
        auth_hdr = self.headers.get("Authorization", "")
        if auth_hdr.startswith("Bearer "):
            token = auth_hdr[7:].strip()
            if token == expected_pin:
                return True

        # 3. 檢查 JSON payload
        if payload and isinstance(payload, dict):
            if str(payload.get("pin", "")).strip() == expected_pin:
                return True

        # 4. 檢查 Query String (?pin=...)
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        if "pin" in qs and qs["pin"][0].strip() == expected_pin:
            return True

        # 5. 若來自外部 Cloudflare 穿透代理，強制需要 PIN 碼認證，不得以本機 IP 放行
        is_proxied = bool(self.headers.get("CF-Ray") or self.headers.get("CF-Connecting-IP") or self.headers.get("X-Forwarded-For"))
        if is_proxied:
            return False

        # 6. 若來自純本機內部調用 (127.0.0.1 Direct)，自動信任放行
        client_ip = self.client_address[0]
        if client_ip in ("127.0.0.1", "::1"):
            return True

        return False

    def end_headers(self):
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _send_json(self, status_code: int, payload: dict):
        """安全輸出標準 JSON（自動淨化 NaN/Infinity 並設定正確 Content-Type）。"""
        safe_payload = sanitize_json_value(payload)
        body = json.dumps(safe_payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _collect_recent_trades(self, limit: int = 50, symbol_filter: str = None):
        """整合 data/trades.jsonl 與 trade_status.json 最新成交紀錄"""
        trades, status_snapshot = _load_deduped_executions(base_dir=CURRENT_DIR)

        if symbol_filter:
            trades = [t for t in trades if symbol_filter.upper() in (t.get("symbol") or "").upper()]

        trades = sorted(trades, key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]
        return trades, status_snapshot

    def _collect_diagnostics_inputs(self, limit_trades: int = 100, limit_incidents: int = 100, log_line_count: int = 200):
        """彙整最新成交、持倉快照、異常稽核與守護引擎尾部日誌"""
        trades, status_snapshot = self._collect_recent_trades(limit=limit_trades)
        incidents = IncidentManager().get_recent_incidents(limit=limit_incidents)
        log_lines = []
        log_path = os.path.join(CURRENT_DIR, "mes_dual_engine_live.log")
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8", errors="replace") as lf:
                log_lines = [line.rstrip("\n") for line in lf.readlines()[-log_line_count:]]
        return trades, incidents, status_snapshot, log_lines

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if parsed.path == "/api/auth/status":
            pin = get_access_pin()
            tunnel = get_tunnel_info()
            client_ip = self.client_address[0]
            is_proxied = bool(self.headers.get("CF-Ray") or self.headers.get("CF-Connecting-IP") or self.headers.get("X-Forwarded-For"))
            is_local = (client_ip in ("127.0.0.1", "::1", "localhost")) and not is_proxied
            resp = {
                "status": "ok",
                "pin_required": bool(pin),
                "is_localhost": is_local,
                "is_remote_tunnel": is_proxied,
                "tunnel": tunnel
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/tunnel_info":
            tunnel = get_tunnel_info()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(tunnel, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/control":
            state = get_control_state()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(state).encode("utf-8"))
            return

        if parsed.path == "/api/reports":
            req_date = query.get("date", [None])[0]
            gen = DailyReportGenerator()
            if req_date:
                rep = gen.get_report(req_date)
                if rep:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps(rep, ensure_ascii=False).encode("utf-8"))
                else:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": f"找不到日期 {req_date} 之結算報告"}, ensure_ascii=False).encode("utf-8"))
            else:
                reports = gen.list_reports()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps(reports, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/reports/latest":
            gen = DailyReportGenerator()
            rep = gen.get_latest_report()
            if rep:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps(rep, ensure_ascii=False).encode("utf-8"))
            else:
                self.send_response(404)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "尚未有可用的結算報告"}, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/incidents":
            try:
                limit = int(query.get("limit", [50])[0])
            except ValueError:
                limit = 50
            category = query.get("category", [None])[0]
            severity = query.get("severity", [None])[0]
            req_date = query.get("date", [None])[0]

            mgr = IncidentManager()
            summary = mgr.get_incident_summary(req_date)
            incidents = mgr.get_recent_incidents(limit=limit, category=category, severity=severity, date_str=req_date)
            resp = {
                "status": "ok",
                "summary": summary,
                "incidents": incidents
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/trades":
            try:
                limit = int(query.get("limit", [50])[0])
            except ValueError:
                limit = 50
            symbol_filter = query.get("symbol", [None])[0]
            trades, _ = self._collect_recent_trades(limit=limit, symbol_filter=symbol_filter)

            resp = {
                "status": "ok",
                "count": len(trades),
                "trades": trades
            }
            self._send_json(200, resp)
            return

        if parsed.path == "/api/pnl/calendar":
            month_param = query.get("month", [None])[0]
            try:
                resp = build_daily_pnl_calendar(month_str=month_param, base_dir=CURRENT_DIR)
                self._send_json(200, resp)
            except Exception as e:
                self._send_json(500, {"status": "error", "message": f"讀取每日盈虧日曆失敗: {str(e)}"})
            return

        if parsed.path == "/api/version":
            check_flag = str(query.get("check", ["0"])[0]).strip().lower() in ("1", "true", "yes")
            try:
                resp = get_version_info(base_dir=CURRENT_DIR, check_remote=check_flag)
                self._send_json(200, resp)
            except Exception as e:
                self._send_json(500, {"status": "error", "message": f"讀取版本資訊失敗: {str(e)}"})
            return

        if parsed.path == "/api/lan_info":
            lan_ip = get_lan_ip()
            port = self.server.server_port
            resp = {
                "status": "ok",
                "lan_ip": lan_ip,
                "port": port,
                "mobile_url": f"http://{lan_ip}:{port}/mobile.html",
                "desktop_url": f"http://{lan_ip}:{port}/dashboard.html"
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path in ("/mobile", "/mobile/"):
            self.send_response(302)
            self.send_header("Location", "/mobile.html")
            self.end_headers()
            return

        if parsed.path == "/api/diagnostics/download":
            fmt = str(query.get("format", ["html"])[0]).lower().strip()
            try:
                limit_trades = int(query.get("limit_trades", [100])[0])
            except ValueError:
                limit_trades = 100
            try:
                limit_incidents = int(query.get("limit_incidents", [100])[0])
            except ValueError:
                limit_incidents = 100
            try:
                log_line_count = int(query.get("log_lines", [200])[0])
            except ValueError:
                log_line_count = 200

            try:
                trades, incidents, status_snapshot, log_lines = self._collect_diagnostics_inputs(
                    limit_trades=limit_trades,
                    limit_incidents=limit_incidents,
                    log_line_count=log_line_count
                )
                from gdrive_report_manager import GoogleDriveReportManager
                gdrive_mgr = GoogleDriveReportManager()
                gen_info = gdrive_mgr.generate_diagnostics_files(
                    trades=trades,
                    incidents=incidents,
                    status_snapshot=status_snapshot,
                    log_lines=log_lines
                )
                target_path = gen_info["csv_path"] if fmt == "csv" else gen_info["html_path"]
                content_type = "text/csv; charset=utf-8" if fmt == "csv" else "text/html; charset=utf-8"
                ext = "csv" if fmt == "csv" else "html"
                ascii_name = f"AutoTrade_diagnostics_{gen_info['ts_tag']}.{ext}"
                utf8_name = quote(os.path.basename(target_path))

                with open(target_path, "rb") as df:
                    file_bytes = df.read()

                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Disposition", f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{utf8_name}")
                self.send_header("Content-Length", str(len(file_bytes)))
                self.end_headers()
                self.wfile.write(file_bytes)
            except Exception as e:
                resp = {"status": "error", "message": f"下載診斷報告失敗: {str(e)}"}
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/audit/latest":
            try:
                from daily_auto_audit_fixer import DailyAutoAuditFixer
                rep = DailyAutoAuditFixer().get_latest_audit()
                if rep:
                    self._send_json(200, rep)
                else:
                    self._send_json(404, {"status": "error", "message": "尚未有雙層閉環巡檢紀錄"})
            except Exception as e:
                self._send_json(500, {"status": "error", "message": str(e)})
            return

        if parsed.path == "/api/audit/status":
            try:
                self._send_json(200, get_audit_job_status())
            except Exception as e:
                self._send_json(500, {"status": "error", "running": False, "message": str(e)})
            return

        if parsed.path in ("/backtest", "/backtest/"):
            self.send_response(302)
            self.send_header("Location", "/backtest_lab.html")
            self.end_headers()
            return

        if parsed.path == "/api/backtest/ibkr-cache-list":
            try:
                from ibkr_history_downloader import list_cached_csvs
                resp = {"status": "ok", "items": list_cached_csvs()}
                self.send_response(200)
            except Exception as e:
                resp = {"status": "error", "message": str(e), "items": []}
                self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/manifest.json":
            manifest_file = os.path.join(CURRENT_DIR, "manifest.json")
            if os.path.exists(manifest_file):
                self.send_response(200)
                self.send_header("Content-Type", "application/manifest+json; charset=utf-8")
                self.end_headers()
                with open(manifest_file, "rb") as f:
                    self.wfile.write(f.read())
                return
        
        # 預設首頁重新導向到 dashboard.html
        if parsed.path == "/" or parsed.path == "":
            self.send_response(302)
            self.send_header("Location", "/dashboard.html")
            self.end_headers()
            return

        super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/reports/generate":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b"{}"
            target_date = None
            try:
                payload = json.loads(post_data.decode("utf-8"))
                target_date = payload.get("date")
            except Exception:
                pass

            gen = DailyReportGenerator()
            md_file, json_file, rep_data = gen.generate_report(target_date)
            md_text = ""
            if os.path.exists(md_file):
                with open(md_file, "r", encoding="utf-8") as f:
                    md_text = f.read()

            resp = {
                "status": "ok",
                "message": f"成功生成 {rep_data['report_date']} 結算報告！",
                "report": rep_data,
                "markdown": md_text
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/reports/push_gdrive":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b"{}"
            target_date = None
            try:
                payload = json.loads(post_data.decode("utf-8"))
                target_date = payload.get("date")
            except Exception:
                pass

            gen = DailyReportGenerator()
            if not target_date:
                latest = gen.get_latest_report()
                if latest:
                    target_date = latest["date"]
                else:
                    target_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

            rep = gen.get_report(target_date)
            if not rep:
                # 若該日無報告，先生成報告
                md_file, json_file, rep_data = gen.generate_report(target_date, push_to_gdrive=True)
                rep = gen.get_report(target_date)

            try:
                from gdrive_report_manager import GoogleDriveReportManager
                gdrive_mgr = GoogleDriveReportManager()
                gdrive_res = gdrive_mgr.push_daily_report(rep["data"], force_reupload=True)
                rep["data"]["gdrive_sync"] = gdrive_res
                json_file = os.path.join(CURRENT_DIR, "reports", f"daily_report_{target_date}.json")
                md_file = os.path.join(CURRENT_DIR, "reports", f"daily_report_{target_date}.md")
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(rep["data"], f, indent=2, ensure_ascii=False)
                new_md = gen._format_markdown_report(rep["data"])
                with open(md_file, "w", encoding="utf-8") as f:
                    f.write(new_md)
                rep["markdown"] = new_md

                resp = {
                    "status": "ok",
                    "message": f"成功推播 {target_date} 報告至 Google Drive！",
                    "gdrive": gdrive_res,
                    "report": rep["data"],
                    "markdown": new_md
                }
            except Exception as e:
                resp = {
                    "status": "error",
                    "message": f"Google Drive 推播失敗: {str(e)}"
                }

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/diagnostics/export_gdrive":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b"{}"
            limit_trades = 100
            limit_incidents = 100
            log_line_count = 200
            try:
                payload = json.loads(post_data.decode("utf-8"))
                limit_trades = int(payload.get("limit_trades", 100))
                limit_incidents = int(payload.get("limit_incidents", 100))
                log_line_count = int(payload.get("log_lines", 200))
            except Exception:
                pass

            try:
                trades, incidents, status_snapshot, log_lines = self._collect_diagnostics_inputs(
                    limit_trades=limit_trades,
                    limit_incidents=limit_incidents,
                    log_line_count=log_line_count
                )

                from gdrive_report_manager import GoogleDriveReportManager
                gdrive_mgr = GoogleDriveReportManager()
                resp = gdrive_mgr.export_diagnostics_to_gdrive(
                    trades=trades,
                    incidents=incidents,
                    status_snapshot=status_snapshot,
                    log_lines=log_lines
                )
            except Exception as e:
                resp = {
                    "status": "error",
                    "message": f"匯出診斷報告至 Google Drive 失敗: {str(e)}"
                }

            self._send_json(200, resp)
            return

        if parsed.path == "/api/incidents/ack":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(post_data.decode("utf-8"))
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}

            try:
                summary = IncidentManager().ack_incidents(
                    ack_total_count=payload.get("total_count"),
                    last_incident_id=payload.get("last_incident_id"),
                    last_timestamp=payload.get("last_timestamp"),
                )
                resp = {"status": "ok", "summary": summary}
                self._send_json(200, resp)
            except Exception as e:
                self._send_json(500, {"status": "error", "message": f"標記異常稽核已讀失敗: {e}"})
            return

        if parsed.path == "/api/audit/run":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(post_data.decode("utf-8"))
            except Exception:
                payload = {}

            if bool(payload.get("sync", False)):
                try:
                    from daily_auto_audit_fixer import DailyAutoAuditFixer
                    resp = DailyAutoAuditFixer().run_daily_closed_loop(
                        hours=int(payload.get("hours", 24)),
                        upload_gdrive=bool(payload.get("upload_gdrive", True)),
                        enable_ai_fix=bool(payload.get("enable_ai_fix", True)),
                        force_ai=bool(payload.get("force_ai", False)),
                        push_branch=bool(payload.get("push_branch", True)),
                        notify_telegram=bool(payload.get("notify_telegram", True)),
                    )
                    self._send_json(200, resp)
                except Exception as e:
                    self._send_json(500, {"status": "error", "message": f"雙層閉環巡檢失敗: {str(e)}"})
                return

            try:
                resp = start_audit_job(payload)
                self._send_json(202, resp)
            except Exception as e:
                self._send_json(500, {"status": "error", "message": f"無法啟動雙層閉環巡檢: {str(e)}"})
            return

        if parsed.path == "/api/auth/verify":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(post_data.decode("utf-8"))
            except Exception:
                payload = {}
            pin_input = str(payload.get("pin", "")).strip()
            expected_pin = get_access_pin()
            if pin_input == expected_pin:
                resp = {"status": "ok", "valid": True, "message": "PIN 碼驗證成功"}
                self.send_response(200)
            else:
                resp = {"status": "error", "valid": False, "message": "安全 PIN 碼不正確"}
                self.send_response(401)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/positions/close":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length)
            symbol = "ALL"
            reason = "WEB_MANUAL_CLOSE"
            payload = {}
            try:
                payload = json.loads(post_data.decode("utf-8"))
                symbol = payload.get("symbol", "ALL").upper()
                reason = payload.get("reason", "WEB_MANUAL_CLOSE")
            except Exception:
                pass

            # 驗證訪問 PIN 碼 (公網遠端防護)
            if not self.is_authorized(payload):
                self.send_response(401)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "未授權：請輸入正確的安全 PIN 碼以執行平倉操作"}, ensure_ascii=False).encode("utf-8"))
                return

            state = get_control_state()
            state["manual_close_symbol"] = symbol
            state["manual_close_reason"] = reason
            state["manual_close_timestamp"] = datetime.now().isoformat()
            state["last_action"] = f"manual_close_{symbol.lower()}"
            save_control_state(state)

            # 記錄審計日誌
            try:
                mgr = IncidentManager()
                strat = "ALL" if symbol == "ALL" else (f"V9.3_{symbol}" if symbol in ("MES", "MNQ", "MHI", "MCL") else symbol)
                mgr.log_incident(
                    incident_type="MANUAL_OVERRIDE",
                    message=f"[Web手動平倉指令] 用戶觸發手動平倉: {symbol} (原因: {reason})",
                    severity="WARNING",
                    code=8001,
                    strategy=strat,
                    details={"symbol": symbol, "reason": reason, "timestamp": datetime.now().isoformat()}
                )
            except Exception as e:
                print(f"Error logging incident: {e}")

            # 方案 C：自動推送手動平倉請求至 Telegram
            try:
                from telegram_notifier import notifier
                notifier.notify_manual_flat(
                    symbol=symbol,
                    cancelled_orders=0,
                    details=[f"來自遠端/網頁端指令 ({reason})"],
                    reason=f"遠端指令觸發 ({reason})"
                )
            except Exception as e:
                print(f"Telegram notify manual flat error: {e}")

            resp = {
                "status": "ok",
                "message": f"已發送 {symbol} 手動平倉指令至交易引擎",
                "symbol": symbol,
                "timestamp": datetime.now().isoformat()
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/control":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length)
            
            action = "toggle"
            payload = {}
            try:
                payload = json.loads(post_data.decode("utf-8"))
                action = payload.get("action", "toggle")
            except Exception:
                pass

            # 驗證訪問 PIN 碼 (公網遠端防護)
            if not self.is_authorized(payload):
                self.send_response(401)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "未授權：請輸入正確的安全 PIN 碼以執行控制操作"}, ensure_ascii=False).encode("utf-8"))
                return

            state = get_control_state()
            if action == "pause":
                state["trading_enabled"] = False
                state["last_action"] = "pause"
            elif action == "resume" or action == "start":
                state["trading_enabled"] = True
                state["last_action"] = "start"
            elif action == "toggle":
                state["trading_enabled"] = not state.get("trading_enabled", True)
                state["last_action"] = "start" if state["trading_enabled"] else "pause"
            elif action == "flat":
                state["emergency_flat"] = True
                state["last_action"] = "flat"
            elif action == "pause_v43":
                state["trading_enabled_v43"] = False
                state["last_action"] = "pause_v43"
            elif action == "start_v43":
                state["trading_enabled_v43"] = True
                state["last_action"] = "start_v43"
            elif action == "flat_v43":
                state["emergency_flat_v43"] = True
                state["last_action"] = "flat_v43"
            elif action == "pause_v50":
                state["trading_enabled_v50"] = False
                state["last_action"] = "pause_v50"
            elif action == "start_v50":
                state["trading_enabled_v50"] = True
                state["last_action"] = "start_v50"
            elif action == "flat_v50":
                state["emergency_flat_v50"] = True
                state["last_action"] = "flat_v50"

            save_control_state(state)

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            resp = {
                "status": "ok",
                "trading_enabled": state.get("trading_enabled", True),
                "trading_enabled_v43": state.get("trading_enabled_v43", True),
                "trading_enabled_v50": state.get("trading_enabled_v50", True),
                "emergency_flat": state.get("emergency_flat", False),
                "last_action": state.get("last_action", "")
            }
            self.wfile.write(json.dumps(resp).encode("utf-8"))
            return

        if parsed.path == "/api/backtest/run":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(post_data.decode("utf-8"))
            except Exception:
                payload = {}

            try:
                from backtest_engine import run_multi_strategy_backtest_api
                resp = run_multi_strategy_backtest_api(payload)
                self.send_response(200)
            except Exception as e:
                resp = {"status": "error", "message": f"回測執行異常: {e}"}
                self.send_response(500)

            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/backtest/export-gdrive":
            try:
                from backtest_engine import export_backtest_to_gdrive
                resp = export_backtest_to_gdrive()
                self.send_response(200)
            except Exception as e:
                resp = {"status": "error", "message": f"報告匯出異常: {e}"}
                self.send_response(500)

            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        if parsed.path == "/api/backtest/ibkr-download":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(post_data.decode("utf-8"))
            except Exception:
                payload = {}

            try:
                from ibkr_history_downloader import download_ibkr_history_to_csv, download_multi_chunks
                port_val = payload.get("port")
                chunks = int(payload.get("chunks", 1))
                sym = str(payload.get("symbol", "MES")).upper().strip()
                tf = str(payload.get("timeframe", "1h")).lower().strip()
                dur = str(payload.get("duration", "1 Y")).strip()
                delay = float(payload.get("delay", 8.0))
                cont = bool(payload.get("continue_from_cache", False))

                if chunks > 1:
                    resp = download_multi_chunks(
                        symbol=sym,
                        timeframe=tf,
                        total_chunks=chunks,
                        chunk_duration=dur,
                        port=int(port_val) if port_val else None,
                        pacing_delay_sec=delay,
                        continue_from_cache=cont
                    )
                else:
                    resp = download_ibkr_history_to_csv(
                        symbol=sym,
                        timeframe=tf,
                        duration=dur,
                        port=int(port_val) if port_val else None
                    )
                self.send_response(200)
            except Exception as e:
                resp = {"status": "error", "message": f"IBKR 下載異常: {e}"}
                self.send_response(500)

            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode("utf-8"))
            return

        self.send_error(404, "Endpoint Not Found")

    def log_message(self, format, *args):
        # 避免洗版終端機日誌
        try:
            msg = format % args
            if any(k in msg for k in ("/api/control", "trade_status.json", "/api/incidents", "/api/reports")):
                return
        except Exception:
            pass
        super().log_message(format, *args)


def run_server(port=8080):
    server_address = ("0.0.0.0", port)
    server_cls = ThreadingHTTPServer if "ThreadingHTTPServer" in globals() else HTTPServer
    try:
        httpd = server_cls(server_address, ControlDashboardHandler)
    except OSError:
        fallback_port = 8088 if port != 8088 else 8080
        print(f"[Dashboard Server] 端口 {port} 被佔用，改用備用端口 http://0.0.0.0:{fallback_port}/")
        server_address = ("0.0.0.0", fallback_port)
        httpd = server_cls(server_address, ControlDashboardHandler)
        port = fallback_port

    lan_ip = get_lan_ip()
    print(f"[Dashboard Server] 正在監聽本機: http://localhost:{port}/")
    print(f"[Dashboard Server] 區域網電腦連線: http://{lan_ip}:{port}/dashboard.html")
    print(f"[Mobile Monitoring] 手機瀏覽器連線: http://{lan_ip}:{port}/mobile.html")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[Dashboard Server] 伺服器安全關閉")
        httpd.server_close()

if __name__ == "__main__":
    port = 8080
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass
    run_server(port)
