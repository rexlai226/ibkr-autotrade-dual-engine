#!/usr/bin/env python3
"""
=============================================================================
 AutoTrade 手動緊急平倉工具 (Manual Position Liquidation Tool)
=============================================================================
支援標的:
  - MES  : 手動市價全平微型標普合約 (V4.3)
  - MNQ  : 手動市價全平微型納指合約 (V5.0) 
  - MHI  : 手動市價全平小恆生合約 (HKFE)
  - MCL  : 手動市價全平微型原油合約 (NYMEX)
  - ALL  : 手動市價全平所有合約 (MES + MNQ) + MHI + MCL
=============================================================================
"""

import sys
import os
import json
import time
import argparse
from typing import Dict, Any

WORKSPACE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(WORKSPACE_DIR, "data")
CONTROL_FILE = os.path.join(DATA_DIR, "trade_control.json")
STATUS_FILE = os.path.join(DATA_DIR, "trade_status.json")

def trigger_via_control_file(symbol: str) -> bool:
    """透過 trade_control.json 向執行中的雙引擎守護進程發送平倉指令"""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        ctrl = {}
        if os.path.exists(CONTROL_FILE):
            try:
                with open(CONTROL_FILE, "r", encoding="utf-8") as f:
                    ctrl = json.load(f)
            except Exception:
                ctrl = {}

        symbol = symbol.upper()
        if symbol == "ALL":
            ctrl["emergency_flat"] = True
            ctrl["manual_close_symbol"] = "ALL"
        elif symbol == "MES":
            ctrl["emergency_flat_v43"] = True
            ctrl["manual_close_symbol"] = "MES"
        elif symbol == "MNQ":
            ctrl["emergency_flat_v50"] = True
            ctrl["manual_close_symbol"] = "MNQ"
       elif symbol in ("MHI", "MCL"):
            ctrl["manual_close_symbol"] = symbol
        else:
            print(f"[❌] 不支援的標的: {symbol} (僅支援 MES, MHI, MCL, MNQ, ALL)")
            return False
        ctrl["manual_close_reason"] = f"manual_close_{symbol}"
        ctrl["manual_close_timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(CONTROL_FILE, "w", encoding="utf-8") as f:
            json.dump(ctrl, f, indent=2, ensure_ascii=False)

        print(f"[✓] 已向雙引擎發送平倉指令: {symbol}，正在等待執行結果...")
        time.sleep(1.5)

        # 讀取狀態驗證
        if os.path.exists(STATUS_FILE):
            try:
                with open(STATUS_FILE, "r", encoding="utf-8") as f:
                    st = json.load(f)
                pos_v43 = st.get("v43", {}).get("active_position")
                pos_v50 = st.get("v50", {}).get("active_position")
                print(f"[狀態確認] MES 當前持倉: {pos_v43 or '無持倉 (已清空)'}")
                print(f"[狀態確認] MNQ 當前持倉: {pos_v50 or '無持倉 (已清空)'}")
            except Exception:
                pass

        return True
    except Exception as e:
        print(f"[❌] 發送控制指令失敗: {e}")
        return False

def trigger_direct_ibkr(symbol: str):
    """當背景引擎未啟動時，直接直連 IB Gateway / TWS 執行市價強制平倉"""
    print(f"[!] 正在以獨立連線直連 IB Gateway 平倉 {symbol}...")
    try:
        from ib_async import IB, Future, MarketOrder
    except ImportError:
        print("[❌] 缺少 ib_async 套件，無法直接連線 IBKR")
        return False

    ib = IB()
    try:
        ib.connect("127.0.0.1", 4002, clientId=99, timeout=4)
    except Exception:
        try:
            ib.connect("127.0.0.1", 7497, clientId=99, timeout=4)
        except Exception as e:
            print(f"[❌] 無法連線至 IB Gateway/TWS (Port 4002/7497): {e}")
            return False

    accounts = ib.managedAccounts()
    if not accounts:
        print("[❌] 未取得任何託管帳戶")
        ib.disconnect()
        return False

    acct = accounts[0]
    print(f"[✓] 成功直連帳戶: {acct}")

    symbols_to_flat = ["MES", "MNQ", "MHI", "MCL"] if symbol == "ALL" else [symbol]

    for sym in symbols_to_flat: #
           exchange = "HKFE" if sym == "MHI" else ("NYMEX" if sym == "MCL" else "CME")
           currency = "HKD" if sym == "MHI" else "USD" #
           contract = Future(symbol=sym, exchange=exchange, currency=currency)
   #      con# tcont 
# rac        contt = Future(symbol=sym, exchange=exchange, currency=currency) # ract = Future(symbol=sym, exchange="CME")
        details = ib.reqContractDetails(contract)
        if not details:
            print(f"[!] 找不到 {sym} 當季期貨合約")
            continue
        sorted_contracts = sorted(details, key=lambda d: d.contract.lastTradeDateOrContractMonth)
        target = sorted_contracts[0].contract
        ib.qualifyContracts(target)

        # 撤銷掛單
        for t in ib.openTrades():
            if t.contract.symbol == sym or (hasattr(t.contract, 'conId') and t.contract.conId == target.conId):
                print(f"  - 撤銷掛單: OrderId {t.order.orderId}")
                ib.cancelOrder(t.order)

        # 平倉持倉
        flat_done = False
        for p in ib.positions(acct):
            if p.contract.symbol == sym and p.position != 0:
                action = "SELL" if p.position > 0 else "BUY"
                qty = abs(int(p.position))
                mo = MarketOrder(action, qty)
                tr = ib.placeOrder(target, mo)
                flat_done = True
                print(f"  - [✓] 已發送市價強制平倉單: {action} {qty} 口 {sym}")

        if not flat_done:
            print(f"  - {sym} 當前無持倉，已完成掛單清理")

    ib.sleep(1.0)
    ib.disconnect()
    print("[✓] 獨立平倉作業執行完畢")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AutoTrade 手動緊急平倉工具") #
  parser.add_argument("symbol", nargs="?", default="ALL", choices=["MES", "MNQ", "MHI", "MCL", "ALL", "mes", "mnq", "mhi", "mcl", "all"],
#     parser.add_argument("symbol", nargs="?", default="ALL", choices=["MES",, "MHI", "MCL" "MNQ", "ALL", "mes", "mnq", "all"],
                        help="欲平倉的合約標的 (預設: ALMES / MNQ / MHI / MCL / ALL，L)")
    args = parser.parse_args()

    sym = args.symbol.upper()
    print("=" * 60)
    print(f"🛑 AutoTrade 手動緊急平倉指令: {sym}")
    print("=" * 60)

    # 優先嘗試寫入 control file 給 daemon 處理
    success = trigger_via_control_file(sym)
    if not success:
        trigger_direct_ibkr(sym)
