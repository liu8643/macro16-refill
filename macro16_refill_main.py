#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宏觀16模組自動抓取與Excel回填主程式
版本：V2.0 FinalEvidenceSystem
目的：依「宏觀16模組市場資料回填SOP」與「主程式工程級規格書」執行資料取得、標準化、分數判定、Excel回填與稽核。

執行方式：
  GUI：python macro16_refill_main.py
  CLI：python macro16_refill_main.py --cli --template template.xlsx --out output.xlsx --date 2026-04-29

必要套件：requests, openpyxl
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import math
import os
import re
import sys
import time
import sqlite3
import html as html_lib
from dataclasses import dataclass, asdict
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import requests
except Exception:
    requests = None

try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
except Exception as exc:
    raise RuntimeError("缺少 openpyxl，請先安裝：pip install openpyxl") from exc

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import numpy as np
except Exception:
    np = None

APP_NAME = "Macro16RefillEngine"
VERSION = "2.7.4-position-engine-fix"
STRATEGY_VERSION = "teacher_strategy_v1.6_position_engine_fix_20260511"
DEFAULT_TIMEOUT = 15
DEFAULT_MAX_FALLBACK_DAYS = 5

MODULES = [
    "美股-S&P500", "美股-NASDAQ", "美股-道瓊", "VIX恐慌",
    "美債10Y", "原油", "戰爭/地緣", "CPI", "非農", "外資",
    "官股", "台股指數", "成交量", "AI產業", "OTC", "台股夜盤"
]

# SOP V2.2：正式輸出模式分離。
# macro_refill：正式日常模式＝宏觀16修正頁 + 原本TOP/00~09報表（若有DB），不得刪除TOP輸出。
# macro_only：只輸出「市場輸入 / 宏觀16模組 / V2技術引擎」三頁，用於單純驗證宏觀回填。
# institutional_report：只輸出00~16老師策略機構級報表。
# macro_teacher：宏觀三頁 + 00~16老師策略完整報表。
# teacher_full：只輸出00~16老師策略完整報表，等同institutional_report語意。
# all：完整debug與機構報表全輸出。
REPORT_MODE_MACRO = "macro_refill"
REPORT_MODE_MACRO_ONLY = "macro_only"
REPORT_MODE_INSTITUTIONAL = "institutional_report"
REPORT_MODE_MACRO_TEACHER = "macro_teacher"
REPORT_MODE_TEACHER_FULL = "teacher_full"
REPORT_MODE_ALL = "all"
MACRO_REFILL_SHEETS = ["市場輸入", "宏觀16模組", "V2技術引擎"]


YAHOO_SYMBOLS = {
    "美股-S&P500": "^GSPC",
    "美股-NASDAQ": "^IXIC",
    "美股-道瓊": "^DJI",
    "VIX恐慌": "^VIX",
    "原油": "CL=F",
}

YAHOO_SYMBOL_CANDIDATES = {
    "OTC": ["^TWOII", "TWOII.TW", "TWOII.TWO"],
}


SOURCE_PRIORITY = ["官方資料", "交易所/期交所", "國際金融數據站", "台灣可信財經站", "人工事件判斷"]

SOURCE_URLS = {
    "reuters_war": "https://www.reuters.com",
    "bloomberg_policy": "https://www.bloomberg.com",
    "federal_reserve": "https://www.federalreserve.gov",
    "bls_api_cpi": "https://api.bls.gov/publicAPI/v2/timeseries/data/CUUR0000SA0",
    "bls_api_nfp": "https://api.bls.gov/publicAPI/v2/timeseries/data/CES0000000001",
    "twse_foreign": "https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={date}&type=day",
    "twse_broker_report": "https://www.twse.com.tw/zh/trading/brokerReport",
    "tpex_indices": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/indices-pricing.html",
    "wantgoo_public_bank": "https://www.wantgoo.com/stock/public-bank/trend",
    "histock": "https://histock.tw",
    "techcrunch_ai": "https://techcrunch.com",
    "isw": "https://www.understandingwar.org",
    "cnn": "https://www.cnn.com",
    "iek": "https://ieknet.iek.org.tw/",
    "taifex_night": "https://www.taifex.com.tw/cht/3/futContractsDateAh",
    "twse_t86": "https://www.twse.com.tw/rwd/zh/fund/T86?date={date}&selectType=ALLBUT0999&response=json",
    "gov_broker_fallback_histock": "https://histock.tw/stock/broker8.aspx",
}


@dataclass
class RawData:
    key: str
    value: Any
    date: str
    source: str
    url: str
    fetched_at: str
    status: str = "OK"
    message: str = ""
    query_date: str = ""
    actual_date: str = ""
    is_fallback: bool = False
    fallback_days: int = 0
    data_status: str = "OK"
    data_note: str = ""
    parse_status: str = "PARSE_OK"
    raw_file_path: str = ""
    confidence: float = 1.0

@dataclass
class MarketInput:
    base_date: str = ""
    close: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    prev_high: Optional[float] = None
    prev_low: Optional[float] = None
    ma5: Optional[float] = None
    turnover_100m: Optional[float] = None
    avg_turnover_5d_100m: Optional[float] = None
    foreign_net_100m: Optional[float] = None
    gov_net_100m: Optional[float] = None
    ai_strength: float = 0.5
    major_event: int = 0
    night_score: Optional[int] = None
    night_net_lots: Optional[int] = None
    source_1: str = ""
    source_2: str = ""
    source_3: str = ""
    source_4: str = ""

@dataclass
class ManualOverride:
    gov_net_100m: Optional[float] = None
    ai_strength: Optional[float] = None
    major_event: Optional[int] = None
    event_note: str = ""
    oil_event_note: str = ""
    night_score: Optional[float] = None

@dataclass
class ModuleScore:
    module: str
    data_text: str
    strength: float
    direction: int
    weighted_score: float
    explanation: str
    source: str
    data_time: str
    trade_usage: str
    status: str = "OK"

@dataclass
class TechnicalRisk:
    below_ma5: int
    lower_high: int
    lower_low: int
    volume_expansion: int
    major_event: int
    risk_score: float
    market_judgement: str
    night_bearish: int = 0
    night_score: Optional[int] = None
    night_net_lots: Optional[int] = None

class Macro16Logger:
    def __init__(self, log_dir: Path):
        log_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = log_dir / f"macro16_debug_{self.run_id}.txt"
        self.raw_dir = log_dir / "raw" / self.run_id[:8] / self.run_id
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.evidence_records: List[Dict[str, Any]] = []
        root = logging.getLogger()
        for handler in list(root.handlers):
            root.removeHandler(handler)
        logging.basicConfig(
            filename=str(self.log_file),
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            encoding="utf-8",
        )
        self.messages: List[str] = []

    def info(self, msg: str):
        logging.info(msg)
        self.messages.append(f"INFO {msg}")

    def warning(self, msg: str):
        logging.warning(msg)
        self.messages.append(f"WARN {msg}")

    def error(self, msg: str):
        logging.error(msg, exc_info=True)
        self.messages.append(f"ERROR {msg}")

    def debug(self, msg: str):
        logging.info("DEBUG " + msg)
        self.messages.append(f"DEBUG {msg}")

    def _safe_filename(self, value: str) -> str:
        return re.sub(r"[^0-9A-Za-z_\\-\\u4e00-\\u9fff]+", "_", str(value)).strip("_")[:80] or "source"

    def _infer_parsed_fields(self, payload: Any) -> Dict[str, Any]:
        """從已解析payload自動產生 parsed_fields，避免 status=OK 但 parse_status=NO_PARSED_VALUE。"""
        if isinstance(payload, dict):
            parsed = {}
            for k, v in payload.items():
                if k in ("raw", "rows", "snippet"):
                    continue
                if isinstance(v, (str, int, float, bool)) or v is None:
                    parsed[k] = v
                elif isinstance(v, (list, tuple)) and len(v) <= 10:
                    parsed[k] = v
                elif isinstance(v, dict):
                    parsed[k] = {kk: vv for kk, vv in list(v.items())[:10] if isinstance(vv, (str, int, float, bool)) or vv is None}
            return parsed
        return {}

    def write_raw_evidence(self, source: str, payload: Any, parsed: Optional[Dict[str, Any]] = None, status: str = "OK", url: str = "", message: str = "") -> str:
        if parsed is None:
            parsed = self._infer_parsed_fields(payload)
        parsed = parsed or {}
        parse_status = "PARSE_OK" if parsed else "NO_PARSED_VALUE"
        if status == "OK" and parse_status != "PARSE_OK":
            status = "WARN"
            message = (message + "；" if message else "") + "已抓到原始資料但尚未形成parsed_fields，避免假OK。"
        record = {
            "run_id": self.run_id,
            "source": source,
            "url": url,
            "fetch_time": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "parse_status": parse_status,
            "parsed_fields": parsed,
            "message": message,
            "raw_excerpt": str(payload)[:4000],
        }
        base = self._safe_filename(source)
        path = self.raw_dir / f"{base}.json"
        # 不允許不同來源覆蓋同一個source.json/模組json；若重複則自動加序號。
        if path.exists():
            idx = 2
            while True:
                candidate = self.raw_dir / f"{base}_{idx}.json"
                if not candidate.exists():
                    path = candidate
                    break
                idx += 1
        try:
            path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            record["raw_file_path"] = str(path)
            self.evidence_records.append(record)
            self.info(f"RAW_EVIDENCE_FILE source={source} path={path} status={status} parse_status={record['parse_status']}")
            return str(path)
        except Exception as exc:
            self.warning(f"RAW_EVIDENCE_WRITE_FAIL source={source} error={exc}")
            return ""

    def raw_snapshot(self, source: str, payload: Any, parsed: Optional[Dict[str, Any]] = None, status: str = "OK", url: str = "", message: str = ""):
        text = str(payload)
        if len(text) > 900:
            text = text[:900] + "..."
        self.info(f"RAW_DATA_SNAPSHOT source={source} payload={text}")
        self.write_raw_evidence(source, payload, parsed=parsed, status=status, url=url, message=message)

    def parsed_value(self, field: str, value: Any, source: str, actual_date: str = ""):
        self.info(f"PARSED_VALUE field={field} value={value} source={source} actual_date={actual_date}")

    def strategy_trace(self, tag: str, payload: Dict[str, Any]):
        """老師策略工程追蹤：輸出可被log grep的固定格式。"""
        try:
            text = json.dumps(payload, ensure_ascii=False, default=str)
        except Exception:
            text = str(payload)
        self.info(f"{tag} {text}")

class HttpClient:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger
        self.session = requests.Session() if requests else None
        if self.session:
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Macro16RefillEngine/2.0",
                "Accept": "application/json,text/csv,text/html,text/plain,*/*",
                "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
                "Referer": "https://www.google.com/",
            })

    def get_text(self, url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
        if not self.session:
            raise RuntimeError("requests 未安裝，無法抓取網路資料")
        self.logger.info(f"GET {url}")
        r = self.session.get(url, timeout=timeout)
        self.logger.debug(f"HTTP status={r.status_code}, content_type={r.headers.get('content-type','')}, len={len(r.text or '')}")
        r.raise_for_status()
        if not r.encoding:
            r.encoding = "utf-8"
        return r.text

    def get_json(self, url: str, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
        text = self.get_text(url, timeout)
        try:
            return json.loads(text)
        except Exception as exc:
            self.logger.warning(f"JSON解析失敗 url={url}; head={text[:200]!r}; error={exc}")
            raise

class SourceConnector:
    def __init__(self, client: HttpClient, logger: Macro16Logger):
        self.client = client
        self.logger = logger

    def _today_str(self) -> str:
        return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _compact_date(self, value: Optional[str]) -> str:
        if not value:
            return dt.date.today().strftime("%Y%m%d")
        return str(value).replace("-", "")

    def _dash_date(self, value: str) -> str:
        v = self._compact_date(value)
        return f"{v[:4]}-{v[4:6]}-{v[6:8]}" if len(v) == 8 else v

    def _previous_calendar_day(self, compact_date: str) -> str:
        """
        V1.5：安全日曆回退。
        目的：
        1. 正常日期：直接回退前一日，例如 20260430 -> 20260429。
        2. 非法日期：先修正成該年月最近合法日，再回退。
           例如 20260431 不是合法日期，會先修正為 20260430，再回退到 20260429。
        """
        raw = self._compact_date(compact_date)
        try:
            d = dt.datetime.strptime(raw, "%Y%m%d").date()
            return (d - dt.timedelta(days=1)).strftime("%Y%m%d")
        except ValueError:
            if len(raw) == 8 and raw[:6].isdigit():
                year = int(raw[:4])
                month = int(raw[4:6])
                # 先取得該年月最後一天
                if month == 12:
                    first_next_month = dt.date(year + 1, 1, 1)
                else:
                    first_next_month = dt.date(year, month + 1, 1)
                last_valid_day = first_next_month - dt.timedelta(days=1)
                # 再回退一天，符合「無效指定日也視為抓不到，要往前退」的需求
                return (last_valid_day - dt.timedelta(days=1)).strftime("%Y%m%d")
            raise

    def _twse_rows(self, data: Dict[str, Any]) -> List[Any]:
        rows = data.get("data", []) or []
        if not rows and data.get("tables"):
            for table in data.get("tables", []):
                rows.extend(table.get("data", []) or [])
        return rows

    def _official_no_data(self, source_name: str, query_date: str, try_date: str, url: str, data: Optional[Dict[str, Any]] = None):
        stat = ""
        try:
            stat = str(data.get("stat", "")) if isinstance(data, dict) else ""
        except Exception:
            stat = ""
        self.logger.warning(f"OFFICIAL_NOT_UPDATED source={source_name} query_date={query_date} try_date={try_date} stat={stat} url={url}")

    def _fallback_note(self, query_date: str, actual_date: str, source_name: str) -> Tuple[bool, int, str]:
        q = self._compact_date(query_date)
        a = self._compact_date(actual_date)
        is_fb = q != a
        if not is_fb:
            return False, 0, ""
        qd = dt.datetime.strptime(q, "%Y%m%d").date()
        ad = dt.datetime.strptime(a, "%Y%m%d").date()
        days = max(0, (qd - ad).days)
        note = f"{source_name} 查詢日 {q} 官網尚未公布/無資料，已使用最近可用資料日 {a}"
        self.logger.info(f"FALLBACK_USED source={source_name} query_date={q} actual_date={a} fallback_days={days}")
        return True, days, note

    def _log_fallback_try(self, source_name: str, query_date: str, try_date: str, attempt_index: int, max_back_days: int):
        """V1.5：記錄固定最多退 5 次的每一次嘗試。"""
        self.logger.info(
            f"FALLBACK_TRY source={source_name} query_date={query_date} "
            f"try_date={try_date} attempt={attempt_index}/{max_back_days}"
        )

    def fetch_yahoo_chart(self, symbol: str, module: str, range_days: str = "10d") -> RawData:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={range_days}&interval=1d"
        try:
            data = self.client.get_json(url)
            result = data.get("chart", {}).get("result", [])[0]
            timestamps = result.get("timestamp", [])
            quote = result.get("indicators", {}).get("quote", [])[0]
            closes = [x for x in quote.get("close", []) if x is not None]
            highs = [x for x in quote.get("high", []) if x is not None]
            lows = [x for x in quote.get("low", []) if x is not None]
            if not closes:
                raise ValueError("Yahoo資料無收盤價")
            last_close = float(closes[-1])
            prev_close = float(closes[-2]) if len(closes) >= 2 else last_close
            pct = ((last_close - prev_close) / prev_close * 100) if prev_close else 0
            last_ts = timestamps[-1] if timestamps else int(time.time())
            data_date = dt.datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d")
            value = {
                "symbol": symbol,
                "close": last_close,
                "prev_close": prev_close,
                "change_pct": pct,
                "high": float(highs[-1]) if highs else None,
                "low": float(lows[-1]) if lows else None,
                "last5_close": closes[-5:] if len(closes) >= 5 else closes,
            }
            self.logger.raw_snapshot(module, value)
            self.logger.parsed_value(f"{module}_close", last_close, "Yahoo Finance", data_date)
            self.logger.parsed_value(f"{module}_change_pct", round(pct, 4), "Yahoo Finance", data_date)
            return RawData(module, value, data_date, "Yahoo Finance", url, self._today_str())
        except Exception as exc:
            self.logger.warning(f"Yahoo抓取失敗 {module}/{symbol}: {exc}")
            return RawData(module, None, "", "Yahoo Finance", url, self._today_str(), "FAIL", str(exc))


    def fetch_yahoo_chart_candidates(self, symbols: List[str], module: str, range_days: str = "10d") -> RawData:
        last_fail = None
        for symbol in symbols:
            data = self.fetch_yahoo_chart(symbol, module, range_days)
            if data.status == "OK":
                data.message = f"使用候選代碼 {symbol}"
                return data
            last_fail = data
        return last_fail or RawData(module, None, "", "Yahoo Finance", ",".join(symbols), self._today_str(), "FAIL", "所有Yahoo候選代碼失敗")

    def fetch_twse_taiex_history(self, base_date: Optional[str] = None, max_back_days: int = DEFAULT_MAX_FALLBACK_DAYS) -> RawData:
        query_date = self._compact_date(base_date)
        try_date = query_date
        last_error = ""
        for fallback_days in range(max_back_days + 1):
            self._log_fallback_try("TWSE_TAIEX", query_date, try_date, fallback_days, max_back_days)
            url = f"https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date={try_date}&response=json"
            try:
                data = self.client.get_json(url)
                rows = data.get("data", []) or []
                if not rows:
                    self._official_no_data("TWSE_TAIEX", query_date, try_date, url, data)
                    try_date = self._previous_calendar_day(try_date)
                    continue
                parsed = []
                for row in rows:
                    try:
                        roc_date = str(row[0])
                        parts = roc_date.split("/")
                        year = int(parts[0]) + 1911 if int(parts[0]) < 1911 else int(parts[0])
                        date_str = f"{year:04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
                        compact = date_str.replace("-", "")
                        if compact > query_date:
                            continue
                        parsed.append({
                            "date": date_str,
                            "open": self._to_float(row[1]),
                            "high": self._to_float(row[2]),
                            "low": self._to_float(row[3]),
                            "close": self._to_float(row[4]),
                        })
                    except Exception:
                        continue
                if not parsed:
                    self._official_no_data("TWSE_TAIEX", query_date, try_date, url, data)
                    try_date = self._previous_calendar_day(try_date)
                    continue
                last = parsed[-1]
                actual = last["date"].replace("-", "")
                is_fb, fb_days, note = self._fallback_note(query_date, actual, "TWSE_TAIEX")
                value = {"rows": parsed, "last": last}
                self.logger.raw_snapshot("TWSE_TAIEX", last)
                self.logger.parsed_value("taiex_close", last.get("close"), "TWSE MI_5MINS_HIST", last.get("date", ""))
                return RawData("台股指數", value, last["date"], "TWSE MI_5MINS_HIST", url, self._today_str(),
                               "OK", note, query_date, actual, is_fb, fb_days, "OK", note)
            except Exception as exc:
                last_error = str(exc)
                self.logger.warning(f"TWSE加權指數來源失敗 try_date={try_date} url={url}: {exc}")
                try_date = self._previous_calendar_day(try_date)
        return RawData("台股指數", None, "", "TWSE MI_5MINS_HIST", "", self._today_str(),
                       "FAIL", last_error or "最近可用台股指數資料未取得", query_date, "", True, max_back_days, "DATA_MISSING", last_error)

    def fetch_twse_turnover_month(self, base_date: Optional[str] = None, max_back_days: int = DEFAULT_MAX_FALLBACK_DAYS) -> RawData:
        """抓取TWSE成交值。V1.4：指定日期無資料時，自動往前找最近有效交易日。"""
        query_date = self._compact_date(base_date)
        try_date = query_date
        last_error = ""
        for fallback_days in range(max_back_days + 1):
            self._log_fallback_try("TWSE_FMTQIK", query_date, try_date, fallback_days, max_back_days)
            month_param = try_date[:6] + "01"
            candidates = [
                f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date={month_param}&response=json",
                f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={month_param}",
                f"https://www.twse.com.tw/rwd/zh/TAIEX/FMTQIK?date={month_param}&response=json",
            ]
            for url in candidates:
                try:
                    data = self.client.get_json(url)
                    rows = self._twse_rows(data)
                    parsed = []
                    for row in rows:
                        try:
                            roc_date = str(row[0]).strip()
                            parts = roc_date.split("/")
                            year = int(parts[0]) + 1911 if int(parts[0]) < 1911 else int(parts[0])
                            date_str = f"{year:04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
                            compact = date_str.replace("-", "")
                            if compact > query_date or compact > try_date:
                                continue
                            amount = self._to_float(row[2])
                            if not math.isnan(amount):
                                parsed.append({"date": date_str, "turnover_100m": amount / 100000000})
                        except Exception as row_exc:
                            self.logger.debug(f"FMTQIK row skipped row={row}; error={row_exc}")
                    if not parsed:
                        self._official_no_data("TWSE_FMTQIK", query_date, try_date, url, data)
                        continue
                    last = parsed[-1]
                    actual = last["date"].replace("-", "")
                    is_fb, fb_days, note = self._fallback_note(query_date, actual, "TWSE_FMTQIK")
                    self.logger.info(f"TWSE成交值取得成功 source={url}, query_date={query_date}, actual_date={actual}, turnover_100m={last['turnover_100m']:.2f}")
                    self.logger.raw_snapshot("TWSE_FMTQIK", last)
                    self.logger.parsed_value("turnover_100m", round(last.get("turnover_100m", 0), 2), "TWSE FMTQIK", last.get("date", ""))
                    return RawData("成交量", {"rows": parsed, "last": last}, last["date"], "TWSE FMTQIK", url, self._today_str(),
                                   "OK", note, query_date, actual, is_fb, fb_days, "OK", note)
                except Exception as exc:
                    last_error = str(exc)
                    self.logger.warning(f"TWSE成交值來源失敗 try_date={try_date} url={url}: {exc}")
            try_date = self._previous_calendar_day(try_date)
        return RawData("成交量", None, "", "TWSE FMTQIK", "", self._today_str(),
                       "FAIL", last_error or "最近可用成交值資料未取得", query_date, "", True, max_back_days, "DATA_MISSING", last_error)

    def fetch_foreign_investor(self, base_date: Optional[str] = None, max_back_days: int = DEFAULT_MAX_FALLBACK_DAYS) -> RawData:
        """抓取外資買賣超。V1.5：指定日期抓不到時，固定最多往前退 5 次。"""
        query_date = self._compact_date(base_date)
        try_date = query_date
        last_error = ""
        for fallback_days in range(max_back_days + 1):
            self._log_fallback_try("TWSE_FOREIGN", query_date, try_date, fallback_days, max_back_days)
            candidates = [
                f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?dayDate={try_date}&type=day&response=json",
                f"https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={try_date}&type=day",
                f"https://www.twse.com.tw/rwd/zh/fund/TWT38U?date={try_date}&response=json",
                f"https://www.twse.com.tw/fund/TWT38U?response=json&date={try_date}",
            ]
            for url in candidates:
                try:
                    data = self.client.get_json(url)
                    rows = self._twse_rows(data)
                    if not rows:
                        self._official_no_data("TWSE_FOREIGN", query_date, try_date, url, data)
                        continue
                    for row in rows:
                        row_text = " ".join([str(x) for x in row])
                        if ("外資" in row_text) or ("外陸資" in row_text):
                            vals = []
                            for cell in row:
                                try:
                                    v = self._to_float(cell)
                                    if not math.isnan(v):
                                        vals.append(v)
                                except Exception:
                                    pass
                            if vals:
                                net_100m = vals[-1] / 100000000
                                is_fb, fb_days, note = self._fallback_note(query_date, try_date, "TWSE_FOREIGN")
                                self.logger.info(f"FETCH_OK source=TWSE_FOREIGN query_date={query_date} actual_date={try_date} net_100m={net_100m:.2f} url={url}")
                                buy_amount = vals[-3] if len(vals) >= 3 else None
                                sell_amount = vals[-2] if len(vals) >= 2 else None
                                net_amount = vals[-1] if len(vals) >= 1 else None
                                parsed = {
                                    "foreign_net_100m": round(net_100m, 2),
                                    "buy_amount": buy_amount,
                                    "sell_amount": sell_amount,
                                    "net_amount": net_amount,
                                    "query_date": query_date,
                                    "actual_date": try_date,
                                    "source_rule": "TWSE BFI82U/TWT38U 外資及陸資列淨買賣超 / 100000000",
                                    "raw_row": row_text[:500],
                                }
                                raw_path = self.logger.write_raw_evidence(
                                    "TWSE_FOREIGN", parsed, parsed=parsed, status="OK", url=url,
                                    message="外資買賣超解析完成，parsed_fields已寫入證據鏈"
                                )
                                self.logger.parsed_value("foreign_net_100m", round(net_100m, 2), "TWSE BFI82U/TWT38U", try_date)
                                return RawData("外資", {"net_100m": net_100m, "raw_hint": row_text[:500]}, self._dash_date(try_date),
                                               "TWSE三大法人", url, self._today_str(), "OK", note, query_date, try_date, is_fb, fb_days, "OK", note, "PARSE_OK", raw_path, 1.0)
                    text = json.dumps(data, ensure_ascii=False)
                    nums = [self._to_float(x) for x in re.findall(r"-?\d[\d,]*\.?\d*", text)]
                    nums = [x for x in nums if abs(x) > 1000000]
                    if nums:
                        net_100m = nums[-1] / 100000000
                        is_fb, fb_days, note = self._fallback_note(query_date, try_date, "TWSE_FOREIGN")
                        msg = "使用fallback解析；" + note if note else "使用fallback解析"
                        parsed = {
                            "foreign_net_100m": round(net_100m, 2),
                            "query_date": query_date,
                            "actual_date": try_date,
                            "source_rule": "TWSE三大法人數值fallback解析 / 100000000",
                            "raw_excerpt": text[:500],
                        }
                        self.logger.warning(f"外資語意列未找到，使用數值fallback source={url}, query_date={query_date}, actual_date={try_date}, net_100m={net_100m:.2f}")
                        raw_path = self.logger.write_raw_evidence(
                            "TWSE_FOREIGN", parsed, parsed=parsed, status="WARN", url=url,
                            message=msg + "；已寫入foreign_net_100m parsed_fields"
                        )
                        return RawData("外資", {"net_100m": net_100m, "raw_hint": text[:500]}, self._dash_date(try_date),
                                       "TWSE三大法人-fallback", url, self._today_str(), "WARN", msg, query_date, try_date, is_fb, fb_days, "OK", msg, "PARSE_OK", raw_path, 0.8)
                    self._official_no_data("TWSE_FOREIGN", query_date, try_date, url, data)
                except Exception as exc:
                    last_error = str(exc)
                    self.logger.warning(f"外資來源失敗 try_date={try_date} url={url}: {exc}")
            try_date = self._previous_calendar_day(try_date)
        return RawData("外資", None, "", "TWSE三大法人", "", self._today_str(),
                       "FAIL", last_error or "最近可用外資資料未取得", query_date, "", True, max_back_days, "DATA_MISSING", last_error)

    def fetch_fred_csv_latest(self, series_id: str, module: str) -> RawData:
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        try:
            text = self.client.get_text(url)
            rows = list(csv.DictReader(StringIO(text)))
            valid = [(r["observation_date"], r[series_id]) for r in rows if r.get(series_id) not in (None, "", ".")]
            if not valid:
                raise ValueError("FRED無有效資料")
            date_str, value = valid[-1]
            self.logger.raw_snapshot(module, {"series": series_id, "date": date_str, "value": value})
            self.logger.parsed_value(module, value, "FRED", date_str)
            return RawData(module, {"value": float(value), "series": series_id}, date_str, "FRED", url, self._today_str())
        except Exception as exc:
            self.logger.warning(f"FRED抓取失敗 {series_id}: {exc}")
            return RawData(module, None, "", "FRED", url, self._today_str(), "FAIL", str(exc))

    def fetch_bls_release_text(self, module: str, url: str) -> RawData:
        try:
            text = self.client.get_text(url)
            compact = re.sub(r"\s+", " ", text)
            snippet = compact[:1200]
            self.logger.raw_snapshot(module, snippet)
            self.logger.parsed_value(f"{module}_source", url, "BLS", "latest_release")
            return RawData(module, {"url": url, "snippet": snippet}, "latest_release", "BLS", url, self._today_str(), "OK", "BLS periodic release fetched")
        except Exception as exc:
            self.logger.warning(f"BLS抓取失敗 {module}: {exc}")
            return RawData(module, None, "", "BLS", url, self._today_str(), "WARN", str(exc), data_status="OPTIONAL_MISSING")

    def fetch_text_snapshot(self, module: str, url: str, source_name: str, status_if_fail: str = "WARN", parse_required: bool = False) -> RawData:
        try:
            text = self.client.get_text(url)
            compact = re.sub(r"\s+", " ", text)
            snippet = compact[:5000]
            value = {"url": url, "snippet": snippet, "snippet_length": len(snippet)}
            if parse_required:
                status = "WARN"
                data_status = "NO_PARSED_VALUE"
                parsed = {}
                parse_status = "NO_PARSED_VALUE"
                message = f"{source_name} source fetched but parser required"
            else:
                status = "OK"
                data_status = "OK"
                parsed = {"source_url": url, "snippet_length": len(snippet)}
                parse_status = "PARSE_OK"
                message = f"{source_name} source fetched"
            raw_path = self.logger.write_raw_evidence(module, value, parsed=parsed, status=status, url=url, message=message)
            self.logger.raw_snapshot(module, value, parsed=parsed, status=status, url=url, message=message)
            self.logger.parsed_value(f"{module}_source_url", url, source_name, "latest")
            return RawData(module, value, "latest", source_name, url, self._today_str(), status, message, data_status=data_status, parse_status=parse_status, raw_file_path=raw_path, confidence=0.5 if parse_required else 0.8)
        except Exception as exc:
            self.logger.warning(f"{source_name}抓取失敗 {module}: {exc}")
            raw_path = self.logger.write_raw_evidence(module, {"url": url, "error": str(exc)}, parsed={}, status=status_if_fail, url=url, message=str(exc))
            return RawData(module, None, "", source_name, url, self._today_str(), status_if_fail, str(exc), data_status="FETCH_FAIL", parse_status="NO_PARSED_VALUE", raw_file_path=raw_path, confidence=0.0)


    def fetch_bls_api_series(self, module: str, series_id: str, source_url: str) -> RawData:
        year = dt.date.today().year
        url = f"{source_url}?startyear={year-2}&endyear={year}"
        try:
            data = self.client.get_json(url)
            series = data.get("Results", {}).get("series", [])
            if not series or not series[0].get("data"):
                raise ValueError("BLS API無有效資料")
            latest = series[0]["data"][0]
            value = self._to_float(latest.get("value"))
            period = f"{latest.get('year')}-{latest.get('periodName')}"
            payload = {"series_id": series_id, "period": period, "value": value, "raw": latest}
            self.logger.raw_snapshot(module, payload)
            self.logger.parsed_value(f"{module}_value", value, "BLS API", period)
            return RawData(module, payload, period, "BLS API", url, self._today_str(), "OK", "BLS API series fetched")
        except Exception as exc:
            self.logger.warning(f"BLS API抓取失敗 {module}/{series_id}: {exc}")
            return RawData(module, None, "", "BLS API", url, self._today_str(), "WARN", str(exc), data_status="FETCH_FAIL")

    def fetch_bls_cpi(self) -> RawData:
        return self.fetch_bls_api_series("CPI", "CUUR0000SA0", SOURCE_URLS["bls_api_cpi"])

    def fetch_bls_nfp(self) -> RawData:
        return self.fetch_bls_api_series("非農", "CES0000000001", SOURCE_URLS["bls_api_nfp"])

    def fetch_reuters_war(self, url: str = SOURCE_URLS["reuters_war"]) -> RawData:
        raw = self.fetch_text_snapshot("戰爭/停火", url, "Reuters", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "").lower()
            risk = 1 if any(k in text for k in ["war", "ceasefire", "israel", "iran", "attack", "missile", "sanction"]) else 0
            raw.value["major_event"] = risk
            self.logger.parsed_value("major_event", risk, "Reuters", raw.date)
        return raw

    def fetch_bloomberg_policy(self, url: str = SOURCE_URLS["bloomberg_policy"]) -> RawData:
        return self.fetch_text_snapshot("外交政策", url, "Bloomberg", "WARN")

    def fetch_fed_policy(self, url: str = SOURCE_URLS["federal_reserve"]) -> RawData:
        return self.fetch_text_snapshot("FED利率政策", url, "Federal Reserve", "WARN")

    def fetch_isw_conflict(self, url: str = SOURCE_URLS["isw"]) -> RawData:
        raw = self.fetch_text_snapshot("ISW衝突分析", url, "ISW", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "").lower()
            risk = 1 if any(k in text for k in ["russia", "ukraine", "iran", "israel", "war", "attack"]) else 0
            raw.value["major_event"] = risk
            self.logger.parsed_value("isw_major_event", risk, "ISW", raw.date)
        return raw

    def fetch_cnn_major_news(self, url: str = SOURCE_URLS["cnn"]) -> RawData:
        raw = self.fetch_text_snapshot("CNN重大新聞", url, "CNN", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "").lower()
            risk = 1 if any(k in text for k in ["breaking", "war", "crisis", "market", "president", "attack"]) else 0
            raw.value["major_event"] = risk
            self.logger.parsed_value("cnn_major_event", risk, "CNN", raw.date)
        return raw

    def fetch_iek_industry(self, url: str = SOURCE_URLS["iek"]) -> RawData:
        raw = self.fetch_text_snapshot("IEK產業分析", url, "IEK Taiwan", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "")
            strength = 0.8 if any(k in text for k in ["AI", "CPO", "半導體", "先進封裝", "產業"]) else 0.5
            raw.value["ai_strength"] = strength
            self.logger.parsed_value("iek_ai_strength", strength, "IEK Taiwan", raw.date)
        return raw

    def fetch_trump_public_news(self, url: str = "https://www.reuters.com/world/us/") -> RawData:
        raw = self.fetch_text_snapshot("美國總統", url, "Reuters US", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "").lower()
            signal = 1 if any(k in text for k in ["trump", "tariff", "president", "trade", "china", "fed"]) else 0
            raw.value["policy_event"] = signal
            self.logger.parsed_value("us_president_policy_event", signal, "Reuters US", raw.date)
        return raw

    def _normalize_gov_unit_to_100m(self, value: float, unit: str = "") -> float:
        """SOP V2.1 P0-04：官股數字統一轉為億元。"""
        unit = str(unit or "")
        if "億元" in unit or unit == "億" or "億" in unit:
            return float(value)
        if "萬" in unit:
            return float(value) / 10000.0
        # 若頁面未給單位，Wantgoo/HiStock常見顯示為億元；保守以億元處理並留證據。
        return float(value)

    def _parse_public_bank_text(self, text: str) -> Optional[Dict[str, Any]]:
        """
        SOP V2.1 第十四章：Wantgoo/第三方八大官股備援Parser。
        目標只做備援回填與P0_WARN，不冒充官方資料。
        """
        if not text:
            return None
        clean = html_lib.unescape(re.sub(r"\s+", " ", str(text)))
        patterns = [
            r"(?:八大|官股|公股|八大官股|八大公股)[^。；;\n]{0,80}?(買超|賣超|買賣超|淨買超|淨賣超)?[^-+0-9]{0,20}([-+−]?\d+(?:,\d{3})*(?:\.\d+)?)\s*(億元|億|萬)?",
            r"(買超|賣超|買賣超|淨買超|淨賣超)[^。；;\n]{0,40}?(?:八大|官股|公股)?[^-+0-9]{0,20}([-+−]?\d+(?:,\d{3})*(?:\.\d+)?)\s*(億元|億|萬)?",
        ]
        candidates = []
        for pat in patterns:
            for m in re.finditer(pat, clean):
                words = " ".join([g for g in m.groups() if g])
                nums = re.findall(r"[-+−]?\d+(?:,\d{3})*(?:\.\d+)?", words)
                if not nums:
                    continue
                num_text = nums[-1].replace(",", "").replace("−", "-")
                try:
                    val = float(num_text)
                except Exception:
                    continue
                unit_m = re.search(r"(億元|億|萬)", words)
                unit = unit_m.group(1) if unit_m else "億"
                if any(k in words for k in ["賣超", "淨賣超"]) and val > 0:
                    val = -val
                val_100m = self._normalize_gov_unit_to_100m(val, unit)
                # 排除明顯不是金額的日期/代號小數。
                if abs(val_100m) < 0.01 or abs(val_100m) > 5000:
                    continue
                candidates.append({"gov_net_100m": round(val_100m, 2), "matched_text": m.group(0)[:180], "unit": unit})
        if not candidates:
            return None
        # 優先選擇包含八大/官股/公股的片段。
        candidates.sort(key=lambda x: (0 if any(k in x["matched_text"] for k in ["八大", "官股", "公股"]) else 1, -abs(x["gov_net_100m"])))
        best = candidates[0]
        sig = "偏多" if best["gov_net_100m"] > 0 else "偏空" if best["gov_net_100m"] < 0 else "中性"
        best.update({"gov_signal": sig, "gov_score": 1 if sig == "偏多" else -1 if sig == "偏空" else 0, "source_rule": "Wantgoo/八大官股資料頁解析；來源保留供追溯，分數只依數值"})
        return best

    def fetch_wantgoo_public_bank(self, url: str = SOURCE_URLS["wantgoo_public_bank"]) -> RawData:
        raw = self.fetch_text_snapshot("官股整理", url, "Wantgoo", "WARN")
        if raw.value:
            text = raw.value.get("snippet", "") if isinstance(raw.value, dict) else str(raw.value)
            parsed = self._parse_public_bank_text(text)
            nums = [self._to_float(x) for x in re.findall(r"-?\d+(?:,\d{3})*(?:\.\d+)?", text)]
            nums = [x for x in nums if not math.isnan(x)]
            if parsed:
                raw.value.update(parsed)
                raw.value["gov_hint_values"] = nums[:20]
                raw.status = "OK"
                raw.data_status = "OK"
                raw.parse_status = "PARSE_OK"
                raw.confidence = 0.55
                raw.message = "Wantgoo八大官股資料解析完成；來源保留供追溯，分數只依數值"
                raw.raw_file_path = self.logger.write_raw_evidence("官股整理", raw.value, parsed=parsed, status="OK", url=url, message=raw.message)
                self.logger.parsed_value("wantgoo_gov_net_100m", parsed.get("gov_net_100m"), "Wantgoo備援", raw.date)
            elif nums:
                raw.value["gov_hint_values"] = nums[:20]
                raw.status = "WARN"
                raw.data_status = "NO_PARSED_VALUE"
                raw.parse_status = "NO_PARSED_VALUE"
                raw.message = "Wantgoo頁面已取得且有數字，但未能定位八大官股淨買賣超欄位"
                self.logger.parsed_value("wantgoo_gov_hint_values", nums[:5], "Wantgoo", raw.date)
            else:
                raw.status = "WARN"
                raw.data_status = "NO_PARSED_VALUE"
                raw.parse_status = "NO_PARSED_VALUE"
                raw.message = "Wantgoo頁面已取得但未解析出官股數字"
        return raw

    def fetch_twse_broker_report(self, url: str = SOURCE_URLS["twse_broker_report"], base_date: Optional[str] = None, max_back_days: int = DEFAULT_MAX_FALLBACK_DAYS) -> RawData:
        """
        V2.4 官股資料修正：
        1. 原 twse_broker_report 頁面會回 404，因此不再把 404 頁面當成可用官股資料。
        2. 先抓 TWSE 官方 T86 作為證交所法人資料證據；但 T86 不是八大官股本體，只能作官方佐證。
        3. 八大官股目前 TWSE 無公開可直接彙總的官方 API；若使用 Histock/Wantgoo，只標記為「第三方輔助」，不得假裝官方。
        """
        query_date = self._compact_date(base_date)
        try_date = query_date
        last_error = ""
        for fallback_days in range(max_back_days + 1):
            self._log_fallback_try("TWSE_T86_GOV_EVIDENCE", query_date, try_date, fallback_days, max_back_days)
            api_url = SOURCE_URLS["twse_t86"].format(date=try_date)
            try:
                data = self.client.get_json(api_url)
                rows = self._twse_rows(data)
                if rows:
                    parsed = {"twse_t86_rows": len(rows), "source_rule": "TWSE官方T86法人資料佐證；非八大官股本體"}
                    is_fb, fb_days, note = self._fallback_note(query_date, try_date, "TWSE_T86_GOV_EVIDENCE")
                    payload = {"official_evidence": parsed, "rows_sample": rows[:3], "gov_net_100m": None}
                    raw_path = self.logger.write_raw_evidence("官股", payload, parsed=parsed, status="WARN", url=api_url, message="TWSE官方T86可取得，但不是八大官股彙總；官股數字需券商分點彙總或人工覆寫")
                    self.logger.parsed_value("twse_t86_rows", len(rows), "TWSE T86", self._dash_date(try_date))
                    return RawData("官股", payload, self._dash_date(try_date), "TWSE T86 官方佐證", api_url, self._today_str(),
                                   "WARN", "TWSE官方T86可取得，但不是八大官股彙總；未提供gov_net_100m，避免非官方數字進主判斷", query_date,
                                   try_date, is_fb, fb_days, "NO_PARSED_VALUE", note, "NO_PARSED_VALUE", raw_path, 0.35)
                self._official_no_data("TWSE_T86_GOV_EVIDENCE", query_date, try_date, api_url, data)
            except Exception as exc:
                last_error = str(exc)
                self.logger.warning(f"TWSE T86官股佐證來源失敗 try_date={try_date} url={api_url}: {exc}")
            try_date = self._previous_calendar_day(try_date)

        # 第三方輔助來源只做證據，不進主分數
        fallback_url = SOURCE_URLS.get("gov_broker_fallback_histock", SOURCE_URLS.get("wantgoo_public_bank", ""))
        raw = self.fetch_text_snapshot("官股", fallback_url, "第三方八大官股輔助", "WARN", parse_required=True)
        raw.status = "WARN"
        raw.data_status = "NO_PARSED_VALUE"
        raw.parse_status = "NO_PARSED_VALUE"
        raw.message = "TWSE官方官股彙總未取得；第三方八大官股頁只作輔助證據，不進主判斷。" + (f" TWSE錯誤={last_error}" if last_error else "")
        raw.confidence = 0.25
        return raw

    def fetch_ranking_result_db(self, db_path: Optional[str] = None, strict: bool = False) -> RawData:
        """
        V2.4 Ranking資料修正：
        - 若使用者指定db_path，只檢查該DB，不再亂掃其他DB。
        - 未指定時才掃描目前目錄與子目錄，並記錄候選DB。
        - ranking_result不存在或空表時，parse_status必須是NO_PARSED_VALUE，不能假OK。
        - strict=True時直接丟出錯誤，避免交易系統在沒有Ranking核心時繼續產出可下單結論。
        """
        candidates: List[Path] = []
        if db_path:
            candidates = [Path(db_path)]
        else:
            candidates.extend(Path.cwd().glob("*.db"))
            candidates.extend(Path.cwd().glob("**/*.db"))
        seen = set()
        candidates = [p for p in candidates if not (str(p) in seen or seen.add(str(p)))]
        checked: List[str] = []
        for path in candidates[:50]:
            checked.append(str(path))
            if not path.exists():
                self.logger.warning(f"RANKING_DB_NOT_FOUND path={path}")
                continue
            try:
                conn = sqlite3.connect(str(path))
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ranking_result'")
                if not cur.fetchone():
                    conn.close()
                    self.logger.warning(f"RANKING_TABLE_MISSING db={path} table=ranking_result")
                    continue
                cur.execute("SELECT COUNT(*) FROM ranking_result")
                count = int(cur.fetchone()[0] or 0)
                if count <= 0:
                    conn.close()
                    msg = f"ranking_result為空 db={path}，交易系統不可運行"
                    self.logger.warning("RANKING_TABLE_EMPTY " + msg)
                    if strict:
                        raise RuntimeError(msg)
                    payload = {"db_path": str(path), "table": "ranking_result", "row_count": 0}
                    raw_path = self.logger.write_raw_evidence("排行分析", payload, parsed={}, status="FAIL", url=str(path), message=msg)
                    return RawData("排行分析", None, "", "SQLite DB", str(path), self._today_str(), "FAIL", msg, data_status="DATA_MISSING", parse_status="NO_PARSED_VALUE", raw_file_path=raw_path, confidence=0.0)
                cur.execute("PRAGMA table_info(ranking_result)")
                columns = [r[1] for r in cur.fetchall()]
                cur.execute("SELECT * FROM ranking_result LIMIT 5")
                rows = cur.fetchall()
                conn.close()
                parsed = {"db_path": str(path), "table": "ranking_result", "row_count": count, "columns": columns[:50]}
                payload = {**parsed, "sample_rows": rows}
                self.logger.raw_snapshot("排行分析", payload, parsed=parsed, status="OK", url=str(path), message="ranking_result loaded")
                self.logger.parsed_value("ranking_result_count", count, "SQLite DB", str(path))
                return RawData("排行分析", payload, self._today_str(), "SQLite DB", str(path), self._today_str(), "OK", "ranking_result loaded", data_status="OK", parse_status="PARSE_OK", confidence=1.0)
            except Exception as exc:
                self.logger.debug(f"ranking_result DB skipped path={path}: {exc}")
                if strict:
                    raise
        msg = "未找到 ranking_result 資料表；已檢查DB=" + ("; ".join(checked[:20]) if checked else "無DB候選")
        if strict:
            raise RuntimeError(msg)
        raw_path = self.logger.write_raw_evidence("排行分析", {"checked_db": checked}, parsed={}, status="FAIL", url="db:ranking_result", message=msg)
        return RawData("排行分析", None, "", "SQLite DB", "db:ranking_result", self._today_str(), "FAIL", msg, data_status="DATA_MISSING", parse_status="NO_PARSED_VALUE", raw_file_path=raw_path, confidence=0.0)




    def _complete_parsed_raw(self, raw: RawData, parsed: Dict[str, Any], message: str, confidence: float = 1.0) -> RawData:
        raw.value = raw.value or {}
        if isinstance(raw.value, dict):
            raw.value.update(parsed)
        raw.status = "OK"
        raw.data_status = "OK"
        raw.parse_status = "PARSE_OK"
        raw.message = message
        raw.confidence = confidence
        raw.raw_file_path = self.logger.write_raw_evidence(raw.key, raw.value, parsed=parsed, status="OK", url=raw.url, message=message)
        return raw

    def fetch_gov_news(self, url: str = SOURCE_URLS["twse_broker_report"]) -> RawData:
        return self.fetch_twse_broker_report(url)

    def fetch_ai_industry_news(self, url: str = SOURCE_URLS["techcrunch_ai"]) -> RawData:
        raw = self.fetch_text_snapshot("AI產業", url, "TechCrunch", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "")
            strength = 0.8 if any(k in text for k in ["AI", "先進封裝", "需求", "資本支出", "成長"]) else 0.5
            raw.value["ai_strength"] = strength
            self.logger.parsed_value("ai_strength", strength, "TechCrunch", raw.date)
        return raw

    def fetch_geopolitical_news(self, url: str = SOURCE_URLS["reuters_war"]) -> RawData:
        raw = self.fetch_text_snapshot("戰爭/地緣", url, "Reuters", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "").lower()
            risk = 1 if any(k in text for k in ["war", "blockade", "israel", "iran", "attack", "missile", "kills"]) else 0
            raw.value["major_event"] = risk
            self.logger.parsed_value("major_event", risk, "Reuters", raw.date)
        return raw

    def _strip_html_cells(self, row_html: str) -> List[str]:
        cells = re.findall(r"<(?:td|th)[^>]*>(.*?)</(?:td|th)>", row_html, flags=re.I | re.S)
        out = []
        for c in cells:
            txt = re.sub(r"<[^>]+>", " ", c)
            txt = html_lib.unescape(txt)
            txt = re.sub(r"\s+", " ", txt).strip()
            if txt:
                out.append(txt)
        return out

    def _parse_taifex_night_html(self, html_text: str) -> Optional[Dict[str, Any]]:
        """解析TAIFEX夜盤頁。優先解析臺股期貨/外資列的多空淨額口數。"""
        decoded = html_lib.unescape(html_text or "")
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", decoded, flags=re.I | re.S)
        last_product = ""
        target_date = ""
        mdate = re.search(r"日期\s*(\d{4}/\d{2}/\d{2}|\d{4}-\d{2}-\d{2}|\d{4}\d{2}\d{2})", re.sub(r"<[^>]+>", " ", decoded))
        if mdate:
            target_date = mdate.group(1).replace("/", "-")
        for row in rows:
            cols = self._strip_html_cells(row)
            if not cols:
                continue
            row_text = " ".join(cols)
            if "臺股期貨" in row_text or "台股期貨" in row_text:
                last_product = "臺股期貨"
            # TAIFEX表格常見結構：商品名稱只出現在第一列，自營商/投信/外資在後續列沿用商品名稱
            if last_product == "臺股期貨" and any(c == "外資" or "外資" in c for c in cols):
                nums = []
                for c in cols:
                    cleaned = c.replace(",", "").replace("−", "-").strip()
                    if re.fullmatch(r"-?\d+(?:\.\d+)?", cleaned):
                        nums.append(float(cleaned))
                if len(nums) >= 6:
                    long_lots = int(nums[-6])
                    long_amount = int(nums[-5])
                    short_lots = int(nums[-4])
                    short_amount = int(nums[-3])
                    net_lots = int(nums[-2])
                    net_amount = int(nums[-1])
                elif len(nums) >= 2:
                    net_lots = int(nums[-2])
                    net_amount = int(nums[-1])
                    long_lots = long_amount = short_lots = short_amount = None
                else:
                    continue
                return {
                    "contract": "TX",
                    "contract_name": "臺股期貨",
                    "identity": "外資",
                    "session": "after_hours",
                    "data_date": target_date or "latest",
                    "long_lots": long_lots,
                    "long_amount": long_amount,
                    "short_lots": short_lots,
                    "short_amount": short_amount,
                    "net_lots": net_lots,
                    "net_amount": net_amount,
                    "night_score": 1 if net_lots > 0 else (-1 if net_lots < 0 else 0),
                    "source_rule": "TAIFEX夜盤臺股期貨外資多空淨額口數"
                }
        # fallback：以純文字行序解析（當HTML表格結構變動時）
        text = re.sub(r"<[^>]+>", "\n", decoded)
        tokens = [t.strip() for t in re.split(r"\n+", text) if t.strip()]
        try:
            i = tokens.index("臺股期貨")
        except ValueError:
            try:
                i = tokens.index("台股期貨")
            except ValueError:
                return None
        window = tokens[i:i+60]
        if "外資" in window:
            j = window.index("外資")
            vals = []
            for t in window[j+1:j+10]:
                c = t.replace(",", "").replace("−", "-")
                if re.fullmatch(r"-?\d+(?:\.\d+)?", c):
                    vals.append(float(c))
            if len(vals) >= 6:
                net_lots = int(vals[4])
                net_amount = int(vals[5])
                return {"contract": "TX", "contract_name": "臺股期貨", "identity": "外資", "session": "after_hours", "data_date": target_date or "latest", "long_lots": int(vals[0]), "long_amount": int(vals[1]), "short_lots": int(vals[2]), "short_amount": int(vals[3]), "net_lots": net_lots, "net_amount": net_amount, "night_score": 1 if net_lots > 0 else (-1 if net_lots < 0 else 0), "source_rule": "TAIFEX夜盤純文字fallback"}
        return None

    def fetch_taifex_night_snapshot(self, url: str = SOURCE_URLS["taifex_night"]) -> RawData:
        try:
            html_text = self.client.get_text(url)
            parsed = self._parse_taifex_night_html(html_text)
            if parsed:
                value = {"url": url, "html_length": len(html_text), **parsed}
                raw_path = self.logger.write_raw_evidence("台股夜盤", value, parsed=parsed, status="OK", url=url, message="TAIFEX夜盤已解析臺股期貨外資多空淨額")
                self.logger.raw_snapshot("TAIFEX_NIGHT_PARSED", parsed, parsed=parsed, status="OK", url=url, message="TAIFEX夜盤已解析臺股期貨外資多空淨額")
                self.logger.parsed_value("night_score", parsed.get("night_score"), "TAIFEX", parsed.get("data_date", "latest"))
                self.logger.parsed_value("taifex_tx_foreign_net_lots", parsed.get("net_lots"), "TAIFEX", parsed.get("data_date", "latest"))
                return RawData("台股夜盤", value, parsed.get("data_date", "latest"), "TAIFEX", url, self._today_str(), "OK", "TAIFEX夜盤已解析臺股期貨外資多空淨額", data_status="OK", parse_status="PARSE_OK", raw_file_path=raw_path, confidence=0.9)
            value = {"url": url, "html_length": len(html_text), "snippet": re.sub(r"\s+", " ", html_text[:5000])}
            raw_path = self.logger.write_raw_evidence("台股夜盤", value, parsed={}, status="WARN", url=url, message="TAIFEX頁面已取得，但未解析出臺股期貨外資多空淨額")
            self.logger.warning(f"TAIFEX夜盤未解析出數值 url={url}")
            return RawData("台股夜盤", value, "latest", "TAIFEX", url, self._today_str(), "WARN", "TAIFEX頁面已取得，但未解析出臺股期貨外資多空淨額", data_status="NO_PARSED_VALUE", parse_status="NO_PARSED_VALUE", raw_file_path=raw_path, confidence=0.3)
        except Exception as exc:
            self.logger.warning(f"TAIFEX夜盤抓取失敗 url={url}: {exc}")
            raw_path = self.logger.write_raw_evidence("台股夜盤", {"url": url, "error": str(exc)}, parsed={}, status="WARN", url=url, message=str(exc))
            return RawData("台股夜盤", None, "", "TAIFEX", url, self._today_str(), "WARN", str(exc), data_status="FETCH_FAIL", parse_status="NO_PARSED_VALUE", raw_file_path=raw_path, confidence=0.0)

    def fetch_tpex_otc_snapshot(self, url: str = SOURCE_URLS["tpex_indices"]) -> RawData:
        raw = self.fetch_text_snapshot("OTC官方來源", url, "TPEX", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "")
            nums = [self._to_float(x) for x in re.findall(r"\d+(?:,\d{3})*(?:\.\d+)?", text)]
            nums = [x for x in nums if not math.isnan(x)]
            if nums:
                raw.value["otc_hint_values"] = nums[:20]
                self.logger.parsed_value("tpex_otc_hint_values", nums[:5], "TPEX", raw.date)
            else:
                raw.status = "WARN"
                raw.data_status = "NO_PARSED_VALUE"
                raw.message = "TPEX官方頁已取得但未解析出OTC指數數值"
        return raw

    def build_manual_raw(self, module: str, value: Any, note: str, source: str = "人工覆寫/Excel註解") -> RawData:
        self.logger.raw_snapshot(module, {"value": value, "note": note})
        self.logger.parsed_value(module, value, source, self._today_str())
        return RawData(module, value, self._today_str(), source, source, self._today_str(), "OK", note)

    def _to_float(self, value: Any) -> float:
        if value is None:
            return math.nan
        s = str(value).replace(",", "").replace("--", "").strip()
        if s in ("", "nan", "None"):
            return math.nan
        return float(s)


class MarketNarrativeBuilder:
    """SOP V2.1 P1-01：市場輸入交易判讀模板化。"""
    def build(self, market: MarketInput) -> str:
        parts = []
        if market.close is not None and market.ma5 is not None:
            parts.append(f"台股收盤{market.close:.2f}，{'站上' if market.close >= market.ma5 else '跌破'}5MA {market.ma5:.2f}")
        if market.foreign_net_100m is not None:
            parts.append(f"外資{'買超' if market.foreign_net_100m >= 0 else '賣超'}{abs(market.foreign_net_100m):.2f}億元")
        else:
            parts.append("外資資料未完成")
        if market.gov_net_100m is not None:
            parts.append(f"官股/八大公股{'買超' if market.gov_net_100m >= 0 else '賣超'}{abs(market.gov_net_100m):.2f}億元")
        else:
            parts.append("官股資料未完成，需TEJ或備援來源")
        if market.turnover_100m is not None and market.avg_turnover_5d_100m is not None:
            vol_state = "放量" if market.turnover_100m > market.avg_turnover_5d_100m * 1.05 else "量能正常/未明顯放大"
            parts.append(f"成交值{market.turnover_100m:.2f}億元，5日均量{market.avg_turnover_5d_100m:.2f}億元，{vol_state}")
        if market.major_event:
            parts.append("重大事件風險=1，需降倉禁追高")
        else:
            parts.append("重大事件風險=0")
        if market.night_score is not None:
            if market.night_score < 0:
                lots = f"{market.night_net_lots}口" if market.night_net_lots is not None else "淨空"
                parts.append(f"夜盤外資偏空({lots})，盤前風險需加分")
            elif market.night_score > 0:
                lots = f"{market.night_net_lots}口" if market.night_net_lots is not None else "淨多"
                parts.append(f"夜盤外資偏多({lots})")
            else:
                parts.append("夜盤外資中性")
        return "；".join(parts) + "。"

class FieldCompletionValidator:
    """SOP V2.1 P0-03：核心欄位最終完成規則。"""
    CORE_FIELDS = ["base_date", "close", "high", "low", "ma5", "turnover_100m", "avg_turnover_5d_100m", "foreign_net_100m"]
    STRICT_FIELDS = ["gov_net_100m"]

    def __init__(self, logger: Macro16Logger):
        self.logger = logger

    def validate_market_input(self, market: MarketInput, strict_gov: bool = False) -> List[str]:
        issues = []
        for field in self.CORE_FIELDS:
            val = getattr(market, field, None)
            if val is None or val == "":
                issues.append(f"P0欄位未完成:{field}")
        if strict_gov and market.gov_net_100m is None:
            issues.append("P0欄位未完成:gov_net_100m，需TEJ或Wantgoo備援解析/人工覆寫")
        elif market.gov_net_100m is None:
            issues.append("P0_WARN:gov_net_100m未完成，正式檔會標示未取得，不可假OK")
        for issue in issues:
            self.logger.warning("FIELD_COMPLETION " + issue)
        return issues

class MacroRefillValidator:
    """SOP V2.2：只在 macro_only 模式清成三頁；macro_refill 不得刪除 TOP/00~09 報表。"""
    def __init__(self, logger: Macro16Logger):
        self.logger = logger

    def ensure_macro_sheets(self, wb):
        """保留既有報表，只確保宏觀三頁存在並移到前面。"""
        if not wb.sheetnames:
            wb.create_sheet("市場輸入")
        for required in MACRO_REFILL_SHEETS:
            if required not in wb.sheetnames:
                wb.create_sheet(required)
        front = [wb[name] for name in MACRO_REFILL_SHEETS if name in wb.sheetnames]
        rest = [ws for ws in wb._sheets if ws.title not in MACRO_REFILL_SHEETS]
        wb._sheets = front + rest
        return wb

    def enforce_macro_only_sheets(self, wb):
        """macro_only 專用：只保留宏觀三頁。"""
        self.ensure_macro_sheets(wb)
        for name in list(wb.sheetnames):
            if name not in MACRO_REFILL_SHEETS:
                del wb[name]
                self.logger.info(f"MACRO_ONLY_REMOVE_EXTRA_SHEET sheet={name}")
        wb._sheets = [wb[name] for name in MACRO_REFILL_SHEETS]
        return wb

class DataProcessor:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger

    def _source_note(self, raw: RawData) -> str:
        parts = [raw.url]
        if raw.query_date:
            parts.append(f"query_date={raw.query_date}")
        if raw.actual_date:
            parts.append(f"actual_date={raw.actual_date}")
        if raw.is_fallback:
            parts.append(f"fallback_days={raw.fallback_days}")
        if raw.data_note:
            parts.append(raw.data_note)
        return " | ".join([p for p in parts if p])

    def apply_manual_override(self, market: MarketInput, override: Optional[ManualOverride]) -> MarketInput:
        if not override:
            return market
        if override.gov_net_100m is not None:
            old = market.gov_net_100m
            market.gov_net_100m = override.gov_net_100m
            self.logger.info(f"MANUAL_OVERRIDE field=gov_net_100m old={old} new={override.gov_net_100m}")
        if override.ai_strength is not None:
            old = market.ai_strength
            market.ai_strength = override.ai_strength
            self.logger.info(f"MANUAL_OVERRIDE field=ai_strength old={old} new={override.ai_strength}")
        if override.major_event is not None:
            old = market.major_event
            market.major_event = int(override.major_event)
            self.logger.info(f"MANUAL_OVERRIDE field=major_event old={old} new={override.major_event} note={override.event_note}")
        return market

    def _merge_major_event(self, raw: Dict[str, RawData]) -> Tuple[int, List[Tuple[str, int, str]]]:
        """P0：合併 Reuters/ISW/CNN/人工事件來源，避免單一來源失敗造成重大事件被漏判。"""
        candidates = ["戰爭/地緣", "戰爭/停火", "ISW衝突分析", "CNN重大新聞"]
        events: List[Tuple[str, int, str]] = []
        for key in candidates:
            item = raw.get(key)
            value = 0
            source = ""
            try:
                source = item.source if item else ""
                if item and isinstance(item.value, dict):
                    value = int(item.value.get("major_event", 0) or 0)
            except Exception:
                value = 0
            events.append((key, value, source))
        merged = max([v for _, v, _ in events], default=0)
        active_sources = [f"{k}:{v}:{s}" for k, v, s in events if v]
        self.logger.info(f"MARKET_EVENT_MERGE merged={merged} detail={events}")
        if active_sources:
            self.logger.parsed_value("market.major_event_sources", ";".join(active_sources), "EventMerge", "latest")
        return merged, events

    def build_market_input(self, raw: Dict[str, RawData], base_date: str = "") -> MarketInput:
        market = MarketInput()
        taiex = raw.get("台股指數")
        turnover = raw.get("成交量")
        foreign = raw.get("外資")
        if taiex and taiex.status == "OK" and taiex.value:
            rows = taiex.value.get("rows", [])
            last = taiex.value.get("last", {})
            market.base_date = last.get("date", base_date)
            market.close = last.get("close")
            market.high = last.get("high")
            market.low = last.get("low")
            if len(rows) >= 2:
                market.prev_high = rows[-2].get("high")
                market.prev_low = rows[-2].get("low")
            if len(rows) >= 5:
                market.ma5 = round(sum([r["close"] for r in rows[-5:]]) / 5, 2)
            market.source_1 = self._source_note(taiex)
        else:
            market.base_date = base_date or dt.date.today().isoformat()
            self.logger.warning("台股指數未取得，市場輸入將保留缺值")
        if turnover and turnover.status == "OK" and turnover.value:
            trows = turnover.value.get("rows", [])
            last_t = turnover.value.get("last", {})
            market.turnover_100m = round(last_t.get("turnover_100m", 0), 2)
            if len(trows) >= 5:
                market.avg_turnover_5d_100m = round(sum([r["turnover_100m"] for r in trows[-5:]]) / 5, 2)
            market.source_2 = self._source_note(turnover)
        if foreign and foreign.status == "OK" and foreign.value:
            market.foreign_net_100m = round(foreign.value.get("net_100m", 0), 2)
            market.source_3 = self._source_note(foreign)

        # V2.5 P0：跨月5日資料優先覆蓋單日資料，補齊前高/前低/5MA/5日均量。
        market5 = raw.get("市場5日資料")
        if market5 and market5.status == "OK" and isinstance(market5.value, dict):
            v = market5.value
            for field in ["base_date", "close", "high", "low", "prev_high", "prev_low", "ma5", "turnover_100m", "avg_turnover_5d_100m"]:
                if v.get(field) is not None:
                    setattr(market, field, v.get(field))
            note = f"TWSE跨月5日：{v.get('taiex_recent_dates','')} / 成交值：{v.get('turnover_recent_dates','')}"
            market.source_1 = note
            market.source_2 = note
            self.logger.parsed_value("market.ma5", market.ma5, "TWSE跨月5日", market.base_date)
            self.logger.parsed_value("market.avg_turnover_5d_100m", market.avg_turnover_5d_100m, "TWSE跨月5日", market.base_date)

        gov_candidates = [raw.get("官股"), raw.get("八大官股"), raw.get("官股整理")]
        gov = next((g for g in gov_candidates if g and getattr(g, "parse_status", "") == "PARSE_OK" and isinstance(g.value, dict) and g.value.get("gov_net_100m") is not None), None)
        if gov:
            market.gov_net_100m = round(float(gov.value.get("gov_net_100m")), 2)
            market.source_4 = self._source_note(gov)
            self.logger.parsed_value("market.gov_net_100m", market.gov_net_100m, gov.source, gov.date)
        else:
            market.gov_net_100m = None

        ai = raw.get("AI產業")
        iek = raw.get("IEK產業分析")
        # V2.5：依P0/P1修正，IEK 台灣產業來源優先於 TechCrunch，避免低估台股AI產業強度。
        if iek and iek.status == "OK" and isinstance(iek.value, dict) and iek.value.get("ai_strength") is not None:
            market.ai_strength = float(iek.value.get("ai_strength"))
            self.logger.parsed_value("market.ai_strength", market.ai_strength, iek.source, iek.date)
        elif ai and ai.status == "OK" and isinstance(ai.value, dict) and ai.value.get("ai_strength") is not None:
            market.ai_strength = float(ai.value.get("ai_strength"))
            self.logger.parsed_value("market.ai_strength", market.ai_strength, ai.source, ai.date)
        else:
            market.ai_strength = 0.5

        merged_event, event_details = self._merge_major_event(raw)
        market.major_event = int(merged_event or 0)
        event_sources = [f"{k}:{v}:{s}" for k, v, s in event_details if v]
        if event_sources:
            if not market.source_4:
                market.source_4 = "重大事件來源=" + ";".join(event_sources)
            self.logger.parsed_value("market.major_event", market.major_event, "EventMerge", "latest")
        else:
            self.logger.parsed_value("market.major_event", market.major_event, "EventMerge", "latest")

        night = raw.get("台股夜盤")
        if night and night.status == "OK" and isinstance(night.value, dict):
            try:
                market.night_score = int(night.value.get("night_score", 0) or 0)
            except Exception:
                market.night_score = 0
            try:
                market.night_net_lots = int(night.value.get("net_lots")) if night.value.get("net_lots") is not None else None
            except Exception:
                market.night_net_lots = None
            self.logger.parsed_value("market.night_score", market.night_score, night.source, night.date)
            self.logger.parsed_value("market.night_net_lots", market.night_net_lots, night.source, night.date)

        self.logger.info(f"市場輸入標準化完成：{asdict(market)}")
        return market

class ScoringEngine:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger

    def score_all(self, raw: Dict[str, RawData], market: MarketInput) -> List[ModuleScore]:
        scores: List[ModuleScore] = []
        for module in MODULES:
            method = getattr(self, f"score_{self._safe_name(module)}", None)
            if method:
                scores.append(method(raw.get(module), market))
            else:
                scores.append(self.score_neutral(module, raw.get(module), "尚未建立自動判定規則，依SOP標記中性"))
        return scores

    def _safe_name(self, module: str) -> str:
        m = module.replace("-", "_").replace("/", "_").replace(" ", "_")
        mapping = {
            "美股_S&P500": "sp500", "美股_NASDAQ": "nasdaq", "美股_道瓊": "dow",
            "VIX恐慌": "vix", "美債10Y": "ust10y", "原油": "oil", "戰爭_地緣": "geopolitical",
            "CPI": "cpi", "非農": "nfp", "外資": "foreign", "官股": "gov",
            "台股指數": "taiex", "成交量": "turnover", "AI產業": "ai", "OTC": "otc", "台股夜盤": "night"
        }
        return mapping.get(m, m)

    def score_neutral(self, module: str, raw: Optional[RawData], reason: str) -> ModuleScore:
        data_text = "未取得" if not raw or raw.status != "OK" else str(raw.value)
        source = "未取得" if not raw else raw.source
        data_time = "" if not raw else raw.date
        return ModuleScore(module, data_text, 0.0, 0, 0.0, reason, source, data_time, "不納入主判斷，保守中性", "WARN")

    def _score_yahoo_index(self, raw: Optional[RawData], module: str, positive_text: str, negative_text: str) -> ModuleScore:
        if not raw or raw.status != "OK" or not raw.value:
            return self.score_neutral(module, raw, f"{module}未取得，依SOP不可編造，列中性")
        close = raw.value["close"]
        pct = raw.value["change_pct"]
        direction = 1 if pct > 0.2 else (-1 if pct < -0.2 else 0)
        strength = min(1.0, max(0.3, abs(pct) / 2)) if direction else 0.2
        weighted = round(direction * strength, 2)
        explanation = f"{module}收{close:.2f}，漲跌幅{pct:.2f}%；{positive_text if direction>0 else negative_text if direction<0 else '小幅震盪，方向中性'}。"
        trade = "提高風險偏好" if direction > 0 else "降低追價意願" if direction < 0 else "維持觀望，不作為主要加減碼依據"
        return ModuleScore(module, f"收盤{close:.2f} / 漲跌幅{pct:.2f}%", round(strength,2), direction, weighted, explanation, raw.source, raw.date, trade)

    def score_sp500(self, raw, market):
        return self._score_yahoo_index(raw, "美股-S&P500", "美股風險偏好偏多", "美股風險偏好降溫")

    def score_nasdaq(self, raw, market):
        return self._score_yahoo_index(raw, "美股-NASDAQ", "科技股仍有支撐", "科技股轉弱，壓抑AI權值股")

    def score_dow(self, raw, market):
        return self._score_yahoo_index(raw, "美股-道瓊", "傳產廣度穩定", "傳產與景氣股偏弱")

    def score_vix(self, raw, market):
        if not raw or raw.status != "OK" or not raw.value:
            return self.score_neutral("VIX恐慌", raw, "VIX未取得，列中性")
        v = raw.value["close"]
        pct = raw.value["change_pct"]
        if v < 20 and pct <= 0:
            direction, strength = 1, 0.4
        elif v > 25 or pct > 5:
            direction, strength = -1, 0.8
        elif v >= 20 or pct > 0:
            direction, strength = -1, 0.5
        else:
            direction, strength = 0, 0.2
        weighted = round(direction * strength, 2)
        explanation = f"VIX {v:.2f}，日變化{pct:.2f}%；依SOP低於20偏穩，但快速上升需提高風險。"
        trade = "恐慌未升溫，可維持正常部位" if direction > 0 else "風險升溫，降低追價與槓桿" if direction < 0 else "風險中性"
        return ModuleScore("VIX恐慌", f"{v:.2f} / {pct:.2f}%", strength, direction, weighted, explanation, raw.source, raw.date, trade)

    def score_ust10y(self, raw, market):
        if not raw or raw.status != "OK" or not raw.value:
            return self.score_neutral("美債10Y", raw, "美債10Y未取得，列中性")
        y = raw.value["value"]
        direction = -1 if y >= 4.0 else (1 if y < 3.5 else 0)
        strength = 0.7 if y >= 4.0 else 0.4 if direction else 0.2
        weighted = round(direction * strength, 2)
        explanation = f"美債10Y殖利率{y:.2f}%；4%以上對高估值科技股有壓力。"
        trade = "壓抑高估值與追價" if direction < 0 else "估值壓力緩和" if direction > 0 else "中性觀察"
        return ModuleScore("美債10Y", f"{y:.2f}%", strength, direction, weighted, explanation, raw.source, raw.date, trade)

    def score_oil(self, raw, market):
        if not raw or raw.status != "OK" or not raw.value:
            return self.score_neutral("原油", raw, "原油未取得，列中性")
        close = raw.value.get("close")
        pct = raw.value.get("change_pct", 0)
        last5 = raw.value.get("last5_close", []) or []
        five_day_change = ((close - last5[0]) / last5[0] * 100) if close and len(last5) >= 5 and last5[0] else pct
        if five_day_change > 3 or pct > 1.5:
            direction, strength = -1, min(1.0, max(0.5, abs(five_day_change) / 5))
            reason = "油價短線上升，通膨與成本壓力提高"
        elif pct < -1.5:
            direction, strength = 1, 0.3
            reason = "油價短線回落，通膨壓力略降"
        else:
            direction, strength = 0, 0.3
            reason = "油價震盪，暫不作主要判斷"
        weighted = round(direction * strength, 2)
        return ModuleScore("原油", f"收{close:.2f}/日變化{pct:.2f}%/5日變化{five_day_change:.2f}%",
                           round(strength,2), direction, weighted, reason, raw.source, raw.date,
                           "油價高檔急升時降低追價與高估值股部位")

    def score_geopolitical(self, raw, market):
        if raw and raw.status == "OK" and isinstance(raw.value, dict):
            risk = int(raw.value.get("major_event", 0) or 0)
            direction = -1 if risk else 0
            strength = 0.8 if risk else 0.2
            weighted = round(direction * strength, 2)
            explanation = "已依 Excel 註解來源優先抓取 Reuters/地緣事件頁面；若含戰爭、封鎖、攻擊等關鍵字，列重大事件風險。"
            trade = "外部事件風險升高，降倉禁追高" if risk else "事件頁面已抓取，未偵測到強風險關鍵字"
            return ModuleScore("戰爭/地緣", raw.value.get("url", ""), strength, direction, weighted, explanation, raw.source, raw.date, trade, "OK")
        return ModuleScore("戰爭/地緣", "未取得", 0.0, 0, 0.0, "地緣新聞來源未取得，列為選用資料缺失，不得自動編造事件。", "Reuters", market.base_date, "需確認資料來源/解析結果事件嚴重度", "WARN")
    def score_cpi(self, raw, market):
        if raw and raw.status == "OK" and raw.value:
            snippet = raw.value.get("snippet", "") if isinstance(raw.value, dict) else str(raw.value)
            nums = re.findall(r"[-+]?\d+(?:\.\d+)?\s*percent|[-+]?\d+(?:\.\d+)?%", snippet, flags=re.I)
            data_text = f"BLS CPI API fetched；value={raw.value.get('value') if isinstance(raw.value, dict) else ''}"
            if nums:
                data_text += "；sample=" + ", ".join(nums[:3])
            return ModuleScore("CPI", data_text, 0.3, 0, 0.0, "CPI 為週期性資料，已依 Excel 註解改抓 BLS API；非公布日使用最近公告，不做每日回退。", raw.source, raw.date, "通膨資料已抓取，需搭配油價/利率判斷", "OK")
        return ModuleScore("CPI", "BLS未取得", 0.0, 0, 0.0, "CPI 為週期性資料；BLS來源抓取失敗時不應假設中性，需標示資料缺失。", "BLS", market.base_date, "需補抓或人工確認", "WARN")
    def score_nfp(self, raw, market):
        if raw and raw.status == "OK" and raw.value:
            snippet = raw.value.get("snippet", "") if isinstance(raw.value, dict) else str(raw.value)
            data_text = f"BLS 非農 API fetched；value={raw.value.get('value') if isinstance(raw.value, dict) else ''}"
            m = re.search(r"nonfarm payroll employment.*?(?:rose|increased).*?([0-9,]+)", snippet, flags=re.I)
            if m:
                data_text += f"；payroll_hint={m.group(1)}"
            return ModuleScore("非農", data_text, 0.3, 0, 0.0, "非農為週期性資料，已依 Excel 註解改抓 BLS API就業資料；非公布日使用最近公告，不做每日回退。", raw.source, raw.date, "就業資料已抓取，需搭配市場預期差判斷", "OK")
        return ModuleScore("非農", "BLS未取得", 0.0, 0, 0.0, "非農為週期性資料；BLS來源抓取失敗時需標示資料缺失，不可編造。", "BLS", market.base_date, "需補抓或人工確認", "WARN")
    def score_foreign(self, raw, market):
        value = market.foreign_net_100m
        if value is None:
            return self.score_neutral("外資", raw, "外資買賣超未取得，列中性")
        direction = 1 if value > 10 else (-1 if value < -10 else 0)
        strength = min(1.0, max(0.3, abs(value) / 300)) if direction else 0.2
        weighted = round(direction * strength, 2)
        explanation = f"外資買賣超{value:.2f}億元；買超為正、賣超為負，依SOP轉為資金方向。"
        trade = "外資偏多，可提高主流股觀察" if direction > 0 else "外資賣壓，降低部位" if direction < 0 else "外資小幅，中性"
        return ModuleScore("外資", f"{value:.2f}億元", round(strength,2), direction, weighted, explanation, raw.source if raw else "", market.base_date, trade)

    def score_gov(self, raw, market):
        value = market.gov_net_100m
        if value is None:
            source = raw.source if raw else "官股資料來源"
            msg = raw.message if raw else "官股資料未取得"
            return ModuleScore("官股", msg, 0.0, 0, 0.0, "官股資料未取得，不編造數字；取得後分數只依gov_net_100m數值，不因來源不同扣分。", source, raw.date if raw else market.base_date, "官股資料不足，不納入主判斷", "WARN")
        direction = 1 if value > 0 else (-1 if value < 0 else 0)
        strength = min(1.0, max(0.3, abs(value)/100)) if direction else 0.2
        weighted = round(direction*strength,2)
        return ModuleScore("官股", f"{value:.2f}億元", strength, direction, weighted, "官股/八大公股資金方向已解析；分數只依數值，不因來源不同扣分。買超代表承接支撐，賣超代表政策資金未護盤。", raw.source if raw else "官股資料來源", market.base_date, "視為支撐判斷，不等於追價依據", "OK")
    def score_taiex(self, raw, market):
        if market.close is None or market.ma5 is None:
            return self.score_neutral("台股指數", raw, "台股收盤或5MA不足，列中性")
        direction = 1 if market.close > market.ma5 else -1
        strength = 0.6
        weighted = round(direction*strength,2)
        explanation = f"加權指數收{market.close:.2f}，5MA {market.ma5:.2f}；依SOP站上/跌破5MA判斷本地市場方向。"
        trade = "允許正常選股，但仍需個股條件" if direction > 0 else "短線轉弱，降倉與禁追高"
        return ModuleScore("台股指數", f"收{market.close:.2f}/5MA{market.ma5:.2f}", strength, direction, weighted, explanation, raw.source if raw else "TWSE", market.base_date, trade)

    def score_turnover(self, raw, market):
        if market.turnover_100m is None or market.avg_turnover_5d_100m is None:
            return self.score_neutral("成交量", raw, "成交值或5日均量不足，列中性")
        if market.turnover_100m > market.avg_turnover_5d_100m * 1.05:
            direction, strength = (1, 0.5) if market.close and market.ma5 and market.close >= market.ma5 else (-1, 0.6)
        else:
            direction, strength = 0, 0.3
        weighted = round(direction*strength,2)
        explanation = f"成交值{market.turnover_100m:.2f}億元，5日均量{market.avg_turnover_5d_100m:.2f}億元；依SOP判斷量能可信度。"
        trade = "價量配合可提高可信度" if direction > 0 else "下跌放量或量能不足，追價風險提高" if direction < 0 else "量能普通，不做主要加減碼依據"
        return ModuleScore("成交量", f"{market.turnover_100m:.2f}/{market.avg_turnover_5d_100m:.2f}億元", strength, direction, weighted, explanation, raw.source if raw else "TWSE", market.base_date, trade)

    def score_ai(self, raw, market):
        strength = float(market.ai_strength or 0.5)
        source = "TechCrunch/IEK/產業來源" if raw and raw.status == "OK" else "人工/預設"
        direction = 1 if strength >= 0.7 else (0 if strength >= 0.4 else -1)
        weighted = round(direction * strength, 2)
        explanation = f"AI主流強度{strength:.2f}；V1.8 已依 Excel 註解抓取 TechCrunch/IEK產業來源，若抓不到才保留預設值。"
        trade = "主線有效，可優先找主流拉回" if direction > 0 else "AI主線中性，避免過度集中" if direction == 0 else "AI題材轉弱，降低權重"
        status = "OK" if raw and raw.status == "OK" else "WARN"
        return ModuleScore("AI產業", f"AI強度{strength:.2f}", strength, direction, weighted, explanation, source, market.base_date, trade, status)
    def score_otc(self, raw, market):
        if raw and raw.status == "OK" and isinstance(raw.value, dict) and "close" in raw.value:
            return self._score_yahoo_index(raw, "OTC", "中小型資金活躍", "中小型資金轉弱")
        if raw and raw.status == "OK":
            return ModuleScore("OTC", "TPEX OTC 官方來源頁已抓取但未解析指數數值", 0.0, 0, 0.0, "抓到頁面不等於抓到數據；未解析出OTC指數與漲跌前列WARN，不納入分數。", raw.source, raw.date, "OTC資料不足，不納入主判斷", "WARN")
        return self.score_neutral("OTC", raw, "OTC資料未取得，列中性")
    def score_night(self, raw, market):
        if raw and raw.status == "OK" and isinstance(raw.value, dict) and raw.value.get("night_score") is not None:
            score = int(raw.value.get("night_score"))
            direction = 1 if score > 0 else (-1 if score < 0 else 0)
            strength = 0.4 if direction else 0.2
            weighted = round(direction * strength, 2)
            return ModuleScore("台股夜盤", str(raw.value), strength, direction, weighted, "已依附件指定 TAIFEX futContractsDateAh 解析台股夜盤數值。", raw.source, raw.date, "作為盤前輔助判斷", "OK")
        if raw and raw.status == "WARN":
            return ModuleScore("台股夜盤", raw.message or "TAIFEX正確網址已抓取但未解析數值", 0.0, 0, 0.0, "已改用附件指定 TAIFEX 夜盤網址，但本次未解析出TX夜盤數值，不可假裝OK。", raw.source, raw.date, "需修正 parser 或人工確認", "WARN")
        return ModuleScore("台股夜盤", "未取得", 0.0, 0, 0.0, "TAIFEX 夜盤來源未取得；屬時間敏感輔助資料，不阻斷主判斷。", "TAIFEX", market.base_date, "盤前需確認夜盤強弱", "WARN")
class IndicatorEngine:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger

    def compute(self, market: MarketInput, macro_total: float) -> TechnicalRisk:
        below_ma5 = int(market.close is not None and market.ma5 is not None and market.close < market.ma5)
        lower_high = int(market.high is not None and market.prev_high is not None and market.high < market.prev_high)
        lower_low = int(market.low is not None and market.prev_low is not None and market.low < market.prev_low)
        volume_expansion = int(market.turnover_100m is not None and market.avg_turnover_5d_100m is not None and market.turnover_100m > market.avg_turnover_5d_100m * 1.05)
        major_event = int(market.major_event or 0)
        night_score = market.night_score if market.night_score is not None else 0
        night_bearish = int(night_score < 0)
        risk_score = below_ma5 + lower_high + lower_low + volume_expansion + major_event + night_bearish
        if night_bearish:
            self.logger.info(f"NIGHT_RISK_APPLIED night_score={market.night_score} net_lots={market.night_net_lots}")
        if macro_total >= 3 and risk_score <= 1:
            judgement = "強多 / 允許交易"
        elif macro_total >= 1 and risk_score <= 2:
            judgement = "震盪偏多 / 可做但禁追高"
        elif macro_total <= -3 or risk_score >= 4:
            judgement = "風險偏空 / 停止新倉"
        elif macro_total <= -1 or risk_score >= 3:
            judgement = "震盪偏空 / 降倉禁追高"
        else:
            judgement = "中性震盪 / 只做最高勝率標的"
        self.logger.info(f"V2技術引擎完成：risk_score={risk_score}, judgement={judgement}, night_bearish={night_bearish}")
        return TechnicalRisk(below_ma5, lower_high, lower_low, volume_expansion, major_event, risk_score, judgement, night_bearish, market.night_score, market.night_net_lots)

class ExplanationEngine:
    def build_summary(self, macro_total: float, tech: TechnicalRisk) -> Dict[str, str]:
        if macro_total >= 3:
            state = "強多"
            switch = "允許交易"
            advice = "可提高主流股權重，但仍需個股進場條件。"
        elif 1 <= macro_total < 3:
            state = "震盪偏多"
            switch = "允許交易但禁追高"
            advice = "優先主流拉回、低位階翻多，避免滿倉。"
        elif -1 < macro_total < 1:
            state = "中性震盪"
            switch = "降低出手頻率"
            advice = "只做最高勝率標的，控制部位。"
        elif -3 < macro_total <= -1:
            state = "震盪偏空"
            switch = "降倉交易 / 禁追高"
            advice = "只做防守與低位階輪動，不追高。"
        else:
            state = "風險偏空"
            switch = "停止新倉"
            advice = "等待風險降溫與技術轉強。"
        if tech.risk_score >= 3 and "停止" not in switch:
            switch = "降倉交易 / 禁追高"
            advice += " 技術風險偏高，需再降部位。"
        return {"宏觀總分": f"{macro_total:.2f}", "技術風險分數": f"{tech.risk_score:.0f}", "市場狀態": state, "交易開關": switch, "操作建議": advice, "核心結論": tech.market_judgement}


# =============================
# V2.5 P0 Institutional Report + TEJ + Market5Day
# =============================
EXPECTED_INSTITUTIONAL_SHEETS = [
    "00_執行摘要", "01_DB資料盤點", "02_模型設計", "03_最終TOP15",
    "04_成長模型TOP30", "05_價值模型TOP30", "06_低位階候選",
    "07_老師點名股檢核", "08_排除與風險", "09_來源與限制",
    "10_老師策略驗收", "11_修改追蹤", "12_策略命中驗證",
    "13_LOW_BUY候選", "14_WATCH觀察池", "15_AVOID排除清單", "16_放行與觀察候選"
]

REPORT_COLUMNS = [
    "排名", "代號", "名稱", "市場", "產業", "題材", "老師分類", "老師決策", "決策原因", "決策追蹤", "老師股票池", "核心股狀態",
    "K線警訊", "波段修正", "避開/換股", "現價",
    "低接區", "停損", "目標1", "目標2", "RR", "是否可下單", "老師執行狀態",
    "老師總分", "成長分", "價值分", "EPS_TTM", "PE", "殖利率%",
    "營收YoY%", "法人分", "20日漲幅%", "低位階分", "K線分",
    "均線支撐分", "量能健康分", "營收EPS分", "操作策略", "排除原因",
    "低位階翻多", "放行理由", "硬性排除", "軟性排除", "壓力來源",
    "Phase5大波", "Phase5小波", "Phase5修正型態", "Phase5回撤比例", "Phase5反彈性質",
    "Phase5修正完成", "Phase5逃命反彈", "Phase5推動浪", "Phase5波段階段", "Phase5突破階段", "Phase5推動階段", "Phase5波段位置分", "Phase5候選池", "Phase5阻擋原因",
    "期間BUY次數", "期間AVOID次數", "最後降級原因"
]

TEACHER_WATCHLIST = [
    "2317", "2382", "3706", "2881", "2330", "3019", "2324", "6533", "2359",
    "3231", "2454", "3034", "3711", "9945", "2603", "2412"
]

class TEJGovBankEngine:
    """V2.5 P0：TEJ八大公股行庫主來源解析。Wantgoo/T86只作佐證，不冒充主資料。"""
    def __init__(self, tej_file: Optional[str], logger: Macro16Logger):
        self.tej_file = tej_file
        self.logger = logger

    def _to_number(self, v: Any) -> float:
        try:
            if v is None:
                return math.nan
            s = str(v).replace(",", "").replace("--", "").strip()
            if s == "" or s.lower() == "nan":
                return math.nan
            return float(s)
        except Exception:
            return math.nan

    def load(self):
        if pd is None:
            self.logger.warning("TEJGovBankEngine 需要 pandas，未安裝時官股只能標示WARN")
            return None
        if not self.tej_file:
            return None
        path = Path(self.tej_file)
        if not path.exists():
            self.logger.warning(f"TEJ_GOV_FILE_NOT_FOUND path={path}")
            return None
        try:
            sheets = pd.read_excel(path, sheet_name=None)
        except Exception as exc:
            self.logger.warning(f"TEJ_GOV_READ_FAIL path={path} error={exc}")
            return None
        raw = None
        for name in ["Raw1", "Raw2", "raw1", "raw2"]:
            if name in sheets:
                raw = sheets[name]
                break
        if raw is None:
            for _, df in sheets.items():
                cols = [str(c) for c in df.columns]
                if any(("買" in c and "超" in c) for c in cols):
                    raw = df
                    break
        if raw is None or raw.empty:
            return None
        raw = raw.copy()
        raw.columns = [str(c).strip() for c in raw.columns]
        return raw

    def parse(self) -> Dict[str, Any]:
        df = self.load()
        if df is None or len(df) == 0:
            return {"status": "WARN", "gov_net_100m": None, "gov_signal": "未知", "gov_score": 0, "message": "TEJ檔案未提供、讀取失敗或無Raw資料", "rows": 0}
        amount_col = next((c for c in df.columns if "買(賣)超金額" in c or "買賣超金額" in c or "買賣超金額" in c.replace(" ", "")), None)
        net_col = next((c for c in df.columns if "買(賣)超" in c or "買賣超" in c), None)
        date_col = next((c for c in df.columns if "日期" in c), None)
        if amount_col:
            amount = sum([self._to_number(x) for x in df[amount_col].tolist() if not math.isnan(self._to_number(x))])
            # TEJ欄位可能已是千元/元；先以欄名金額總和保守轉億元，證據保留原欄位。
            gov_net_100m = amount / 100000000.0
            amount_source_col = amount_col
        elif net_col:
            gov_net_100m = None
            amount_source_col = net_col
        else:
            return {"status": "WARN", "gov_net_100m": None, "gov_signal": "未知", "gov_score": 0, "message": "TEJ欄位缺少買賣超或買賣超金額", "rows": len(df)}
        if gov_net_100m is None:
            net_vals = [self._to_number(x) for x in df[net_col].tolist()]
            net_vals = [x for x in net_vals if not math.isnan(x)]
            direction_sum = sum(net_vals) if net_vals else 0
            gov_signal = "偏多" if direction_sum > 0 else "偏空" if direction_sum < 0 else "中性"
        else:
            gov_signal = "偏多" if gov_net_100m > 0 else "偏空" if gov_net_100m < 0 else "中性"
        gov_score = 1 if gov_signal == "偏多" else -1 if gov_signal == "偏空" else 0
        actual_date = "latest"
        if date_col and df[date_col].notna().any():
            actual_date = str(df[date_col].dropna().iloc[0])
        payload = {
            "status": "OK", "source": "TEJ八大公股行庫", "actual_date": actual_date,
            "gov_net_100m": gov_net_100m, "gov_signal": gov_signal, "gov_score": gov_score,
            "rows": len(df), "amount_column": amount_source_col,
            "message": "TEJ八大官股解析完成；TWSE T86/Wantgoo僅作佐證"
        }
        self.logger.write_raw_evidence("官股_TEJ", payload, parsed=payload, status="OK", url=self.tej_file or "", message=payload["message"])
        self.logger.parsed_value("gov_net_100m", gov_net_100m, "TEJ八大公股行庫", actual_date)
        return payload

class Market5DayEngine:
    """V2.5 P0：跨月回補最近5個有效交易日，避免前高/前低/5MA/5日均量空白。"""
    def __init__(self, client: HttpClient, logger: Macro16Logger):
        self.client = client
        self.logger = logger

    def _num(self, value: Any) -> float:
        try:
            return float(str(value).replace(",", "").strip())
        except Exception:
            return math.nan

    def _iso(self, compact: str) -> str:
        compact = str(compact).replace("-", "")
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"

    def _tw_date_to_iso(self, value: Any) -> str:
        s = str(value).strip()
        parts = s.split("/")
        if len(parts) >= 3:
            year = int(parts[0]) + 1911 if int(parts[0]) < 1911 else int(parts[0])
            return f"{year:04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        if re.fullmatch(r"\d{8}", s):
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return s

    def _month_candidates(self, base_date: str, months_back: int = 4) -> List[str]:
        base = dt.datetime.strptime(str(base_date).replace("-", ""), "%Y%m%d").date().replace(day=1)
        out = []
        y, m = base.year, base.month
        for _ in range(months_back):
            out.append(f"{y:04d}{m:02d}01")
            m -= 1
            if m == 0:
                y -= 1; m = 12
        return out

    def fetch_taiex_recent_days(self, base_date: str, need: int = 5):
        frames = []
        for month_start in self._month_candidates(base_date, 4):
            url = f"https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date={month_start}&response=json"
            try:
                js = self.client.get_json(url)
            except Exception as exc:
                self.logger.warning(f"MARKET5_TAIEX_FETCH_FAIL url={url} error={exc}")
                continue
            for row in js.get("data", []) or []:
                try:
                    frames.append({"date": self._tw_date_to_iso(row[0]), "open": self._num(row[1]), "high": self._num(row[2]), "low": self._num(row[3]), "close": self._num(row[4])})
                except Exception:
                    continue
        if pd is None:
            return []
        df = pd.DataFrame(frames)
        if df.empty:
            return df
        df = df.dropna(subset=["date", "close"]).drop_duplicates("date")
        df = df[df["date"] <= self._iso(base_date)].sort_values("date", ascending=False).head(need)
        return df.sort_values("date")

    def fetch_turnover_recent_days(self, base_date: str, need: int = 5):
        frames = []
        for month_start in self._month_candidates(base_date, 4):
            url = f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date={month_start}&response=json"
            try:
                js = self.client.get_json(url)
            except Exception as exc:
                self.logger.warning(f"MARKET5_TURNOVER_FETCH_FAIL url={url} error={exc}")
                continue
            for row in js.get("data", []) or []:
                try:
                    frames.append({"date": self._tw_date_to_iso(row[0]), "turnover_100m": self._num(row[2]) / 100000000.0})
                except Exception:
                    continue
        if pd is None:
            return []
        df = pd.DataFrame(frames)
        if df.empty:
            return df
        df = df.dropna(subset=["date", "turnover_100m"]).drop_duplicates("date")
        df = df[df["date"] <= self._iso(base_date)].sort_values("date", ascending=False).head(need)
        return df.sort_values("date")

    def build_market_features(self, base_date: str) -> Dict[str, Any]:
        compact = str(base_date).replace("-", "")
        px = self.fetch_taiex_recent_days(compact, 5)
        tv = self.fetch_turnover_recent_days(compact, 5)
        if pd is None or getattr(px, "empty", True) or getattr(tv, "empty", True) or len(px) < 5 or len(tv) < 5:
            msg = f"P0_FAIL: 最近5交易日不足 taiex={0 if pd is None or getattr(px,'empty',True) else len(px)} turnover={0 if pd is None or getattr(tv,'empty',True) else len(tv)}"
            self.logger.warning(msg)
            return {"status": "FAIL", "message": msg}
        latest = px.iloc[-1]
        prev = px.iloc[-2]
        result = {
            "status": "OK", "base_date": latest["date"], "close": float(latest["close"]),
            "high": float(latest["high"]), "low": float(latest["low"]),
            "prev_high": float(prev["high"]), "prev_low": float(prev["low"]),
            "ma5": round(float(px["close"].mean()), 2),
            "turnover_100m": round(float(tv.iloc[-1]["turnover_100m"]), 2),
            "avg_turnover_5d_100m": round(float(tv["turnover_100m"].mean()), 2),
            "taiex_recent_dates": ",".join(px["date"].astype(str).tolist()),
            "turnover_recent_dates": ",".join(tv["date"].astype(str).tolist()),
        }
        self.logger.write_raw_evidence("市場5日資料", result, parsed=result, status="OK", url="TWSE MI_5MINS_HIST/FMTQIK", message="跨月5日資料已補齊")
        return result

class DBRepository:
    """V2.5：從股票DB建立機構級報告資料集。"""
    def __init__(self, db_path: str, logger: Optional[Macro16Logger] = None):
        self.db_path = db_path
        self.logger = logger

    def connect(self):
        return sqlite3.connect(self.db_path)

    def table_exists(self, table: str) -> bool:
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            return cur.fetchone() is not None

    def get_trade_date(self) -> str:
        with self.connect() as conn:
            return pd.read_sql("SELECT MAX(date) AS d FROM ranking_result", conn).iloc[0]["d"]

    def table_count(self, table: str) -> int:
        if not self.table_exists(table):
            return 0
        with self.connect() as conn:
            return int(pd.read_sql(f"SELECT COUNT(*) AS n FROM {table}", conn).iloc[0]["n"])

    def latest_date(self, table: str) -> str:
        if not self.table_exists(table):
            return ""
        candidates = ["date", "snapshot_date", "trade_date", "data_date", "feature_date", "plan_date", "source_date", "data_year"]
        with self.connect() as conn:
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            for c in candidates:
                if c in cols:
                    try:
                        return str(pd.read_sql(f"SELECT MAX({c}) AS d FROM {table}", conn).iloc[0]["d"])
                    except Exception:
                        pass
        return ""

    def load_latest_table(self, conn, table: str, date_col: str, trade_date: str):
        if not self.table_exists(table):
            return pd.DataFrame()
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if date_col in cols:
            try:
                return pd.read_sql(f"SELECT * FROM {table} WHERE {date_col}=(SELECT MAX({date_col}) FROM {table} WHERE {date_col}<=?)", conn, params=[trade_date])
            except Exception:
                pass
        return pd.read_sql(f"SELECT * FROM {table}", conn)

    def load_base_universe(self, trade_date: str):
        with self.connect() as conn:
            r = pd.read_sql("SELECT * FROM ranking_result WHERE date=?", conn, params=[trade_date])
            ms = self.load_latest_table(conn, "market_snapshot", "snapshot_date", trade_date)
            sm = self.load_latest_table(conn, "stocks_master", "update_date", trade_date)
            val = self.load_latest_table(conn, "external_valuation", "data_date", trade_date)
            rev = self.load_latest_table(conn, "external_revenue", "source_date", trade_date)
            inst = self.load_latest_table(conn, "external_institutional", "trade_date", trade_date)
            margin = self.load_latest_table(conn, "external_margin", "trade_date", trade_date)
            ph = pd.read_sql("SELECT * FROM price_history WHERE date<=?", conn, params=[trade_date]) if self.table_exists("price_history") else pd.DataFrame()
        for df in [r, ms, sm, val, rev, inst, margin, ph]:
            if not df.empty and "stock_id" in df.columns:
                df["stock_id"] = df["stock_id"].astype(str).str.zfill(4)
        df = r.copy()
        if not ms.empty:
            df = df.merge(ms.drop_duplicates("stock_id"), on="stock_id", how="left", suffixes=("", "_m"))
        if not sm.empty:
            df = df.merge(sm.drop_duplicates("stock_id"), on="stock_id", how="left", suffixes=("", "_master"))
        if not val.empty:
            df = df.merge(val.drop_duplicates("stock_id"), on="stock_id", how="left", suffixes=("", "_valuation"))
        if not rev.empty:
            df = df.merge(rev.drop_duplicates("stock_id"), on="stock_id", how="left", suffixes=("", "_revenue"))
        if not inst.empty:
            df = df.merge(inst.drop_duplicates("stock_id"), on="stock_id", how="left", suffixes=("", "_inst"))
        if not margin.empty:
            df = df.merge(margin.drop_duplicates("stock_id"), on="stock_id", how="left", suffixes=("", "_margin"))
        if not ph.empty:
            feats = self._price_features(ph)
            df = df.merge(feats, on="stock_id", how="left")
        return df.drop_duplicates("stock_id")

    def _price_features(self, ph):
        out = []
        for sid, g in ph.groupby("stock_id"):
            g = g.sort_values("date")
            close = pd.to_numeric(g["close"], errors="coerce")
            high = pd.to_numeric(g["high"], errors="coerce")
            low = pd.to_numeric(g["low"], errors="coerce")
            vol = pd.to_numeric(g.get("volume"), errors="coerce") if "volume" in g.columns else pd.Series(dtype=float)
            last_close = close.iloc[-1] if len(close) else math.nan
            high120 = high.tail(120).max() if len(high) else math.nan
            low120 = low.tail(120).min() if len(low) else math.nan
            pos120 = (last_close - low120) / (high120 - low120) if high120 and low120 and high120 != low120 else math.nan
            pct20 = (last_close / close.iloc[-20] - 1) * 100 if len(close) >= 20 and close.iloc[-20] else math.nan
            ma20_calc = close.tail(20).mean() if len(close) >= 20 else math.nan
            ma60_calc = close.tail(60).mean() if len(close) >= 60 else math.nan
            vol5 = vol.tail(5).mean() if len(vol) >= 5 else math.nan
            vol20 = vol.tail(20).mean() if len(vol) >= 20 else math.nan
            ma5_calc = close.tail(5).mean() if len(close) >= 5 else math.nan
            ma65_calc = close.tail(65).mean() if len(close) >= 65 else ma60_calc
            prev_close = close.iloc[-2] if len(close) >= 2 else math.nan
            last_open = pd.to_numeric(g.get("open"), errors="coerce").iloc[-1] if "open" in g.columns and len(g) else math.nan
            last_high = high.iloc[-1] if len(high) else math.nan
            last_low = low.iloc[-1] if len(low) else math.nan
            prev_high = high.iloc[-2] if len(high) >= 2 else math.nan
            prev2_high = high.iloc[-3] if len(high) >= 3 else math.nan
            high20_prev = high.iloc[:-1].tail(20).max() if len(high) >= 21 else math.nan
            high60_prev = high.iloc[:-1].tail(60).max() if len(high) >= 61 else math.nan
            low20_prev = low.iloc[:-1].tail(20).min() if len(low) >= 21 else math.nan
            ema12 = close.ewm(span=12, adjust=False).mean() if len(close) >= 12 else pd.Series(dtype=float)
            ema26 = close.ewm(span=26, adjust=False).mean() if len(close) >= 26 else pd.Series(dtype=float)
            macd_dif = (ema12 - ema26) if len(ema26) else pd.Series(dtype=float)
            macd_dea = macd_dif.ewm(span=9, adjust=False).mean() if len(macd_dif) >= 9 else pd.Series(dtype=float)
            macd_hist = (macd_dif - macd_dea) if len(macd_dea) else pd.Series(dtype=float)
            macd_hist_last = macd_hist.iloc[-1] if len(macd_hist) else math.nan
            macd_hist_prev = macd_hist.iloc[-2] if len(macd_hist) >= 2 else math.nan
            low9 = low.rolling(9).min()
            high9 = high.rolling(9).max()
            rsv = (close - low9) / (high9 - low9).replace(0, math.nan) * 100
            k = rsv.ewm(com=2, adjust=False).mean()
            d = k.ewm(com=2, adjust=False).mean()
            k_last = k.iloc[-1] if len(k) else math.nan
            d_last = d.iloc[-1] if len(d) else math.nan
            k_prev = k.iloc[-2] if len(k) >= 2 else math.nan
            d_prev = d.iloc[-2] if len(d) >= 2 else math.nan
            out.append({
                "stock_id": str(sid).zfill(4), "pos_120d": pos120, "pct_20d": pct20,
                "ma5_calc": ma5_calc, "ma20_calc": ma20_calc, "ma60_calc": ma60_calc, "ma65_calc": ma65_calc,
                "vol5": vol5, "vol20": vol20, "last_open": last_open, "last_high": last_high, "last_low": last_low,
                "prev_close": prev_close, "prev_high_price": prev_high, "prev2_high_price": prev2_high,
                "high20_prev": high20_prev, "high60_prev": high60_prev, "low20_prev": low20_prev,
                "macd_hist": macd_hist_last, "macd_hist_prev": macd_hist_prev,
                "kd_k": k_last, "kd_d": d_last, "kd_k_prev": k_prev, "kd_d_prev": d_prev
            })
        return pd.DataFrame(out)


def _safe_series(df, column: str, default=math.nan, numeric: bool = True):
    """Phase5 FIX：保證任何欄位都回傳與 df.index 對齊的 pandas Series。
    目的：避免缺欄、重複欄名、單一 scalar 被 pd.to_numeric 後變成 numpy.float64，
    導致 .abs() / .fillna() / .notna() 失敗。
    """
    if pd is None:
        return None
    idx = getattr(df, "index", None)
    if idx is None:
        idx = pd.RangeIndex(0)
    if df is None or column is None:
        s = pd.Series(default, index=idx)
    elif hasattr(df, "columns") and column in df.columns:
        value = df[column]
        # 若merge後出現重複欄名，df[column] 會是 DataFrame；取第一欄作為主欄位。
        if isinstance(value, pd.DataFrame):
            value = value.iloc[:, 0]
        if isinstance(value, pd.Series):
            s = value.reindex(idx)
        else:
            s = pd.Series(value, index=idx)
    else:
        s = pd.Series(default, index=idx)
    if numeric:
        s = pd.to_numeric(s, errors="coerce")
    return s

def _safe_bool_series(df, column: str, default: bool = False):
    s = _safe_series(df, column, default=default, numeric=False)
    if s is None:
        return s
    return s.fillna(default).astype(bool)

def _safe_str_series(df, column: str, default: str = ""):
    s = _safe_series(df, column, default=default, numeric=False)
    if s is None:
        return s
    return s.fillna(default).astype(str)

class StageTrace:
    """InstitutionalReportEngine 階段追蹤，用於精準定位是哪個 Engine / 欄位失敗。"""
    def __init__(self, logger: Optional[Macro16Logger], stage: str, df_getter=None):
        self.logger = logger
        self.stage = stage
        self.df_getter = df_getter

    def __enter__(self):
        if self.logger:
            self.logger.strategy_trace("STAGE_START", {"stage": self.stage})
        return self

    def __exit__(self, exc_type, exc, tb):
        if not self.logger:
            return False
        if exc_type is None:
            payload = {"stage": self.stage, "status": "OK"}
            try:
                df = self.df_getter() if callable(self.df_getter) else None
                if df is not None:
                    payload["rows"] = int(len(df))
                    payload["columns"] = list(map(str, list(df.columns)[:80]))
            except Exception:
                pass
            self.logger.strategy_trace("STAGE_OK", payload)
            return False
        payload = {"stage": self.stage, "status": "FAIL", "error_type": exc_type.__name__, "error": str(exc)}
        try:
            df = self.df_getter() if callable(self.df_getter) else None
            if df is not None:
                payload["rows"] = int(len(df))
                payload["columns"] = list(map(str, list(df.columns)[:120]))
        except Exception as meta_exc:
            payload["meta_error"] = str(meta_exc)
        self.logger.strategy_trace("STAGE_FAIL", payload)
        return False


class MarketRegimeEngine:
    """顧奎國老師策略P0：大盤多空總閘與乖離風控。
    原Macro16只用5MA/高低點/量能，本層補上「短線拉回 vs 波段修正」語義。
    """
    def __init__(self, logger: Optional[Macro16Logger] = None):
        self.logger = logger

    def apply(self, df):
        if df is None or df.empty:
            return df
        close = _safe_series(df, "close")
        ma60 = _safe_series(df, "ma60_final")
        ma60 = ma60.fillna(_safe_series(df, "ma60_calc"))
        macd = _safe_series(df, "macd_hist")
        macd_prev = _safe_series(df, "macd_hist_prev")
        k = _safe_series(df, "kd_k")
        d = _safe_series(df, "kd_d")
        k_prev = _safe_series(df, "kd_k_prev")
        d_prev = _safe_series(df, "kd_d_prev")
        denom = ma60.replace(0, math.nan)
        df["deviation_from_ma60"] = ((close - ma60) / denom * 100).replace([math.inf, -math.inf], math.nan)
        df["deviation_risk_flag"] = df["deviation_from_ma60"].abs().fillna(0) >= 20
        df["macd_turn_negative"] = (macd < 0) & (macd_prev >= 0)
        df["macd_near_zero"] = macd.abs().fillna(999) <= 0.20
        df["kd_dead_cross_below_80"] = (k < d) & (k_prev >= d_prev) & (k < 80)
        df["wave_correction_flag"] = df["macd_turn_negative"].fillna(False) & df["kd_dead_cross_below_80"].fillna(False)
        df["market_pullback_type"] = "短線拉回"
        df.loc[df["wave_correction_flag"], "market_pullback_type"] = "波段修正"
        df.loc[df["deviation_risk_flag"] & ~df["wave_correction_flag"], "market_pullback_type"] = "乖離過大/禁追高"
        if self.logger:
            wave_count = int(df["wave_correction_flag"].fillna(False).sum())
            self.logger.strategy_trace("WAVE_CORRECTION", {
                "rows": int(len(df)),
                "wave_correction_count": wave_count,
                "macd_turn_negative_count": int(df["macd_turn_negative"].fillna(False).sum()),
                "kd_dead_cross_below_80_count": int(df["kd_dead_cross_below_80"].fillna(False).sum()),
                "deviation_risk_count": int(df["deviation_risk_flag"].fillna(False).sum()),
            })
            if wave_count == 0:
                self.logger.warning("WAVE_CORRECTION_ZERO_WARN wave_correction_count=0，請確認macd_hist/macd_hist_prev與KD欄位來源是否完整；此為驗收WARN，不阻斷報表。")
        return df

class SakataRiskPatternEngine:
    """顧奎國老師策略P0：量化流星、空頭新星十字、墓碑線、長黑K。"""
    def __init__(self, logger: Optional[Macro16Logger] = None):
        self.logger = logger

    def apply(self, df):
        if df is None or df.empty:
            return df
        o = _safe_series(df, "last_open")
        h = _safe_series(df, "last_high")
        l = _safe_series(df, "last_low")
        c = _safe_series(df, "close")
        prev_c = _safe_series(df, "prev_close")
        body = (c - o).abs()
        rng = (h - l).replace(0, math.nan)
        upper = h - pd.concat([o, c], axis=1).max(axis=1)
        lower = pd.concat([o, c], axis=1).min(axis=1) - l
        df["shooting_star_flag"] = (upper >= body * 2) & ((body / rng) <= 0.35) & ((h - c) / rng >= 0.45)
        df["tombstone_flag"] = (upper >= rng * 0.60) & (lower <= rng * 0.10) & ((body / rng) <= 0.25)
        df["bearish_star_cross_flag"] = ((body / rng) <= 0.15) & (prev_c.notna()) & (c >= prev_c * 1.02)
        df["long_black_flag"] = (c < o) & ((o - c) / rng >= 0.60)
        warn = []
        for i in df.index:
            labels = []
            if bool(df.at[i, "shooting_star_flag"]): labels.append("流星")
            if bool(df.at[i, "bearish_star_cross_flag"]): labels.append("空頭新星十字")
            if bool(df.at[i, "tombstone_flag"]): labels.append("墓碑線")
            if bool(df.at[i, "long_black_flag"]): labels.append("長黑K")
            warn.append("/".join(labels) if labels else "")
        df["k_warning_type"] = warn
        df["two_day_break_high"] = _safe_series(df, "last_high") > _safe_series(df, "high20_prev")
        df["kline_score"] = _safe_series(df, "kline_score", default=50).fillna(50)
        df.loc[df["k_warning_type"].ne(""), "kline_score"] = (df["kline_score"] - 25).clip(0, 100)
        if self.logger:
            self.logger.strategy_trace("SAKATA_PATTERN_SUMMARY", {
                "shooting_star_count": int(df["shooting_star_flag"].fillna(False).sum()),
                "bearish_star_cross_count": int(df["bearish_star_cross_flag"].fillna(False).sum()),
                "tombstone_count": int(df["tombstone_flag"].fillna(False).sum()),
                "long_black_count": int(df["long_black_flag"].fillna(False).sum()),
                "warning_count": int(df["k_warning_type"].astype(str).ne("").sum()),
            })
        return df

class CoreLeaderEngine:
    """顧奎國老師策略P0：台積電(2330)作為大盤核心風向。"""
    def __init__(self, logger: Optional[Macro16Logger] = None):
        self.logger = logger

    def infer_state(self, df):
        default = {"core_leader_state": "NE", "core_leader_reason": "未取得2330資料，核心股風向不進主判斷"}
        if df is None or df.empty or "stock_id" not in df.columns:
            return default
        sid = df["stock_id"].astype(str).str.zfill(4)
        if not sid.eq("2330").any():
            return default
        r = df.loc[sid.eq("2330")].iloc[0]
        close = _safe_float(r.get("close"))
        ma20 = _safe_float(r.get("ma20_final"))
        ma60 = _safe_float(r.get("ma60_final"))
        high20 = _safe_float(r.get("high20_prev"))
        warning = str(r.get("k_warning_type", "") or "")
        if high20 and close and close > high20:
            state, reason = "突破續強", "2330突破近20日壓力，主流續強"
        elif close and ma20 and close >= ma20 and (not warning):
            state, reason = "回測不破", "2330守住MA20/回測不破，仍屬多方拉回"
        elif close and ma20 and close < ma20 and (ma60 and close >= ma60):
            state, reason = "假突破/短線降檔", "2330跌回MA20下方但未破中期線，降追價"
        elif close and ma60 and close < ma60:
            state, reason = "核心轉弱", "2330跌破MA60，市場風險升級"
        else:
            state, reason = "觀察", "2330資料不足，僅列觀察"
        if warning:
            reason += f"；K線警訊={warning}"
        return {"core_leader_state": state, "core_leader_reason": reason}

    def apply(self, df):
        state = self.infer_state(df)
        df["core_leader_state"] = state["core_leader_state"]
        df["core_leader_reason"] = state["core_leader_reason"]
        if self.logger:
            self.logger.strategy_trace("CORE_LEADER_STATUS", state)
            if state.get("core_leader_state") in ("突破續強", "假突破/短線降檔", "核心轉弱"):
                self.logger.strategy_trace("2330_BREAKOUT", state)
        return df

class AvoidSwapEngine:
    """Phase5：排除換股層升級。
    修改前：swap_reason非空即avoid_flag=True，導致軟性壓力也會把全市場封殺。
    修改後：區分 hard_avoid 與 soft_avoid；低位階翻多可覆蓋 soft avoid，但不可覆蓋 hard risk。
    """
    def __init__(self, logger: Optional[Macro16Logger] = None):
        self.logger = logger

    def apply(self, df):
        if df is None or df.empty:
            return df
        close = _safe_series(df, "close")
        ma20 = _safe_series(df, "ma20_final")
        ma60 = _safe_series(df, "ma60_final")
        high20 = _safe_series(df, "high20_prev")
        high60 = _safe_series(df, "high60_prev")
        prev_high = _safe_series(df, "prev_high_price")
        prev2_high = _safe_series(df, "prev2_high_price")
        k_warning = _safe_str_series(df, "k_warning_type")
        low_base_reversal = _safe_bool_series(df, "low_base_reversal_flag")
        wave_correction = _safe_bool_series(df, "wave_correction_flag")
        deviation_risk = _safe_bool_series(df, "deviation_risk_flag")
        df["two_high_not_passed_flag"] = (prev_high < high60 * 0.995) & (prev2_high < high60 * 0.995) & (close < high20)
        df["downtrend_flag"] = (close < ma20) & (ma20 < ma60)
        df["neckline_pressure_flag"] = (close < high20) & (high20.notna()) & ((high20 - close) / close.replace(0, math.nan) <= 0.05)
        df["hard_k_risk_flag"] = k_warning.str.contains("長黑K|墓碑線", na=False)
        hard_reasons, soft_reasons, release_reasons, pressure_states = [], [], [], []
        for i in df.index:
            hard, soft, release = [], [], []
            if bool(df.at[i, "downtrend_flag"]): hard.append("下降軌道")
            if bool(wave_correction.loc[i]): hard.append("MACD+KD波段修正")
            if bool(df.at[i, "hard_k_risk_flag"]): hard.append("硬K線風險")
            if bool(df.at[i, "two_high_not_passed_flag"]): soft.append("兩高不過")
            if bool(df.at[i, "neckline_pressure_flag"]): soft.append("頸線/前高壓力未突破")
            if bool(deviation_risk.loc[i]): soft.append("乖離過大禁追高")
            if bool(low_base_reversal.loc[i]) and soft and not hard:
                release.append("低位階翻多覆蓋軟性壓力")
            hard_reasons.append(";".join(hard))
            soft_reasons.append(";".join(soft))
            release_reasons.append(";".join(release))
            pressure_states.append(";".join(hard + soft) if (hard or soft) else "未觸發")
        df["hard_avoid_reason"] = hard_reasons
        df["soft_avoid_reason"] = soft_reasons
        df["avoid_release_reason"] = release_reasons
        df["hard_avoid_flag"] = df["hard_avoid_reason"].astype(str).ne("")
        df["soft_avoid_flag"] = df["soft_avoid_reason"].astype(str).ne("")
        df["avoid_flag"] = df["hard_avoid_flag"] | (df["soft_avoid_flag"] & ~low_base_reversal)
        df["swap_reason"] = ""
        for idx, r in df.iterrows():
            parts = []
            if str(r.get("hard_avoid_reason", "") or ""): parts.append(str(r.get("hard_avoid_reason")))
            if str(r.get("soft_avoid_reason", "") or "") and not bool(r.get("low_base_reversal_flag", False)):
                parts.append(str(r.get("soft_avoid_reason")))
            if str(r.get("soft_avoid_reason", "") or "") and bool(r.get("low_base_reversal_flag", False)) and not str(r.get("hard_avoid_reason", "") or ""):
                parts.append("軟性壓力已由低位階翻多覆蓋")
            df.at[idx, "swap_reason"] = ";".join(parts)
        df["avoid_level"] = "NONE"
        df.loc[df["soft_avoid_flag"], "avoid_level"] = "SOFT"
        df.loc[df["hard_avoid_flag"], "avoid_level"] = "HARD"
        df.loc[df["soft_avoid_flag"] & low_base_reversal & ~df["hard_avoid_flag"], "avoid_level"] = "SOFT_RELEASED"
        df["pressure_line_state"] = pressure_states
        if self.logger:
            summary = {
                "avoid_count": int(df["avoid_flag"].fillna(False).sum()),
                "hard_avoid_count": int(df["hard_avoid_flag"].fillna(False).sum()),
                "soft_avoid_count": int(df["soft_avoid_flag"].fillna(False).sum()),
                "soft_released_by_low_base_count": int((df["soft_avoid_flag"] & low_base_reversal & ~df["hard_avoid_flag"]).sum()),
                "two_high_not_passed_count": int(df["two_high_not_passed_flag"].fillna(False).sum()),
                "downtrend_count": int(df["downtrend_flag"].fillna(False).sum()),
                "neckline_pressure_count": int(df["neckline_pressure_flag"].fillna(False).sum()),
            }
            self.logger.strategy_trace("AVOID_SWAP_SUMMARY", summary)
            sample = df.loc[df["avoid_flag"].fillna(False), ["stock_id", "swap_reason", "avoid_level"]].head(20)
            for _, row in sample.iterrows():
                self.logger.strategy_trace("AVOID_REASON", {"stock": str(row.get("stock_id", "")).zfill(4), "level": row.get("avoid_level", ""), "reason": row.get("swap_reason", "")})
        return df


class TeacherPhase5SemanticEngine:
    """V2.7.3：Phase5語義一致性修正層。
    目的：把差異分析報告指出的「第3浪訊號 vs 主跌修正浪」、「主升池混入弱反彈」、
    「修正反彈只剩文字但沒有位置」轉成可被 TeacherDecisionEngine 使用的欄位。
    本層不取代既有老師模型，而是建立 Phase5 Gate 與候選池，使報表/決策/原因一致。
    """
    def __init__(self, logger: Optional[Macro16Logger] = None):
        self.logger = logger

    def apply(self, df):
        if df is None or df.empty:
            return df
        close = _safe_series(df, "close")
        ma20 = _safe_series(df, "ma20_final")
        ma60 = _safe_series(df, "ma60_final")
        rsi = _safe_series(df, "rsi", default=50).fillna(50)
        vol5 = _safe_series(df, "vol5")
        vol20 = _safe_series(df, "vol20")
        high20 = _safe_series(df, "high20_prev")
        low20 = _safe_series(df, "low20_prev")
        high60 = _safe_series(df, "high60_prev")
        low60 = _safe_series(df, "low60_prev")
        high120 = _safe_series(df, "high120_prev")
        low120 = _safe_series(df, "low120_prev")
        rr = _safe_series(df, "rr", default=0).fillna(0)
        score = _safe_series(df, "teacher_score", default=0).fillna(0)
        wave_correction = _safe_bool_series(df, "wave_correction_flag")
        low_base_reversal = _safe_bool_series(df, "low_base_reversal_flag")
        hard_avoid = _safe_bool_series(df, "hard_avoid_flag")
        k_warning = _safe_str_series(df, "k_warning_type")
        ratio = (vol5 / vol20.replace(0, math.nan)).replace([math.inf, -math.inf], math.nan).fillna(1.0)
        # 大波：避免只用「第3浪」文字，先判定主升/主跌背景。
        main_up = close.notna() & ma20.notna() & ma60.notna() & (close >= ma20) & (ma20 >= ma60 * 0.98)
        main_down = close.notna() & ma20.notna() & ma60.notna() & ((close < ma20) & (ma20 < ma60))
        box = ~(main_up | main_down)
        df["phase5_major_wave"] = "大箱型整理"
        df.loc[main_up, "phase5_major_wave"] = "主升推動浪"
        df.loc[main_down, "phase5_major_wave"] = "主跌修正浪"
        # 費波回撤/反彈比例：使用120/60/20日高低點依序取可用區間。
        swing_high = high120.fillna(high60).fillna(high20)
        swing_low = low120.fillna(low60).fillna(low20)
        span = (swing_high - swing_low).replace(0, math.nan)
        fibo_retrace = ((swing_high - close) / span).where(main_up, (close - swing_low) / span)
        fibo_retrace = fibo_retrace.clip(lower=0, upper=1.5).fillna(0.5)
        df["phase5_fibo_retrace"] = fibo_retrace.round(3)
        zone = pd.Series("0.382~0.618中性回撤", index=df.index)
        zone.loc[fibo_retrace < 0.382] = "0~0.382淺回/弱反彈"
        zone.loc[(fibo_retrace >= 0.382) & (fibo_retrace <= 0.618)] = "0.382~0.618健康回撤"
        zone.loc[(fibo_retrace > 0.618) & (fibo_retrace <= 0.786)] = "0.618~0.786深回撤"
        zone.loc[fibo_retrace > 0.786] = "0.786以上結構破壞"
        df["phase5_fibo_retrace_zone"] = zone
        # 小波定位。
        minor = pd.Series("小波待確認", index=df.index)
        minor.loc[main_up & (fibo_retrace <= 0.618) & low_base_reversal] = "第2浪/第4浪拉回"
        minor.loc[main_up & (close > high20) & (ratio >= 1.2)] = "第3浪推動"
        minor.loc[main_down & (close < ma20)] = "C浪反彈待確認"
        minor.loc[main_down & (close >= ma20) & (close < ma60)] = "B浪弱反彈"
        minor.loc[main_down & (fibo_retrace > 0.618)] = "C浪延伸/深修正"
        df["phase5_minor_wave"] = minor
        # 修正型態。
        corr = pd.Series("修正型態待確認", index=df.index)
        corr.loc[main_up & low_base_reversal] = "主升回撤修正"
        corr.loc[main_down & (fibo_retrace <= 0.382)] = "Zigzag弱反彈"
        corr.loc[main_down & (fibo_retrace > 0.382) & (fibo_retrace <= 0.618)] = "ABC修正反彈"
        corr.loc[main_down & (fibo_retrace > 0.618)] = "C浪延伸修正"
        corr.loc[box & (fibo_retrace <= 0.618)] = "箱型整理修正"
        df["phase5_correction_type"] = corr
        # 修正完成：必須同時具備站回MA20、量能、RSI、壓力突破任一確認。
        correction_completed = (
            close.notna() & ma20.notna() & (close >= ma20)
            & (ratio >= 1.2)
            & (rsi >= 45)
            & ((high20.isna()) | (close >= high20 * 0.995) | low_base_reversal)
            & ~hard_avoid
        )
        df["phase5_correction_completed"] = correction_completed
        # 逃命反彈/弱反彈：主跌、未站回MA60、量不足或RSI不足，且未完成。
        escape = main_down & (close < ma60) & (~correction_completed) & ((ratio < 1.0) | (rsi < 50) | (fibo_retrace < 0.5))
        df["phase5_escape_rally"] = escape
        rebound = pd.Series("一般反彈", index=df.index)
        rebound.loc[main_up & low_base_reversal & ~correction_completed] = "主升拉回待確認"
        rebound.loc[main_up & correction_completed] = "主升拉回完成"
        rebound.loc[main_down & ~escape] = "跌深技術反彈"
        rebound.loc[escape] = "逃命反彈"
        rebound.loc[main_down & (close < low20)] = "反彈失敗"
        rebound.loc[box & correction_completed] = "箱型突破轉強"
        df["phase5_rebound_type"] = rebound

        # V2.7.4 Position Engine：
        # 依 Word《彩晶（6116）Position Engine缺口分析報告》補齊 wave_phase / breakout_stage / position_score。
        # 重點：彩晶類低價量價轉強股不應因「尚未正式突破」被壓成 position_score=0。
        bottom_complete = (
            (close >= ma20.fillna(close) * 0.98)
            & (ma20.fillna(close) >= ma60.fillna(ma20).fillna(close) * 0.96)
            & (ratio >= 1.15)
            & (rsi.between(45, 72))
            & (score >= 60)
            & ~hard_avoid
        )
        # 大波補強：底部完成不同於一般大箱型整理。
        df.loc[box & bottom_complete, "phase5_major_wave"] = "底部完成"

        compression = (
            (close >= ma20.fillna(close) * 0.98)
            & (close <= high20.fillna(close * 1.08) * 1.01)
            & (ratio >= 1.1)
            & (rsi.between(45, 72))
            & ~hard_avoid
        )
        impulsive = (
            (main_up | df["phase5_major_wave"].astype(str).eq("底部完成"))
            & (close > high20.fillna(close * 9))
            & (ratio >= 1.2)
            & (rsi.between(45, 72))
            & ~hard_avoid
            & ~wave_correction
        )
        # Wave3預突破：不是推動浪True，但要保留為觀察，不可誤判為主跌。
        prebreakout = (
            (score >= 60)
            & (rr >= 1.0)
            & (main_up | box | df["phase5_major_wave"].astype(str).eq("底部完成"))
            & (close >= ma20.fillna(close) * 0.98)
            & (ratio >= 1.1)
            & (rsi.between(45, 72))
            & ~hard_avoid
            & ~wave_correction
        )

        wave_phase = pd.Series("Unknown", index=df.index)
        wave_phase.loc[main_down & ~correction_completed] = "A/B/C_Correction"
        wave_phase.loc[main_up & low_base_reversal & ~correction_completed] = "Wave2_or_Wave4_Pullback"
        wave_phase.loc[df["phase5_major_wave"].astype(str).eq("底部完成") & prebreakout] = "Wave3_PreBreakout"
        wave_phase.loc[main_up & prebreakout & ~impulsive] = "Wave3_PreBreakout"
        wave_phase.loc[impulsive] = "Wave3_Breakout"
        wave_phase.loc[impulsive & (ratio >= 1.8)] = "Wave3_Expansion"
        wave_phase.loc[(rsi > 72) & main_up & (close >= high60.fillna(high20).fillna(close) * 0.98)] = "Wave5_Risk"
        df["phase5_wave_phase"] = wave_phase

        breakout_stage = pd.Series("None", index=df.index)
        breakout_stage.loc[compression & ~prebreakout] = "Compression"
        breakout_stage.loc[prebreakout & ~impulsive] = "PreBreakout"
        breakout_stage.loc[impulsive] = "Breakout"
        breakout_stage.loc[impulsive & (ratio >= 1.8)] = "Expansion"
        breakout_stage.loc[(rsi > 72) & main_up] = "Exhaustion"
        breakout_stage.loc[main_down & ~correction_completed] = "Correction"
        df["phase5_breakout_stage"] = breakout_stage

        df["phase5_impulsive_wave"] = impulsive
        df["phase5_impulse_stage"] = "None"
        df.loc[prebreakout & ~impulsive, "phase5_impulse_stage"] = "Early"
        df.loc[impulsive, "phase5_impulse_stage"] = "Confirmed"
        df.loc[impulsive & (ratio >= 1.8), "phase5_impulse_stage"] = "Expansion"
        df.loc[(rsi > 72) & main_up, "phase5_impulse_stage"] = "Exhausted"
        df.loc[main_down & ~impulsive, "phase5_impulse_stage"] = "Failed/Correction"

        # position_score：正式波段位置分，替代原DB position_score=0造成的放行缺口。
        position_score = pd.Series(0.0, index=df.index)
        position_score += main_up.astype(float) * 20
        position_score += df["phase5_major_wave"].astype(str).eq("底部完成").astype(float) * 18
        position_score += box.astype(float) * 8
        position_score -= main_down.astype(float) * 15
        position_score += df["phase5_minor_wave"].astype(str).isin(["第2浪/第4浪拉回", "第3浪推動"]).astype(float) * 15
        position_score += df["phase5_wave_phase"].astype(str).eq("Wave3_PreBreakout").astype(float) * 20
        position_score += df["phase5_wave_phase"].astype(str).eq("Wave3_Breakout").astype(float) * 24
        position_score += df["phase5_wave_phase"].astype(str).eq("Wave3_Expansion").astype(float) * 18
        position_score += df["phase5_breakout_stage"].astype(str).eq("Compression").astype(float) * 8
        position_score += df["phase5_breakout_stage"].astype(str).eq("PreBreakout").astype(float) * 15
        position_score += df["phase5_breakout_stage"].astype(str).eq("Breakout").astype(float) * 18
        position_score += df["phase5_impulse_stage"].astype(str).eq("Early").astype(float) * 10
        position_score += df["phase5_impulse_stage"].astype(str).eq("Confirmed").astype(float) * 14
        position_score += df["phase5_correction_completed"].astype(float) * 10
        position_score += zone.astype(str).str.contains("健康回撤", na=False).astype(float) * 8
        position_score += (ratio >= 1.2).astype(float) * 5
        position_score -= df["phase5_escape_rally"].astype(float) * 35
        position_score -= hard_avoid.astype(float) * 25
        df["phase5_position_score"] = position_score.clip(lower=0, upper=100).round(2)
        df["position_score"] = df[["phase5_position_score"]].max(axis=1)

        # 彩晶/PreBreakout 類：若原本 position_stage 未知，補成可讀波段位置。
        df["position_stage"] = _safe_str_series(df, "position_stage", default="未知")
        df.loc[df["phase5_wave_phase"].astype(str).eq("Wave3_PreBreakout"), "position_stage"] = "Wave3_PreBreakout"
        df.loc[df["phase5_wave_phase"].astype(str).eq("Wave3_Breakout"), "position_stage"] = "Wave3_Breakout"
        df.loc[df["phase5_major_wave"].astype(str).eq("底部完成") & df["phase5_breakout_stage"].astype(str).eq("PreBreakout"), "position_stage"] = "底部完成_PreBreakout"
        block_reason = pd.Series("", index=df.index)
        block_reason.loc[escape] = "Phase5逃命反彈：主跌修正浪且修正未完成，禁止追價或主動布局"
        block_reason.loc[main_down & ~escape & ~correction_completed] = "Phase5主跌弱反彈：大波仍是主跌修正浪，僅可觀察"
        block_reason.loc[main_up & ~correction_completed & low_base_reversal] = "Phase5主升拉回未完成：等待量價與壓力突破確認"
        block_reason.loc[df["phase5_wave_phase"].astype(str).eq("Wave3_PreBreakout") & (df["phase5_position_score"] < 60)] = "Phase5預突破位置分不足：等待量能/壓力確認"
        df["phase5_block_reason"] = block_reason
        df["phase5_hard_block"] = escape | (main_down & ~correction_completed)
        df["phase5_wait_block"] = (main_up & low_base_reversal & ~correction_completed) | (box & ~correction_completed & ~impulsive)
        pool = pd.Series("觀察池", index=df.index)
        pool.loc[df["phase5_hard_block"],] = "高風險反彈池"
        pool.loc[escape] = "禁追風控池"
        pool.loc[main_up & low_base_reversal & ~correction_completed] = "主升拉回觀察池"
        pool.loc[prebreakout & ~impulsive & ~df["phase5_hard_block"]] = "主升預突破觀察池"
        pool.loc[df["phase5_wave_phase"].astype(str).eq("Wave3_PreBreakout") & (df["phase5_position_score"] >= 65) & ~df["phase5_hard_block"]] = "主升預突破觀察池"
        pool.loc[impulsive] = "主升確認池"
        df["phase5_candidate_pool"] = pool
        labels = []
        for _, r in df.iterrows():
            label = f"{r.get('phase5_major_wave','')}/{r.get('phase5_minor_wave','')}/{r.get('phase5_rebound_type','')}"
            if bool(r.get('phase5_escape_rally', False)):
                label += "（逃命反彈風控）"
            elif not bool(r.get('phase5_correction_completed', False)) and "反彈" in str(r.get('phase5_rebound_type','')):
                label += "（修正未完成）"
            labels.append(label)
        df["phase5_wave_label"] = labels
        if self.logger:
            self.logger.strategy_trace("PHASE5_SEMANTIC_SUMMARY", {
                "rows": int(len(df)),
                "escape_rally": int(df["phase5_escape_rally"].fillna(False).sum()),
                "impulsive_wave": int(df["phase5_impulsive_wave"].fillna(False).sum()),
                "hard_block": int(df["phase5_hard_block"].fillna(False).sum()),
                "prebreakout": int(df["phase5_breakout_stage"].astype(str).eq("PreBreakout").sum()),
                "bottom_complete": int(df["phase5_major_wave"].astype(str).eq("底部完成").sum()),
                "avg_position_score": round(float(df["phase5_position_score"].fillna(0).mean()), 2),
            })
        return df

class TeacherDecisionEngine:
    """Phase5：老師五態決策重平衡。
    決策順序：硬性風險先擋，再由低位階翻多、RR、分數與核心股狀態決定 BUY/LOW_BUY。
    """
    def __init__(self, logger: Optional[Macro16Logger] = None):
        self.logger = logger

    def apply(self, df):
        if df is None or df.empty:
            return df
        # V2.7.3：先建立Phase5語義/風控欄位，再進老師五態決策，避免報表與決策不同步。
        df = TeacherPhase5SemanticEngine(self.logger).apply(df)
        score = _safe_series(df, "teacher_score", default=0).fillna(0)
        rr = _safe_series(df, "rr", default=0).fillna(0)
        close = _safe_series(df, "close")
        entry_high = _safe_series(df, "entry_high")
        entry_low = _safe_series(df, "entry_low")
        high20 = _safe_series(df, "high20_prev")
        low_base = _safe_series(df, "low_base_score", default=50).fillna(50)
        low_base_reversal = _safe_bool_series(df, "low_base_reversal_flag")
        hard_avoid = _safe_bool_series(df, "hard_avoid_flag")
        avoid = _safe_bool_series(df, "avoid_flag")
        soft_released = _safe_str_series(df, "avoid_level", default="NONE").eq("SOFT_RELEASED")
        k_warning = _safe_str_series(df, "k_warning_type")
        wave = _safe_bool_series(df, "wave_correction_flag")
        deviation = _safe_bool_series(df, "deviation_risk_flag")
        core_state = _safe_str_series(df, "core_leader_state", default="NE")
        core_ok = ~core_state.eq("核心轉弱")
        phase5_hard_block = _safe_bool_series(df, "phase5_hard_block")
        phase5_wait_block = _safe_bool_series(df, "phase5_wait_block")
        phase5_escape = _safe_bool_series(df, "phase5_escape_rally")
        phase5_impulsive = _safe_bool_series(df, "phase5_impulsive_wave")
        phase5_pool = _safe_str_series(df, "phase5_candidate_pool")
        phase5_position_score = _safe_series(df, "phase5_position_score", default=0).fillna(0)
        phase5_wave_phase = _safe_str_series(df, "phase5_wave_phase")
        phase5_block_reason = _safe_str_series(df, "phase5_block_reason")
        df["teacher_decision"] = "WATCH"
        df.loc[hard_avoid | core_state.eq("核心轉弱") | phase5_escape, "teacher_decision"] = "AVOID"
        reduce_mask = ((k_warning.ne("") | wave | deviation | phase5_hard_block) & ~hard_avoid & ~low_base_reversal & ~phase5_escape)
        df.loc[reduce_mask & ~df["teacher_decision"].eq("AVOID"), "teacher_decision"] = "REDUCE"
        low_buy = (
            (score >= 55)
            & (rr >= 1.20)
            & (low_base >= 55)
            & low_base_reversal
            & core_ok
            & ~hard_avoid
            & ~phase5_hard_block
            & (close <= entry_high * 1.05)
        )
        breakout_or_pullback_confirm = low_base_reversal | (close > high20) | soft_released
        buy = (
            (score >= 72)
            & (rr >= 1.55)
            & breakout_or_pullback_confirm
            & core_ok
            & ~hard_avoid
            & ~phase5_hard_block
            & (phase5_impulsive | ((phase5_pool.isin(["主升確認池", "主升預突破觀察池", "主升拉回觀察池"])) & (phase5_position_score >= 65)))
            & (close <= entry_high * 1.035)
            & ~wave
            & k_warning.eq("")
        )
        df.loc[low_buy & ~df["teacher_decision"].eq("AVOID"), "teacher_decision"] = "LOW_BUY"
        df.loc[buy & ~df["teacher_decision"].eq("AVOID"), "teacher_decision"] = "BUY"
        reasons = []
        traces = []
        release = []
        for i, r in df.iterrows():
            reason_parts = []
            release_parts = []
            if str(r.get("hard_avoid_reason", "") or ""): reason_parts.append("硬性排除=" + str(r.get("hard_avoid_reason")))
            if str(r.get("phase5_block_reason", "") or ""): reason_parts.append(str(r.get("phase5_block_reason")))
            if str(r.get("phase5_candidate_pool", "") or ""): release_parts.append("Phase5候選池=" + str(r.get("phase5_candidate_pool")))
            if str(r.get("phase5_wave_phase", "") or ""): release_parts.append("波段階段=" + str(r.get("phase5_wave_phase")))
            if _safe_float(r.get("phase5_position_score"), 0) > 0: release_parts.append("波段位置分=" + str(round(_safe_float(r.get("phase5_position_score"),0),2)))
            if str(r.get("soft_avoid_reason", "") or ""):
                if bool(r.get("low_base_reversal_flag", False)) and not str(r.get("hard_avoid_reason", "") or ""):
                    release_parts.append("低位階翻多覆蓋軟性壓力=" + str(r.get("soft_avoid_reason")))
                else:
                    reason_parts.append("軟性壓力=" + str(r.get("soft_avoid_reason")))
            if str(r.get("k_warning_type", "") or ""): reason_parts.append("K線警訊=" + str(r.get("k_warning_type")))
            if bool(r.get("wave_correction_flag", False)): reason_parts.append("MACD+KD波段修正")
            if bool(r.get("deviation_risk_flag", False)): reason_parts.append("乖離過大")
            if _safe_float(r.get("rr"), 0) < 1.2: reason_parts.append("RR不足")
            if _safe_float(r.get("teacher_score"), 0) < 55: reason_parts.append("老師總分不足")
            if bool(r.get("low_base_reversal_flag", False)): reason_parts.append("低位階翻多=" + str(r.get("low_base_reversal_reason", "")))
            if str(r.get("teacher_decision")) in ("BUY", "LOW_BUY"):
                release_parts.append("分數/RR/位置/核心股狀態符合放行")
            if not reason_parts and not release_parts:
                reason_parts.append("條件未完全確認，列觀察")
            trace = (
                f"stock={str(r.get('stock_id','')).zfill(4)};"
                f"decision={r.get('teacher_decision')};"
                f"score={round(_safe_float(r.get('teacher_score'),0),2)};"
                f"rr={round(_safe_float(r.get('rr'),0),2)};"
                f"low_base={r.get('low_base_reversal_flag','')};"
                f"core={r.get('core_leader_state','')};"
                f"wave={r.get('market_pullback_type','')};"
                f"k_warning={r.get('k_warning_type','')};"
                f"avoid_level={r.get('avoid_level','')};"
                f"phase5={r.get('phase5_wave_label','')};"
                f"wave_phase={r.get('phase5_wave_phase','')};"
                f"breakout_stage={r.get('phase5_breakout_stage','')};"
                f"position_score={round(_safe_float(r.get('phase5_position_score'),0),2)};"
                f"phase5_pool={r.get('phase5_candidate_pool','')};"
                f"phase5_block={r.get('phase5_block_reason','')};"
                f"avoid={r.get('swap_reason','')}"
            )
            reasons.append(";".join(reason_parts))
            release.append(";".join(release_parts))
            traces.append(trace)
        df["teacher_decision_reason"] = reasons
        df["decision_release_reason"] = release
        df["decision_trace"] = traces
        pool = []
        for i, r in df.iterrows():
            div = _safe_float(r.get("dividend_yield"), 0)
            rev = _safe_float(r.get("yoy", r.get("revenue_yoy")), 0)
            theme = str(r.get("theme", "") or "") + " " + str(r.get("sub_theme", "") or "") + " " + str(r.get("industry", "") or "")
            if str(r.get("stock_id", "")).zfill(4) == "2330":
                pool.append("主流核心")
            elif bool(r.get("low_base_reversal_flag", False)):
                pool.append("低位階翻多")
            elif rev >= 20 or any(k in theme for k in ["AI", "半導體", "CPO", "伺服器", "散熱"]):
                pool.append("營收成長/主流題材")
            elif div >= 4:
                pool.append("高配息防守")
            else:
                pool.append("觀察")
        df["teacher_pool_type"] = pool
        # V2.7.3：外顯老師股票池優先接Phase5候選池，避免主跌弱反彈混入主升池。
        df.loc[phase5_pool.astype(str).ne(""), "teacher_pool_type"] = phase5_pool

        # Phase5 semantic fix：以老師五態重新同步 YES / WAIT / NO，避免 YES 與 BUY/LOW_BUY 語義打架。
        in_entry_zone = close.notna() & entry_low.notna() & entry_high.notna() & (close >= entry_low) & (close <= entry_high)
        df["teacher_execution_status"] = "NO"
        df["teacher_execution_reason"] = ""
        df["是否可下單"] = "NO"
        buy_mask = df["teacher_decision"].eq("BUY")
        low_buy_mask = df["teacher_decision"].eq("LOW_BUY")
        watch_mask = df["teacher_decision"].eq("WATCH")
        reduce_mask2 = df["teacher_decision"].eq("REDUCE")
        avoid_mask2 = df["teacher_decision"].eq("AVOID")
        buy_entry_ok = buy_mask & (in_entry_zone | (close <= entry_high * 1.01)) & ~phase5_hard_block & ~phase5_wait_block
        df.loc[buy_entry_ok, "teacher_execution_status"] = "YES"
        df.loc[buy_entry_ok, "teacher_execution_reason"] = "BUY主攻：核心風向未轉弱、無硬性排除、RR與Phase5位置符合"
        df.loc[buy_mask & ~buy_entry_ok, "teacher_execution_status"] = "WAIT"
        df.loc[buy_mask & ~buy_entry_ok, "teacher_execution_reason"] = "BUY條件成立但價格/Phase5尚待確認，等待回測或突破確認"
        df.loc[low_buy_mask & in_entry_zone, "teacher_execution_status"] = "YES"
        df.loc[low_buy_mask & in_entry_zone, "teacher_execution_reason"] = "LOW_BUY且已進入低接區，可分批"
        df.loc[low_buy_mask & ~in_entry_zone, "teacher_execution_status"] = "WAIT"
        df.loc[low_buy_mask & ~in_entry_zone, "teacher_execution_reason"] = "LOW_BUY但尚未進入低接區，等待回落"
        df.loc[watch_mask & ~hard_avoid & (rr >= 1.20), "teacher_execution_status"] = "WAIT"
        df.loc[watch_mask & ~hard_avoid & (rr >= 1.20), "teacher_execution_reason"] = "WATCH：條件未完整確認，只觀察不主攻"
        df.loc[reduce_mask2, "teacher_execution_status"] = "NO"
        df.loc[reduce_mask2, "teacher_execution_reason"] = "REDUCE：壓力或波段風險，禁止新增"
        df.loc[avoid_mask2, "teacher_execution_status"] = "NO"
        df.loc[avoid_mask2, "teacher_execution_reason"] = "AVOID：硬性排除或核心風向轉弱"
        df.loc[phase5_escape, "teacher_execution_status"] = "NO"
        df.loc[phase5_escape, "teacher_execution_reason"] = "Phase5逃命反彈風控：禁止新增"
        df.loc[phase5_hard_block & ~phase5_escape, "teacher_execution_status"] = "WAIT"
        df.loc[phase5_hard_block & ~phase5_escape, "teacher_execution_reason"] = "Phase5主跌弱反彈或修正未完成：只觀察不主攻"
        df["是否可下單"] = df["teacher_execution_status"]

        if self.logger:
            counts = df["teacher_decision"].value_counts(dropna=False).to_dict()
            self.logger.strategy_trace("TEACHER_DECISION_SUMMARY", counts)
            self.logger.strategy_trace("TEACHER_EXECUTION_SUMMARY", df["teacher_execution_status"].value_counts(dropna=False).to_dict())
            if int((df["teacher_decision"].isin(["BUY", "LOW_BUY"])).sum()) == 0:
                self.logger.warning("TEACHER_DECISION_STRICT_WARNING BUY與LOW_BUY皆為0，請檢查RR/低位階翻多/硬性風險條件")
            for _, r in df.sort_values(["teacher_score", "rr"], ascending=False).head(30).iterrows():
                self.logger.strategy_trace("DECISION_TRACE", {
                    "stock": str(r.get("stock_id", "")).zfill(4),
                    "decision": r.get("teacher_decision", ""),
                    "execution": r.get("teacher_execution_status", ""),
                    "reason": r.get("teacher_decision_reason", ""),
                    "release": r.get("decision_release_reason", ""),
                    "score": round(_safe_float(r.get("teacher_score"),0), 2),
                    "rr": round(_safe_float(r.get("rr"),0), 2),
                    "low_base": bool(r.get("low_base_reversal_flag", False)),
                    "wave": r.get("market_pullback_type", ""),
                    "k_warning": r.get("k_warning_type", ""),
                    "avoid": r.get("swap_reason", ""),
                    "phase5": r.get("phase5_wave_label", ""),
                    "phase5_pool": r.get("phase5_candidate_pool", ""),
                    "phase5_block": r.get("phase5_block_reason", ""),
                    "impulse_stage": r.get("phase5_impulse_stage", ""),
                })
        return df

def _safe_float(v, default=math.nan):
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        x = float(v)
        if math.isnan(x):
            return default
        return x
    except Exception:
        return default

class FeatureBuilder:
    def build(self, df):
        for c in ["close", "ma20", "ma60", "ma20_calc", "ma60_calc", "rsi", "volume", "vol5", "vol20", "revenue_yoy", "yoy", "eps_ttm", "eps_yoy", "pe", "dividend_yield", "roe", "institutional_score", "risk_score"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df["close"] = df.get("close", pd.Series(index=df.index, dtype=float)).fillna(df.get("close_m", pd.Series(index=df.index, dtype=float)))
        df["ma20_final"] = df.get("ma20", pd.Series(index=df.index, dtype=float)).fillna(df.get("ma20_calc", pd.Series(index=df.index, dtype=float)))
        df["ma60_final"] = df.get("ma60", pd.Series(index=df.index, dtype=float)).fillna(df.get("ma60_calc", pd.Series(index=df.index, dtype=float)))
        df["low_base_score"] = ((1 - df.get("pos_120d", pd.Series(index=df.index, dtype=float)).clip(0,1)) * 100).fillna(50)
        df["ma_support_score"] = 50
        df.loc[df["close"] >= df["ma20_final"], "ma_support_score"] += 25
        df.loc[df["ma20_final"] >= df["ma60_final"] * 0.98, "ma_support_score"] += 25
        df["ma_support_score"] = df["ma_support_score"].clip(0,100)
        df["kline_score"] = df.get("reversal_score", pd.Series(50, index=df.index)).fillna(50)
        ratio = df.get("vol5", pd.Series(index=df.index, dtype=float)) / df.get("vol20", pd.Series(index=df.index, dtype=float)).replace(0, math.nan)
        df["volume_health_score"] = (50 + (ratio.fillna(1) - 1) * 50).clip(0,100)
        rev = df.get("yoy", df.get("revenue_yoy", pd.Series(index=df.index, dtype=float)))
        eps_yoy = df.get("eps_yoy", pd.Series(index=df.index, dtype=float))
        df["revenue_eps_score"] = (normalize_series(rev) * 0.55 + normalize_series(eps_yoy) * 0.45).fillna(50)
        theme_text = (df.get("theme", df.get("sub_theme", pd.Series("", index=df.index))).astype(str) + " " + df.get("industry", pd.Series("", index=df.index)).astype(str))
        df["theme_score"] = theme_text.apply(lambda x: 85 if any(k in x for k in ["AI", "半導體", "伺服器", "CPO", "散熱", "電源", "網通"]) else 50)
        return df

def normalize_series(series, low=None, high=None):
    """安全正規化：支援 None、scalar、Series，避免 scalar 造成後續運算失敗。"""
    if pd is None:
        return series
    if series is None:
        return pd.Series(dtype=float)
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    if not isinstance(series, pd.Series):
        series = pd.Series([series])
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty:
        return pd.Series(50, index=s.index)
    lo = s.quantile(0.05) if low is None else low
    hi = s.quantile(0.95) if high is None else high
    if hi == lo:
        return pd.Series(50, index=s.index)
    return ((s - lo) / (hi - lo) * 100).clip(0,100).fillna(50)

class DualModelScoringEngine:
    def score(self, df):
        rev = df.get("yoy", df.get("revenue_yoy", pd.Series(index=df.index, dtype=float)))
        df["growth_score"] = (
            0.25 * normalize_series(rev) +
            0.20 * normalize_series(df.get("eps_yoy")) +
            0.20 * normalize_series(df.get("ai_score")) +
            0.15 * normalize_series(df.get("momentum_score")) +
            0.10 * normalize_series(df.get("volume_score")) +
            0.10 * df.get("theme_score", pd.Series(50, index=df.index))
        )
        df["value_score"] = (
            0.25 * (100 - normalize_series(df.get("pe"))) +
            0.20 * normalize_series(df.get("dividend_yield")) +
            0.20 * normalize_series(df.get("roe")) +
            0.15 * normalize_series(100 - pd.to_numeric(df.get("risk_score", pd.Series(50, index=df.index)), errors="coerce")) +
            0.10 * df.get("ma_support_score", pd.Series(50, index=df.index)) +
            0.10 * normalize_series(df.get("institutional_score"))
        )
        df["teacher_score"] = (
            0.20 * df.get("low_base_score", pd.Series(50, index=df.index)) +
            0.20 * df.get("kline_score", pd.Series(50, index=df.index)) +
            0.15 * df.get("ma_support_score", pd.Series(50, index=df.index)) +
            0.10 * df.get("volume_health_score", pd.Series(50, index=df.index)) +
            0.15 * df.get("revenue_eps_score", pd.Series(50, index=df.index)) +
            0.10 * normalize_series(df.get("dividend_yield")) +
            0.10 * df.get("theme_score", pd.Series(50, index=df.index))
        )
        return df

class ReportClassifier:
    def classify(self, df):
        df["老師分類"] = "觀察"
        rev = pd.to_numeric(df.get("yoy", df.get("revenue_yoy", pd.Series(index=df.index))), errors="coerce")
        df.loc[(rev >= 20) & (df["growth_score"] >= 70), "老師分類"] = "主流成長"
        df.loc[(pd.to_numeric(df.get("eps_ttm", 0), errors="coerce") > 0) & (pd.to_numeric(df.get("pe", 999), errors="coerce") <= 18), "老師分類"] = "價值防守"
        df.loc[(df.get("pos_120d", pd.Series(1,index=df.index)) <= 0.45) & (df["ma_support_score"] >= 60), "老師分類"] = "低位階翻多"
        return df


class LowBaseReversalEngine:
    """Phase5：顧奎國老師低位階翻多/第二浪末端辨識層。
    目的：補足「主升辨識層」，避免所有低位階回測不破標的都被 AvoidSwap 軟壓力直接封殺。
    這一層只允許覆蓋 soft avoid，不覆蓋長黑K、波段修正、下降軌道等 hard risk。
    """
    def __init__(self, logger: Optional[Macro16Logger] = None):
        self.logger = logger

    def apply(self, df):
        if df is None or df.empty:
            return df
        close = _safe_series(df, "close")
        ma20 = _safe_series(df, "ma20_final")
        ma60 = _safe_series(df, "ma60_final")
        low_base = _safe_series(df, "low_base_score", default=50).fillna(50)
        ma_support = _safe_series(df, "ma_support_score", default=50).fillna(50)
        volume_health = _safe_series(df, "volume_health_score", default=50).fillna(50)
        revenue_eps = _safe_series(df, "revenue_eps_score", default=50).fillna(50)
        k_warning = _safe_str_series(df, "k_warning_type")
        core_state = _safe_str_series(df, "core_leader_state", default="NE")
        wave_correction = _safe_bool_series(df, "wave_correction_flag")
        deviation_risk = _safe_bool_series(df, "deviation_risk_flag")
        hard_k_warning = k_warning.str.contains("長黑K|墓碑線", na=False)
        pullback_hold_ma20 = close.notna() & ma20.notna() & (close >= ma20 * 0.97)
        mid_trend_not_broken = close.notna() & ma60.notna() & (close >= ma60 * 0.92)
        df["low_base_reversal_flag"] = (
            (low_base >= 58)
            & (ma_support >= 55)
            & (volume_health >= 42)
            & (revenue_eps >= 45)
            & pullback_hold_ma20
            & mid_trend_not_broken
            & ~wave_correction
            & ~hard_k_warning
            & ~core_state.eq("核心轉弱")
        )
        df["low_base_reversal_strength"] = (
            low_base * 0.35 + ma_support * 0.25 + volume_health * 0.15 + revenue_eps * 0.15
            + (~deviation_risk).astype(int) * 10
        ).clip(0, 100)
        stage, reason, release_reason = [], [], []
        for _, r in df.iterrows():
            parts = []
            if bool(r.get("low_base_reversal_flag", False)):
                parts.extend(["低位階分>=58", "回測MA20不破", "中期線未破壞", "量能健康"])
                if _safe_float(r.get("revenue_eps_score"), 0) >= 55:
                    parts.append("營收/EPS支持")
                stage.append("第二浪末端/低位階翻多")
                reason.append(";".join(parts))
                release_reason.append("低位階翻多可覆蓋軟性頸線壓力，但不可覆蓋硬性風險")
            else:
                miss = []
                if _safe_float(r.get("low_base_score"), 0) < 58: miss.append("位階不足")
                if _safe_float(r.get("ma_support_score"), 0) < 55: miss.append("均線支撐不足")
                if bool(r.get("wave_correction_flag", False)): miss.append("波段修正")
                warning_text = str(r.get("k_warning_type", ""))
                if any(x in warning_text for x in ["長黑K", "墓碑線"]): miss.append("硬K線風險")
                stage.append("未確認")
                reason.append(";".join(miss) if miss else "條件未完全確認")
                release_reason.append("")
        df["low_base_stage"] = stage
        df["low_base_reversal_reason"] = reason
        df["low_base_release_reason"] = release_reason
        if self.logger:
            self.logger.strategy_trace("LOW_BASE_REVERSAL_SUMMARY", {
                "rows": int(len(df)),
                "low_base_reversal_count": int(df["low_base_reversal_flag"].fillna(False).sum()),
                "avg_strength": round(float(pd.to_numeric(df["low_base_reversal_strength"], errors="coerce").fillna(0).mean()), 2),
            })
        return df

class TradePlanEngine:
    def build(self, df):
        close = _safe_series(df, "close")
        ma20 = _safe_series(df, "ma20_final")
        ma60 = _safe_series(df, "ma60_final")
        atr = _safe_series(df, "atr")
        df["entry_low"] = pd.Series(np.maximum(ma20 * 0.98, close * 0.935), index=df.index) if np is not None else close * 0.935
        df["entry_high"] = pd.Series(np.minimum(ma20 * 1.01, close * 0.965), index=df.index) if np is not None else close * 0.965
        fallback = df["entry_low"].isna() | df["entry_high"].isna() | (close < df["entry_low"])
        df.loc[fallback, "entry_low"] = close * 0.995
        df.loc[fallback, "entry_high"] = close * 1.010
        reverse_mask = df["entry_low"].notna() & df["entry_high"].notna() & (df["entry_low"] > df["entry_high"])
        if reverse_mask.any():
            tmp_entry_low = df.loc[reverse_mask, "entry_low"].copy()
            df.loc[reverse_mask, "entry_low"] = df.loc[reverse_mask, "entry_high"]
            df.loc[reverse_mask, "entry_high"] = tmp_entry_low
            if "entry_zone_fix_flag" not in df.columns:
                df["entry_zone_fix_flag"] = ""
            df.loc[reverse_mask, "entry_zone_fix_flag"] = "低接區上下限反向已自動修正"
        if not (df.loc[df["entry_low"].notna() & df["entry_high"].notna(), "entry_low"] <= df.loc[df["entry_low"].notna() & df["entry_high"].notna(), "entry_high"]).all():
            raise ValueError("P0_FAIL: TradePlanEngine entry_low/entry_high 區間仍存在反向")
        df["stop_loss"] = pd.Series(np.minimum(ma60 * 0.97, df["entry_low"] * 0.94), index=df.index) if np is not None else df["entry_low"] * 0.94
        df["stop_loss"] = df["stop_loss"].fillna(df["entry_low"] * 0.94)
        df["target_1"] = pd.Series(np.where(atr.notna(), close + atr * 1.382, close * 1.05), index=df.index) if np is not None else close * 1.05
        df["target_2"] = pd.Series(np.where(atr.notna(), close + atr * 1.618, close * 1.125), index=df.index) if np is not None else close * 1.125
        risk_denom = (df["entry_high"] - df["stop_loss"]).replace(0, math.nan)
        df["rr"] = ((df["target_1"] - df["entry_high"]) / risk_denom).replace([math.inf, -math.inf], math.nan)
        df["exclude_reason"] = ""
        if "entry_zone_fix_flag" in df.columns:
            df.loc[_safe_str_series(df, "entry_zone_fix_flag").ne(""), "exclude_reason"] += "低接區上下限反向已自動修正;"
        df.loc[_safe_series(df, "volume", default=0).fillna(0) < 500, "exclude_reason"] += "成交量不足;"
        df.loc[df["rr"].fillna(0) < 1.2, "exclude_reason"] += "RR不足;"
        df.loc[_safe_series(df, "rsi", default=50).fillna(50) > 78, "exclude_reason"] += "RSI過熱;"
        if "hard_avoid_reason" in df.columns:
            mask = _safe_str_series(df, "hard_avoid_reason").ne("")
            df.loc[mask, "exclude_reason"] += _safe_str_series(df, "hard_avoid_reason")[mask] + ";"
        elif "swap_reason" in df.columns:
            mask = _safe_str_series(df, "swap_reason").ne("")
            df.loc[mask, "exclude_reason"] += _safe_str_series(df, "swap_reason")[mask] + ";"
        if "wave_correction_flag" in df.columns:
            df.loc[_safe_bool_series(df, "wave_correction_flag"), "exclude_reason"] += "MACD+KD波段修正;"
        df["exclude_flag"] = df["exclude_reason"].ne("")
        df["是否可下單"] = "NO"
        yes = (_safe_series(df, "teacher_score", default=0) >= 55) & (df["rr"].fillna(0) >= 1.5) & (~df["exclude_flag"]) & (close <= df["entry_high"] * 1.03)
        wait = (_safe_series(df, "teacher_score", default=0) >= 55) & (df["rr"].fillna(0) >= 1.5) & (~df["exclude_flag"]) & (~yes)
        df.loc[yes, "是否可下單"] = "YES"
        df.loc[wait, "是否可下單"] = "WAIT"
        df["操作策略"] = df["是否可下單"].map({"YES":"低接區內可分批", "WAIT":"等待回到低接區或RR改善", "NO":"不符合下單條件"}).fillna("觀察")
        df.loc[_safe_str_series(df, "k_warning_type").ne(""), "操作策略"] = "K線警訊，壓力先賣/等待2日確認"
        df.loc[_safe_bool_series(df, "wave_correction_flag"), "操作策略"] = "波段修正確認，停止追高並降檔"
        df.loc[_safe_bool_series(df, "avoid_flag"), "操作策略"] = "硬性風險或未釋放壓力，反彈換股"
        df.loc[_safe_str_series(df, "avoid_level").eq("SOFT_RELEASED"), "操作策略"] = "低位階翻多覆蓋軟性壓力，可列LOW_BUY觀察"
        return df

class InstitutionalReportEngine:
    def __init__(self, db_path: str, logger: Macro16Logger):
        self.repo = DBRepository(db_path, logger)
        self.logger = logger

    def run(self):
        if pd is None:
            self.logger.warning("pandas未安裝，無法產出機構級股票投資規劃報表")
            return None
        df = None
        trade_date = ""
        try:
            with StageTrace(self.logger, "get_trade_date"):
                trade_date = self.repo.get_trade_date()
            with StageTrace(self.logger, "load_base_universe", lambda: df):
                df = self.repo.load_base_universe(trade_date)
            stages = [
                ("FeatureBuilder", lambda x: FeatureBuilder().build(x)),
                ("MarketRegimeEngine", lambda x: MarketRegimeEngine(self.logger).apply(x)),
                ("SakataRiskPatternEngine", lambda x: SakataRiskPatternEngine(self.logger).apply(x)),
                ("CoreLeaderEngine", lambda x: CoreLeaderEngine(self.logger).apply(x)),
                ("DualModelScoringEngine", lambda x: DualModelScoringEngine().score(x)),
                ("ReportClassifier", lambda x: ReportClassifier().classify(x)),
                ("LowBaseReversalEngine", lambda x: LowBaseReversalEngine(self.logger).apply(x)),
                ("AvoidSwapEngine", lambda x: AvoidSwapEngine(self.logger).apply(x)),
                ("TradePlanEngine", lambda x: TradePlanEngine().build(x)),
                ("TeacherDecisionEngine", lambda x: TeacherDecisionEngine(self.logger).apply(x)),
            ]
            for stage_name, fn in stages:
                with StageTrace(self.logger, stage_name, lambda df=df: df):
                    df = fn(df)
            with StageTrace(self.logger, "finalize_report_name", lambda: df):
                df["report_name"] = df.get("stock_name", df.get("stock_name_master", df.get("name", "")))
            result = {
                "trade_date": trade_date,
                "db_path": self.repo.db_path,
                "counts": {t: self.repo.table_count(t) for t in ["ranking_result", "market_snapshot", "price_history", "external_revenue", "external_valuation", "external_institutional", "external_margin", "trade_plan"]},
                "latest_dates": {t: self.repo.latest_date(t) for t in ["ranking_result", "market_snapshot", "price_history", "external_revenue", "external_valuation", "external_institutional", "external_margin", "trade_plan"]},
                "all": df
            }
            self.logger.info(f"INSTITUTIONAL_REPORT_READY date={trade_date} rows={len(df)}")
            return result
        except Exception as exc:
            payload = {"db_path": self.repo.db_path, "trade_date": trade_date, "error": str(exc), "error_type": type(exc).__name__}
            if df is not None:
                try:
                    payload["rows"] = int(len(df))
                    payload["columns"] = list(map(str, list(df.columns)[:160]))
                    payload["dtypes"] = {str(k): str(v) for k, v in df.dtypes.astype(str).head(80).items()}
                except Exception as meta_exc:
                    payload["meta_error"] = str(meta_exc)
            self.logger.strategy_trace("TEACHER_REPORT_FAIL_DETAIL", payload)
            raise

class ReportValidator:
    def validate_workbook(self, wb) -> List[str]:
        errors = []
        for name in EXPECTED_INSTITUTIONAL_SHEETS:
            if name not in wb.sheetnames:
                errors.append(f"缺少Sheet:{name}")
        # 驗證03~08欄位
        for name in EXPECTED_INSTITUTIONAL_SHEETS[3:9]:
            if name in wb.sheetnames:
                ws = wb[name]
                cols = [ws.cell(1, i).value for i in range(1, len(REPORT_COLUMNS)+1)]
                if cols != REPORT_COLUMNS:
                    errors.append(f"{name}欄位不一致")
        return errors


class TeacherFullReportBuilder:
    """Phase5：集中建立老師策略完整報表資料集，避免Writer層同時負責資料切分與寫表。"""
    def __init__(self, logger: Optional[Macro16Logger] = None):
        self.logger = logger

    def build(self, df):
        if df is None or df.empty:
            empty = pd.DataFrame() if pd is not None else None
            return {"final_top15": empty, "growth_top30": empty, "value_top30": empty, "low_base": empty, "watchlist": empty, "excluded": empty, "low_buy": empty, "watch": empty, "avoid": empty, "reduce": empty, "release": empty}
        teacher_decision = df.get("teacher_decision", pd.Series("WATCH", index=df.index)).astype(str)
        exclude_flag = df.get("exclude_flag", pd.Series(False, index=df.index)).fillna(False)
        score_cols = ["teacher_score", "growth_score", "value_score"]
        for col in score_cols:
            if col not in df.columns:
                df[col] = 0
        execution_status = df.get("teacher_execution_status", pd.Series("NO", index=df.index)).astype(str)
        final = df[(teacher_decision.isin(["BUY", "LOW_BUY", "WATCH"])) & (~teacher_decision.eq("AVOID"))].sort_values(score_cols, ascending=False).head(15)
        if final.empty:
            final = df[~teacher_decision.eq("AVOID")].sort_values(score_cols, ascending=False).head(15)
        release_or_watch = df[teacher_decision.isin(["BUY", "LOW_BUY"]) | execution_status.eq("WAIT") | df.get("avoid_level", pd.Series("", index=df.index)).astype(str).eq("SOFT_RELEASED")].sort_values(["teacher_score", "rr"], ascending=False)
        datasets = {
            "final_top15": final,
            "growth_top30": df.sort_values("growth_score", ascending=False).head(30),
            "value_top30": df.sort_values("value_score", ascending=False).head(30),
            "low_base": df[df.get("low_base_reversal_flag", pd.Series(False, index=df.index)).fillna(False)].sort_values("teacher_score", ascending=False).head(50),
            "watchlist": df[df["stock_id"].astype(str).str.zfill(4).isin(TEACHER_WATCHLIST)].sort_values("teacher_score", ascending=False),
            "excluded": df[df.get("exclude_flag", pd.Series(False, index=df.index)).fillna(False) | teacher_decision.eq("AVOID")].sort_values("teacher_score", ascending=False),
            "low_buy": df[teacher_decision.eq("LOW_BUY")].sort_values(["teacher_score", "rr"], ascending=False),
            "watch": df[teacher_decision.eq("WATCH")].sort_values(["teacher_score", "rr"], ascending=False),
            "avoid": df[teacher_decision.eq("AVOID")].sort_values(["teacher_score", "rr"], ascending=False),
            "reduce": df[teacher_decision.eq("REDUCE")].sort_values(["teacher_score", "rr"], ascending=False),
            "release": release_or_watch,
        }
        if self.logger:
            self.logger.strategy_trace("TEACHER_FULL_REPORT_BUILDER", {k: int(len(v)) for k, v in datasets.items() if v is not None})
        return datasets

class InstitutionalExcelWriter:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger


    def _write_teacher_failure_diagnostic(self, wb, error_info: Optional[Dict[str, Any]], expected_sheets: Optional[List[str]] = None):
        ws = self._sheet(wb, "99_老師策略失敗診斷")
        ws.append(["項目", "內容"])
        ws.append(["狀態", "老師策略報表未產出，已進入失敗診斷頁"])
        ws.append(["錯誤摘要", json.dumps(error_info or {}, ensure_ascii=False, default=str)[:3000]])
        ws.append(["必須存在Sheet", ",".join(expected_sheets or EXPECTED_INSTITUTIONAL_SHEETS)])
        ws.append(["修正方向", "先修 safe_series / Engine Stage Trace / 欄位型別，再重跑 InstitutionalReportEngine"])
        ws.append([])
        ws.append(["驗收規則", "成功時Log必須出現 TEACHER_REPORT_READY；失敗時必須出現 TEACHER_REPORT_FAIL_DETAIL，且本Sheet不得空白。"])
        self.logger.warning("TEACHER_REPORT_VALIDATE_FAIL diagnostic_sheet=99_老師策略失敗診斷")

    def _sheet(self, wb, name):
        if name in wb.sheetnames:
            ws = wb[name]
            ws.delete_rows(1, ws.max_row)
        else:
            ws = wb.create_sheet(name)
        return ws

    def _report_rows(self, df, topn=None):
        if df is None or df.empty:
            return []
        if topn:
            df = df.head(topn)
        rows=[]
        for i, (_, r) in enumerate(df.iterrows(), 1):
            name = r.get("stock_name") or r.get("stock_name_master") or r.get("report_name") or ""
            rows.append([
                i, str(r.get("stock_id", "")).zfill(4), name, r.get("market", ""), r.get("industry", r.get("industry_master", "")), r.get("theme", r.get("sub_theme", "")), r.get("老師分類", ""),
                r.get("teacher_decision", "WATCH"), r.get("teacher_decision_reason", ""), r.get("decision_trace", ""), r.get("teacher_pool_type", ""), r.get("core_leader_state", ""),
                r.get("k_warning_type", ""), r.get("market_pullback_type", ""), r.get("swap_reason", ""), round(float(r.get("close", 0) or 0),2),
                f"{round(float(r.get('entry_low',0) or 0),2)}~{round(float(r.get('entry_high',0) or 0),2)}", round(float(r.get("stop_loss",0) or 0),2), round(float(r.get("target_1",0) or 0),2), round(float(r.get("target_2",0) or 0),2), round(float(r.get("rr",0) or 0),2), r.get("是否可下單", "NO"), r.get("teacher_execution_status", r.get("是否可下單", "NO")),
                round(float(r.get("teacher_score",0) or 0),2), round(float(r.get("growth_score",0) or 0),2), round(float(r.get("value_score",0) or 0),2), round(float(r.get("eps_ttm", r.get("valuation_eps_ttm",0)) or 0),2), round(float(r.get("pe",0) or 0),2), round(float(r.get("dividend_yield",0) or 0),2),
                round(float(r.get("yoy", r.get("revenue_yoy",0)) or 0),2), round(float(r.get("institutional_score",0) or 0),2), round(float(r.get("pct_20d",0) or 0),2), round(float(r.get("low_base_score",0) or 0),2), round(float(r.get("kline_score",0) or 0),2),
                round(float(r.get("ma_support_score",0) or 0),2), round(float(r.get("volume_health_score",0) or 0),2), round(float(r.get("revenue_eps_score",0) or 0),2), r.get("操作策略", ""), r.get("exclude_reason", ""),
                "Y" if bool(r.get("low_base_reversal_flag", False)) else "", r.get("decision_release_reason", ""), r.get("hard_avoid_reason", ""), r.get("soft_avoid_reason", ""), r.get("pressure_line_state", ""),
                r.get("phase5_major_wave", ""), r.get("phase5_minor_wave", ""), r.get("phase5_correction_type", ""), r.get("phase5_fibo_retrace", ""), r.get("phase5_rebound_type", ""),
                "Y" if bool(r.get("phase5_correction_completed", False)) else "N", "Y" if bool(r.get("phase5_escape_rally", False)) else "N", "Y" if bool(r.get("phase5_impulsive_wave", False)) else "N",
                r.get("phase5_wave_phase", ""), r.get("phase5_breakout_stage", ""), r.get("phase5_impulse_stage", ""), round(float(r.get("phase5_position_score", 0) or 0), 2),
                r.get("phase5_candidate_pool", ""), r.get("phase5_block_reason", ""),
                r.get("decision_buy_count", ""), r.get("decision_avoid_count", ""), r.get("latest_downgrade_reason", r.get("teacher_execution_reason", ""))
            ])
        return rows

    def write_into_workbook(self, wb, report: Optional[Dict[str, Any]], gov_result: Optional[Dict[str, Any]] = None, market5_result: Optional[Dict[str, Any]] = None):
        if report is None:
            return
        df = report["all"]
        # 00
        ws = self._sheet(wb, "00_執行摘要")
        ws.append(["項目", "內容"])
        ws.append(["報告日期", report.get("trade_date")])
        ws.append(["DB路徑", report.get("db_path")])
        ws.append(["總股票數", len(df)])
        ws.append(["策略版本", STRATEGY_VERSION])
        ws.append(["YES", int((df["是否可下單"]=="YES").sum())])
        ws.append(["WAIT", int((df["是否可下單"]=="WAIT").sum())])
        ws.append(["NO", int((df["是否可下單"]=="NO").sum())])
        ws.append(["老師執行狀態YES", int((df.get("teacher_execution_status", pd.Series(index=df.index)).astype(str)=="YES").sum())])
        ws.append(["老師執行狀態WAIT", int((df.get("teacher_execution_status", pd.Series(index=df.index)).astype(str)=="WAIT").sum())])
        ws.append(["老師執行狀態NO", int((df.get("teacher_execution_status", pd.Series(index=df.index)).astype(str)=="NO").sum())])
        gov_source = str((gov_result or {}).get("source", ""))
        if "TEJ" in gov_source:
            gov_level = "正式"
        elif gov_result and (gov_result or {}).get("gov_net_100m") is not None:
            gov_level = "備援"
        else:
            gov_level = "缺資料/佐證"
        ws.append(["官股資料等級", gov_level])
        ws.append([])
        ws.append(["老師五態決策統計", "數量"])
        for decision in ["BUY", "LOW_BUY", "WATCH", "REDUCE", "AVOID"]:
            ws.append([decision, int((df.get("teacher_decision", pd.Series(index=df.index)).astype(str)==decision).sum())])
        ws.append([])
        ws.append(["策略原因統計", "數量"])
        reason_map = {
            "兩高不過": df.get("swap_reason", pd.Series("", index=df.index)).astype(str).str.contains("兩高不過", na=False),
            "下降軌道": df.get("swap_reason", pd.Series("", index=df.index)).astype(str).str.contains("下降軌道", na=False),
            "頸線/前高壓力": df.get("swap_reason", pd.Series("", index=df.index)).astype(str).str.contains("頸線", na=False),
            "流星": df.get("k_warning_type", pd.Series("", index=df.index)).astype(str).str.contains("流星", na=False),
            "空頭新星十字": df.get("k_warning_type", pd.Series("", index=df.index)).astype(str).str.contains("空頭新星十字", na=False),
            "墓碑線": df.get("k_warning_type", pd.Series("", index=df.index)).astype(str).str.contains("墓碑", na=False),
            "長黑K": df.get("k_warning_type", pd.Series("", index=df.index)).astype(str).str.contains("長黑", na=False),
            "KD死叉": df.get("kd_dead_cross_below_80", pd.Series(False, index=df.index)).fillna(False),
            "MACD翻負": df.get("macd_turn_negative", pd.Series(False, index=df.index)).fillna(False),
            "乖離過大": df.get("deviation_risk_flag", pd.Series(False, index=df.index)).fillna(False),
            "波段修正": df.get("wave_correction_flag", pd.Series(False, index=df.index)).fillna(False),
            "RR不足": pd.to_numeric(df.get("rr", pd.Series(index=df.index)), errors="coerce").fillna(0) < 1.5,
        }
        for reason, mask in reason_map.items():
            ws.append([reason, int(mask.sum())])
        ws.append([])
        ws.append(["TEJ官股", json.dumps(gov_result or {}, ensure_ascii=False)[:800]])
        ws.append(["跨月5日", json.dumps(market5_result or {}, ensure_ascii=False)[:800]])
        # 01
        ws = self._sheet(wb, "01_DB資料盤點")
        ws.append(["資料表", "筆數", "最新日期", "用途"])
        usages = {"ranking_result":"模型排行主資料", "market_snapshot":"價格/RSI/ATR/MA", "price_history":"日K/20日漲幅/120日位階", "external_revenue":"營收YoY", "external_valuation":"PE/EPS/殖利率", "external_institutional":"法人分", "external_margin":"融資風險", "trade_plan":"既有交易計畫參考"}
        for t,n in report.get("counts",{}).items():
            ws.append([t,n,report.get("latest_dates",{}).get(t,""),usages.get(t,"")])
        # 02
        ws = self._sheet(wb, "02_模型設計")
        ws.append(["模型", "權重/規則", "說明"])
        ws.append(["老師總分", "低位階20% + 阪田K線20% + 均線15% + 量能10% + 營收EPS15% + 殖利率10% + 題材10%", "依Word/Excel策略SOP落實"])
        ws.append(["核心股風向", "CoreLeaderEngine：2330突破續強/回測不破/假突破/核心轉弱", "台積電為老師方法的大盤風向核心"])
        ws.append(["波段修正", "MarketRegimeEngine：MACD翻負 + KD破80死叉", "雙條件成立才視為波段修正，不因單一K棒全面轉空"])
        ws.append(["阪田警訊", "SakataRiskPatternEngine：流星/空頭新星十字/墓碑線/長黑K", "觸發後壓力先賣或等待2日確認"])
        ws.append(["排除換股", "AvoidSwapEngine：兩高不過/下降軌道/頸線壓力未突破/乖離過大", "符合者不得列主攻"])
        ws.append(["老師決策", "BUY / LOW_BUY / WATCH / REDUCE / AVOID", "保留是否可下單YES/WAIT/NO，但新增老師五態決策"])
        ws.append(["Phase5語義一致性", "大波/小波/回撤/反彈/逃命/推動浪/候選池", "主跌弱反彈不得混入主升池；BUY需通過Phase5與進場區Gate"])
        # Phase5 datasets：由TeacherFullReportBuilder集中切分，Writer只負責寫表。
        datasets = TeacherFullReportBuilder(self.logger).build(df)
        report_sheet_map = [
            ("03_最終TOP15", datasets["final_top15"], 15),
            ("04_成長模型TOP30", datasets["growth_top30"], 30),
            ("05_價值模型TOP30", datasets["value_top30"], 30),
            ("06_低位階候選", datasets["low_base"], 50),
            ("07_老師點名股檢核", datasets["watchlist"], None),
            ("08_排除與風險", datasets["excluded"], 200),
            ("13_LOW_BUY候選", datasets["low_buy"], 100),
            ("14_WATCH觀察池", datasets["watch"], 200),
            ("15_AVOID排除清單", datasets["avoid"], 300),
            ("16_放行與觀察候選", datasets["release"], 200),
        ]
        for sheet, data, topn in report_sheet_map:
            ws = self._sheet(wb, sheet)
            ws.append(REPORT_COLUMNS)
            for row in self._report_rows(data, topn):
                ws.append(row)
        ws = self._sheet(wb, "09_來源與限制")
        ws.append(["項目", "內容"])
        ws.append(["資料來源", "stock_system DB + TEJ八大公股行庫（若提供）+ TWSE/TPEX/TAIFEX宏觀來源"])
        ws.append(["官股/八大公股", "TEJ或Wantgoo等來源只作證據追溯；只要gov_net_100m解析正確，分數只依數值，不因來源不同扣分"])
        ws.append(["Macro16跨月5日", "本版已提供Market5DayEngine；不足5日標P0_FAIL，停止市場技術判斷"])
        ws.append(["老師策略P0", "已新增CoreLeaderEngine、MarketRegimeEngine、SakataRiskPatternEngine、AvoidSwapEngine與TeacherDecisionEngine"])
        ws.append(["格式驗收", "00~16固定；03~08與13~16共用Phase5欄位；10/11/12為驗收、修改追蹤與命中驗證"])
        ws = self._sheet(wb, "10_老師策略驗收")
        ws.append(["TC", "驗收情境", "程式欄位", "預期結果"] )
        ws.append(["TC01", "流星但MACD/KD未雙翻空", "k_warning_type + wave_correction_flag", "REDUCE或WATCH，不全面轉空"] )
        ws.append(["TC02", "MACD翻負且KD破80死叉", "wave_correction_flag", "波段修正，停止追高"] )
        ws.append(["TC03", "2330假突破/回測失敗", "core_leader_state", "市場風險升級"] )
        ws.append(["TC04", "個股兩高不過或下降軌道", "avoid_flag/swap_reason", "AVOID/REDUCE，不列主攻"] )
        ws.append(["TC05", "低位階翻多且RR足夠", "low_base_reversal_flag/teacher_decision", "LOW_BUY或BUY"] )
        ws.append(["TC06", "軟性頸線壓力但低位階翻多", "avoid_level/decision_release_reason", "SOFT_RELEASED且可進LOW_BUY觀察"] )
        ws.append(["TC07", "硬性風險成立", "hard_avoid_flag", "AVOID，不可被LowBase覆蓋"] )
        ws.append([])
        ws.append(["驗收項目", "實際結果", "Pass/Fail", "命中筆數/說明"])
        validation_rows = [
            ("五態決策欄位", "teacher_decision" in df.columns, "PASS" if "teacher_decision" in df.columns else "FAIL", int(df.get("teacher_decision", pd.Series(index=df.index)).notna().sum()) if "teacher_decision" in df.columns else 0),
            ("決策原因欄位", "teacher_decision_reason" in df.columns, "PASS" if "teacher_decision_reason" in df.columns else "FAIL", int(df.get("teacher_decision_reason", pd.Series(index=df.index)).astype(str).ne("").sum()) if "teacher_decision_reason" in df.columns else 0),
            ("決策追蹤欄位", "decision_trace" in df.columns, "PASS" if "decision_trace" in df.columns else "FAIL", int(df.get("decision_trace", pd.Series(index=df.index)).astype(str).ne("").sum()) if "decision_trace" in df.columns else 0),
            ("CoreLeader狀態", "core_leader_state" in df.columns, "PASS" if "core_leader_state" in df.columns else "FAIL", str(df.get("core_leader_state", pd.Series(["NE"])).iloc[0]) if len(df) else ""),
            ("阪田警訊", "k_warning_type" in df.columns, "PASS" if "k_warning_type" in df.columns else "FAIL", int(df.get("k_warning_type", pd.Series("", index=df.index)).astype(str).ne("").sum()) if "k_warning_type" in df.columns else 0),
            ("避開換股", "swap_reason" in df.columns, "PASS" if "swap_reason" in df.columns else "FAIL", int(df.get("swap_reason", pd.Series("", index=df.index)).astype(str).ne("").sum()) if "swap_reason" in df.columns else 0),
            ("波段修正", "wave_correction_flag" in df.columns, "WARN" if ("wave_correction_flag" in df.columns and int(df.get("wave_correction_flag", pd.Series(False, index=df.index)).fillna(False).sum()) == 0) else ("PASS" if "wave_correction_flag" in df.columns else "FAIL"), int(df.get("wave_correction_flag", pd.Series(False, index=df.index)).fillna(False).sum()) if "wave_correction_flag" in df.columns else 0),
            ("低位階翻多", "low_base_reversal_flag" in df.columns, "PASS" if "low_base_reversal_flag" in df.columns else "FAIL", int(df.get("low_base_reversal_flag", pd.Series(False, index=df.index)).fillna(False).sum()) if "low_base_reversal_flag" in df.columns else 0),
            ("Hard/Soft Avoid", "hard_avoid_flag" in df.columns and "soft_avoid_flag" in df.columns, "PASS" if "hard_avoid_flag" in df.columns and "soft_avoid_flag" in df.columns else "FAIL", f"hard={int(df.get('hard_avoid_flag', pd.Series(False,index=df.index)).fillna(False).sum()) if 'hard_avoid_flag' in df.columns else 0};soft={int(df.get('soft_avoid_flag', pd.Series(False,index=df.index)).fillna(False).sum()) if 'soft_avoid_flag' in df.columns else 0}"),
            ("Phase5候選池", "phase5_candidate_pool" in df.columns, "PASS" if "phase5_candidate_pool" in df.columns else "FAIL", int(df.get("phase5_candidate_pool", pd.Series("", index=df.index)).astype(str).ne("").sum()) if "phase5_candidate_pool" in df.columns else 0),
            ("Phase5逃命反彈", "phase5_escape_rally" in df.columns, "PASS" if "phase5_escape_rally" in df.columns else "FAIL", int(df.get("phase5_escape_rally", pd.Series(False, index=df.index)).fillna(False).sum()) if "phase5_escape_rally" in df.columns else 0),
            ("Phase5推動浪", "phase5_impulsive_wave" in df.columns, "PASS" if "phase5_impulsive_wave" in df.columns else "FAIL", int(df.get("phase5_impulsive_wave", pd.Series(False, index=df.index)).fillna(False).sum()) if "phase5_impulsive_wave" in df.columns else 0),
        ]
        for row in validation_rows:
            ws.append(list(row))
        ws = self._sheet(wb, "11_修改追蹤")
        ws.append(["修改ID", "修改項目", "狀態", "對應類別/函式", "說明"] )
        ws.append(["P0-01", "CoreLeaderEngine", "已修改", "CoreLeaderEngine", "新增2330核心股風向判斷"] )
        ws.append(["P0-02", "MACD+KD波段修正", "已修改", "MarketRegimeEngine", "新增wave_correction_flag與market_pullback_type"] )
        ws.append(["P0-03", "阪田K線警訊", "已修改", "SakataRiskPatternEngine", "新增流星/空頭新星十字/墓碑/長黑K"] )
        ws.append(["P0-04", "兩高不過/下降軌道排除", "已修改", "AvoidSwapEngine", "新增avoid_flag/swap_reason"] )
        ws.append(["P0-05", "老師五態決策", "已修改", "TeacherDecisionEngine", "新增BUY/LOW_BUY/WATCH/REDUCE/AVOID"] )
        ws.append(["P0-06", "五態Summary統計", "已修改", "InstitutionalExcelWriter.write_into_workbook", "00_執行摘要新增BUY/LOW_BUY/WATCH/REDUCE/AVOID數量"] )
        ws.append(["P0-07", "策略原因統計", "已修改", "InstitutionalExcelWriter.write_into_workbook", "00_執行摘要新增兩高不過/流星/KD死叉/MACD翻負/乖離過大等統計"] )
        ws.append(["P0-08", "Decision Trace", "已修改", "TeacherDecisionEngine + Logger", "新增teacher_decision_reason與decision_trace欄位並輸出DECISION_TRACE log"] )
        ws.append(["P0-09", "策略版本凍結", "已修改", "VERSION/STRATEGY_VERSION", "新增STRATEGY_VERSION並寫入log與00摘要"] )
        ws.append(["P5-01", "報表輸出語意", "已修改", "ExcelWriter.write / InstitutionalExcelWriter", "新增TEACHER_REPORT_READY與00_12/00_16語意，移除00_09誤解"] )
        ws.append(["P5-02", "TeacherFullReportBuilder", "已修改", "TeacherFullReportBuilder", "集中建立TOP15/LOW_BUY/WATCH/AVOID/REDUCE/放行原因資料集"] )
        ws.append(["P5-03", "LowBaseReversalEngine", "已修改", "LowBaseReversalEngine", "新增low_base_reversal_flag與LOW_BASE_REVERSAL_SUMMARY"] )
        ws.append(["P5-04", "Avoid hard/soft分層", "已修改", "AvoidSwapEngine", "soft avoid可被低位階翻多覆蓋，hard avoid不可覆蓋"] )
        ws.append(["P5-05", "BUY/LOW_BUY重平衡", "已修改", "TeacherDecisionEngine", "先擋硬風險，再依LowBase/RR/Score/Core決策"] )
        ws.append(["P5-06", "新增13~16分池Sheet", "已修改", "InstitutionalExcelWriter", "新增LOW_BUY候選/WATCH觀察/AVOID排除/放行與觀察候選"] )
        ws.append(["P5-07", "Strategy Validation", "已修改", "12_策略命中驗證", "保留待追蹤；新增樣本分布與後續計算接口"] )
        ws.append(["P5-08", "GUI/CLI語意", "已修改", "run_gui / argparse", "新增macro_teacher/teacher_full模式並更新說明"] )
        ws.append(["P5-09", "報表缺失防呆", "已修改", "ReportValidator/ExcelWriter", "輸出後檢查00~16，缺失寫TEACHER_REPORT_VALIDATE_FAIL"] )
        ws.append(["P5-10", "成熟度標記", "已修改", "VERSION/STRATEGY_VERSION", "升級為2.7.3-teacher-phase5-consistency-fix"] )
        ws.append(["P7-01", "Phase5語義一致性引擎", "已修改", "TeacherPhase5SemanticEngine", "新增大波/小波/回撤/反彈/逃命/推動浪與候選池欄位"] )
        ws.append(["P7-02", "Phase5候選池Gate", "已修改", "TeacherDecisionEngine", "主跌修正浪/逃命反彈不得進BUY/YES；只進高風險反彈池或禁追風控池"] )
        ws.append(["P7-03", "動態決策欄位", "已修改", "REPORT_COLUMNS/_report_rows", "新增期間BUY/AVOID次數與最後降級原因欄位，供盤中log差異追蹤"] )
        ws.append(["P8-01", "Position Engine欄位", "已修改", "TeacherPhase5SemanticEngine/REPORT_COLUMNS/_report_rows", "新增wave_phase、breakout_stage、impulse_stage、position_score，修正彩晶類PreBreakout position_score=0問題"] )
        ws.append(["P6-01", "YES/WAIT/NO語義同步", "已修改", "TeacherDecisionEngine", "以老師五態重新同步是否可下單與teacher_execution_status，避免YES與BUY/LOW_BUY打架"] )
        ws.append(["P6-02", "16表名稱修正", "已修改", "EXPECTED_INSTITUTIONAL_SHEETS/Writer", "16_BUY_LOW_BUY放行原因改為16_放行與觀察候選，避免內含WATCH時語義錯誤"] )
        ws.append(["P6-03", "波段修正零樣本WARN", "已修改", "MarketRegimeEngine/10_老師策略驗收", "wave_correction_count=0改列WARN並寫入log，不再誤判為完全PASS"] )
        ws.append(["P6-04", "官股資料等級", "已修改", "00_執行摘要", "新增正式/備援/缺資料等級，TEJ未提供時不偽裝正式"] )
        ws.append(["P0-10", "策略命中驗證", "已修改", "12_策略命中驗證", "若有未來價格資料則計算，否則明確標示待追蹤"] )
        ws = self._sheet(wb, "12_策略命中驗證")
        ws.append(["項目", "內容"])
        ws.append(["策略版本", STRATEGY_VERSION])
        ws.append(["驗證目的", "追蹤老師策略五態決策的隔日/5日表現、最大回撤；若DB尚無未來價格，先標示待追蹤，不假造勝率"])
        ws.append(["目前資料狀態", "本報表依當前DB產出，若price_history尚未包含決策日後資料，命中率不可計算"])
        ws.append([])
        ws.append(["決策", "樣本數", "可計算樣本", "平均隔日%", "平均5日%", "最大回撤%", "狀態"])
        for decision in ["BUY", "LOW_BUY", "WATCH", "REDUCE", "AVOID"]:
            sample_count = int((df.get("teacher_decision", pd.Series(index=df.index)).astype(str)==decision).sum())
            ws.append([decision, sample_count, 0, "", "", "", "待後續price_history資料回補後驗證"])
        ready_payload = {
            "rows": int(len(df)),
            "sheets": [name for name in EXPECTED_INSTITUTIONAL_SHEETS if name in wb.sheetnames],
            "decision_counts": df.get("teacher_decision", pd.Series(index=df.index)).astype(str).value_counts(dropna=False).to_dict() if len(df) else {},
        }
        self.logger.strategy_trace("TEACHER_REPORT_READY", ready_payload)
        errors = ReportValidator().validate_workbook(wb)
        if errors:
            self.logger.warning("TEACHER_REPORT_VALIDATE_FAIL " + ";".join(errors))
            self.logger.warning("INSTITUTIONAL_REPORT_VALIDATE_FAIL " + ";".join(errors))
        else:
            self.logger.info("TEACHER_REPORT_VALIDATE_OK sheets=00_16_teacher_strategy_reports")
            self.logger.info("INSTITUTIONAL_REPORT_VALIDATE_OK")

class ExcelWriter:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger
        self.header_fill = PatternFill("solid", fgColor="DDEBF7")
        self.sub_fill = PatternFill("solid", fgColor="E2F0D9")
        self.warn_fill = PatternFill("solid", fgColor="FFF2CC")
        self.thin = Side(style="thin", color="D9E2F3")

    def write(self, template: Optional[str], out_path: str, market: MarketInput, scores: List[ModuleScore], tech: TechnicalRisk, summary: Dict[str, str], logs: List[str], raw: Optional[Dict[str, RawData]] = None, institutional_report: Optional[Dict[str, Any]] = None, gov_result: Optional[Dict[str, Any]] = None, market5_result: Optional[Dict[str, Any]] = None, report_mode: str = REPORT_MODE_MACRO, institutional_error: Optional[Dict[str, Any]] = None) -> str:
        if template and Path(template).exists():
            try:
                wb = load_workbook(template)
                self.logger.info(f"已載入Excel模板：{template}")
            except Exception as exc:
                self.logger.warning(f"模板載入失敗，改建新檔：{exc}")
                wb = Workbook()
        else:
            wb = Workbook()
            # 新檔第一個預設Sheet重新命名，避免產生Sheet頁。
            if wb.sheetnames == ["Sheet"]:
                wb["Sheet"].title = "市場輸入"
        report_mode = report_mode or REPORT_MODE_MACRO
        validator = MacroRefillValidator(self.logger)
        if report_mode in (REPORT_MODE_MACRO, REPORT_MODE_MACRO_TEACHER):
            # 正式日常模式：修正宏觀16，但不得關閉/刪除老師策略00~16報表。
            validator.ensure_macro_sheets(wb)
            self._write_market_input(wb, market)
            self._write_macro_modules(wb, scores)
            self._write_technical(wb, tech)
            if institutional_report is not None:
                InstitutionalExcelWriter(self.logger).write_into_workbook(wb, institutional_report, gov_result=gov_result, market5_result=market5_result)
                self.logger.info("MACRO_REFILL_TEACHER_OUTPUT_RESTORED sheets=00_16_teacher_strategy_reports")
            else:
                self.logger.warning("MACRO_REFILL_TOP_OUTPUT_SKIPPED reason=未提供DB或InstitutionalReportEngine失敗，無法產出TOP報表")
                self._write_teacher_failure_diagnostic(wb, institutional_error)
        elif report_mode == REPORT_MODE_MACRO_ONLY:
            # 單純驗證宏觀回填時才只保留三頁。
            validator.enforce_macro_only_sheets(wb)
            self._write_market_input(wb, market)
            self._write_macro_modules(wb, scores)
            self._write_technical(wb, tech)
        elif report_mode in (REPORT_MODE_INSTITUTIONAL, REPORT_MODE_TEACHER_FULL):
            # institutional_report/teacher_full模式只輸出00~16老師策略報表，不混入macro/debug頁。
            for name in list(wb.sheetnames):
                del wb[name]
            if institutional_report is not None:
                InstitutionalExcelWriter(self.logger).write_into_workbook(wb, institutional_report, gov_result=gov_result, market5_result=market5_result)
            else:
                ws = wb.create_sheet("00_執行摘要")
                ws.append(["項目", "內容"]); ws.append(["狀態", "未提供DB，無法產出institutional_report"])
                self._write_teacher_failure_diagnostic(wb, institutional_error)
        else:
            if institutional_report is not None:
                InstitutionalExcelWriter(self.logger).write_into_workbook(wb, institutional_report, gov_result=gov_result, market5_result=market5_result)
            self._write_market_input(wb, market)
            self._write_macro_modules(wb, scores)
            self._write_technical(wb, tech)
            self._write_audit(wb, market, scores, tech, summary, logs)
            self._write_data_source_status(wb, market, raw or {})
            self._write_evidence_index(wb, raw or {})
        self._format_all(wb)
        wb.save(out_path)
        if institutional_report is not None and report_mode in (REPORT_MODE_MACRO, REPORT_MODE_MACRO_TEACHER, REPORT_MODE_INSTITUTIONAL, REPORT_MODE_TEACHER_FULL, REPORT_MODE_ALL):
            try:
                check_wb = load_workbook(out_path, read_only=True)
                missing = [name for name in EXPECTED_INSTITUTIONAL_SHEETS if name not in check_wb.sheetnames]
                check_wb.close()
                if missing:
                    self.logger.warning("TEACHER_REPORT_VALIDATE_FAIL_AFTER_SAVE missing=" + ",".join(missing))
                else:
                    self.logger.info("TEACHER_REPORT_VALIDATE_OK_AFTER_SAVE sheets=00_16_teacher_strategy_reports")
            except Exception as exc:
                self.logger.warning(f"TEACHER_REPORT_VALIDATE_AFTER_SAVE_ERROR {exc}")
        self.logger.info(f"Excel已輸出：{out_path} report_mode={report_mode}")
        return out_path

    def _sheet(self, wb, name):
        if name in wb.sheetnames:
            ws = wb[name]
            ws.delete_rows(1, ws.max_row)
        else:
            ws = wb.create_sheet(name)
        return ws

    def _write_market_input(self, wb, market: MarketInput):
        ws = self._sheet(wb, "市場輸入")
        headers = ["日期", "收盤", "最高", "最低", "前高", "前低", "5MA", "成交量(億)", "5日均量(億)", "外資買賣超(億)", "官股買賣超(億)", "AI主流強度(0-1)", "重大事件(0/1)", "來源1", "來源2", "來源3", "來源4"]
        values = [market.base_date, market.close, market.high, market.low, market.prev_high, market.prev_low, market.ma5, market.turnover_100m, market.avg_turnover_5d_100m, market.foreign_net_100m, market.gov_net_100m, market.ai_strength, market.major_event, market.source_1, market.source_2, market.source_3, market.source_4]
        ws.append(headers)
        ws.append(values)
        ws.append([])
        ws.append(["欄位說明", "本表由主程式自動回填。若數值為空白/None，代表資料來源未取得，程式不編造數字，請依Log與回填紀錄修正資料來源或人工確認。"] + [None]*(len(headers)-2))
        ws.append(["交易判讀", MarketNarrativeBuilder().build(market)] + [None]*(len(headers)-2))

    def _write_macro_modules(self, wb, scores: List[ModuleScore]):
        # SOP V2.1 P0-02：正式Sheet必須是宏觀16模組，不得再建立宏觀15模組。
        ws = self._sheet(wb, "宏觀16模組")
        ws.append(["模組", "風險/強度分數(0-1)", "方向(+1/0/-1)", "加權分數", "說明", "資料來源", "資料時間"])
        for item in scores:
            weighted = round(float(item.strength or 0) * int(item.direction or 0), 2)
            # 以ScoringEngine輸出為主，若空值則強制回補，避免D欄空白。
            if item.weighted_score is not None:
                weighted = item.weighted_score
            ws.append([item.module, item.strength, item.direction, weighted, item.explanation, item.source, item.data_time])
        ws.append([])
        ws.append(["補充欄位", "狀態", "數據/事件", "交易用途"] )
        for item in scores:
            ws.append([item.module, item.status, item.data_text, item.trade_usage])

    def _write_technical(self, wb, tech: TechnicalRisk):
        ws = self._sheet(wb, "V2技術引擎")
        ws.append(["跌破5MA", "高不過高", "低破低", "放量", "重大事件", "夜盤偏空", "技術/風險分數", "大盤判定", "夜盤分數", "夜盤外資淨口數"])
        ws.append([tech.below_ma5, tech.lower_high, tech.lower_low, tech.volume_expansion, tech.major_event, getattr(tech, "night_bearish", 0), tech.risk_score, tech.market_judgement, getattr(tech, "night_score", None), getattr(tech, "night_net_lots", None)])
        ws.append(["判讀說明", "收盤<5MA為1", "最高<前高為1", "最低<前低為1", "成交值>5日均量*1.05為1", "合併Reuters/ISW/CNN/manual", "night_score<0為1", "六項加總", "供下單清單參考", "TAIFEX解析值", "TAIFEX外資淨多空口數"])

    def _write_audit(self, wb, market: MarketInput, scores: List[ModuleScore], tech: TechnicalRisk, summary: Dict[str,str], logs: List[str]):
        name = f"回填紀錄_{market.base_date.replace('-', '') if market.base_date else dt.date.today().strftime('%Y%m%d')}"
        ws = self._sheet(wb, name)
        ws.append(["回填項目", "數據/判斷", "方向", "分數", "資料日期", "來源URL/資料站", "回填邏輯", "交易用途"])
        for s in scores:
            ws.append([s.module, s.data_text, s.direction, s.weighted_score, s.data_time, s.source, s.explanation, s.trade_usage])
        start = 2
        ws.cell(start, 10, "總結項目")
        ws.cell(start, 11, "輸出")
        i = start + 1
        for k,v in summary.items():
            ws.cell(i,10,k); ws.cell(i,11,v); i += 1
        i += 1
        ws.cell(i,10,"程式Log摘要")
        for msg in logs[-30:]:
            i += 1
            ws.cell(i,10,msg[:180])

    def _write_data_source_status(self, wb, market: MarketInput, raw: Dict[str, RawData]):
        ws = self._sheet(wb, "資料來源狀態")
        ws.append(["資料源", "狀態", "資料分類", "解析狀態", "信心分數", "查詢日", "實際資料日", "是否回退", "回退天數", "來源", "URL", "RAW證據檔", "說明/訊息"])
        for name, item in raw.items():
            ws.append([
                name, item.status, item.data_status, getattr(item, "parse_status", ""), getattr(item, "confidence", ""),
                item.query_date, item.actual_date or item.date, item.is_fallback, item.fallback_days, item.source, item.url,
                getattr(item, "raw_file_path", ""), item.message or item.data_note
            ])
        ws.append([])
        ws.append(["說明", "query_date=使用者查詢日；actual_date=實際資料日；fallback_days=往前回退天數；RAW_DATA_SNAPSHOT 與 PARSED_VALUE 會寫入 log 證明實際抓取資料。"])


    def _write_evidence_index(self, wb, raw: Dict[str, RawData]):
        """寫入資料證據索引，修復V2.0缺少方法導致Excel輸出中斷。"""
        ws = self._sheet(wb, "資料證據索引")
        headers = [
            "項次", "模組/資料源", "狀態", "資料分類", "解析狀態", "parsed_fields摘要",
            "查詢日", "實際資料日", "是否回退", "回退天數", "來源", "URL",
            "RAW證據檔", "信心分數", "問題判定", "處理建議"
        ]
        ws.append(headers)
        for idx, (name, item) in enumerate(raw.items(), start=1):
            parsed_summary = ""
            issue = "OK"
            action = "可作為證據鏈追溯"
            raw_path = getattr(item, "raw_file_path", "") or ""
            try:
                if raw_path and Path(raw_path).exists():
                    payload = json.loads(Path(raw_path).read_text(encoding="utf-8"))
                    parsed_fields = payload.get("parsed_fields", {})
                    parsed_summary = json.dumps(parsed_fields, ensure_ascii=False)[:500]
                    if item.status == "OK" and not parsed_fields:
                        issue = "假OK風險：status=OK但parsed_fields為空"
                        action = "需補parser或降為WARN，不可進主分數"
                elif item.status == "OK" and getattr(item, "parse_status", "") != "PARSE_OK":
                    issue = "缺RAW證據或解析欄位"
                    action = "需確認write_raw_evidence與parser"
            except Exception as exc:
                issue = f"RAW讀取失敗：{exc}"
                action = "需檢查RAW證據檔路徑"
            if item.status != "OK":
                issue = item.message or item.data_status or item.status
                action = "依資料來源狀態修正來源或人工確認"
            ws.append([
                idx, name, item.status, item.data_status, getattr(item, "parse_status", ""), parsed_summary,
                item.query_date, item.actual_date or item.date, item.is_fallback, item.fallback_days, item.source, item.url,
                raw_path, getattr(item, "confidence", ""), issue, action
            ])
        ws.append([])
        ws.append(["驗收規則", "所有核心來源若status=OK，parse_status必須為PARSE_OK且parsed_fields不得為空；未解析資料必須WARN，不得假OK。"] )


    def _format_all(self, wb):
        for ws in wb.worksheets:
            for row in ws.iter_rows():
                for cell in row:
                    cell.alignment = Alignment(vertical="top", wrap_text=True)
                    cell.border = Border(top=self.thin, left=self.thin, right=self.thin, bottom=self.thin)
            for cell in ws[1]:
                cell.font = Font(bold=True)
                cell.fill = self.header_fill
            for col in range(1, ws.max_column + 1):
                width = 14
                for row in range(1, min(ws.max_row, 30) + 1):
                    val = ws.cell(row, col).value
                    if val:
                        width = max(width, min(48, len(str(val)) * 1.2))
                ws.column_dimensions[get_column_letter(col)].width = width
            ws.freeze_panes = "A2"

class WordEvidenceReportWriter:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger

    def write(self, out_path: str, raw: Dict[str, RawData], summary: Dict[str, str]) -> str:
        try:
            from docx import Document
        except Exception:
            self.logger.warning("python-docx未安裝，略過Word證據報告")
            return ""
        path = Path(out_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        doc = Document()
        doc.add_heading("Macro16 資料抓取證據報告", 0)
        doc.add_paragraph(f"Run ID：{self.logger.run_id}")
        doc.add_paragraph(f"產出時間：{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        doc.add_heading("總結", level=1)
        for k, v in summary.items():
            doc.add_paragraph(f"{k}：{v}")
        doc.add_heading("逐資料源證據", level=1)
        for name, item in raw.items():
            doc.add_heading(name, level=2)
            doc.add_paragraph(f"來源：{item.source}")
            doc.add_paragraph(f"URL：{item.url}")
            doc.add_paragraph(f"狀態：{item.status} / {item.data_status} / {getattr(item, 'parse_status', '')}")
            doc.add_paragraph(f"信心分數：{getattr(item, 'confidence', '')}")
            doc.add_paragraph(f"RAW證據檔：{getattr(item, 'raw_file_path', '')}")
            doc.add_paragraph(f"說明：{item.message or item.data_note}")
        doc.save(str(path))
        self.logger.info(f"WORD_EVIDENCE_REPORT path={path}")
        return str(path)


class AuditEngine:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger

    def check(self, market: MarketInput, scores: List[ModuleScore], tech: TechnicalRisk) -> List[str]:
        warnings = []
        if not market.base_date:
            warnings.append("市場輸入缺資料基準日")
        for field in ["close", "high", "low", "ma5"]:
            if getattr(market, field) is None:
                warnings.append(f"市場輸入缺{field}")
        for s in scores:
            if s.status != "OK":
                warnings.append(f"{s.module} 狀態={s.status}，需確認資料來源/解析結果")
            if s.direction not in (-1, 0, 1):
                warnings.append(f"{s.module} 方向不是+1/0/-1")
        if warnings:
            for w in warnings:
                self.logger.warning(f"QA: {w}")
        else:
            self.logger.info("QA檢查完成，未發現重大缺失")
        return warnings

class Macro16Engine:
    def __init__(self, log_dir: Path):
        self.logger = Macro16Logger(log_dir)
        self.client = HttpClient(self.logger)
        self.source = SourceConnector(self.client, self.logger)
        self.processor = DataProcessor(self.logger)
        self.scoring = ScoringEngine(self.logger)
        self.indicator = IndicatorEngine(self.logger)
        self.explain = ExplanationEngine()
        self.audit = AuditEngine(self.logger)
        self.writer = ExcelWriter(self.logger)
        self.word_evidence_writer = WordEvidenceReportWriter(self.logger)

    def run(self, template: Optional[str], out_path: str, base_date: Optional[str] = None, override: Optional[ManualOverride] = None, db_path: Optional[str] = None, strict_ranking: bool = False, tej_gov_file: Optional[str] = None, report_mode: str = REPORT_MODE_MACRO) -> Dict[str, Any]:
        self.logger.info(f"開始執行 {APP_NAME} v{VERSION}")
        self.logger.info("CHANGELOG v2.7.4: Position Engine fix - wave_phase, breakout_stage, impulsive_stage, position_score, 6116 PreBreakout trace, report fields")
        self.logger.strategy_trace("STRATEGY_VERSION", {"strategy_version": STRATEGY_VERSION, "program_version": VERSION})
        raw: Dict[str, RawData] = {}
        requested_date = base_date.replace("-", "") if base_date else None
        raw["台股指數"] = self.source.fetch_twse_taiex_history(requested_date)
        # V1.3：先用TWSE實際回傳的最新完整交易日作為後續台股資料基準日，避免使用未收盤日查詢成交值/外資。
        actual_twse_date = requested_date
        try:
            if raw["台股指數"].status == "OK" and raw["台股指數"].value:
                actual_twse_date = raw["台股指數"].value.get("last", {}).get("date", "").replace("-", "") or requested_date
                self.logger.info(f"資料基準日校正：requested={requested_date}, actual_twse_date={actual_twse_date}")
        except Exception as exc:
            self.logger.warning(f"資料基準日校正失敗：{exc}")
        raw["成交量"] = self.source.fetch_twse_turnover_month(actual_twse_date)
        raw["外資"] = self.source.fetch_foreign_investor(actual_twse_date)
        # V2.5 P0：跨月補齊最近5個有效交易日，產出前高/前低/5MA/5日均量。
        market5_result = Market5DayEngine(self.client, self.logger).build_market_features(actual_twse_date or requested_date or dt.date.today().strftime("%Y%m%d"))
        raw["市場5日資料"] = RawData("市場5日資料", market5_result, market5_result.get("base_date", ""), "TWSE跨月5日", "TWSE MI_5MINS_HIST/FMTQIK", self.source._today_str(), "OK" if market5_result.get("status") == "OK" else "FAIL", market5_result.get("message", ""), requested_date or "", market5_result.get("base_date", ""), False, 0, "OK" if market5_result.get("status") == "OK" else "DATA_MISSING", market5_result.get("message", ""), "PARSE_OK" if market5_result.get("status") == "OK" else "NO_PARSED_VALUE", confidence=1.0 if market5_result.get("status") == "OK" else 0.0)
        for module, symbol in YAHOO_SYMBOLS.items():
            raw[module] = self.source.fetch_yahoo_chart(symbol, module)
        for module, symbols in YAHOO_SYMBOL_CANDIDATES.items():
            raw[module] = self.source.fetch_yahoo_chart_candidates(symbols, module)
        raw["美債10Y"] = self.source.fetch_fred_csv_latest("DGS10", "美債10Y")
        raw["FED利率政策"] = self.source.fetch_fed_policy()
        raw["CPI"] = self.source.fetch_bls_cpi()
        raw["非農"] = self.source.fetch_bls_nfp()
        raw["戰爭/停火"] = self.source.fetch_reuters_war()
        raw["外交政策"] = self.source.fetch_bloomberg_policy()
        raw["戰爭/地緣"] = raw["戰爭/停火"] if raw["戰爭/停火"].status == "OK" else self.source.fetch_geopolitical_news()
        raw["ISW衝突分析"] = self.source.fetch_isw_conflict()
        raw["CNN重大新聞"] = self.source.fetch_cnn_major_news()
        raw["美國總統"] = self.source.fetch_trump_public_news()
        # V2.5.1 SOP：TEJ為主來源；TEJ缺檔時，Wantgoo/第三方備援必須嘗試解析gov_net_100m並標P0_WARN。
        gov_result = TEJGovBankEngine(tej_gov_file, self.logger).parse()
        raw["官股整理"] = self.source.fetch_wantgoo_public_bank()
        if gov_result.get("status") == "OK" and gov_result.get("gov_net_100m") is not None:
            raw["官股"] = RawData("官股", gov_result, gov_result.get("actual_date", ""), "TEJ八大公股行庫", tej_gov_file or "", self.source._today_str(), "OK", gov_result.get("message", ""), actual_twse_date or "", gov_result.get("actual_date", ""), False, 0, "OK", gov_result.get("message", ""), "PARSE_OK", confidence=0.95)
        elif raw.get("官股整理") and raw["官股整理"].parse_status == "PARSE_OK" and isinstance(raw["官股整理"].value, dict) and raw["官股整理"].value.get("gov_net_100m") is not None:
            fallback_value = dict(raw["官股整理"].value)
            fallback_value["status"] = "OK"
            fallback_value["message"] = "TEJ未提供，使用Wantgoo八大官股資料解析；來源保留供追溯，分數只依數值"
            raw["官股"] = RawData("官股", fallback_value, raw["官股整理"].date, "Wantgoo八大官股資料", raw["官股整理"].url, self.source._today_str(), "OK", fallback_value["message"], actual_twse_date or "", raw["官股整理"].date, False, 0, "OK", fallback_value["message"], "PARSE_OK", confidence=1.0)
            self.logger.info(f"GOV_DATA_ACCEPTED source=Wantgoo value={fallback_value.get('gov_net_100m')}")
        else:
            raw["官股"] = RawData("官股", gov_result, "", "TEJ八大公股行庫", tej_gov_file or "", self.source._today_str(), "WARN", gov_result.get("message", "TEJ未提供且Wantgoo備援未解析"), actual_twse_date or "", "", False, 0, "NO_PARSED_VALUE", gov_result.get("message", ""), "NO_PARSED_VALUE", confidence=0.0)
        raw["官股TWSE佐證"] = self.source.fetch_twse_broker_report(base_date=actual_twse_date)
        raw["AI產業"] = self.source.fetch_ai_industry_news()
        raw["IEK產業分析"] = self.source.fetch_iek_industry()
        raw["排行分析"] = self.source.fetch_ranking_result_db(db_path=db_path, strict=strict_ranking)
        raw["台股夜盤"] = self.source.fetch_taifex_night_snapshot()
        tpex_otc_raw = self.source.fetch_tpex_otc_snapshot()
        raw["櫃買官方來源"] = tpex_otc_raw
        if tpex_otc_raw.status == "OK" and isinstance(tpex_otc_raw.value, dict) and "close" in tpex_otc_raw.value:
            raw["OTC"] = tpex_otc_raw
        institutional_report = None
        institutional_report_error = None
        if db_path:
            try:
                institutional_report = InstitutionalReportEngine(db_path, self.logger).run()
            except Exception as exc:
                institutional_report_error = {"db_path": db_path, "error": str(exc), "error_type": type(exc).__name__}
                self.logger.warning(f"INSTITUTIONAL_REPORT_FAIL db_path={db_path} error={exc}")
                self.logger.strategy_trace("TEACHER_REPORT_FAIL_DETAIL", institutional_report_error)
        if override and override.event_note:
            raw["戰爭/地緣"] = self.source.build_manual_raw("戰爭/地緣", {"event_note": override.event_note, "major_event": 1}, override.event_note)
        if override and override.night_score is not None:
            raw["台股夜盤"] = self.source.build_manual_raw("台股夜盤", {"score": override.night_score}, "manual night score")
        # V1.3：完整記錄每個資料源狀態，方便後續增修與問題追蹤
        for k, v in raw.items():
            try:
                self.logger.debug(f"RAW_STATUS {k}: status={v.status}, query_date={v.query_date}, actual_date={v.actual_date or v.date}, is_fallback={v.is_fallback}, fallback_days={v.fallback_days}, data_status={v.data_status}, source={v.source}, url={v.url}, message={v.message}")
            except Exception:
                pass
        market = self.processor.build_market_input(raw, base_date or "")
        market = self.processor.apply_manual_override(market, override)
        completion_issues = FieldCompletionValidator(self.logger).validate_market_input(market, strict_gov=False)
        scores = self.scoring.score_all(raw, market)
        macro_total = round(sum(s.weighted_score for s in scores), 2)
        tech = self.indicator.compute(market, macro_total)
        summary = self.explain.build_summary(macro_total, tech)
        warnings = self.audit.check(market, scores, tech)
        warnings = (locals().get("completion_issues", []) or []) + warnings
        if warnings:
            summary["QA警告"] = "; ".join(warnings[:8])
        output = self.writer.write(template, out_path, market, scores, tech, summary, self.logger.messages, raw, institutional_report=institutional_report, gov_result=locals().get("gov_result"), market5_result=locals().get("market5_result"), report_mode=report_mode, institutional_error=institutional_report_error)
        evidence_word = ""
        if report_mode == REPORT_MODE_ALL:
            evidence_word = self.word_evidence_writer.write(str(Path(out_path).with_name(Path(out_path).stem + "_資料證據報告.docx")), raw, summary)
        self.logger.info("執行完成")
        return {"output": output, "evidence_word": evidence_word, "raw_dir": str(self.logger.raw_dir), "summary": summary, "warnings": warnings, "log_file": str(self.logger.log_file)}

def run_gui():
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title("宏觀16模組 自動回填主程式")
    root.geometry("1000x720")

    template_var = tk.StringVar()
    out_var = tk.StringVar(value=str(Path.cwd() / f"宏觀16模組_自動回填_{dt.date.today().strftime('%Y%m%d')}.xlsx"))
    date_var = tk.StringVar(value=dt.date.today().strftime("%Y-%m-%d"))
    db_var = tk.StringVar()
    tej_gov_var = tk.StringVar()
    strict_ranking_var = tk.BooleanVar(value=False)
    report_mode_var = tk.StringVar(value=REPORT_MODE_MACRO)
    status_var = tk.StringVar(value="待執行")

    def browse_template():
        p = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if p:
            template_var.set(p)

    def browse_out():
        p = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
        if p:
            out_var.set(p)

    def browse_db():
        p = filedialog.askopenfilename(filetypes=[("SQLite DB", "*.db"), ("All files", "*.*")])
        if p:
            db_var.set(p)

    def browse_tej_gov():
        p = filedialog.askopenfilename(filetypes=[("TEJ Excel", "*.xls *.xlsx"), ("All files", "*.*")])
        if p:
            tej_gov_var.set(p)

    frm = ttk.Frame(root, padding=12)
    frm.pack(fill="both", expand=True)
    ttk.Label(frm, text="宏觀16模組 自動抓取與Excel回填", font=("Microsoft JhengHei", 16, "bold")).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0,10))
    ttk.Label(frm, text="Excel模板").grid(row=1, column=0, sticky="w")
    ttk.Entry(frm, textvariable=template_var, width=90).grid(row=1, column=1, sticky="we")
    ttk.Button(frm, text="選擇", command=browse_template).grid(row=1, column=2)
    ttk.Label(frm, text="輸出檔案").grid(row=2, column=0, sticky="w")
    ttk.Entry(frm, textvariable=out_var, width=90).grid(row=2, column=1, sticky="we")
    ttk.Button(frm, text="另存", command=browse_out).grid(row=2, column=2)
    ttk.Label(frm, text="基準日(YYYY-MM-DD)").grid(row=3, column=0, sticky="w")
    ttk.Entry(frm, textvariable=date_var, width=20).grid(row=3, column=1, sticky="w")
    ttk.Label(frm, text="主DB檔案(選填)").grid(row=4, column=0, sticky="w")
    ttk.Entry(frm, textvariable=db_var, width=90).grid(row=4, column=1, sticky="we")
    ttk.Button(frm, text="選擇DB", command=browse_db).grid(row=4, column=2)
    ttk.Label(frm, text="TEJ八大官股檔(選填)").grid(row=5, column=0, sticky="w")
    ttk.Entry(frm, textvariable=tej_gov_var, width=90).grid(row=5, column=1, sticky="we")
    ttk.Button(frm, text="選擇TEJ", command=browse_tej_gov).grid(row=5, column=2)
    ttk.Checkbutton(frm, text="Ranking缺失時中止輸出", variable=strict_ranking_var).grid(row=6, column=1, sticky="w")
    ttk.Label(frm, text="輸出模式").grid(row=7, column=0, sticky="w")
    ttk.Combobox(frm, textvariable=report_mode_var, values=[REPORT_MODE_MACRO, REPORT_MODE_MACRO_TEACHER, REPORT_MODE_MACRO_ONLY, REPORT_MODE_INSTITUTIONAL, REPORT_MODE_TEACHER_FULL, REPORT_MODE_ALL], width=36, state="readonly").grid(row=7, column=1, sticky="w")
    ttk.Label(frm, textvariable=status_var, foreground="blue").grid(row=8, column=0, columnspan=3, sticky="w", pady=8)

    log_text = tk.Text(frm, height=26, wrap="word")
    log_text.grid(row=10, column=0, columnspan=3, sticky="nsew", pady=(10,0))
    frm.rowconfigure(10, weight=1)
    frm.columnconfigure(1, weight=1)

    def append_log(text):
        log_text.insert("end", text + "\n")
        log_text.see("end")
        root.update_idletasks()

    def execute():
        try:
            status_var.set("執行中：抓資料、計分、回填Excel...")
            log_text.delete("1.0", "end")
            engine = Macro16Engine(Path("logs"))
            result = engine.run(template_var.get() or None, out_var.get(), date_var.get(), db_path=(db_var.get() or None), strict_ranking=bool(strict_ranking_var.get()), tej_gov_file=(tej_gov_var.get() or None), report_mode=report_mode_var.get())
            for msg in engine.logger.messages:
                append_log(msg)
            append_log("\n總結：" + json.dumps(result["summary"], ensure_ascii=False, indent=2))
            append_log("Log檔：" + result["log_file"])
            status_var.set("完成")
            messagebox.showinfo("完成", f"已輸出：{result['output']}")
        except Exception as exc:
            status_var.set("失敗")
            append_log("ERROR " + str(exc))
            messagebox.showerror("錯誤", str(exc))

    ttk.Button(frm, text="執行回填", command=execute).grid(row=9, column=0, sticky="w", pady=6)
    ttk.Button(frm, text="離開", command=root.destroy).grid(row=9, column=2, sticky="e", pady=6)
    root.mainloop()

def main():
    parser = argparse.ArgumentParser(description="宏觀16模組自動抓取與Excel回填主程式")
    parser.add_argument("--cli", action="store_true", help="使用CLI模式")
    parser.add_argument("--template", default="", help="Excel模板路徑")
    parser.add_argument("--out", default=f"宏觀16模組_自動回填_{dt.date.today().strftime('%Y%m%d')}.xlsx", help="輸出Excel路徑")
    parser.add_argument("--date", default=dt.date.today().strftime("%Y-%m-%d"), help="基準日 YYYY-MM-DD")
    parser.add_argument("--log-dir", default="logs", help="Log目錄")
    parser.add_argument("--gov-net", type=float, default=None, help="人工覆寫官股買賣超(億元)")
    parser.add_argument("--ai-strength", type=float, default=None, help="人工覆寫AI主流強度0~1")
    parser.add_argument("--major-event", type=int, default=None, help="人工覆寫重大事件0/1")
    parser.add_argument("--event-note", default="", help="人工覆寫戰爭/地緣/重大事件說明")
    parser.add_argument("--night-score", type=float, default=None, help="人工覆寫台股夜盤分數")
    parser.add_argument("--db-path", default="", help="指定主SQLite DB路徑；用於ranking_result驗證與機構級股票投資規劃報表")
    parser.add_argument("--tej-gov-file", default="", help="TEJ八大公股行庫買賣超排名xls/xlsx；用於gov_net_100m主來源")
    parser.add_argument("--strict-ranking", action="store_true", help="ranking_result缺失或空表時直接中止，避免輸出可下單結論")
    parser.add_argument("--report-mode", default=REPORT_MODE_MACRO, choices=[REPORT_MODE_MACRO, REPORT_MODE_MACRO_TEACHER, REPORT_MODE_MACRO_ONLY, REPORT_MODE_INSTITUTIONAL, REPORT_MODE_TEACHER_FULL, REPORT_MODE_ALL], help="輸出模式：macro_refill/macro_teacher輸出宏觀16+老師策略00~16；macro_only只輸出3頁；institutional_report/teacher_full只輸出老師策略00~16；all輸出完整debug")
    args = parser.parse_args()
    if args.cli:
        engine = Macro16Engine(Path(args.log_dir))
        override = ManualOverride(gov_net_100m=args.gov_net, ai_strength=args.ai_strength, major_event=args.major_event, event_note=args.event_note, night_score=args.night_score)
        result = engine.run(args.template or None, args.out, args.date, override, db_path=(args.db_path or None), strict_ranking=args.strict_ranking, tej_gov_file=(args.tej_gov_file or None), report_mode=args.report_mode)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        run_gui()

if __name__ == "__main__":
    main()
