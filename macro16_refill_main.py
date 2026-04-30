#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宏觀16模組自動抓取與Excel回填主程式
版本：V1.6 ExcelCommentAligned
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

APP_NAME = "Macro16RefillEngine"
VERSION = "1.6.0-excel-comment-aligned"
DEFAULT_TIMEOUT = 15
DEFAULT_MAX_FALLBACK_DAYS = 5

MODULES = [
    "美股-S&P500", "美股-NASDAQ", "美股-道瓊", "VIX恐慌",
    "美債10Y", "原油", "戰爭/地緣", "CPI", "非農", "外資",
    "官股", "台股指數", "成交量", "AI產業", "OTC", "台股夜盤"
]

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

class Macro16Logger:
    def __init__(self, log_dir: Path):
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = log_dir / f"macro16_debug_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
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

    def raw_snapshot(self, source: str, payload: Any):
        text = str(payload)
        if len(text) > 900:
            text = text[:900] + "..."
        self.info(f"RAW_DATA_SNAPSHOT source={source} payload={text}")

    def parsed_value(self, field: str, value: Any, source: str, actual_date: str = ""):
        self.info(f"PARSED_VALUE field={field} value={value} source={source} actual_date={actual_date}")

class HttpClient:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger
        self.session = requests.Session() if requests else None
        if self.session:
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 Macro16RefillEngine/1.0",
                "Accept": "application/json,text/csv,text/plain,*/*",
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
                                self.logger.raw_snapshot("TWSE_FOREIGN", row_text[:500])
                                self.logger.parsed_value("foreign_net_100m", round(net_100m, 2), "TWSE BFI82U/TWT38U", try_date)
                                return RawData("外資", {"net_100m": net_100m, "raw_hint": row_text[:500]}, self._dash_date(try_date),
                                               "TWSE三大法人", url, self._today_str(), "OK", note, query_date, try_date, is_fb, fb_days, "OK", note)
                    text = json.dumps(data, ensure_ascii=False)
                    nums = [self._to_float(x) for x in re.findall(r"-?\d[\d,]*\.?\d*", text)]
                    nums = [x for x in nums if abs(x) > 1000000]
                    if nums:
                        net_100m = nums[-1] / 100000000
                        is_fb, fb_days, note = self._fallback_note(query_date, try_date, "TWSE_FOREIGN")
                        msg = "使用fallback解析；" + note if note else "使用fallback解析"
                        self.logger.warning(f"外資語意列未找到，使用數值fallback source={url}, query_date={query_date}, actual_date={try_date}, net_100m={net_100m:.2f}")
                        return RawData("外資", {"net_100m": net_100m, "raw_hint": text[:500]}, self._dash_date(try_date),
                                       "TWSE三大法人-fallback", url, self._today_str(), "WARN", msg, query_date, try_date, is_fb, fb_days, "OK", msg)
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

    def fetch_text_snapshot(self, module: str, url: str, source_name: str, status_if_fail: str = "WARN") -> RawData:
        try:
            text = self.client.get_text(url)
            compact = re.sub(r"\s+", " ", text)
            snippet = compact[:1500]
            value = {"url": url, "snippet": snippet}
            self.logger.raw_snapshot(module, value)
            self.logger.parsed_value(f"{module}_source_url", url, source_name, "latest")
            return RawData(module, value, "latest", source_name, url, self._today_str(), "OK", f"{source_name} source fetched")
        except Exception as exc:
            self.logger.warning(f"{source_name}抓取失敗 {module}: {exc}")
            return RawData(module, None, "", source_name, url, self._today_str(), status_if_fail, str(exc), data_status="OPTIONAL_MISSING")

    def fetch_gov_news(self, url: str = "https://money.udn.com/money/story/5607/9472868") -> RawData:
        raw = self.fetch_text_snapshot("官股", url, "UDN Money", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "")
            m = re.search(r"(?:八大公股|官股|公股).*?(?:買超|買進|承接).*?([0-9]+(?:\.[0-9]+)?)\s*億", text)
            if m:
                value = float(m.group(1))
                raw.value["gov_net_100m"] = value
                raw.message = "UDN官股新聞已解析買超億元"
                self.logger.parsed_value("gov_net_100m", value, "UDN Money", raw.date)
            else:
                self.logger.info("PARSED_VALUE field=gov_net_100m value=UNPARSED source=UDN Money actual_date=latest")
        return raw

    def fetch_ai_industry_news(self, url: str = "https://finance.technews.tw/2026/04/29/ase-technology-holding-is-optimistic-about-the-explosive-growth-in-demand-for-advanced-packaging-business-in-the-next-two-years/") -> RawData:
        raw = self.fetch_text_snapshot("AI產業", url, "TechNews", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "")
            strength = 0.8 if any(k in text for k in ["AI", "先進封裝", "需求", "資本支出", "成長"]) else 0.5
            raw.value["ai_strength"] = strength
            self.logger.parsed_value("ai_strength", strength, "TechNews", raw.date)
        return raw

    def fetch_geopolitical_news(self, url: str = "https://www.aljazeera.com/news/liveblog/2026/4/29/iran-war-live-trump-says-tehran-wants-end-to-blockade-israel-kills-medics") -> RawData:
        raw = self.fetch_text_snapshot("戰爭/地緣", url, "Al Jazeera", "WARN")
        if raw.status == "OK" and raw.value:
            text = raw.value.get("snippet", "").lower()
            risk = 1 if any(k in text for k in ["war", "blockade", "israel", "iran", "attack", "missile", "kills"]) else 0
            raw.value["major_event"] = risk
            self.logger.parsed_value("major_event", risk, "Al Jazeera", raw.date)
        return raw

    def fetch_taifex_night_snapshot(self, url: str = "https://www.taifex.com.tw/enl/eng3/futDailyMarketReport") -> RawData:
        return self.fetch_text_snapshot("台股夜盤", url, "TAIFEX", "WARN")

    def fetch_tpex_otc_snapshot(self, url: str = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/indices-pricing.html") -> RawData:
        return self.fetch_text_snapshot("OTC官方來源", url, "TPEX", "WARN")

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

        gov = raw.get("官股")
        if gov and gov.status == "OK" and isinstance(gov.value, dict) and gov.value.get("gov_net_100m") is not None:
            market.gov_net_100m = round(float(gov.value.get("gov_net_100m")), 2)
            self.logger.parsed_value("market.gov_net_100m", market.gov_net_100m, gov.source, gov.date)
        else:
            market.gov_net_100m = None

        ai = raw.get("AI產業")
        if ai and ai.status == "OK" and isinstance(ai.value, dict) and ai.value.get("ai_strength") is not None:
            market.ai_strength = float(ai.value.get("ai_strength"))
            self.logger.parsed_value("market.ai_strength", market.ai_strength, ai.source, ai.date)
        else:
            market.ai_strength = 0.5

        geo = raw.get("戰爭/地緣")
        if geo and geo.status == "OK" and isinstance(geo.value, dict) and geo.value.get("major_event") is not None:
            market.major_event = int(geo.value.get("major_event"))
            self.logger.parsed_value("market.major_event", market.major_event, geo.source, geo.date)
        else:
            market.major_event = 0

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
            explanation = "已依 Excel 註解來源抓取 Al Jazeera/地緣事件頁面；若含戰爭、封鎖、攻擊等關鍵字，列重大事件風險。"
            trade = "外部事件風險升高，降倉禁追高" if risk else "事件頁面已抓取，未偵測到強風險關鍵字"
            return ModuleScore("戰爭/地緣", raw.value.get("url", ""), strength, direction, weighted, explanation, raw.source, raw.date, trade, "OK")
        return ModuleScore("戰爭/地緣", "未取得", 0.0, 0, 0.0, "地緣新聞來源未取得，列為選用資料缺失，不得自動編造事件。", "Al Jazeera", market.base_date, "需人工確認事件嚴重度", "WARN")
    def score_cpi(self, raw, market):
        if raw and raw.status == "OK" and raw.value:
            snippet = raw.value.get("snippet", "") if isinstance(raw.value, dict) else str(raw.value)
            nums = re.findall(r"[-+]?\d+(?:\.\d+)?\s*percent|[-+]?\d+(?:\.\d+)?%", snippet, flags=re.I)
            data_text = "BLS CPI release fetched"
            if nums:
                data_text += "；sample=" + ", ".join(nums[:3])
            return ModuleScore("CPI", data_text, 0.3, 0, 0.0, "CPI 為週期性資料，已依 Excel 註解改抓 BLS 官方發布頁；非公布日使用最近公告，不做每日回退。", raw.source, raw.date, "通膨資料已抓取，需搭配油價/利率判斷", "OK")
        return ModuleScore("CPI", "BLS未取得", 0.0, 0, 0.0, "CPI 為週期性資料；BLS來源抓取失敗時不應假設中性，需標示資料缺失。", "BLS", market.base_date, "需補抓或人工確認", "WARN")
    def score_nfp(self, raw, market):
        if raw and raw.status == "OK" and raw.value:
            snippet = raw.value.get("snippet", "") if isinstance(raw.value, dict) else str(raw.value)
            data_text = "BLS Employment Situation release fetched"
            m = re.search(r"nonfarm payroll employment.*?(?:rose|increased).*?([0-9,]+)", snippet, flags=re.I)
            if m:
                data_text += f"；payroll_hint={m.group(1)}"
            return ModuleScore("非農", data_text, 0.3, 0, 0.0, "非農為週期性資料，已依 Excel 註解改抓 BLS 官方就業報告；非公布日使用最近公告，不做每日回退。", raw.source, raw.date, "就業資料已抓取，需搭配市場預期差判斷", "OK")
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
            if raw and raw.status == "OK":
                return ModuleScore("官股", "UDN官股來源已抓取但未解析出買賣超億元", 0.2, 0, 0.0, "已依 Excel 註解抓取官股新聞來源，但未解析出明確數字；不自動編造，列中性觀察。", raw.source, raw.date, "需確認新聞文字或人工補入", "WARN")
            return ModuleScore("官股", "未取得", 0.0, 0, 0.0, "官股/八大公股來源未取得，本版不編造數字。", "UDN Money", market.base_date, "需補抓或人工確認", "WARN")
        direction = 1 if value > 0 else (-1 if value < 0 else 0)
        strength = min(1.0, max(0.3, abs(value)/100)) if direction else 0.2
        weighted = round(direction*strength,2)
        return ModuleScore("官股", f"{value:.2f}億元", strength, direction, weighted, "已依 Excel 註解抓取/解析官股來源；官股買超代表承接支撐，賣超代表政策資金未護盤。", raw.source if raw else "UDN Money", market.base_date, "視為支撐判斷，不等於追價依據", "OK")
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
        source = "TechNews/產業來源" if raw and raw.status == "OK" else "人工/預設"
        direction = 1 if strength >= 0.7 else (0 if strength >= 0.4 else -1)
        weighted = round(direction * strength, 2)
        explanation = f"AI主流強度{strength:.2f}；V1.7 已依 Excel 註解抓取 TechNews/先進封裝來源，若抓不到才保留預設值。"
        trade = "主線有效，可優先找主流拉回" if direction > 0 else "AI主線中性，避免過度集中" if direction == 0 else "AI題材轉弱，降低權重"
        status = "OK" if raw and raw.status == "OK" else "WARN"
        return ModuleScore("AI產業", f"AI強度{strength:.2f}", strength, direction, weighted, explanation, source, market.base_date, trade, status)
    def score_otc(self, raw, market):
        if raw and raw.status == "OK" and isinstance(raw.value, dict) and "close" in raw.value:
            return self._score_yahoo_index(raw, "OTC", "中小型資金活躍", "中小型資金轉弱")
        if raw and raw.status == "OK":
            return ModuleScore("OTC", "TPEX OTC 官方來源頁已抓取；未解析指數數值", 0.3, 0, 0.0, "已依 Excel 註解抓取 TPEX 櫃買指數來源頁；若需自動給分，下一版需解析指數與漲跌。", raw.source, raw.date, "OTC 資金強弱輔助判斷", "OK")
        return self.score_neutral("OTC", raw, "OTC資料未取得，列中性")
    def score_night(self, raw, market):
        if raw and raw.status == "OK":
            return ModuleScore("台股夜盤", "TAIFEX 夜盤來源已抓取；本版依 Excel 註解維持中性，不作主判斷", 0.3, 0, 0.0, "已抓取 TAIFEX 夜盤來源頁並寫入 Log；因夜盤具時間敏感性，未完整驗證前不自動給多空分數。", raw.source, raw.date, "作為盤前輔助，不作主要交易開關", "OK")
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
        risk_score = below_ma5 + lower_high + lower_low + volume_expansion + major_event
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
        self.logger.info(f"V2技術引擎完成：risk_score={risk_score}, judgement={judgement}")
        return TechnicalRisk(below_ma5, lower_high, lower_low, volume_expansion, major_event, risk_score, judgement)

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

class ExcelWriter:
    def __init__(self, logger: Macro16Logger):
        self.logger = logger
        self.header_fill = PatternFill("solid", fgColor="DDEBF7")
        self.sub_fill = PatternFill("solid", fgColor="E2F0D9")
        self.warn_fill = PatternFill("solid", fgColor="FFF2CC")
        self.thin = Side(style="thin", color="D9E2F3")

    def write(self, template: Optional[str], out_path: str, market: MarketInput, scores: List[ModuleScore], tech: TechnicalRisk, summary: Dict[str, str], logs: List[str], raw: Optional[Dict[str, RawData]] = None) -> str:
        if template and Path(template).exists():
            try:
                wb = load_workbook(template)
                self.logger.info(f"已載入Excel模板：{template}")
            except Exception as exc:
                self.logger.warning(f"模板載入失敗，改建新檔：{exc}")
                wb = Workbook()
        else:
            wb = Workbook()
        self._write_market_input(wb, market)
        self._write_macro_modules(wb, scores)
        self._write_technical(wb, tech)
        self._write_audit(wb, market, scores, tech, summary, logs)
        self._write_data_source_status(wb, market, raw or {})
        self._format_all(wb)
        wb.save(out_path)
        self.logger.info(f"Excel已輸出：{out_path}")
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
        judge = []
        if market.close and market.ma5:
            judge.append(f"收盤 {market.close} {'站上' if market.close >= market.ma5 else '跌破'} 5MA {market.ma5}")
        if market.foreign_net_100m is None:
            judge.append("外資未取得")
        if market.turnover_100m is None:
            judge.append("成交值未取得")
        ws.append(["交易判讀", "；".join(judge) if judge else "資料不足，需檢查來源與Log。"] + [None]*(len(headers)-2))

    def _write_macro_modules(self, wb, scores: List[ModuleScore]):
        ws = self._sheet(wb, "宏觀15模組")
        ws.append(["模組", "風險/強度分數(0-1)", "方向(+1/0/-1)", "加權分數", "說明", "資料來源", "資料時間"])
        for s in scores:
            ws.append([s.module, s.strength, s.direction, s.weighted_score, s.explanation, s.source, s.data_time])
        ws.append([])
        ws.append(["補充欄位", "狀態", "數據/事件", "交易用途"] )
        for s in scores:
            ws.append([s.module, s.status, s.data_text, s.trade_usage])

    def _write_technical(self, wb, tech: TechnicalRisk):
        ws = self._sheet(wb, "V2技術引擎")
        ws.append(["跌破5MA", "高不過高", "低破低", "放量", "重大事件", "技術/風險分數", "大盤判定"])
        ws.append([tech.below_ma5, tech.lower_high, tech.lower_low, tech.volume_expansion, tech.major_event, tech.risk_score, tech.market_judgement])
        ws.append(["判讀說明", "收盤<5MA為1", "最高<前高為1", "最低<前低為1", "成交值>5日均量*1.05為1", "需有明確來源", "五項加總", "供下單清單參考"])

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
        ws.append(["資料源", "狀態", "資料分類", "查詢日", "實際資料日", "是否回退", "回退天數", "來源", "URL", "說明/訊息"])
        for name, item in raw.items():
            ws.append([
                name, item.status, item.data_status, item.query_date, item.actual_date or item.date,
                item.is_fallback, item.fallback_days, item.source, item.url, item.message or item.data_note
            ])
        ws.append([])
        ws.append(["說明", "query_date=使用者查詢日；actual_date=實際資料日；fallback_days=往前回退天數；RAW_DATA_SNAPSHOT 與 PARSED_VALUE 會寫入 log 證明實際抓取資料。"])


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
                warnings.append(f"{s.module} 狀態={s.status}，需人工確認")
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

    def run(self, template: Optional[str], out_path: str, base_date: Optional[str] = None, override: Optional[ManualOverride] = None) -> Dict[str, Any]:
        self.logger.info(f"開始執行 {APP_NAME} v{VERSION}")
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
        for module, symbol in YAHOO_SYMBOLS.items():
            raw[module] = self.source.fetch_yahoo_chart(symbol, module)
        for module, symbols in YAHOO_SYMBOL_CANDIDATES.items():
            raw[module] = self.source.fetch_yahoo_chart_candidates(symbols, module)
        raw["美債10Y"] = self.source.fetch_fred_csv_latest("DGS10", "美債10Y")
        raw["CPI"] = self.source.fetch_bls_release_text("CPI", "https://www.bls.gov/news.release/cpi.nr0.htm")
        raw["非農"] = self.source.fetch_bls_release_text("非農", "https://www.bls.gov/news.release/empsit.nr0.htm")
        raw["戰爭/地緣"] = self.source.fetch_geopolitical_news()
        raw["官股"] = self.source.fetch_gov_news()
        raw["AI產業"] = self.source.fetch_ai_industry_news()
        raw["台股夜盤"] = self.source.fetch_taifex_night_snapshot()
        tpex_otc_raw = self.source.fetch_tpex_otc_snapshot()
        if tpex_otc_raw.status == "OK":
            raw["OTC"] = tpex_otc_raw
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
        scores = self.scoring.score_all(raw, market)
        macro_total = round(sum(s.weighted_score for s in scores), 2)
        tech = self.indicator.compute(market, macro_total)
        summary = self.explain.build_summary(macro_total, tech)
        warnings = self.audit.check(market, scores, tech)
        if warnings:
            summary["QA警告"] = "; ".join(warnings[:8])
        output = self.writer.write(template, out_path, market, scores, tech, summary, self.logger.messages, raw)
        self.logger.info("執行完成")
        return {"output": output, "summary": summary, "warnings": warnings, "log_file": str(self.logger.log_file)}

def run_gui():
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title("宏觀16模組 自動回填主程式")
    root.geometry("1000x720")

    template_var = tk.StringVar()
    out_var = tk.StringVar(value=str(Path.cwd() / f"宏觀16模組_自動回填_{dt.date.today().strftime('%Y%m%d')}.xlsx"))
    date_var = tk.StringVar(value=dt.date.today().strftime("%Y-%m-%d"))
    status_var = tk.StringVar(value="待執行")

    def browse_template():
        p = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if p:
            template_var.set(p)

    def browse_out():
        p = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
        if p:
            out_var.set(p)

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
    ttk.Label(frm, textvariable=status_var, foreground="blue").grid(row=4, column=0, columnspan=3, sticky="w", pady=8)

    log_text = tk.Text(frm, height=26, wrap="word")
    log_text.grid(row=6, column=0, columnspan=3, sticky="nsew", pady=(10,0))
    frm.rowconfigure(6, weight=1)
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
            result = engine.run(template_var.get() or None, out_var.get(), date_var.get())
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

    ttk.Button(frm, text="執行回填", command=execute).grid(row=5, column=0, sticky="w", pady=6)
    ttk.Button(frm, text="離開", command=root.destroy).grid(row=5, column=2, sticky="e", pady=6)
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
    args = parser.parse_args()
    if args.cli:
        engine = Macro16Engine(Path(args.log_dir))
        override = ManualOverride(gov_net_100m=args.gov_net, ai_strength=args.ai_strength, major_event=args.major_event, event_note=args.event_note, night_score=args.night_score)
        result = engine.run(args.template or None, args.out, args.date, override)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        run_gui()

if __name__ == "__main__":
    main()
