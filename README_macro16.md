# Macro16 Refill System（宏觀16模組自動回填系統）

## 📌 專案目的
本系統依據《宏觀16模組市場資料回填SOP》建立，
將外部市場資料轉換為「可量化分數」，並自動回填至 Excel。

👉 核心目標：
- 自動抓取市場資料
- 依固定規則轉換為方向與強度分數
- 自動回填 Excel
- 輸出市場狀態與交易決策

---

## 🧠 系統架構
資料取得層 → 資料標準化層 → 判定引擎 → Excel回填與稽核

---

## ⚙️ 功能模組
- DataCollector（資料抓取）
- DataProcessor（資料標準化）
- IndicatorEngine（技術指標）
- ScoringEngine（分數計算）
- ExplanationEngine（說明生成）
- DecisionEngine（決策判定）
- ExcelWriter（回填Excel）
- AuditEngine（稽核檢查）

---

## 🚀 使用方式
```bash
pip install -r requirements.txt
python macro16_refill_main.py
```

---

## 🏗 GitHub自動產EXE
1. 上傳專案到GitHub
2. 點選 Actions
3. 執行 Build Windows EXE
4. 下載產出 exe

---

## 📦 專案結構
macro16_refill_project/
├── macro16_refill_main.py
├── requirements.txt
├── README.md
└── .github/workflows/build-windows-exe.yml

---

## ⚠️ 注意事項
- 所有資料必須有來源與日期
- 不可填假資料
- 新聞不可取代市場數據

---

## 🔥 系統定位
交易決策引擎（Trading Decision Engine）
