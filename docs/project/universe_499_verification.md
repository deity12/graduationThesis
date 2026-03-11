# 499 只缺失原因核实报告

对「为何 feature panel 为 499 只、缺失 4 只为 GEV/Q/SNDK/SOLV」做了直接核实，结论如下。

---

## 1. Wikipedia 解析是否出错？

**结论：未发现解析错误。**

- 代码用 `pd.read_html(StringIO(html))[0]` 取页面**第一个表**（S&P 500 component stocks）。
- 四只 ticker 均为该表内**正常数据行**，具备完整列：Symbol、Security、GICS Sector、Date added、CIK、Founded 等。
- **没有**把脚注、引用或其它单元格误读为 Symbol 的情况；表中确有「Q | Qnity Electronics」「SNDK | Sandisk」等行。

---

## 2. Symbol 清洗规则是否产生异常？

**结论：未发现清洗导致异常。**

- `download_universe.py` 仅做：`ticker.upper()`、`normalize_price_ticker`（strip、upper、`.` → `-`）。
- 「Q」在 Wikipedia 表内 Symbol 列即为单字母 `Q`，清洗后仍为 `Q`，并非由其它列或规则误生成。

---

## 3. 四只缺失的真实原因（逐只）

| Ticker | 表中信息 | 核实结论 |
|--------|----------|----------|
| **GEV** | GE Vernova，Date added 2024-04-02，Founded 2024 | 2024 年 GE 分拆上市，**在 2015–2023 评估窗内无交易**。无价格 = 预期内，非解析/清洗错误。 |
| **SOLV** | Solventum，Date added 2024-04-01，Founded 2023 | 2024 年 3M 分拆上市，**在 2015–2023 评估窗内无交易**。无价格 = 预期内，非解析/清洗错误。 |
| **Q** | Qnity Electronics，Date added **2025-11-03**，Founded **2025** | Wikipedia 主表包含**计划加入**成分（DuPont 分拆 Qnity）。页内「Changes」表写明：2025-11-03 Q 加入。评估窗为 2015–2023，该成分尚未存在，故无价格。**非解析错误**，而是**静态快照未按 date_added 过滤，包含了“未来”行」。** |
| **SNDK** | Sandisk，Date added **2025-11-28** | 历史上 SNDK 于 **2016-05-13 被 WDC 收购**后移出指数（Wikipedia 「Changes」表可查）。当前主表又出现 SNDK、且 Date added 为 2025-11-28，属**计划再度纳入或编者误植**。对 2015–2023 拉价时，SNDK 已退市/代码失效，yfinance 返回空表。**非解析错误**，是**成分表含未来/历史行 + 该代码在样本期无可用数据**。 |

---

## 4. 与「解析/清洗」的直接回答

- **是不是爬 Wikipedia 时解析错了？**  
  **否。** 四只均为表内正常行，无单元格/脚注/引用被误读为 ticker。

- **是不是某个单元格/脚注/引用标记被误读成 ticker？**  
  **否。** 未发现此类误读；Q 在表中就是 Symbol 列的单字母 Q（对应 Qnity Electronics 计划成分）。

- **是不是 symbol cleaning 规则出了问题？**  
  **否。** 仅做 strip/upper/点号替换，未引入 Q 或其它异常；Q 来自表内容本身。

---

## 5. 根本原因归纳

- **GEV、SOLV**：**评估窗与上市时间不重叠**——均为 2024 年才进入指数/上市，2015–2023 无行情。
- **Q、SNDK**：**静态快照未按时间过滤**——主表混合了“当前”与“未来/历史”成分；Q 为 2025-11 计划加入，SNDK 为 2025-11 再纳入或误行。用同一份名单拉 2015–2023 价格时，这两只在该区间无有效数据。

因此：**不是“抓错/洗错”，而是“成分表时点与评估窗不一致 + 未按 date_added 过滤”。**

---

## 6. 建议的工程修正

在 `download_universe.py` 或下游使用成分表时，**仅保留在评估窗内已存在的成分**，例如：

- 若评估结束日为 `2023-12-31`，则过滤：`date_added <= "2023-12-31"`（或按需用协议中的 `evaluation_end`）。
- 这样可排除 GEV、SOLV、Q 以及当前表内错误的 SNDK 行，使「有效成分」与 2015–2023 一致，避免 503 只中含 4 只“在样本期无数据”的条目。

后续若升级为 point-in-time 成分，可在此基础上再按「每个 as_of_date 的当时成分」过滤。

---

## 7. 补充说明（边界）

按 `date_added <= evaluation_end` 过滤当前成分表，可以修复当前已识别的未来成分股误入问题（如 GEV、SOLV、Q、SNDK），但这一步**仍不等同于严格的 point-in-time S&P 500 membership 重建**。原因是 Wikipedia 该页面本质上是**现行成分股快照**，而非研究区间内逐日历史成分数据库。因此，在 2016–2023 期间曾经是 S&P 500 成分、但到当前页面已不在现行成分表中的公司，仍可能被漏掉。

本文当前可将 499 只股票视为 current implementation 下的 effective usable universe；与此同时，**仍需将 universe definition / survivorship-bias 风险作为限制项明确记录**。
