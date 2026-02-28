# -*- coding: utf-8 -*-
import asyncio, re, math
from pathlib import Path
from typing import  List

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
# 后续可能网页格式更改 可以在这里替换
SEL = 'div[class*="HeaderInfo_totalAssetInner__"]'
EXCEL_PATH = Path("资产结果.xlsx")
ADDR_FILE = "钱包地址.txt"


async def fetch_value_for_wallet(context, wallet_address: str, retries=5) -> float:
    last_err = None
    for attempt in range(1, retries + 1):
        page = None
        try:
            page = await context.new_page()
            url = f"https://debank.com/profile/{wallet_address}"
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_selector(SEL, timeout=30000)

            # 等“稳定值”（不依赖 $0）
            raw = await page.evaluate("""
              async (selector) => {
                const el = document.querySelector(selector);
                if (!el) throw new Error("element not found");

                const getRaw = () => {
                  const t = Array.from(el.childNodes).find(n => n.nodeType === 3);
                  return (t ? t.textContent : '').trim();
                };
                const getNum = () => parseFloat(getRaw().replace(/[^0-9.]/g, '')) || 0;

                const STABLE_MS = 800;
                const TIMEOUT_MS = 25000;
                const start = Date.now();

                return await new Promise((resolve) => {
                  let last = null, stableSince = null;

                  const check = () => {
                    const v = getNum();
                    const now = Date.now();

                    // 前1秒不判稳定：防止旧值先显示
                    if (now - start < 1000) { last = v; stableSince = now; return; }

                    if (v !== last) { last = v; stableSince = now; return; }

                    if (stableSince && (now - stableSince) >= STABLE_MS) {
                      obs.disconnect();
                      resolve(getRaw());
                    }
                  };

                  const obs = new MutationObserver(check);
                  obs.observe(el, { childList:true, characterData:true, subtree:true });
                  check();

                  setTimeout(() => {
                    obs.disconnect();
                    resolve(getRaw()); // 超时返回当前看到的值，避免卡死
                  }, TIMEOUT_MS);
                });
              }
            """, SEL)

            s = re.sub(r"[^0-9.]", "", raw or "")
            value = float(s) if s else float("nan")

            # NaN 或 <=0 算失败，触发重试
            if math.isnan(value) or value <= 0:
                raise ValueError(f"bad value raw={raw!r}, parsed={value}")

            return value

        except (PWTimeoutError, Exception) as e:
            last_err = e
            await asyncio.sleep(min(2 * attempt, 8))  # 退避
        finally:
            if page:
                await page.close()

    return float("nan")


def load_wallets() -> List[str]:
    wallets = [l.strip() for l in open(ADDR_FILE, encoding="utf-8") if l.strip()]
    return wallets


def init_or_load_excel(wallets: List[str]) -> pd.DataFrame:
    """
    返回一个 DataFrame：钱包/地址/资产(value)
    若 Excel 已存在：读入并对齐到当前 wallets（以地址为准）
    """
    base = pd.DataFrame({
        "钱包": [f"钱包{i+1}" for i in range(len(wallets))],
        "地址": wallets,
        "资产(value)": [float("nan")] * len(wallets)
    })

    if EXCEL_PATH.exists():
        old = pd.read_excel(EXCEL_PATH)

        # 如果旧表结构不对，直接用 base 重新来
        if not {"钱包", "地址", "资产(value)"}.issubset(set(old.columns)):
            return base

        # 用“地址”做主键，把旧资产并回 base（断点续跑关键）
        old2 = old[["地址", "资产(value)"]].drop_duplicates(subset=["地址"])
        merged = base.merge(old2, on="地址", how="left", suffixes=("", "_old"))

        # 如果旧值存在且非 NaN，用旧值覆盖
        merged["资产(value)"] = merged["资产(value)_old"].combine_first(merged["资产(value)"])
        merged = merged.drop(columns=["资产(value)_old"])
        return merged

    return base


def save_excel(df: pd.DataFrame) -> None:
    # 原子写入：先写临时文件再替换，防止中途崩导致 Excel 损坏
    tmp = EXCEL_PATH.with_suffix(".tmp.xlsx")
    df.to_excel(tmp, index=False)
    tmp.replace(EXCEL_PATH)


async def main():
    wallets = load_wallets()
    if not wallets:
        raise RuntimeError("地址文件为空")

    # 断点续跑：先加载/初始化 Excel 数据
    df = init_or_load_excel(wallets)

    # 只跑“还没有有效资产”的地址
    pending_mask = df["资产(value)"].isna()
    pending_indices = df.index[pending_mask].tolist()

    print(f"总钱包数: {len(wallets)}，待抓取: {len(pending_indices)}（已更新的资产将跳过）")

    concurrency = 5
    batch_size = 20
    sem = asyncio.Semaphore(concurrency)
    print_lock = asyncio.Lock()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36"),
        )

        async def worker(row_idx: int):
            addr = df.at[row_idx, "地址"]
            wallet_name = df.at[row_idx, "钱包"]

            async with sem:
                # 额外兜底：NaN 继续重试几轮
                v = float("nan")
                for r in range(1, 4):
                    v = await fetch_value_for_wallet(context, addr, retries=5)
                    if not math.isnan(v):
                        break
                    await asyncio.sleep(min(3 * r, 10))

                # ✅ 只在成功时写入 df（NaN 不覆盖旧值）
                if not math.isnan(v):
                    df.at[row_idx, "资产(value)"] = v

                async with print_lock:
                    print(f"完成 {wallet_name} | 资产: {v} | 地址: {addr}")

        # 分批处理 pending
        for i in range(0, len(pending_indices), batch_size):
            batch_rows = pending_indices[i:i + batch_size]
            tasks = [asyncio.create_task(worker(row_idx)) for row_idx in batch_rows]
            await asyncio.gather(*tasks)

            # ✅ 每 batch 落盘一次
            save_excel(df)
            print(f"本批已写入 Excel：{i + len(batch_rows)}/{len(pending_indices)}")

            if i + batch_size < len(pending_indices):
                print(f"休息45秒…")
                await asyncio.sleep(45)

        await context.close()
        await browser.close()

    print("全部完成，Excel 已更新：资产结果.xlsx")


if __name__ == "__main__":
    asyncio.run(main())