import argparse
import csv
import json
import time
from pathlib import Path
from typing import Dict, List

from playwright.sync_api import Playwright, sync_playwright


INDEX_URL = "https://www.szse.cn/disclosure/supervision/inquire/index.html"
API_URL = "https://www.szse.cn/api/report/ShowReport/data"
COLUMNS = ["公司代码", "公司简称", "发函日期", "函件类别"]


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def parse_page_payload(payload: object) -> tuple[int, List[Dict[str, str]]]:
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("深交所接口返回结构异常")

    first = payload[0] or {}
    metadata = first.get("metadata", {}) or {}
    page_count = int(metadata.get("pagecount", 1) or 1)

    rows = []
    for row in first.get("data", []) or []:
        rows.append(
            {
                "公司代码": normalize_text(row.get("gsdm")),
                "公司简称": normalize_text(row.get("gsjc")),
                "发函日期": normalize_text(row.get("fhrq"))[:10],
                "函件类别": normalize_text(row.get("hjlb")),
            }
        )
    return page_count, rows


def fetch_page_via_browser_session(page, page_no: int, page_size: int) -> object:
    # 在页面上下文执行 fetch，最大化复用浏览器真实会话和指纹
    script = """
    async ({ apiUrl, pageNo, pageSize }) => {
      const url = new URL(apiUrl);
      url.searchParams.set('SHOWTYPE', 'JSON');
      url.searchParams.set('CATALOGID', 'main_wxhj');
      url.searchParams.set('PAGENO', String(pageNo));
      url.searchParams.set('PAGESIZE', String(pageSize));

      const resp = await fetch(url.toString(), {
        method: 'GET',
        credentials: 'include',
        headers: {
          'Accept': 'application/json, text/plain, */*',
          'X-Requested-With': 'XMLHttpRequest'
        }
      });
      const text = await resp.text();
      return { ok: resp.ok, status: resp.status, text };
    }
    """
    result = page.evaluate(script, {"apiUrl": API_URL, "pageNo": page_no, "pageSize": page_size})
    if not result.get("ok"):
        raise RuntimeError(f"HTTP {result.get('status')} on page {page_no}")
    try:
        return json.loads(result.get("text", ""))
    except Exception as exc:
        raise RuntimeError(f"第 {page_no} 页返回非JSON，前80字符: {result.get('text', '')[:80]}") from exc


def append_rows(csv_path: Path, rows: List[Dict[str, str]], write_header: bool) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def dedupe_csv_inplace(csv_path: Path) -> int:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        data = list(reader)

    seen = set()
    uniq = []
    for row in data:
        key = tuple(row.get(k, "") for k in COLUMNS)
        if key in seen:
            continue
        seen.add(key)
        uniq.append({k: row.get(k, "") for k in COLUMNS})

    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(uniq)
    return len(uniq)


def run(
    playwright: Playwright,
    out_csv: Path,
    state_file: Path,
    page_size: int,
    headless: bool,
    executable_path: str | None,
) -> None:
    launch_kwargs = {
        "headless": headless,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    }
    if executable_path:
        browser = playwright.chromium.launch(executable_path=executable_path, **launch_kwargs)
    else:
        # 优先调起本机 Chrome（更像真人环境），没有的话再退回 Playwright Chromium
        try:
            browser = playwright.chromium.launch(channel="chrome", **launch_kwargs)
        except Exception:
            browser = playwright.chromium.launch(**launch_kwargs)
    context = browser.new_context(
        locale="zh-CN",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1440, "height": 900},
    )
    context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
    page = context.new_page()

    try:
        page.goto(INDEX_URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3500)

        start_page = 1
        if state_file.exists():
            try:
                state = json.loads(state_file.read_text(encoding="utf-8"))
                start_page = int(state.get("next_page", 1) or 1)
            except Exception:
                start_page = 1

        if start_page == 1 and out_csv.exists():
            out_csv.unlink()

        total_pages = None
        write_header = not out_csv.exists()
        page_no = start_page

        while True:
            last_error = None
            payload = None
            for attempt in range(10):
                try:
                    payload = fetch_page_via_browser_session(page, page_no=page_no, page_size=page_size)
                    break
                except Exception as exc:
                    last_error = exc
                    # 刷新页面重建会话，规避风控临时封锁
                    page.wait_for_timeout(700 + attempt * 300)
                    if attempt in (3, 6):
                        page.goto(INDEX_URL, wait_until="domcontentloaded", timeout=60_000)
                        page.wait_for_timeout(1500)

            if payload is None:
                raise RuntimeError(f"第 {page_no} 页连续失败: {last_error}")

            page_count, rows = parse_page_payload(payload)
            if total_pages is None:
                total_pages = page_count
                print(f"total_pages = {total_pages}")

            append_rows(out_csv, rows, write_header=write_header)
            write_header = False

            state_file.write_text(
                json.dumps({"next_page": page_no + 1, "total_pages": total_pages}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            if page_no % 10 == 0 or page_no == total_pages:
                print(f"page {page_no}/{total_pages}, appended={len(rows)}")

            if page_no >= total_pages:
                break
            page_no += 1
            time.sleep(0.15)

        uniq_count = dedupe_csv_inplace(out_csv)
        print(f"done: {out_csv.resolve()}")
        print(f"unique_rows: {uniq_count}")
    finally:
        context.close()
        browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="深交所问询函 Playwright 会话抓取")
    parser.add_argument("--output", default="output/szse_inquiry_letters.csv", help="输出 CSV 路径")
    parser.add_argument("--state-file", default="output/szse_progress.json", help="断点状态文件")
    parser.add_argument("--page-size", type=int, default=200, help="每页条数")
    parser.add_argument("--headless", action="store_true", help="启用无头模式")
    parser.add_argument(
        "--executable-path",
        default="",
        help="本机 Chrome/Chromium 可执行文件路径（可选，填了可跳过 playwright install）",
    )
    args = parser.parse_args()

    out_csv = Path(args.output)
    state_file = Path(args.state_file)

    with sync_playwright() as playwright:
        run(
            playwright=playwright,
            out_csv=out_csv,
            state_file=state_file,
            page_size=args.page_size,
            headless=args.headless,
            executable_path=args.executable_path.strip() or None,
        )


if __name__ == "__main__":
    main()
