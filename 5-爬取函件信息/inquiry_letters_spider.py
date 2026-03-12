import argparse
import csv
import time
from pathlib import Path
from typing import Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


SSE_URL = "https://query.sse.com.cn/commonSoaQuery.do"
SSE_REFERER = "https://www.sse.com.cn/regulation/supervision/inquiries/"
SZSE_URLS = [
    "http://www.szse.cn/api/report/ShowReport/data",
    "https://www.szse.cn/api/report/ShowReport/data",
]

OUTPUT_COLUMNS = ["公司代码", "公司简称", "发函日期", "函件类别"]


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=1,
        connect=1,
        read=1,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_date(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    return text[:10]


def fetch_sse(session: requests.Session, page_size: int, sleep_sec: float) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    page_no = 1
    total_pages = None

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": SSE_REFERER,
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    while True:
        params = {
            "isPagination": "true",
            "pageHelp.pageSize": str(page_size),
            "pageHelp.pageNo": str(page_no),
            "pageHelp.beginPage": "1",
            "pageHelp.cacheSize": "1",
            "pageHelp.endPage": "1",
            "sqlId": "BS_KCB_GGLL_NEW",
            "siteId": "28",
            "channelId": "10743,10744,10012",
            "type": "",
            "stockcode": "",
            "extGGDL": "",
            "createTime": "",
            "createTimeEnd": "",
            "order": "createTime|desc,stockcode|asc",
        }
        resp = session.get(SSE_URL, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if total_pages is None:
            page_help = data.get("pageHelp", {}) or {}
            total_pages = int(page_help.get("pageCount", 1) or 1)

        page_rows = data.get("result", []) or []
        for row in page_rows:
            records.append(
                {
                    "公司代码": normalize_text(row.get("stockcode")),
                    "公司简称": normalize_text(row.get("extGSJC")),
                    "发函日期": normalize_date(row.get("createTime")),
                    "函件类别": normalize_text(row.get("extWTFL")),
                }
            )

        if page_no >= total_pages:
            break
        if page_no % 20 == 0:
            print(f"[SSE] 已抓取到第 {page_no} / {total_pages} 页")
        page_no += 1
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    return records


def fetch_szse(session: requests.Session, page_size: int, sleep_sec: float) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    page_no = 1
    total_pages = None

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://www.szse.cn/disclosure/supervision/inquire/index.html",
        "Accept": "application/json, text/plain, */*",
    }

    while True:
        params = {
            "SHOWTYPE": "JSON",
            "CATALOGID": "main_wxhj",
            "PAGENO": str(page_no),
            "PAGESIZE": str(page_size),
        }
        payload = None
        last_error: Exception | None = None
        for attempt in range(3):
            for url in SZSE_URLS:
                try:
                    resp = session.get(url, params=params, headers=headers, timeout=8)
                    resp.raise_for_status()
                    payload = resp.json()
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    continue
            if payload is not None:
                break
            time.sleep(min(2 ** attempt * 0.2, 1.2))

        if payload is None:
            raise RuntimeError("深交所接口请求失败") from last_error

        if not isinstance(payload, list) or not payload:
            break

        first_block = payload[0] or {}
        metadata = first_block.get("metadata", {}) or {}
        page_rows = first_block.get("data", []) or []

        if total_pages is None:
            total_pages = int(metadata.get("pagecount", 1) or 1)

        for row in page_rows:
            records.append(
                {
                    "公司代码": normalize_text(row.get("gsdm")),
                    "公司简称": normalize_text(row.get("gsjc")),
                    "发函日期": normalize_date(row.get("fhrq")),
                    "函件类别": normalize_text(row.get("hjlb")),
                }
            )

        if page_no >= total_pages:
            break
        if page_no % 20 == 0:
            print(f"[SZSE] 已抓取到第 {page_no} / {total_pages} 页")
        page_no += 1
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    return records


def deduplicate(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    result: List[Dict[str, str]] = []
    for row in records:
        key = tuple(row.get(col, "") for col in OUTPUT_COLUMNS)
        if key in seen:
            continue
        seen.add(key)
        result.append({col: row.get(col, "") for col in OUTPUT_COLUMNS})
    return result


def write_csv(records: List[Dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(records)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取上交所+深交所问询函信息")
    parser.add_argument(
        "--output",
        default="output/inquiry_letters.csv",
        help="输出 CSV 路径（默认: output/inquiry_letters.csv）",
    )
    parser.add_argument("--page-size", type=int, default=50, help="每页抓取条数（默认: 50）")
    parser.add_argument("--sleep", type=float, default=0.1, help="分页间隔秒数（默认: 0.1）")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="严格模式：任一交易所抓取失败时直接抛错退出",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = build_session()

    sse_rows: List[Dict[str, str]] = []
    szse_rows: List[Dict[str, str]] = []

    try:
        sse_rows = fetch_sse(session=session, page_size=args.page_size, sleep_sec=args.sleep)
    except Exception as exc:  # noqa: BLE001
        if args.strict:
            raise
        print(f"[WARN] 上交所抓取失败: {exc}")

    try:
        szse_rows = fetch_szse(session=session, page_size=args.page_size, sleep_sec=args.sleep)
    except Exception as exc:  # noqa: BLE001
        if args.strict:
            raise
        print(f"[WARN] 深交所抓取失败: {exc}")

    merged_rows = deduplicate(sse_rows + szse_rows)

    output_path = Path(args.output)
    write_csv(merged_rows, output_path)

    print(f"上交所条数: {len(sse_rows)}")
    print(f"深交所条数: {len(szse_rows)}")
    print(f"去重后总条数: {len(merged_rows)}")
    print(f"输出文件: {output_path.resolve()}")


if __name__ == "__main__":
    main()
