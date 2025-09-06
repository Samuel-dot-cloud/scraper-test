import asyncio, json, logging, re
from typing import List, Tuple, Set, Dict, Any, Optional, TypedDict
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, Page, Frame, TimeoutError as PWTimeout

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("scraper")

START_URL       = "https://transparencia.e-publica.net/epublica-portal/#/palmeira/portal/compras/contratoTable"
DETAIL_URL_PART = "/portal/compras/contratoView"
TABLE_URL_PART  = "/portal/compras/contratoTable"

ROW_DETAIL_LINK = (
    "table tbody tr:has(a[href*='#/palmeira/portal/compras/contratoView']) "
    "a[href*='#/palmeira/portal/compras/contratoView']"
)
ANY_DETAIL_LINK = "a[href*='#/palmeira/portal/compras/contratoView']"
VISIBLE_ROW_LINK = f"{ROW_DETAIL_LINK}:visible, {ANY_DETAIL_LINK}:visible"

class ContractData(TypedDict):
    contract: Optional[str]
    amount: Optional[str]


def _extract(text: str) -> ContractData:
    t = " ".join(text.split())
    m1 = re.search(r"Contrato\s+([0-9./-]{3,})", t, re.I)
    m2 = re.search(r"Valor\s*total\s*R\$\s*([\d.\,]+)", t, re.I)
    return {"contract": m1.group(1) if m1 else None, "amount": m2.group(1) if m2 else None}

async def extract_from_context(ctx: Page | Frame) -> ContractData:
    try:
        ct = await ctx.locator("text=/Contrato\\s+[0-9./-]{3,}/").first.inner_text(timeout=2500)
        vt = await ctx.locator("text=/Valor\\s*total\\s*R\\$/").first.inner_text(timeout=2500)
        d = _extract(f"{ct} {vt}")
        if d["contract"] and d["amount"]:
            return d
    except PWTimeout:
        pass
    html = await ctx.content()
    return _extract(BeautifulSoup(html, "html.parser").get_text(" "))

async def pick_ctx(page: Page) -> Page | Frame:
    if await page.locator("text=Dados do contrato").first.count():
        return page
    for fr in page.frames:
        if fr == page.main_frame:
            continue
        try:
            if await fr.locator("text=Dados do contrato").first.count():
                return fr
        except Exception:
            pass
    return page

async def wait_table_ready(page: Page, timeout_ms: int = 10000):
    step, waited = 200, 0
    while waited < timeout_ms:
        if await page.locator(VISIBLE_ROW_LINK).count() > 0:
            return
        try: await page.mouse.wheel(0, 600)
        except Exception: pass
        await page.wait_for_timeout(step)
        waited += step
    await page.wait_for_selector("table tbody", timeout=4000)

async def page_signature(page: Page) -> str:
    hrefs = await page.locator(VISIBLE_ROW_LINK).evaluate_all(
        "els => els.map(a => a.getAttribute('href')).filter(Boolean)"
    )
    hrefs = [h for h in hrefs if DETAIL_URL_PART in h]
    return "|".join(hrefs)

async def click_next(page: Page) -> bool:
    prev_sig = await page_signature(page)
    try:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(100)
    except Exception:
        pass

    btn = page.locator(".pagination a.pagination-next").first
    if not await btn.count():
        log.info("Next button not found.")
        return False

    cls  = (await btn.get_attribute("class") or "").lower()
    aria = (await btn.get_attribute("aria-disabled") or "").lower()
    if "disabled" in cls or aria in ("true", "disabled"):
        log.info("Next button disabled.")
        return False

    try:
        await btn.scroll_into_view_if_needed()
        await btn.click()
    except Exception:
        await btn.evaluate("el => el.click()")
    log.info("Clicked Next")

    step, waited, changed = 200, 0, False
    while waited < 10000:
        sig = await page_signature(page)
        if sig and sig != prev_sig:
            changed = True
            break
        await page.wait_for_timeout(step)
        waited += step
    if not changed:
        return False

    await page.evaluate("window.scrollTo(0, 0)")
    await wait_table_ready(page)
    return True

async def collect_rows(page: Page, visited: Set[str]) -> List[Tuple[str, str]]:
    """
    One JS roundtrip: get [{href, text}] for visible anchors, then filter in Python.
    """
    items: List[Dict[str, Any]] = await page.locator(VISIBLE_ROW_LINK).evaluate_all(
        """els => els.map(a => {
              const href = a.getAttribute('href') || '';
              const tr = a.closest('tr');
              const text = (tr?.innerText || a.innerText || '').replace(/\\s+/g,' ').trim();
              return { href, text };
           })"""
    )
    out: List[Tuple[str, str]] = []
    seen: Set[str] = set()
    for it in items:
        href = it["href"]
        if not href or DETAIL_URL_PART not in href:
            continue
        if href in visited or href in seen:
            continue
        seen.add(href)
        out.append((href, it["text"]))
    return out


def make_abs_url(current_url: str, href: str) -> str:
    """
    Convert '#/palmeira/...' to absolute using current page base.
    """
    base = current_url.split("#")[0]
    return (base + href) if href.startswith("#") else href

async def open_details_and_extract(page: Page, href: str) -> ContractData:
    """
    Open details in a new tab
    """
    url = make_abs_url(page.url, href)
    context = page.context
    dp = await context.new_page()
    try:
        await dp.goto(url, wait_until="domcontentloaded")
        try:
            await dp.wait_for_selector("text=Dados do contrato", timeout=3000)
        except PWTimeout:
            try:
                await dp.wait_for_selector("text=/Contrato\\s+[0-9./-]{3,}/", timeout=3000)
            except PWTimeout:
                pass

        ctx = await pick_ctx(dp)
        return await extract_from_context(ctx)
    finally:
        await dp.close()

async def process_current_table(page: Page, stash: list[ContractData], visited: Set[str]):
    await wait_table_ready(page)
    rows = await collect_rows(page, visited)
    if not rows:
        return

    log.info(f"Found {len(rows)} new rows")
    for idx, (href, row_text) in enumerate(rows, 1):
        log.info(f"Clicking row {idx}: {row_text}")
        try:
            details = await open_details_and_extract(page, href)
            if details.get("contract") and details.get("amount"):
                log.info(f"Extracted: {details}")
                stash.append(details)
            else:
                log.warning(f"Failed to extract fields from: {href}")
        finally:
            visited.add(href)

async def scrape() -> None:
    out = "contracts.jsonl"
    stash: list[ContractData] = []
    visited: Set[str] = set()
    log.info(f"Starting scraper for: {START_URL}")

    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context()
        page = await ctx.new_page()
        page.set_default_timeout(30000)

        log.info("Navigating to target URL...")
        await page.goto(START_URL, wait_until="domcontentloaded")
        log.info("Successfully loaded initial page")

        page_idx = 1
        while True:
            log.info(f"Processing page {page_idx}...")
            await process_current_table(page, stash, visited)
            if not await click_next(page):
                log.info("No more pages. Done.")
                break
            page_idx += 1

        log.info(f"Writing {len(stash)} contracts to {out}")
        with open(out, "w", encoding="utf-8") as f:
            for item in stash:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        await ctx.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape())
