import asyncio
import pandas as pd
from playwright.async_api import async_playwright

EXCLUDE_HREFS = {
    '/promotions/trend',
    '/promotions',
    'https://wibes.ru/clips',
    '',
}

EXCLUDE_CONTAINS = [
    'promo', 'brand', 'trends', 'wibes'
]


def is_valid_category(href: str) -> bool:
    if not href:
        return False
    if href in EXCLUDE_HREFS:
        return False
    if not href.startswith('/catalog/'):
        return False
    if href == '/catalog/tsvety':
        return False
    for bad in EXCLUDE_CONTAINS:
        if bad in href:
            return False
    return True


async def get_active_menu_set(page):
    sets = await page.locator('ul.menu-burger__set').all()
    for s in reversed(sets):
        items = s.locator('li.menu-burger__item')
        if await items.count() > 0:
            return s
    return None


async def parse_subcategories_flat(page, level=1, max_level=8):
    if level > max_level:
        return []
    results = []
    try:
        menu_set = await get_active_menu_set(page)
        if not menu_set:
            return []
        sub_items = menu_set.locator('li.menu-burger__item')

        for i in range(await sub_items.count()):
            sub = sub_items.nth(i)
            link = sub.locator('a.menu-burger__link')
            span = sub.locator('span.menu-burger__link')
            name = None
            has_children = False

            if await link.count() > 0:
                name = (await link.text_content()) or ""
                if name.strip():
                    results.append({'name': name.strip(), 'level': level})

            elif await span.count() > 0:
                name = (await span.text_content()) or ""
                span_class = await span.get_attribute('class') or ""
                has_children = "menu-burger__link--next" in span_class

                if name.strip():
                    results.append({'name': name.strip(), 'level': level})

                if has_children:
                    try:
                        try:
                            await page.evaluate(
                              '(el) => el.click()',
                              await span.element_handle())
                        except Exception:
                            await span.click(timeout=2000, force=True)
                        await asyncio.sleep(1.1)
                        results += await parse_subcategories_flat(
                            page, level=level+1,
                            max_level=max_level)

                        back_btns = page.locator("button.menu-burger__title-link--second.j-menu-return-desktop")
                        count = await back_btns.count()
                        if count > 0:
                            try:
                                await back_btns.nth(-1).click(
                                    timeout=2000,
                                    force=True)
                                await asyncio.sleep(0.4)
                            except Exception:
                                for j in range(count - 2, -1, -1):
                                    try:
                                        await back_btns.nth(j).click(
                                            timeout=2000,
                                            force=True)
                                        await asyncio.sleep(0.4)
                                        break
                                    except Exception:
                                        continue
                        else:
                            print(f'[!] Не  "назад" после {name}')
                    except Exception as ex:
                        print(f"[!] Ошибка вложенной рекурсии {name}: {ex}")
    except Exception as ex:
        print(f'Ошибка в parse_subcategories_flat: {ex}')
    return results


async def parse_main_wildberries_to_excel():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={'width': 1200, 'height': 900},
            user_agent=(
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/91.0.4472.124 Safari/537.36'
            ),
            java_script_enabled=True,
            ignore_https_errors=True
        )
        page = await context.new_page()
        await page.route(
            '**/*.{png,jpg,jpeg,webp}',
            lambda route: route.abort())
        await page.goto('https://www.wildberries.ru/')
        burger_button = page.locator('.nav-element__burger.j-menu-burger-btn')
        await asyncio.sleep(3)
        await burger_button.wait_for(timeout=15000)
        await burger_button.click()
        await asyncio.sleep(2)

        await page.locator('.menu-burger__main-list-item').first.wait_for(
            timeout=15000)
        category_items = page.locator('.menu-burger__main-list-item')
        await asyncio.sleep(2)

        excel_dict = {}
        await asyncio.sleep(5)
        for i in range(await category_items.count()):
            item = category_items.nth(i)
            print(f"[DEBUG] Парсим категорию {i}")
            link = item.locator('a.menu-burger__main-list-link')
            has_link = await link.count() > 0
            href = await link.get_attribute('href') if has_link else None
            menu_id = await item.get_attribute('data-menu-id')
            sheet_name = await link.text_content() if has_link else None

            if not has_link or not is_valid_category(href):
                continue

            rows = [{'name': sheet_name, 'level': 0, 'menu_id': menu_id}]

            try:
                await item.scroll_into_view_if_needed(timeout=2000)
                await item.click(timeout=3000)
                await asyncio.sleep(1)
                subs = await parse_subcategories_flat(page, level=1)
                rows += subs
            except Exception as ex:
                print(f"Ошибка {sheet_name}: {ex}")

            df = pd.DataFrame(rows)

            safe_sheet_name = sheet_name[:31]
            excel_dict[safe_sheet_name] = df

        with pd.ExcelWriter('wb_menu_by_category.xlsx') as writer:
            for sheet, df in excel_dict.items():
                if sheet in writer.sheets:
                    base = sheet[:28]
                    i = 2
                    new_sheet = f"{base}{i}"
                    while new_sheet in writer.sheets:
                        i += 1
                        new_sheet = f"{base}{i}"
                    sheet = new_sheet
                df.to_excel(writer, sheet_name=sheet, index=False)

        await context.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(parse_main_wildberries_to_excel())
