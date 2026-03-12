#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
from collections import defaultdict
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

REGION_ORDER = [
#    "Ukraine",
    "autonomous_Republic_of_Crimea",
    "11_reg_Vinnytsya",
    "12_reg_Volyn",
    "13_reg_Dnipropetrovsk",
    "14_reg_Donetsk",
    "15_reg_Zhytomyr",
    "16_reg_Zakarpattya",
    "17_reg_Zaporizhzhya",
    "18_reg_Ivano-Frankivsk",
    "19_reg_Kyiv",
    "20_reg_Kirovohrad",
    "21_reg_Luhansk",
    "22_reg_Lviv",
    "23_reg_Mykolayiv",
    "24_reg_Odesa",
    "25_reg_Poltava",
    "26_reg_Rivne",
    "27_reg_Sumy",
    "28_reg_Ternopil",
    "29_reg_Kharkiv",
    "30_reg_Kherson",
    "31_reg_Khmelnytskiy",
    "32_reg_Cherkasy",
    "33_reg_Chernivtsi",
    "34_reg_Chernihiv",
    "35_reg_Kyiv_city",
    "city_Sevastopol",
]

REGION_INDEX = {value: index for index, value in enumerate(REGION_ORDER)}


def detect_csv_dialect(sample_text: str) -> csv.Dialect:
    try:
        return csv.Sniffer().sniff(sample_text, delimiters=",;\t")
    except csv.Error:
        class SimpleDialect(csv.excel):
            delimiter = ","
        return SimpleDialect()


def is_archive_indicator(csv_path: Path) -> bool:
    return bool(re.search(r"-a$", csv_path.stem))


def get_field_name(fieldnames: list[str], target: str) -> str | None:
    for field in fieldnames:
        if field and field.strip().lower() == target.lower():
            return field
    return None


def region_sort_key(row: dict, region_field: str):
    region = (row.get(region_field) or "").strip()
    if region == "":
        return (0, -1, "")
    if region in REGION_INDEX:
        return (1, REGION_INDEX[region], "")
    return (2, 999999, region.lower())


def process_csv(csv_path: Path) -> tuple[bool, int]:
    raw_text = csv_path.read_text(encoding="utf-8-sig")
    if not raw_text.strip():
        return False, 0

    dialect = detect_csv_dialect(raw_text[:4096])

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, dialect=dialect)
        fieldnames = reader.fieldnames or []
        if not fieldnames:
            return False, 0

        year_field = get_field_name(fieldnames, "Year")
        region_field = get_field_name(fieldnames, "Region")
        value_field = get_field_name(fieldnames, "Value")

        if not region_field or not year_field:
            return False, 0

        rows = list(reader)

    if not rows:
        return True, 0

    # Группируем строки по году
    rows_by_year = defaultdict(list)
    for row in rows:
        year = (row.get(year_field) or "").strip()
        rows_by_year[year].append(row)

    new_rows = list(rows)
    added_count = 0

    for year, year_rows in rows_by_year.items():
        # Берём только строки этого года, где Region заполнен
        region_rows = [r for r in year_rows if (r.get(region_field) or "").strip() != ""]
        if not region_rows:
            continue

        existing_regions = {(r.get(region_field) or "").strip() for r in region_rows}
        missing_regions = [r for r in REGION_ORDER if r not in existing_regions]

        if not missing_regions:
            continue

        # Любая строка этого года с заполненным Region подходит как шаблон
        template_row = dict(region_rows[0])

        for missing_region in missing_regions:
            new_row = dict(template_row)
            new_row[region_field] = missing_region
            if value_field:
                new_row[value_field] = ""
            new_rows.append(new_row)
            added_count += 1

    if added_count == 0:
        return True, 0

    # Сохраняем более предсказуемый порядок:
    # сначала Year, внутри года пустой Region / известные регионы / прочие
    new_rows.sort(
        key=lambda row: (
            (row.get(year_field) or "").strip(),
            *region_sort_key(row, region_field),
        )
    )

    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, dialect=dialect)
        writer.writeheader()
        writer.writerows(new_rows)

    return True, added_count


def main():
    if not DATA_DIR.exists():
        print(f"[ERROR] data dir not found: {DATA_DIR}")
        return

    scanned = 0
    processed = 0
    changed_files = 0
    added_rows_total = 0
    skipped_archive = 0

    for csv_path in sorted(DATA_DIR.glob("indicator_*.csv")):
        scanned += 1

        if is_archive_indicator(csv_path):
            skipped_archive += 1
            print(f"[SKIP] archive: {csv_path.name}")
            continue

        has_region, added_rows = process_csv(csv_path)
        if not has_region:
            continue

        processed += 1

        if added_rows > 0:
            changed_files += 1
            added_rows_total += added_rows
            print(f"[UPDATED] {csv_path.name} — added rows: {added_rows}")
        else:
            print(f"[OK]      {csv_path.name}")

    print()
    print("[DONE]")
    print(f"CSV scanned: {scanned}")
    print(f"CSV with Region+Year: {processed}")
    print(f"Changed files: {changed_files}")
    print(f"Added rows total: {added_rows_total}")
    print(f"Skipped archive: {skipped_archive}")


if __name__ == "__main__":
    main()