#!/usr/bin/env python3
"""Render LinkedIn carousel slides for the DOE 2029/2030 refrigerator transition."""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

OUT = Path(__file__).resolve().parent
W, H = 1080, 1350

NAVY = (11, 28, 46)
NAVY_2 = (16, 38, 60)
CARD = (20, 46, 72)
TEAL = (38, 166, 154)
TEAL_DK = (22, 110, 104)
GOLD = (232, 176, 74)
WHITE = (248, 250, 252)
MUTED = (168, 188, 206)
ROW_A = (24, 52, 80)
ROW_B = (18, 42, 68)
HEADER = (18, 96, 92)
LINE = (42, 78, 108)
FOOTER = (132, 156, 176)

FONT_REG = "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf"
FONT_BOLD = "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf"


def font(size, bold=False):
    return ImageFont.truetype(FONT_BOLD if bold else FONT_REG, size)


def wrap(draw, text, fnt, max_w):
    words = text.split()
    lines, cur = [], ""
    for word in words:
        trial = word if not cur else f"{cur} {word}"
        if draw.textlength(trial, font=fnt) <= max_w:
            cur = trial
        else:
            if cur:
                lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)
    return lines or [""]


def new_canvas():
    img = Image.new("RGB", (W, H), NAVY)
    d = ImageDraw.Draw(img)
    d.rectangle((0, 0, W, 8), fill=TEAL)
    d.rectangle((0, H - 8, W, H), fill=TEAL)
    return img, d


def header(d, kicker, title, subtitle=None):
    d.text((64, 36), kicker.upper(), font=font(22, True), fill=TEAL)
    d.text((64, 78), title, font=font(42, True), fill=WHITE)
    y = 140
    if subtitle:
        for line in wrap(d, subtitle, font(24), W - 128):
            d.text((64, y), line, font=font(24), fill=MUTED)
            y += 34
        y += 12
    d.line((64, y, W - 64, y), fill=LINE, width=2)
    return y + 28


def footer(d, page, total=5):
    d.line((64, H - 78, W - 64, H - 78), fill=LINE, width=1)
    d.text(
        (64, H - 58),
        "Source: 10 CFR 430.32  ·  Applies to products manufactured or imported into the U.S.",
        font=font(18),
        fill=FOOTER,
    )
    label = f"{page} / {total}"
    tw = d.textlength(label, font=font(18, True))
    d.text((W - 64 - tw, H - 58), label, font=font(18, True), fill=TEAL)


def rounded_rect(d, box, fill, radius=18):
    d.rounded_rectangle(box, radius=radius, fill=fill)


def draw_table(d, x, y, width, col_specs, rows, row_h=None, font_size=22, header_size=20):
    """col_specs: list of (header, weight, align) where weight is relative width."""
    total_w = sum(spec[1] for spec in col_specs)
    col_w = [int(width * spec[1] / total_w) for spec in col_specs]
    col_w[-1] += width - sum(col_w)
    f_body = font(font_size)
    f_head = font(header_size, True)
    f_body_b = font(font_size, True)

    # Measure wrapped rows first if row_h is None
    wrapped = []
    for row in rows:
        cell_lines = []
        max_lines = 1
        for i, cell in enumerate(row):
            pad = 20
            lines = wrap(d, str(cell), f_body, col_w[i] - pad * 2)
            cell_lines.append(lines)
            max_lines = max(max_lines, len(lines))
        wrapped.append((cell_lines, max_lines))

    header_h = 56
    rounded_rect(d, (x, y, x + width, y + header_h), HEADER, 0)
    cx = x
    for i, spec in enumerate(col_specs):
        d.text((cx + 16, y + 16), spec[0], font=f_head, fill=WHITE)
        cx += col_w[i]
    y += header_h

    for r, (cell_lines, nlines) in enumerate(wrapped):
        h = row_h if row_h else 18 + nlines * (font_size + 8)
        bg = ROW_A if r % 2 == 0 else ROW_B
        d.rectangle((x, y, x + width, y + h), fill=bg)
        cx = x
        for i, lines in enumerate(cell_lines):
            align = col_specs[i][2] if len(col_specs[i]) > 2 else "left"
            ty = y + (h - nlines * (font_size + 6)) // 2
            for line in lines:
                tx = cx + 16
                if align == "right":
                    tx = cx + col_w[i] - 16 - d.textlength(line, font=f_body)
                elif align == "center":
                    tx = cx + (col_w[i] - d.textlength(line, font=f_body)) / 2
                use = f_body_b if i == 0 else f_body
                if align != "left":
                    use = f_body
                d.text((tx, ty), line, font=use if i == 0 and align == "left" else f_body, fill=WHITE)
                ty += font_size + 6
            cx += col_w[i]
        y += h
    return y


def badge(d, x, y, w, h, title, body_lines, accent):
    rounded_rect(d, (x, y, x + w, y + h), CARD, 22)
    d.rectangle((x, y, x + 10, y + h), fill=accent)
    d.text((x + 32, y + 22), title, font=font(26, True), fill=accent)
    ty = y + 64
    for line in body_lines:
        d.text((x + 32, ty), line, font=font(22), fill=WHITE)
        ty += 32


def slide_1():
    img, d = new_canvas()
    d.text((64, 48), "U.S. DEPARTMENT OF ENERGY", font=font(22, True), fill=TEAL)
    d.text((64, 92), "Refrigerator & freezer", font=font(52, True), fill=WHITE)
    d.text((64, 156), "standards are changing.", font=font(52, True), fill=WHITE)

    for i, line in enumerate(
        wrap(
            d,
            "A two-phase compliance transition under 10 CFR 430.32. Current 2014 limits stay in force until the dates below, by product class.",
            font(26),
            W - 128,
        )
    ):
        d.text((64, 240 + i * 36), line, font=font(26), fill=MUTED)

    badge(
        d,
        64,
        380,
        452,
        300,
        "JANUARY 31, 2029",
        [
            "Built-in products",
            "Compact products",
            "Chest freezers",
            "Selected special classes",
            "including new class 9A-BI",
        ],
        GOLD,
    )
    badge(
        d,
        564,
        380,
        452,
        300,
        "JANUARY 31, 2030",
        [
            "Freestanding standard",
            "refrigerator-freezers",
            "Top / side / bottom mount",
            "All-refrigerators",
            "Upright auto-defrost freezers",
        ],
        TEAL,
    )

    rounded_rect(d, (64, 720, W - 64, 1120), CARD, 22)
    d.text((96, 752), "What else is changing", font=font(28, True), fill=WHITE)
    points = [
        ("Tighter kWh/yr limits", "Maximum annual energy use is calculated from adjusted volume (AV)."),
        ("Icemaker factor I", "No separate icemaker classes. I = 1 adds 28 kWh/yr; I = 0 if none."),
        ("Door coefficients K", "Transparent door +10%. Door-in-door +6%. Extra external doors +2% each."),
        ("National impact", "DOE estimates ~11% energy savings vs. no-new-standards, or 5.6 quads over 30 years."),
    ]
    y = 810
    for title, body in points:
        d.ellipse((96, y + 8, 112, y + 24), fill=TEAL)
        d.text((132, y), title, font=font(24, True), fill=GOLD)
        for line in wrap(d, body, font(22), W - 220):
            d.text((132, y + 34), line, font=font(22), fill=MUTED)
            y += 28
        y += 42

    footer(d, 1)
    img.save(OUT / "01_overview.png", "PNG")


def slide_2():
    img, d = new_canvas()
    y = header(
        d,
        "Compliance calendar",
        "Who must comply when",
        "Dates apply to products manufactured in, or imported into, the United States.",
    )
    specs = [
        ("Date", 1.15, "left"),
        ("Product group", 1.7, "left"),
        ("Product classes", 2.15, "left"),
    ]
    rows = [
        ["Jan 31, 2029", "Built-in refrigerator-freezers and freezers", "3-BI, 3A-BI, 4-BI, 5-BI, 5A-BI, 7-BI, 9-BI, 9A-BI"],
        ["Jan 31, 2029", "Bottom-mount with through-the-door ice (freestanding)", "5A"],
        ["Jan 31, 2029", "Upright manual-defrost freezers", "8"],
        ["Jan 31, 2029", "Chest freezers (except compact)", "10, 10A"],
        ["Jan 31, 2029", "Compact refrigerators, refrigerator-freezers, and freezers", "11, 11A, 12, 13, 13A, 14, 15, 16, 17, 18"],
        ["Jan 31, 2030", "Freestanding standard refrigerator-freezers", "1, 1A, 2, 3, 3A, 4, 5, 6, 7"],
        ["Jan 31, 2030", "Freestanding upright auto-defrost freezers", "9"],
    ]
    y = draw_table(d, 48, y, W - 96, specs, rows, font_size=21, header_size=20)

    y += 28
    rounded_rect(d, (48, y, W - 48, y + 230), CARD, 18)
    d.text((76, y + 24), "Until then", font=font(24, True), fill=GOLD)
    note = (
        "Products still follow the September 15, 2014 standards in Table 1 to 10 CFR 430.32(a)(1) "
        "until their class-specific 2029 or 2030 date. Limits do not apply to refrigerators / "
        "refrigerator-freezers over 39 ft³ or freezers over 30 ft³."
    )
    ty = y + 68
    for line in wrap(d, note, font(22), W - 160):
        d.text((76, ty), line, font=font(22), fill=MUTED)
        ty += 32

    footer(d, 2)
    img.save(OUT / "02_compliance_calendar.png", "PNG")


def slide_3():
    img, d = new_canvas()
    y = header(
        d,
        "Table 2  ·  10 CFR 430.32(a)(2)",
        "Maximum energy use from Jan 31, 2029",
        "kWh/yr from adjusted volume AV (ft³). I = 1 with automatic icemaker. K = door coefficient.",
    )
    specs = [
        ("Class", 0.7, "left"),
        ("Product", 2.15, "left"),
        ("Max energy use (kWh/yr)", 2.0, "left"),
    ]
    rows = [
        ["3-BI", "Built-in RF, top freezer", "8.24AV + 238.4 + 28I"],
        ["3A-BI", "Built-in all-refrigerator, auto defrost", "(7.22AV + 205.7) × K3ABI"],
        ["4-BI", "Built-in RF, side freezer", "(8.79AV + 307.4) × K4BI + 28I"],
        ["5-BI", "Built-in RF, bottom freezer", "(8.65AV + 309.9) × K5BI + 28I"],
        ["5A", "RF, bottom freezer + through-the-door ice", "(7.76AV + 351.9) × K5A"],
        ["5A-BI", "Built-in RF, bottom freezer + TTD ice", "(8.21AV + 370.7) × K5ABI"],
        ["7-BI", "Built-in RF, side freezer + TTD ice", "(8.82AV + 384.1) × K7BI"],
        ["8", "Upright freezer, manual defrost", "5.57AV + 193.7"],
        ["9-BI", "Built-in upright freezer, auto defrost", "(9.37AV + 247.9) × K9BI + 28I"],
        ["9A-BI", "Built-in upright freezer, auto defrost + TTD ice", "9.86AV + 288.9"],
        ["10", "Chest freezer (except compact)", "7.29AV + 107.8"],
        ["10A", "Chest freezer, auto defrost", "10.24AV + 148.1"],
        ["11", "Compact RF / refrigerator, manual defrost", "7.68AV + 214.5"],
        ["11A", "Compact all-refrigerator, manual defrost", "6.66AV + 186.2"],
        ["12", "Compact RF, partial auto defrost", "(5.32AV + 302.2) × K12"],
        ["13", "Compact RF, top freezer, auto defrost", "10.62AV + 305.3 + 28I"],
        ["13A", "Compact all-refrigerator, auto defrost", "(8.25AV + 233.4) × K13A"],
        ["14", "Compact RF, side freezer, auto defrost", "6.14AV + 411.2 + 28I"],
        ["15", "Compact RF, bottom freezer, auto defrost", "10.62AV + 305.3 + 28I"],
        ["16", "Compact upright freezer, manual defrost", "7.35AV + 191.8"],
        ["17", "Compact upright freezer, auto defrost", "9.15AV + 316.7"],
        ["18", "Compact chest freezer", "7.86AV + 107.8"],
    ]
    draw_table(d, 36, y, W - 72, specs, rows, font_size=18, header_size=18)
    footer(d, 3)
    img.save(OUT / "03_standards_2029.png", "PNG")


def slide_4():
    img, d = new_canvas()
    y = header(
        d,
        "Table 4  ·  10 CFR 430.32(a)(3)",
        "Maximum energy use from Jan 31, 2030",
        "Freestanding standard classes. kWh/yr from AV (ft³). I = 1 with automatic icemaker.",
    )
    specs = [
        ("Class", 0.7, "left"),
        ("Product", 2.2, "left"),
        ("Max energy use (kWh/yr)", 2.0, "left"),
    ]
    rows = [
        ["1", "RF / refrigerators other than all-refrigerators, manual defrost", "6.79AV + 191.3"],
        ["1A", "All-refrigerators, manual defrost", "5.77AV + 164.6"],
        ["2", "RF, partial automatic defrost", "(6.79AV + 191.3) × K2"],
        ["3", "RF, auto defrost, top-mounted freezer", "6.86AV + 198.6 + 28I"],
        ["3A", "All-refrigerators, automatic defrost", "(6.01AV + 171.4) × K3A"],
        ["4", "RF, auto defrost, side-mounted freezer", "(7.28AV + 254.9) × K4 + 28I"],
        ["5", "RF, auto defrost, bottom-mounted freezer", "(7.61AV + 272.6) × K5 + 28I"],
        ["6", "RF, auto defrost, top freezer + TTD ice", "7.14AV + 280.0"],
        ["7", "RF, auto defrost, side freezer + TTD ice", "(7.31AV + 322.5) × K7"],
        ["9", "Upright freezers, automatic defrost", "(7.33AV + 194.1) × K9 + 28I"],
    ]
    y = draw_table(d, 36, y - 4, W - 72, specs, rows, font_size=22, header_size=20)

    y += 32
    rounded_rect(d, (36, y, W - 36, min(y + 280, H - 100)), CARD, 18)
    d.text((64, y + 22), "How to read the equation", font=font(24, True), fill=GOLD)
    bullets = [
        "AV = total adjusted volume in cubic feet (appendices A and B to subpart B).",
        "I = 1 if the product has an automatic icemaker; I = 0 if it does not.",
        "K2, K3A, K4, K5, K7, K9 are door coefficients from Table 5 (next slide).",
        "Round the result to the nearest kWh/yr; round .5 upward.",
    ]
    ty = y + 68
    for b in bullets:
        for line in wrap(d, "•  " + b, font(22), W - 140):
            d.text((64, ty), line, font=font(22), fill=MUTED)
            ty += 30
        ty += 6

    footer(d, 4)
    img.save(OUT / "04_standards_2030.png", "PNG")


def slide_5():
    img, d = new_canvas()
    y = header(
        d,
        "Tables 3 and 5  ·  Door coefficients",
        "K factors for special doors",
        "These multipliers raise the allowed kWh/yr for transparent doors, door-in-door, and extra doors.",
    )

    d.text((48, y), "How the allowance works", font=font(24, True), fill=WHITE)
    y += 40
    chips = [
        ("Transparent door", "K = 1.10  (+10%)", TEAL),
        ("Door-in-door", "K = 1.06  (+6%)", GOLD),
        ("Each extra external door", "+2%  (formula with Nd)", (120, 164, 214)),
    ]
    x = 48
    for title, body, color in chips:
        rounded_rect(d, (x, y, x + 318, y + 110), CARD, 16)
        d.rectangle((x, y, x + 8, y + 110), fill=color)
        d.text((x + 24, y + 22), title, font=font(20, True), fill=WHITE)
        d.text((x + 24, y + 58), body, font=font(20), fill=MUTED)
        x += 334
    y += 140

    d.text((48, y), "2029 classes  ·  Table 3", font=font(22, True), fill=TEAL)
    y += 36
    specs = [
        ("K", 0.7, "left"),
        ("Transparent door", 1.15, "left"),
        ("Door-in-door", 1.05, "left"),
        ("Added external doors", 1.7, "left"),
    ]
    rows_2029 = [
        ["K3ABI", "1.10", "1.00", "1.00"],
        ["K4BI / K5BI / K7BI", "1.10", "1.06", "1 + 0.02 × (Nd − 2)"],
        ["K5A / K5ABI", "1.10", "1.06", "1 + 0.02 × (Nd − 3)"],
        ["K9BI / K12", "1.00", "1.00", "1 + 0.02 × (Nd − 1)"],
        ["K13A", "1.10", "1.00", "1.00"],
    ]
    y = draw_table(d, 48, y, W - 96, specs, rows_2029, font_size=20, header_size=18)

    y += 28
    d.text((48, y), "2030 classes  ·  Table 5", font=font(22, True), fill=TEAL)
    y += 36
    rows_2030 = [
        ["K2", "1.00", "1.00", "1 + 0.02 × (Nd − 1)"],
        ["K3A", "1.10", "1.00", "1.00"],
        ["K4 / K5 / K7", "1.10", "1.06", "1 + 0.02 × (Nd − 2)"],
        ["K9", "1.00", "1.00", "1 + 0.02 × (Nd − 1)"],
    ]
    y = draw_table(d, 48, y, W - 96, specs, rows_2030, font_size=20, header_size=18)

    y += 24
    note = (
        "Nd = number of external doors. Maximum Nd: 2 for K2 and K12; 3 for K9BI; 5 for all other K values."
    )
    for line in wrap(d, note, font(20), W - 96):
        d.text((48, y), line, font=font(20), fill=MUTED)
        y += 28

    footer(d, 5)
    img.save(OUT / "05_door_coefficients.png", "PNG")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    slide_1()
    slide_2()
    slide_3()
    slide_4()
    slide_5()
    print("Wrote slides to", OUT)
