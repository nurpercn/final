#!/usr/bin/env python3
"""Render LinkedIn carousel slides for worst-case model selection."""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from reportlab.lib.pagesizes import portrait
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas as pdf_canvas

OUT = Path(__file__).resolve().parent
W, H = 1080, 1350
FEED_W, FEED_H = 1200, 627

NAVY = (11, 28, 46)
NAVY_2 = (16, 38, 60)
CARD = (20, 46, 72)
TEAL = (38, 166, 154)
TEAL_DK = (22, 110, 104)
GOLD = (232, 176, 74)
WHITE = (248, 250, 252)
MUTED = (168, 188, 206)
ROW_A = (24, 52, 80)
LINE = (42, 78, 108)
FOOTER = (132, 156, 176)
DIM = (28, 58, 86)
HIGHLIGHT = (38, 166, 154)

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


def new_canvas(size=None):
    w, h = size or (W, H)
    img = Image.new("RGB", (w, h), NAVY)
    d = ImageDraw.Draw(img)
    d.rectangle((0, 0, w, 8), fill=TEAL)
    d.rectangle((0, h - 8, w, h), fill=TEAL)
    return img, d


def header(d, kicker, title, subtitle=None):
    d.text((64, 36), kicker.upper(), font=font(20, True), fill=TEAL)
    y = 72
    for line in wrap(d, title, font(40, True), W - 128):
        d.text((64, y), line, font=font(40, True), fill=WHITE)
        y += 48
    if subtitle:
        y += 4
        for line in wrap(d, subtitle, font(23), W - 128):
            d.text((64, y), line, font=font(23), fill=MUTED)
            y += 32
        y += 8
    d.line((64, y, W - 64, y), fill=LINE, width=2)
    return y + 28


def footer(d, page, total=5):
    d.line((64, H - 78, W - 64, H - 78), fill=LINE, width=1)
    d.text(
        (64, H - 58),
        "Certification strategy  ·  Representative testing of a product family",
        font=font(17),
        fill=FOOTER,
    )
    label = f"{page} / {total}"
    tw = d.textlength(label, font=font(18, True))
    d.text((W - 64 - tw, H - 58), label, font=font(18, True), fill=TEAL)


def rounded_rect(d, box, fill, radius=18, outline=None, width=1):
    d.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def draw_wrapped(d, x, y, text, fnt, fill, max_w, leading=None):
    leading = leading or (fnt.size + 10)
    for line in wrap(d, text, fnt, max_w):
        d.text((x, y), line, font=fnt, fill=fill)
        y += leading
    return y


def mini_appliance(d, x, y, w, h, fill, accent=None, selected=False):
    rounded_rect(d, (x, y, x + w, y + h), fill, 8)
    door = (x + 6, y + 10, x + w - 6, y + h - 8)
    d.rounded_rectangle(door, radius=4, outline=accent or LINE, width=2 if selected else 1)
    handle_y = y + h * 0.42
    d.rectangle((x + w - 12, handle_y, x + w - 8, handle_y + 16), fill=accent or LINE)
    if selected:
        d.ellipse((x + w / 2 - 5, y + 4, x + w / 2 + 5, y + 10), fill=GOLD)


def slide_1():
    img, d = new_canvas()
    y = header(
        d,
        "Certification strategy",
        "Why this model?",
        "Not the other 47 in the family.",
    )

    # 8 x 6 grid = 48 models
    cols, rows = 8, 6
    grid_x, grid_y = 64, y + 8
    grid_w, grid_h = W - 128, 430
    gap_x, gap_y = 12, 12
    cell_w = (grid_w - gap_x * (cols - 1)) // cols
    cell_h = (grid_h - gap_y * (rows - 1)) // rows
    selected = (4, 2)

    for r in range(rows):
        for c in range(cols):
            cx = grid_x + c * (cell_w + gap_x)
            cy = grid_y + r * (cell_h + gap_y)
            is_sel = (c, r) == selected
            mini_appliance(
                d,
                cx,
                cy,
                cell_w,
                cell_h,
                GOLD if is_sel else DIM,
                accent=NAVY if is_sel else LINE,
                selected=is_sel,
            )

    y = grid_y + grid_h + 36
    rounded_rect(d, (64, y, W - 64, y + 210), CARD, 22)
    d.rectangle((64, y, 74, y + 210), fill=GOLD)
    d.text((96, y + 28), "The usual shortcut", font=font(22, True), fill=GOLD)
    y = draw_wrapped(
        d,
        96,
        y + 68,
        "Pick the biggest, most powerful or most complex model and call it the worst case.",
        font(24),
        WHITE,
        W - 192,
        34,
    )
    y += 18
    d.text((96, y), "That is often the wrong question.", font=font(24, True), fill=WHITE)

    footer(d, 1)
    img.save(OUT / "01_hook.png", "PNG")
    return img


def slide_2():
    img, d = new_canvas()
    y = header(
        d,
        "Worst case for what?",
        "One model is not worst-case for every test",
        "A configuration can be the most demanding for one requirement and irrelevant for another.",
    )

    cards = [
        (
            TEAL,
            "Temperature / heating",
            [
                "Often driven by:",
                "• higher power input",
                "• component loading",
                "• restricted ventilation",
                "• demanding operating mode",
            ],
            "Model A may be worst case",
        ),
        (
            GOLD,
            "Insulation / construction",
            [
                "Often driven by:",
                "• smaller creepage / clearance",
                "• different insulation system",
                "• thinner walls or barriers",
                "• a lower-rated variant",
            ],
            "Model B may be worst case",
        ),
    ]
    card_w = (W - 64 * 2 - 28) // 2
    card_h = 520
    x = 64
    for accent, title, lines, footer_txt in cards:
        rounded_rect(d, (x, y, x + card_w, y + card_h), CARD, 22)
        d.rectangle((x, y, x + 10, y + card_h), fill=accent)
        d.text((x + 36, y + 28), title, font=font(26, True), fill=accent)
        ty = y + 90
        for i, line in enumerate(lines):
            fill = MUTED if i == 0 else WHITE
            fnt = font(22, i == 0)
            d.text((x + 36, ty), line, font=fnt, fill=fill)
            ty += 42
        rounded_rect(d, (x + 28, y + card_h - 92, x + card_w - 20, y + card_h - 28), NAVY_2, 14)
        d.text((x + 44, y + card_h - 74), footer_txt, font=font(20, True), fill=accent)
        x += card_w + 28

    y = y + card_h + 28
    note = "Higher power is not automatically the worst case. A lower value of a parameter can be more critical."
    draw_wrapped(d, 64, y, note, font(23), MUTED, W - 128, 32)

    footer(d, 2)
    img.save(OUT / "02_worst_case_for_what.png", "PNG")
    return img


def slide_3():
    img, d = new_canvas()
    y = header(
        d,
        "Start with the family",
        "Map the differences first",
        "Worst-case selection starts with what actually varies — not with a favourite model.",
    )

    items = [
        ("01", "Components", "Which parts, suppliers or constructions change across the family?"),
        ("02", "Ratings", "Power, voltage, current, capacity, speed, load — what moves?"),
        ("03", "Construction & dimensions", "Size, materials, barriers, ventilation paths, mounting."),
        ("04", "Insulation", "Insulation system, creepage, clearance, working voltage."),
        ("05", "Protective devices", "Fuses, thermostats, software limits, cut-outs."),
        ("06", "Controls & modes", "Control functions and operating modes that change the stress."),
    ]

    col_w = (W - 128 - 24) // 2
    row_h = 150
    for i, (num, title, body) in enumerate(items):
        c = i % 2
        r = i // 2
        x = 64 + c * (col_w + 24)
        yy = y + r * (row_h + 18)
        rounded_rect(d, (x, yy, x + col_w, yy + row_h), CARD, 18)
        d.text((x + 24, yy + 22), num, font=font(22, True), fill=TEAL)
        d.text((x + 80, yy + 22), title, font=font(24, True), fill=WHITE)
        draw_wrapped(d, x + 24, yy + 68, body, font(20), MUTED, col_w - 48, 28)

    footer(d, 3)
    img.save(OUT / "03_map_the_family.png", "PNG")
    return img


def slide_4():
    img, d = new_canvas()
    y = header(
        d,
        "Then filter by the requirement",
        "Not every difference matters",
        "Keep only the differences that can influence the test you are evaluating.",
    )

    d.text((64, y), "Example — temperature-related tests", font=font(24, True), fill=TEAL)
    y += 44

    drivers = [
        ("Power input", "Higher dissipation, hotter surfaces"),
        ("Component loading", "Stressed parts, not just the nameplate"),
        ("Ventilation", "Restricted airflow can beat higher power"),
        ("Construction", "Materials, layout, heat paths"),
        ("Operating conditions", "Mode, duty cycle, ambient, load"),
    ]
    for title, body in drivers:
        rounded_rect(d, (64, y, W - 64, y + 78), CARD, 14)
        d.ellipse((86, y + 26, 106, y + 46), fill=TEAL)
        d.text((128, y + 14), title, font=font(22, True), fill=WHITE)
        d.text((128, y + 44), body, font=font(20), fill=MUTED)
        y += 90

    y += 8
    rounded_rect(d, (64, y, W - 64, y + 130), (32, 42, 58), 18)
    d.rectangle((64, y, 74, y + 130), fill=GOLD)
    draw_wrapped(
        d,
        96,
        y + 28,
        "Other safety tests may be driven by completely different parameters. There may not be one worst-case model for the entire product.",
        font(22),
        WHITE,
        W - 176,
        32,
    )

    footer(d, 4)
    img.save(OUT / "04_parameter_to_test.png", "PNG")
    return img


def slide_5():
    img, d = new_canvas()
    y = header(
        d,
        "Sufficient evidence",
        "Cover the family without gaps",
        "The goal is not simply to test fewer products.",
    )

    steps = [
        ("1", "Map", "List the differences across the product family."),
        ("2", "Connect", "Link each difference to the tests it can influence."),
        ("3", "Select", "Choose the configurations that give sufficient evidence."),
        ("4", "Justify", "If one result covers many models, write why it is representative."),
    ]
    for num, title, body in steps:
        rounded_rect(d, (64, y, W - 64, y + 108), CARD, 18)
        d.rounded_rectangle((88, y + 28, 148, y + 80), 12, fill=TEAL)
        tw = d.textlength(num, font=font(26, True))
        d.text((88 + (60 - tw) / 2, y + 36), num, font=font(26, True), fill=NAVY)
        d.text((176, y + 24), title, font=font(26, True), fill=WHITE)
        d.text((176, y + 62), body, font=font(20), fill=MUTED)
        y += 122

    y += 12
    rounded_rect(d, (64, y, W - 64, y + 196), CARD, 22)
    d.rectangle((64, y, 74, y + 196), fill=GOLD)
    d.text((96, y + 28), "Where certification strategy starts", font=font(20, True), fill=GOLD)
    quote_lines = wrap(
        d,
        "Worst-case selection starts with the parameter, not the model.",
        font(30, True),
        W - 192,
    )
    qy = y + 72
    for line in quote_lines:
        d.text((96, qy), line, font=font(30, True), fill=WHITE)
        qy += 40

    footer(d, 5)
    img.save(OUT / "05_closes.png", "PNG")
    return img


def feed_image():
    img, d = new_canvas((FEED_W, FEED_H))
    d.text((56, 36), "CERTIFICATION STRATEGY", font=font(18, True), fill=TEAL)

    title = "Worst-case selection starts with the parameter, not the model."
    y = 92
    for line in wrap(d, title, font(36, True), FEED_W - 420):
        d.text((56, y), line, font=font(36, True), fill=WHITE)
        y += 48

    y += 16
    bullets = [
        "Not the biggest or most powerful model by default",
        "Worst case depends on the requirement being evaluated",
        "Map family differences → keep only those that drive the test",
    ]
    for b in bullets:
        d.ellipse((62, y + 8, 78, y + 24), fill=TEAL)
        d.text((96, y), b, font=font(20), fill=MUTED)
        y += 40

    # 48-model mini grid on the right
    cols, rows = 6, 5
    gx, gy = 780, 110
    cell_w, cell_h, gap = 58, 78, 8
    selected = (3, 2)
    for r in range(rows):
        for c in range(cols):
            cx = gx + c * (cell_w + gap)
            cy = gy + r * (cell_h + gap)
            is_sel = (c, r) == selected
            mini_appliance(
                d,
                cx,
                cy,
                cell_w,
                cell_h,
                GOLD if is_sel else DIM,
                accent=NAVY if is_sel else LINE,
                selected=is_sel,
            )

    d.line((56, FEED_H - 58, FEED_W - 56, FEED_H - 58), fill=LINE, width=1)
    d.text(
        (56, FEED_H - 42),
        "Product family  ·  representative testing  ·  sufficient evidence",
        font=font(16),
        fill=FOOTER,
    )
    img.save(OUT / "feed-1200x627.png", "PNG")
    return img


def write_pdf(images):
    path = OUT / "Worst_case_selection_carousel.pdf"
    page = portrait((W, H))
    c = pdf_canvas.Canvas(str(path), pagesize=page)
    for im in images:
        c.drawImage(ImageReader(im), 0, 0, width=W, height=H)
        c.showPage()
    c.save()
    return path


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    slides = [slide_1(), slide_2(), slide_3(), slide_4(), slide_5()]
    feed_image()
    pdf = write_pdf(slides)
    print("Wrote slides to", OUT)
    print("PDF:", pdf)
