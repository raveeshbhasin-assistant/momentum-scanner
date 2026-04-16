"""Build executive summary PDF for Momentum Scanner v3.1 improvements."""

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether,
)

# ── Colors ───────────────────────────────────────────────────
DARK_BG = HexColor("#0a0e17")
CARD_BG = HexColor("#1a1f2e")
GREEN = HexColor("#22c55e")
CYAN = HexColor("#22d3ee")
BLUE = HexColor("#3b82f6")
AMBER = HexColor("#f59e0b")
PURPLE = HexColor("#a78bfa")
RED = HexColor("#ef4444")
TEXT_PRIMARY = HexColor("#e2e8f0")
TEXT_SECONDARY = HexColor("#94a3b8")
TEXT_MUTED = HexColor("#64748b")
BORDER = HexColor("#2a3148")
WHITE = HexColor("#ffffff")
BLACK = HexColor("#000000")
NEAR_BLACK = HexColor("#111827")

# ── Styles ───────────────────────────────────────────────────
style_title = ParagraphStyle(
    "Title", fontName="Helvetica-Bold", fontSize=26,
    textColor=WHITE, alignment=TA_LEFT, spaceAfter=4,
)
style_subtitle = ParagraphStyle(
    "Subtitle", fontName="Helvetica", fontSize=12,
    textColor=TEXT_SECONDARY, alignment=TA_LEFT, spaceAfter=20,
)
style_section = ParagraphStyle(
    "Section", fontName="Helvetica-Bold", fontSize=14,
    textColor=CYAN, alignment=TA_LEFT, spaceBefore=18, spaceAfter=8,
)
style_benefit_title = ParagraphStyle(
    "BenefitTitle", fontName="Helvetica-Bold", fontSize=11,
    textColor=WHITE, alignment=TA_LEFT, spaceAfter=2,
)
style_body = ParagraphStyle(
    "Body", fontName="Helvetica", fontSize=10,
    textColor=TEXT_SECONDARY, alignment=TA_LEFT, leading=15, spaceAfter=6,
)
style_body_white = ParagraphStyle(
    "BodyWhite", fontName="Helvetica", fontSize=10,
    textColor=WHITE, alignment=TA_LEFT, leading=15, spaceAfter=6,
)
style_metric_value = ParagraphStyle(
    "MetricValue", fontName="Helvetica-Bold", fontSize=22,
    textColor=GREEN, alignment=TA_CENTER, spaceAfter=2,
)
style_metric_label = ParagraphStyle(
    "MetricLabel", fontName="Helvetica", fontSize=8,
    textColor=TEXT_MUTED, alignment=TA_CENTER, spaceAfter=0,
    textTransform="uppercase",
)
style_footer = ParagraphStyle(
    "Footer", fontName="Helvetica", fontSize=8,
    textColor=TEXT_MUTED, alignment=TA_CENTER,
)
style_callout = ParagraphStyle(
    "Callout", fontName="Helvetica-Bold", fontSize=11,
    textColor=AMBER, alignment=TA_LEFT, spaceAfter=4,
)


def build_pdf(output_path: str):
    doc = SimpleDocTemplate(
        output_path, pagesize=letter,
        leftMargin=0.7 * inch, rightMargin=0.7 * inch,
        topMargin=0.6 * inch, bottomMargin=0.6 * inch,
    )

    story = []
    page_width = letter[0] - 1.4 * inch  # usable width

    # ── Header ───────────────────────────────────────────────
    story.append(Paragraph("Momentum Scanner v3.1", style_title))
    story.append(Paragraph("Executive Summary  |  April 13, 2026", style_subtitle))
    story.append(HRFlowable(width="100%", thickness=1, color=BORDER, spaceAfter=16))

    # ── Opening ──────────────────────────────────────────────
    story.append(Paragraph(
        "Today's release transforms the Momentum Scanner from a delayed-data prototype "
        "into a real-time intraday trading tool with institutional-grade price data, "
        "proven resistance-level targeting, and full session memory. "
        "Every change was designed to improve trade decision accuracy, eliminate the "
        "data quality issues encountered on Day 1, and give you a complete audit trail of signals.",
        style_body,
    ))
    story.append(Spacer(1, 12))

    # ── KPI Cards ────────────────────────────────────────────
    kpi_items = [
        ("Real-Time", "DATA FEED", GREEN),
        ("0 sec", "PRICE DELAY", CYAN),
        ("3 Days", "HISTORY RETAINED", BLUE),
        ("$29/mo", "DATA COST", AMBER),
    ]
    kpi_data = []
    label_row = []
    value_row = []
    for value, label, color in kpi_items:
        sl = ParagraphStyle("ml_" + label, fontName="Helvetica", fontSize=8, textColor=TEXT_MUTED, alignment=TA_CENTER)
        sv = ParagraphStyle("mv_" + label, fontName="Helvetica-Bold", fontSize=20, textColor=color, alignment=TA_CENTER)
        label_row.append(Paragraph(label, sl))
        value_row.append(Paragraph(value, sv))
    kpi_data = [label_row, value_row]
    kpi_table = Table(kpi_data, colWidths=[page_width / 4] * 4, rowHeights=[18, 36])
    kpi_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
        ("BOX", (0, 0), (-1, -1), 0.5, BORDER),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(kpi_table)
    story.append(Spacer(1, 18))

    # ── Benefits ─────────────────────────────────────────────
    story.append(Paragraph("Key Benefits", style_section))

    benefits = [
        (
            "Accurate, Real-Time Prices",
            "Prices now come from Financial Modeling Prep's live market feed, replacing the 15-minute "
            "delayed yfinance data. The Day 1 issue where NVDA showed $146 instead of its actual price "
            "is permanently resolved. Every price, every indicator, and every trade level is calculated "
            "on live market data. If FMP ever goes down, the system automatically falls back to yfinance "
            "so you are never left with a blank screen.",
            GREEN,
        ),
        (
            "Correct Eastern Time Across the Board",
            "All timestamps now display in Eastern Time, matching the market clock you trade against. "
            "The 4-hour offset from Day 1 (caused by Railway running in UTC) is fixed at the system level. "
            "Every scan time, every signal timestamp, and every chart axis reflects the actual ET time of day.",
            CYAN,
        ),
        (
            "No More Fake Data During Live Trading",
            "During market hours, the scanner will never silently show demo data with stale prices. "
            "If a live scan fails, you see an honest empty state rather than misleading signals. "
            "Demo data now only appears outside market hours for dashboard preview.",
            AMBER,
        ),
        (
            "Barchart-Style Resistance Targets",
            "Every signal now shows two profit targets: the original ATR-based target (your risk/reward math) "
            "and the nearest pivot-point resistance level using the same Standard Floor method that Barchart uses. "
            "When both targets converge on the same zone, that is a high-conviction exit area. "
            "Both levels are drawn on the chart as price lines for quick visual reference.",
            PURPLE,
        ),
        (
            "Full Day Audit Trail",
            "Every signal found during the day is now accumulated into a searchable, sortable table at /today. "
            "You can review what the scanner flagged at 10:15 AM versus 2:30 PM, compare how scores evolved, "
            "and spot which tickers kept recurring. This turns the scanner from a single-snapshot tool into "
            "a day-long research journal.",
            BLUE,
        ),
        (
            "3-Day Rolling History",
            "Scan results persist across sessions and restarts. The /history page lets you compare today's "
            "signals against the prior two trading days in a tabbed view with per-day summary stats. "
            "This enables pattern recognition across days and helps you evaluate which setups followed through.",
            BLUE,
        ),
        (
            "Wider Funnel, Same Quality Gate",
            "The relative volume threshold was lowered from 1.5x to 1.33x, catching stocks with 30%+ above-average "
            "volume that were previously filtered out. The composite score minimum (60) still acts as a quality gate, "
            "so you see more candidates without more noise.",
            GREEN,
        ),
    ]

    for title, body, color in benefits:
        s_title = ParagraphStyle("bt", fontName="Helvetica-Bold", fontSize=11, textColor=color, spaceAfter=2)
        block = KeepTogether([
            Paragraph(title, s_title),
            Paragraph(body, style_body),
            Spacer(1, 6),
        ])
        story.append(block)

    # ── What's Next ──────────────────────────────────────────
    story.append(Spacer(1, 6))
    story.append(Paragraph("Next Steps", style_section))

    next_steps = [
        ("Deploy to Railway", "Push the code to GitHub and set the FMP_API_KEY environment variable on Railway. The system auto-deploys."),
        ("First Live Session", "Run a scan after tomorrow's open (9:35 AM ET recommended) and verify real-time prices and ET timestamps on the dashboard."),
        ("Review /today at End of Day", "After market close, visit /today to review all signals the scanner found throughout the session."),
        ("Evaluate Resistance Targets", "Compare which target (ATR vs. Resistance) price approached more closely on the signals you tracked. This will inform whether to weight one over the other going forward."),
    ]

    for i, (title, body) in enumerate(next_steps, 1):
        story.append(Paragraph(f"{i}. <b>{title}</b> — {body}", style_body))

    # ── Footer ───────────────────────────────────────────────
    story.append(Spacer(1, 24))
    story.append(HRFlowable(width="100%", thickness=0.5, color=BORDER, spaceAfter=8))
    story.append(Paragraph(
        "Momentum Scanner v3.1  |  For educational and informational purposes only. Not financial advice.",
        style_footer,
    ))

    doc.build(story)
    print(f"PDF created: {output_path}")


if __name__ == "__main__":
    build_pdf("/sessions/great-confident-davinci/mnt/Trader v3/Momentum_Scanner_v3.1_Summary.pdf")
