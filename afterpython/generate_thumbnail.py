"""Generate a 1600x900 (16:9) thumbnail for pfeed's website.

Usage (from afterpython/; needs rsvg-convert, e.g. `brew install librsvg`):
    # text only, centered: the default thumbnail for normal posts
    python generate_thumbnail.py --title Examples \
        --tagline "Local Data Lakehouse" --tagline "Clean Once, Share with All" --out examples
    # with a candlestick chart on the right: for the banner (featured) post
    python generate_thumbnail.py ... --art candles --out examples_banner
    # optional tags, shown as pills
    python generate_thumbnail.py ... --tag polars --tag parquet

Writes <out>.png. The chart uses a fixed random seed, so only the texts change between runs.
"""

import argparse
import random
import subprocess
from html import escape
from pathlib import Path

W, H = 1600, 900
GREEN, RED, WHITE = "#34d399", "#f87171", "#fbfcfd"
SANS = "Helvetica Neue, Helvetica, Arial, sans-serif"
MONO = "Menlo, monospace"

parser = argparse.ArgumentParser()
parser.add_argument("--title", required=True)
parser.add_argument("--tagline", action="append", default=[])
parser.add_argument("--tag", action="append", default=[])
parser.add_argument("--art", choices=["none", "candles"], default="none", help="right-hand side art")
parser.add_argument("--out", required=True)
args = parser.parse_args()

has_art = args.art != "none"
# text block: left-aligned when there is art on the right, centered otherwise
tx, anchor = (124, "start") if has_art else (W / 2, "middle")


def candles_svg():
    random.seed(7)
    x0, x1, ytop, ybot = 860, 1500, 210, 690
    n = 26
    step = (x1 - x0) / n
    p = 100.0
    cs = []
    for _ in range(n):
        o = p
        c = o + random.gauss(0.35, 2.2)
        hi = max(o, c) + abs(random.gauss(0, 1.2))
        lo = min(o, c) - abs(random.gauss(0, 1.2))
        cs.append((o, hi, lo, c))
        p = c
    lo_all = min(x[2] for x in cs)
    hi_all = max(x[1] for x in cs)

    def Y(v):
        return ybot - (v - lo_all) / (hi_all - lo_all) * (ybot - ytop)

    out = []
    for i, (o, hi, lo, c) in enumerate(cs):
        cx = x0 + step * (i + 0.5)
        col = GREEN if c >= o else RED
        bw = step * 0.56
        top, bot = Y(max(o, c)), Y(min(o, c))
        out.append(f'<line x1="{cx:.1f}" y1="{Y(hi):.1f}" x2="{cx:.1f}" y2="{Y(lo):.1f}" stroke="{col}" stroke-width="3" stroke-linecap="round"/>')
        out.append(f'<rect x="{cx - bw / 2:.1f}" y="{top:.1f}" width="{bw:.1f}" height="{max(bot - top, 3):.1f}" rx="3" fill="{col}"/>')
    # close-price line through the candles
    pts = " ".join(f"{x0 + step * (i + 0.5):.1f},{Y(c[3]):.1f}" for i, c in enumerate(cs))
    out.append(f'<polyline points="{pts}" fill="none" stroke="{WHITE}" stroke-opacity="0.35" stroke-width="2" stroke-dasharray="6 8"/>')
    return "".join(out)


art = {"none": "", "candles": candles_svg}[args.art]
art = art() if callable(art) else art

grid = [f'<line x1="{gx}" y1="0" x2="{gx}" y2="{H}"/>' for gx in range(0, W + 1, 80)]
grid += [f'<line x1="0" y1="{gy}" x2="{W}" y2="{gy}"/>' for gy in range(0, H + 1, 80)]

texts = []
y = 652
for line in args.tagline:
    texts.append(f'<text x="{tx}" y="{y}" text-anchor="{anchor}" font-family="{MONO}" font-size="28" fill="{WHITE}" fill-opacity="0.6">{escape(line)}</text>')
    y += 44
# tags: pills, in a row that starts at the text's left edge or is centered
if args.tag:
    widths = [22 + 17 * len(t) for t in args.tag]
    gap = 14
    px = tx if has_art else W / 2 - (sum(widths) + gap * (len(widths) - 1)) / 2
    y += 32
    for t, pw in zip(args.tag, widths):
        texts.append(
            f'<rect x="{px}" y="{y - 32}" width="{pw}" height="46" rx="23" fill="{GREEN}" fill-opacity="0.12" stroke="{GREEN}" stroke-opacity="0.5"/>'
            f'<text x="{px + pw / 2}" y="{y - 1}" text-anchor="middle" font-family="{MONO}" font-size="26" fill="{GREEN}">{escape(t)}</text>'
        )
        px += pw + gap
    y += 30
texts.append(f'<text x="{tx}" y="{y + 30}" text-anchor="{anchor}" font-family="{MONO}" font-size="24" fill="{WHITE}" fill-opacity="0.35">pfeed.pfund.ai</text>')

logo_x = 120 if has_art else W / 2 - 60
bar_x = 124 if has_art else W / 2 - 48
glow_cx = 0.72 if has_art else 0.5
# with art, the grid fades in from the left; without, it is uniform
grid_mask = ' mask="url(#gridmask)"' if has_art else ""

svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#1b1e25"/><stop offset="1" stop-color="#0e1014"/>
    </linearGradient>
    <radialGradient id="glow" cx="{glow_cx}" cy="0.5" r="0.55">
      <stop offset="0" stop-color="{GREEN}" stop-opacity="0.16"/><stop offset="1" stop-color="{GREEN}" stop-opacity="0"/>
    </radialGradient>
    <linearGradient id="fade" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0" stop-color="#fff" stop-opacity="0"/><stop offset="0.5" stop-color="#fff" stop-opacity="1"/>
    </linearGradient>
    <mask id="gridmask"><rect width="{W}" height="{H}" fill="url(#fade)"/></mask>
  </defs>
  <rect width="{W}" height="{H}" fill="url(#bg)"/>
  <rect width="{W}" height="{H}" fill="url(#glow)"/>
  <g stroke="#ffffff" stroke-opacity="0.05" stroke-width="1"{grid_mask}>{"".join(grid)}</g>
  {art}
  <!-- logo mark -->
  <g transform="translate({logo_x} 170)">
    <rect width="120" height="120" rx="26" fill="{WHITE}" fill-opacity="0.06" stroke="{WHITE}" stroke-opacity="0.15"/>
    <g transform="translate(24.8 31.2) scale(0.32)" fill="{WHITE}">
      <path d="m194.26,13.92c-12.05-6.52-27-10.74-44.83-12.68l-4.37,34.4c1.28.5,2.51,1.020,3.66,1.6,7.87,3.99,11.8,10.67,11.8,20.03s-3.9,16.2-11.7,20.52c-2.8,1.52-6.01,2.76-9.59,3.75l-4.25,33.41c25-1.15,44.78-5.43,59.29-12.86,17.16-8.79,25.74-23.81,25.74-45.06,0-19.46-8.58-33.83-25.74-43.11Z"/>
      <path d="m112.12,84.26h-52.43V31.26h55.97c1.09,0,2.14.03,3.19.06l3.98-31.32H0v180h59.68v-64.72h48.49l3.94-31.02Z"/>
    </g>
  </g>
  <text x="{tx - 4 if has_art else tx}" y="440" text-anchor="{anchor}" font-family="{SANS}" font-size="128" font-weight="700" fill="{WHITE}" letter-spacing="-3">pfeed</text>
  <text x="{tx}" y="530" text-anchor="{anchor}" font-family="{SANS}" font-size="64" font-weight="300" fill="{WHITE}" fill-opacity="0.85">{escape(args.title)}</text>
  <rect x="{bar_x}" y="580" width="96" height="6" rx="3" fill="{GREEN}"/>
  {"".join(texts)}
</svg>'''

svg_path = Path(args.out + ".svg")
svg_path.write_text(svg, encoding="utf-8")
subprocess.run(["rsvg-convert", str(svg_path), "-o", args.out + ".png"], check=True)
svg_path.unlink()
