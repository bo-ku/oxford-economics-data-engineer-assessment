"""
Report generation for AAPL stock analysis.

I noticed in the JD you use Tableau. As a DE, I like to generate lightweight
HTML plots during my ETL process just to visually sanity-check the data before
handing it off to the next team. I used Claude Code to generate the HTML/CSS
scaffolding here, templating inline HTML by hand is tedious and error-prone,
and it's exactly the kind of boilerplate where an LLM saves real time for me.
"""

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def generate_report(df: pd.DataFrame, inc: pd.Series, dec: pd.Series,
                    bst: dict, trades: list[dict], output_path: Path) -> None:
    """Generate a clean interactive HTML report."""
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3],
                        subplot_titles=("AAPL Close Price with Trade Signals", "Volume"),
                        vertical_spacing=0.08)

    fig.add_trace(go.Scatter(x=df["Date"], y=df["Close"], mode="lines",
                             name="Close", line=dict(color="#2563eb", width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=[t["buy_date"] for t in trades],
                             y=[t["buy_price"] for t in trades],
                             mode="markers", name="Buy",
                             marker=dict(symbol="triangle-up", size=11, color="#16a34a")), row=1, col=1)
    fig.add_trace(go.Scatter(x=[t["sell_date"] for t in trades],
                             y=[t["sell_price"] for t in trades],
                             mode="markers", name="Sell",
                             marker=dict(symbol="triangle-down", size=11, color="#dc2626")), row=1, col=1)
    fig.add_vrect(x0=bst["buy_date"], x1=bst["sell_date"], fillcolor="#16a34a", opacity=0.07,
                  annotation_text=f"Best Trade: +${bst['profit']:.2f}",
                  annotation_position="top left", row=1, col=1)
    fig.add_trace(go.Bar(x=df["Date"], y=df["Volume"], name="Volume",
                         marker_color="#94a3b8", opacity=0.6), row=2, col=1)
    fig.update_layout(template="plotly_white", height=600, margin=dict(t=60, b=40),
                      legend=dict(orientation="h", yanchor="bottom", y=1.08),
                      font=dict(family="Inter, system-ui, sans-serif"))

    total_return = sum(t["return"] for t in trades)
    trade_rows = "\n".join(
        f'<tr><td>{t["buy_date"].strftime("%Y-%m-%d")}</td><td>BUY</td><td>${t["buy_price"]:.2f}</td>'
        f'<td>{t["sell_date"].strftime("%Y-%m-%d")}</td><td>SELL</td><td>${t["sell_price"]:.2f}</td>'
        f'<td style="color:{"#16a34a" if t["return"]>=0 else "#dc2626"};font-weight:600">${t["return"]:.2f}</td></tr>'
        for t in trades
    )

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AAPL Analysis</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:Inter,system-ui,sans-serif;background:#f8fafc;color:#1e293b;padding:2rem}}
.c{{max-width:960px;margin:0 auto}}
h1{{font-size:1.4rem;margin-bottom:.15rem}}
.sub{{color:#64748b;margin-bottom:1.5rem;font-size:.85rem}}
.card{{background:#fff;border-radius:8px;padding:1.25rem;margin-bottom:1rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
.card h2{{font-size:.9rem;color:#475569;margin-bottom:.75rem;text-transform:uppercase;letter-spacing:.05em}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem}}
.stat{{text-align:center}}.stat .v{{font-size:1.3rem;font-weight:700}}.stat .l{{font-size:.78rem;color:#64748b}}
table{{width:100%;border-collapse:collapse;font-size:.82rem}}
th,td{{padding:.4rem .6rem;text-align:left;border-bottom:1px solid #e2e8f0}}
th{{font-weight:600;color:#475569}}.g{{color:#16a34a}}.r{{color:#dc2626}}
</style></head><body><div class="c">
<h1>AAPL Stock Analysis</h1>
<p class="sub">Oxford Economics &mdash; Data Engineer Assessment</p>
<div class="card"><h2>Key Findings</h2><div class="stats">
<div class="stat"><div class="v g">+${inc["Change"]:.2f}</div><div class="l">Largest Increase ({inc["Date"].strftime("%m/%d/%Y")})</div></div>
<div class="stat"><div class="v r">${dec["Change"]:.2f}</div><div class="l">Largest Decrease ({dec["Date"].strftime("%m/%d/%Y")})</div></div>
<div class="stat"><div class="v g">+${bst["profit"]:.2f}</div><div class="l">Best Single Trade ({bst["buy_date"].strftime("%m/%d")} &rarr; {bst["sell_date"].strftime("%m/%d")})</div></div>
<div class="stat"><div class="v g">+${total_return:.2f}</div><div class="l">Greedy Strategy ({len(trades)} trades)</div></div>
</div></div>
<div class="card"><h2>Price Chart</h2>{fig.to_html(full_html=False, include_plotlyjs="cdn")}</div>
<div class="card"><h2>Trade Log</h2>
<table><thead><tr><th>Buy Date</th><th></th><th>Price</th><th>Sell Date</th><th></th><th>Price</th><th>Return</th></tr></thead>
<tbody>{trade_rows}</tbody>
<tfoot><tr><td colspan="6" style="text-align:right;font-weight:600">Total</td>
<td style="font-weight:700" class="g">${total_return:.2f}</td></tr></tfoot></table></div>
</div></body></html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
