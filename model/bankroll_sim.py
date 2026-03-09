"""
Bankroll simulation with variable bet sizing and profit stashing.

Strategy:
  - Only bet games where model confidence > THRESHOLD (default 62%)
  - Bet size = quarter-Kelly fraction of current bankroll
      Kelly formula: f = (p*(b+1) - 1) / b   where b = 10/11 at -110 odds
      Quarter-Kelly  = f / 4  (much safer, still captures the edge signal)
  - After each week: split net profit 50% to stash / 50% back into bankroll
      Stash is locked — it never goes back to the betting pool
      Losses come out of bankroll only

Start: $1,000 bankroll, $0 stash

Output: chart saved to model/bankroll_chart.png

Usage:
    python3 model/bankroll_sim.py
    python3 model/bankroll_sim.py --threshold 0.62 --start 1000
    python3 model/bankroll_sim.py --test-year 2024
"""

import argparse
import os
import warnings
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_IN = os.path.join(ROOT, "data", "combined", "all_games.csv")
OUT_HTML = os.path.join(ROOT, "bankroll.html")  # repo root → served by GitHub Pages

BREAKEVEN   = 0.5238
ODDS_PAYOUT = 100 / 110  # -110: win $100 for every $110 wagered

FEATURES = [
    "opening_spread",
    "opening_moneyline",
    "spread_bet_pct",
    "spread_dollar_pct",
    "moneyline_bet_pct",
    "moneyline_dollar_pct",
    "line_movement",
]
DOLLAR_COLS = ["spread_dollar_pct", "moneyline_dollar_pct"]
TARGET = "covered"


# ---------------------------------------------------------------------------
# Data prep (same as basic_model.py)
# ---------------------------------------------------------------------------

def load_and_prepare(test_year):
    df = pd.read_csv(DATA_IN, low_memory=False)

    # Build opponent name lookup: for each row, find the other team in the same game
    opp_names = (
        df[["game_id", "team_id", "team"]]
        .rename(columns={"team_id": "opp_team_id", "team": "opp_team"})
    )
    df = df.merge(opp_names, on="game_id", how="left")
    df = df[df["team_id"] != df["opp_team_id"]].drop(columns=["opp_team_id"])

    train_df = df[df["season"] <  test_year].copy()
    test_df  = df[df["season"] == test_year].copy()

    def prep(data, medians=None):
        meta_cols = ["week", "team", "opp_team", "score", "opp_score", "opening_spread", "line_movement"]
        all_cols  = list(dict.fromkeys(FEATURES + [TARGET] + meta_cols))
        sub = data[all_cols].copy()
        for col in FEATURES:
            sub[col] = pd.to_numeric(sub[col], errors="coerce")
        for col in DOLLAR_COLS:
            if col in sub.columns:
                sub[col] = np.log1p(sub[col].clip(lower=0))
        sub = sub.dropna(subset=[TARGET]).reset_index(drop=True)
        X    = sub[FEATURES].fillna(sub[FEATURES].median() if medians is None else medians)
        y    = sub[TARGET].astype(int)
        meta = sub[meta_cols].copy()
        if medians is None:
            medians = sub[FEATURES].median()
        return X, y, meta, medians

    X_train, y_train, _, medians = prep(train_df)
    X_test,  y_test,  meta, _   = prep(test_df, medians)
    return X_train, y_train, X_test, y_test, meta


# ---------------------------------------------------------------------------
# Kelly bet sizing
# ---------------------------------------------------------------------------

def kelly_fraction(p, b=ODDS_PAYOUT, fraction=0.25):
    """
    Kelly criterion bet size as a fraction of bankroll.
    f* = (p*(b+1) - 1) / b
    We use quarter-Kelly (fraction=0.25) for safety.
    Returns 0 if Kelly is negative (no edge).
    """
    f = (p * (b + 1) - 1) / b
    return max(0.0, f * fraction)


# ---------------------------------------------------------------------------
# Simulate a season week by week
# ---------------------------------------------------------------------------

def simulate_season(probs, y_true, meta, threshold, start_bankroll,
                    kelly_fraction_val, max_buyin, topup_amount, topup_threshold):
    bankroll   = float(start_bankroll)
    stash      = 0.0
    total_in   = float(start_bankroll)   # total money ever put in (start + top-ups)
    topup_left = float(max_buyin)        # remaining top-up budget

    weeks   = sorted(meta["week"].unique())
    history = []

    for week in weeks:
        topup_this_week = 0.0

        # Top up before betting if bankroll is too low
        if bankroll < topup_threshold and topup_left > 0:
            amount       = min(topup_amount, topup_left)
            bankroll    += amount
            total_in    += amount
            topup_left  -= amount
            topup_this_week = amount
            print(f"  Week {week}: topped up ${amount:.0f}  (${topup_left:.0f} remaining budget)")

        mask      = (meta["week"] == week).values & (probs > threshold)
        week_bets = meta[mask].copy().reset_index(drop=True)
        week_bets["prob"]    = probs[mask]
        week_bets["covered"] = y_true.values[mask]
        week_bets["kelly"]   = week_bets["prob"].apply(
            lambda p: kelly_fraction(p, fraction=kelly_fraction_val)
        )
        week_bets["bet_amt"] = (week_bets["kelly"] * bankroll).round(2)

        # Settle bets
        week_bets["payout"] = week_bets.apply(
            lambda r: r["bet_amt"] * ODDS_PAYOUT if r["covered"] == 1 else -r["bet_amt"],
            axis=1
        )
        net_pnl   = week_bets["payout"].sum()
        bankroll += net_pnl

        # Stash 50% of any profit; reinvest the other 50%
        if net_pnl > 0:
            stash    += net_pnl * 0.50
            bankroll -= net_pnl * 0.50

        history.append({
            "week":            int(week),
            "bets":            len(week_bets),
            "net_pnl":         round(net_pnl, 2),
            "bankroll":        round(max(bankroll, 0), 2),
            "stash":           round(stash, 2),
            "total":           round(max(bankroll, 0) + stash, 2),
            "total_in":        round(total_in, 2),
            "topup":           round(topup_this_week, 2),
            "week_bets":       week_bets,
        })

        if bankroll <= 0:
            bankroll = 0
            if topup_left <= 0:
                print(f"  !! Bankroll busted at week {week} — no top-up budget left")
                break

    return history, total_in


# ---------------------------------------------------------------------------
# Print weekly log
# ---------------------------------------------------------------------------

def print_weekly_log(history, total_in, threshold, kelly_fraction_val):
    print(f"\n{'='*82}")
    print(f"  Week-by-week simulation  (>{threshold:.0%} conf, {kelly_fraction_val:.0%}-Kelly, 50% stash)")
    print(f"{'='*82}")
    print(f"  {'Wk':>3}  {'Bets':>4}  {'Profit/Loss':>11}  {'Bankroll':>10}  {'Stash':>10}  {'Total':>10}  Picks")
    print(f"  {'--':>3}  {'----':>4}  {'-'*11}  {'-'*10}  {'-'*10}  {'-'*10}  -----")

    for h in history:
        topup_note = f"  [+${h['topup']:.0f} added]" if h["topup"] > 0 else ""
        picks_str  = "  ".join(
            f"{r['team']} ({r['opening_spread']:+.1f},{r['prob']:.0%},{'✓' if r['covered']==1 else '✗'})"
            for _, r in h["week_bets"].iterrows()
        )
        print(
            f"  {h['week']:>3}  {h['bets']:>4}  ${h['net_pnl']:>+10,.0f}"
            f"  ${h['bankroll']:>9,.0f}  ${h['stash']:>9,.0f}  ${h['total']:>9,.0f}"
            f"{topup_note}  {picks_str}"
        )

    final = history[-1]
    roi   = (final["total"] - total_in) / total_in * 100
    print(f"\n  Total invested : ${total_in:,.2f}")
    print(f"  Final bankroll : ${final['bankroll']:,.2f}")
    print(f"  Final stash    : ${final['stash']:,.2f}")
    print(f"  Total wealth   : ${final['total']:,.2f}")
    print(f"  ROI on invested: {roi:+.1f}%")

    all_bets = pd.concat([h["week_bets"] for h in history if len(h["week_bets"]) > 0])
    total_bets = len(all_bets)
    total_wins = (all_bets["payout"] > 0).sum()
    print(f"\n  Season record  : {total_wins}/{total_bets} ({total_wins/total_bets:.1%})")
    print(f"  Total wagered  : ${all_bets['bet_amt'].sum():,.2f}")


# ---------------------------------------------------------------------------
# Build hover text for each week
# ---------------------------------------------------------------------------

def week_hover(h, start_bankroll):
    """Build the detailed tooltip string shown when hovering over a week."""
    lines = [f"<b>Week {h['week']}</b>"]

    if h["bets"] == 0:
        lines.append("No qualifying bets this week")
    else:
        lines.append(f"<b>{h['bets']} bet{'s' if h['bets'] > 1 else ''} placed — betting on spread covers</b>")
        lines.append("━" * 36)
        for _, r in h["week_bets"].iterrows():
            icon     = "✅" if r["covered"] == 1 else "❌"
            sprd     = f"{r['opening_spread']:+.1f}" if pd.notna(r["opening_spread"]) else "?"
            score    = int(r["score"])    if pd.notna(r.get("score"))    else "?"
            opp_score= int(r["opp_score"])if pd.notna(r.get("opp_score"))else "?"
            opp      = r.get("opp_team", "Opponent")
            # How much they covered/missed by
            if pd.notna(r.get("score")) and pd.notna(r.get("opp_score")) and pd.notna(r["opening_spread"]):
                margin = score + r["opening_spread"] - opp_score
                cover_by = f"covered by {margin:.1f}" if margin > 0 else f"missed by {abs(margin):.1f}"
            else:
                cover_by = "covered" if r["covered"] == 1 else "did not cover"
            profit_str = f"+${r['payout']:,.0f}" if r["payout"] > 0 else f"-${abs(r['payout']):,.0f}"

            lines.append(
                f"{icon} <b>Bet: {r['team']} to cover {sprd}</b><br>"
                f"   vs {opp} &nbsp;|&nbsp; Final: {r['team']} {score} – {opp} {opp_score}<br>"
                f"   {cover_by} &nbsp;|&nbsp; Conf: {r['prob']:.0%} &nbsp;|&nbsp; "
                f"Wagered: ${r['bet_amt']:,.0f} &nbsp;→&nbsp; <b>{profit_str}</b>"
            )
        lines.append("━" * 36)
        profit_label = f"+${h['net_pnl']:,.0f}" if h["net_pnl"] >= 0 else f"-${abs(h['net_pnl']):,.0f}"
        lines.append(f"Week profit/loss: <b>{profit_label}</b>")

    if h.get("topup", 0) > 0:
        lines.append(f"💰 <b>Top-up this week: +${h['topup']:,.0f}</b>  (bankroll was low)")
    lines.append(
        f"Bankroll: ${h['bankroll']:,.0f}  |  Stash: ${h['stash']:,.0f}  |  "
        f"Total invested: ${h['total_in']:,.0f}  |  <b>Total wealth: ${h['total']:,.0f}</b>"
    )
    return "<br>".join(lines)


# ---------------------------------------------------------------------------
# Interactive Plotly chart
# ---------------------------------------------------------------------------

def plot_season(history, start_bankroll, total_in, threshold, kelly_fraction_val, test_year, out_path):
    BG      = "#1a1a2e"
    PANEL   = "#16213e"
    GRID    = "#2a2a4a"
    C_TOTAL = "#00b4d8"
    C_BANK  = "#f72585"
    C_STASH = "#90e0ef"
    C_WIN   = "#06d6a0"
    C_LOSS  = "#ef476f"

    # Prepend week 0
    all_weeks    = [0]              + [h["week"]        for h in history]
    all_bankroll = [start_bankroll] + [h["bankroll"]    for h in history]
    all_stash    = [0.0]            + [h["stash"]       for h in history]
    all_total    = [start_bankroll] + [h["total"]       for h in history]
    all_invested = [start_bankroll] + [h["total_in"]    for h in history]

    # Top-up event markers
    topup_weeks  = [h["week"]    for h in history if h.get("topup", 0) > 0]
    topup_totals = [h["total"]   for h in history if h.get("topup", 0) > 0]
    topup_labels = [f"+${h['topup']:,.0f} added" for h in history if h.get("topup", 0) > 0]

    # Hover text for the wealth lines — week 0 is just the starting point
    hover_texts = ["<b>Week 0</b><br>Starting bankroll: $" + f"{start_bankroll:,.0f}"]
    hover_texts += [week_hover(h, start_bankroll) for h in history]

    final    = history[-1]
    roi      = (final["total"] - total_in) / total_in * 100
    roi_str  = f"{roi:+.0f}%"

    bar_weeks  = [h["week"]    for h in history]
    bar_pnl    = [h["net_pnl"] for h in history]
    bar_colors = [C_WIN if p >= 0 else C_LOSS for p in bar_pnl]
    bar_hover  = [week_hover(h, start_bankroll) for h in history]

    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.68, 0.32],
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=["Bankroll & Wealth Over the Season", "Weekly Profit/Loss"],
    )

    # ── Total invested (dashed white baseline showing money put in) ───────
    fig.add_trace(go.Scatter(
        x=all_weeks, y=all_invested,
        mode="lines",
        name="Total invested",
        line=dict(color="rgba(255,255,255,0.45)", width=1.5, dash="dot"),
        hoverinfo="skip",
        showlegend=True,
    ), row=1, col=1)

    # ── Shaded fill between stash and total ──────────────────────────────
    fig.add_trace(go.Scatter(
        x=all_weeks + all_weeks[::-1],
        y=all_total + all_stash[::-1],
        fill="toself", fillcolor="rgba(0,180,216,0.08)",
        line=dict(width=0), hoverinfo="skip", showlegend=False,
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=all_weeks + all_weeks[::-1],
        y=all_stash + [0]*len(all_stash),
        fill="toself", fillcolor="rgba(144,224,239,0.12)",
        line=dict(width=0), hoverinfo="skip", showlegend=False,
    ), row=1, col=1)

    # ── Wealth lines ──────────────────────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=all_weeks, y=all_total,
        mode="lines+markers",
        name="Total wealth",
        line=dict(color=C_TOTAL, width=3),
        marker=dict(size=7, color=C_TOTAL),
        hovertemplate="%{customdata}<extra></extra>",
        customdata=hover_texts,
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=all_weeks, y=all_bankroll,
        mode="lines+markers",
        name="Active bankroll",
        line=dict(color=C_BANK, width=2, dash="dash"),
        marker=dict(size=6, color=C_BANK),
        hovertemplate="%{customdata}<extra></extra>",
        customdata=hover_texts,
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=all_weeks, y=all_stash,
        mode="lines+markers",
        name="Locked stash (50% of profits)",
        line=dict(color=C_STASH, width=2),
        marker=dict(size=6, color=C_STASH),
        hovertemplate="%{customdata}<extra></extra>",
        customdata=hover_texts,
    ), row=1, col=1)

    # ── Top-up event markers ──────────────────────────────────────────────
    if topup_weeks:
        fig.add_trace(go.Scatter(
            x=topup_weeks, y=topup_totals,
            mode="markers+text",
            name="Top-up",
            marker=dict(symbol="triangle-up", size=14, color="#ffd166", line=dict(color="white", width=1)),
            text=topup_labels,
            textposition="top center",
            textfont=dict(color="#ffd166", size=9),
            hovertemplate="<b>Top-up</b>: %{text}<extra></extra>",
        ), row=1, col=1)

    # Start line
    fig.add_hline(
        y=start_bankroll, line=dict(color="white", width=1, dash="dot"),
        opacity=0.3, row=1, col=1,
        annotation_text=f"  start ${start_bankroll:,.0f}",
        annotation_font_color="rgba(255,255,255,0.4)",
        annotation_font_size=10,
    )

    # ── Weekly Profit/Loss bars ───────────────────────────────────────────────────
    fig.add_trace(go.Bar(
        x=bar_weeks, y=bar_pnl,
        name="Weekly Profit/Loss",
        marker_color=bar_colors,
        hovertemplate="%{customdata}<extra></extra>",
        customdata=bar_hover,
        showlegend=False,
    ), row=2, col=1)

    fig.add_hline(y=0, line=dict(color="white", width=1), opacity=0.4, row=2, col=1)

    # ── Layout ────────────────────────────────────────────────────────────
    axis_style = dict(
        gridcolor=GRID, zerolinecolor=GRID,
        tickfont=dict(color="white"), title_font=dict(color="white"),
        showline=False,
    )

    fig.update_layout(
        title=dict(
            text=(
                f"NFL Spread Model — {test_year} Season  |  "
                f">{threshold:.0%} confidence  |  {kelly_fraction_val:.0%}-Kelly sizing  |  "
                f"50% profit stash  |  Started: ${start_bankroll:,.0f}  |  "
                f"Total in: ${total_in:,.0f}  |  Final: ${final['total']:,.0f}  |  ROI: {roi_str}"
            ),
            font=dict(color="white", size=14), x=0.5,
        ),
        paper_bgcolor=BG,
        plot_bgcolor=PANEL,
        font=dict(color="white"),
        legend=dict(
            bgcolor="rgba(22,33,62,0.8)", bordercolor="#444466",
            borderwidth=1, font=dict(color="white"),
            x=0.01, y=0.99,
        ),
        hoverlabel=dict(
            bgcolor="#0d1b2a", bordercolor="#00b4d8",
            font=dict(color="white", size=12),
        ),
        hovermode="x unified" if False else "closest",
        xaxis=dict(title="", **axis_style),
        xaxis2=dict(title="Week", **axis_style),
        yaxis=dict(
            title="Dollars ($)",
            tickprefix="$", tickformat=",.0f",
            **axis_style,
        ),
        yaxis2=dict(
            title="Week Profit/Loss ($)",
            tickprefix="$", tickformat="+,.0f",
            **axis_style,
        ),
        height=700,
        margin=dict(t=70, b=40, l=80, r=40),
    )

    # Style subplot title fonts
    for ann in fig.layout.annotations:
        ann.font.color = "rgba(255,255,255,0.7)"
        ann.font.size  = 11

    fig.write_html(out_path, include_plotlyjs="cdn", config={"responsive": True})
    print(f"\nChart saved to: {os.path.relpath(out_path)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-year",        type=int,   default=2025)
    parser.add_argument("--threshold",        type=float, default=0.62)
    parser.add_argument("--start",            type=float, default=200.0,
                        help="Starting bankroll (default: $200)")
    parser.add_argument("--kelly",            type=float, default=0.5,
                        help="Kelly fraction: 0.25=quarter, 0.5=half (default: 0.5)")
    parser.add_argument("--max-buyin",        type=float, default=1000.0,
                        help="Max total top-up budget over the season (default: $1000)")
    parser.add_argument("--topup-amount",     type=float, default=100.0,
                        help="How much to inject each time bankroll is low (default: $100)")
    parser.add_argument("--topup-threshold",  type=float, default=50.0,
                        help="Top up when bankroll falls below this amount (default: $50)")
    args = parser.parse_args()

    print(f"Loading data and training model (test year: {args.test_year})...")
    X_train, y_train, X_test, y_test, meta = load_and_prepare(args.test_year)

    lr = Pipeline([
        ("scaler", StandardScaler()),
        ("clf",    LogisticRegression(max_iter=1000, random_state=42)),
    ])
    lr.fit(X_train, y_train)
    probs = lr.predict_proba(X_test)[:, 1]

    qualifying = (probs > args.threshold).sum()
    print(f"Games meeting {args.threshold:.0%} threshold: {qualifying} / {len(probs)}")
    print(f"Starting bankroll: ${args.start:,.0f}  |  Kelly: {args.kelly:.0%}  |  "
          f"Top-up: ${args.topup_amount:.0f} when below ${args.topup_threshold:.0f}  |  "
          f"Max top-up budget: ${args.max_buyin:,.0f}\n")

    history, total_in = simulate_season(
        probs, y_test, meta,
        threshold         = args.threshold,
        start_bankroll    = args.start,
        kelly_fraction_val= args.kelly,
        max_buyin         = args.max_buyin,
        topup_amount      = args.topup_amount,
        topup_threshold   = args.topup_threshold,
    )

    print_weekly_log(history, total_in, args.threshold, args.kelly)
    plot_season(history, args.start, total_in, args.threshold, args.kelly, args.test_year, OUT_HTML)
    import webbrowser
    webbrowser.open(f"file://{OUT_HTML}")


if __name__ == "__main__":
    main()
