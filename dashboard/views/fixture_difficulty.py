import streamlit as st
import pandas as pd
from dashboard.data_loader import get_upcoming_fixtures, get_current_gameweek


def elo_diff_to_color(elo_diff):
    """Map Elo difference to a background color. Positive = easier (green), negative = harder (red)."""
    if elo_diff > 150:
        return "#1b7a2b"  # dark green
    elif elo_diff > 75:
        return "#4caf50"  # green
    elif elo_diff > 25:
        return "#8bc34a"  # light green
    elif elo_diff > -25:
        return "#ffeb3b"  # yellow
    elif elo_diff > -75:
        return "#ff9800"  # orange
    elif elo_diff > -150:
        return "#f44336"  # red
    else:
        return "#b71c1c"  # dark red


def text_color_for_bg(elo_diff):
    if elo_diff > 25 or (-75 < elo_diff <= -25):
        return "#000000"
    elif elo_diff > -25:
        return "#000000"
    else:
        return "#ffffff"


def render():
    st.header("Fixture Difficulty Rating")
    st.caption("Based on team Elo ratings. Green = easier fixture, Red = harder fixture.")

    current_gw = get_current_gameweek()
    n_gws = st.slider("Gameweeks to show", 3, 10, 6)

    fixtures_df = get_upcoming_fixtures(n_gws)

    if fixtures_df.empty:
        st.warning("No upcoming fixtures found.")
        return

    # Build the grid
    teams_sorted = sorted(fixtures_df["team_short"].unique())
    gws = sorted(fixtures_df["gw"].unique())

    # Build HTML table for precise color control
    html = '<table style="width:100%; border-collapse:collapse; font-family:sans-serif; font-size:14px;">'
    html += "<thead><tr>"
    html += '<th style="padding:8px; text-align:left; border:1px solid #ddd;">Team</th>'
    for gw in gws:
        html += f'<th style="padding:8px; text-align:center; border:1px solid #ddd;">GW{gw}</th>'
    html += "</tr></thead><tbody>"

    for team in teams_sorted:
        html += "<tr>"
        html += f'<td style="padding:8px; font-weight:bold; border:1px solid #ddd;">{team}</td>'
        team_fixtures = fixtures_df[fixtures_df["team_short"] == team]

        for gw in gws:
            gw_fix = team_fixtures[team_fixtures["gw"] == gw]
            if gw_fix.empty:
                html += '<td style="padding:8px; text-align:center; border:1px solid #ddd; background:#eee;">-</td>'
            else:
                # A team might have multiple fixtures in a GW (double gameweek)
                cells = []
                for _, row in gw_fix.iterrows():
                    venue = "H" if row["is_home"] else "A"
                    label = f"{row['opponent_short']} ({venue})"
                    cells.append((label, row["elo_diff"]))

                # Use average elo_diff for color
                avg_elo = sum(c[1] for c in cells) / len(cells)
                bg = elo_diff_to_color(avg_elo)
                fg = text_color_for_bg(avg_elo)
                text = "<br>".join(c[0] for c in cells)
                html += (
                    f'<td style="padding:8px; text-align:center; border:1px solid #ddd; '
                    f'background:{bg}; color:{fg}; font-weight:500;">{text}</td>'
                )

        html += "</tr>"

    html += "</tbody></table>"

    st.markdown(html, unsafe_allow_html=True)

    # Legend
    st.markdown("---")
    legend_html = (
        '<div style="display:flex; gap:12px; align-items:center; font-size:13px;">'
        '<span style="font-weight:bold;">Difficulty:</span>'
        '<span style="background:#1b7a2b; color:white; padding:3px 8px; border-radius:3px;">Very Easy</span>'
        '<span style="background:#4caf50; color:black; padding:3px 8px; border-radius:3px;">Easy</span>'
        '<span style="background:#8bc34a; color:black; padding:3px 8px; border-radius:3px;">Fairly Easy</span>'
        '<span style="background:#ffeb3b; color:black; padding:3px 8px; border-radius:3px;">Neutral</span>'
        '<span style="background:#ff9800; color:black; padding:3px 8px; border-radius:3px;">Tough</span>'
        '<span style="background:#f44336; color:white; padding:3px 8px; border-radius:3px;">Hard</span>'
        '<span style="background:#b71c1c; color:white; padding:3px 8px; border-radius:3px;">Very Hard</span>'
        "</div>"
    )
    st.markdown(legend_html, unsafe_allow_html=True)
