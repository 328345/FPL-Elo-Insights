import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np


COMPARE_METRICS = {
    "total_points": "Total Points",
    "form": "Form",
    "points_per_game": "PPG",
    "now_cost": "Price",
    "expected_goals": "xG",
    "expected_assists": "xA",
    "expected_goal_involvements": "xGI",
    "bonus": "Bonus",
    "bps": "BPS",
    "ict_index": "ICT Index",
    "influence": "Influence",
    "creativity": "Creativity",
    "threat": "Threat",
    "selected_by_percent": "Ownership %",
    "minutes": "Minutes",
    "goals_scored": "Goals",
    "assists": "Assists",
    "clean_sheets": "Clean Sheets",
    "value_form": "Value (Form)",
    "value_season": "Value (Season)",
    "ep_next": "EP Next",
}

RADAR_METRICS = [
    "form", "points_per_game", "expected_goal_involvements",
    "bonus", "ict_index", "value_season",
]
RADAR_LABELS = ["Form", "PPG", "xGI", "Bonus", "ICT", "Value"]


def render(df: pd.DataFrame):
    st.header("Player Comparison")

    player_names = sorted(df["web_name"].dropna().unique())

    col1, col2, col3 = st.columns(3)
    with col1:
        p1 = st.selectbox("Player 1", player_names, index=0)
    with col2:
        p2 = st.selectbox("Player 2", player_names, index=min(1, len(player_names) - 1))
    with col3:
        p3 = st.selectbox("Player 3 (optional)", ["None"] + player_names, index=0)

    selected = [p1, p2]
    if p3 != "None":
        selected.append(p3)

    players_data = []
    for name in selected:
        row = df[df["web_name"] == name]
        if not row.empty:
            players_data.append(row.iloc[0])

    if len(players_data) < 2:
        st.warning("Select at least 2 valid players.")
        return

    # Comparison table
    st.subheader("Side-by-Side Stats")
    available_metrics = {k: v for k, v in COMPARE_METRICS.items() if k in df.columns}

    table_data = {"Metric": list(available_metrics.values())}
    for p in players_data:
        vals = []
        for col in available_metrics:
            val = p.get(col, "")
            if isinstance(val, float):
                val = round(val, 2)
            vals.append(val)
        table_data[p["web_name"]] = vals

    comparison_df = pd.DataFrame(table_data)
    st.dataframe(comparison_df, use_container_width=True, hide_index=True)

    # Radar chart
    st.subheader("Radar Comparison")

    available_radar = [m for m in RADAR_METRICS if m in df.columns]
    available_labels = [RADAR_LABELS[i] for i, m in enumerate(RADAR_METRICS) if m in df.columns]

    if len(available_radar) >= 3:
        # Normalize to 0-100 using the full dataset's min/max
        fig = go.Figure()
        colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

        for idx, p in enumerate(players_data):
            values = []
            for metric in available_radar:
                col_min = df[metric].min()
                col_max = df[metric].max()
                raw = p.get(metric, 0)
                if col_max > col_min:
                    normalized = ((raw - col_min) / (col_max - col_min)) * 100
                else:
                    normalized = 50
                values.append(round(normalized, 1))

            # Close the radar
            values.append(values[0])
            labels = available_labels + [available_labels[0]]

            fig.add_trace(go.Scatterpolar(
                r=values,
                theta=labels,
                fill="toself",
                name=p["web_name"],
                line=dict(color=colors[idx % len(colors)]),
                opacity=0.6,
            ))

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            showlegend=True,
            height=450,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Quick info cards
    st.subheader("Quick Info")
    info_cols = st.columns(len(players_data))
    for i, p in enumerate(players_data):
        with info_cols[i]:
            st.markdown(f"**{p['web_name']}**")
            st.markdown(f"_{p.get('position', '')} | {p.get('team_short_name', '')}_")
            status = p.get("status", "a")
            news = p.get("news", "")
            if status != "a" and pd.notna(news) and news:
                st.warning(f"Status: {news}")
            else:
                st.success("Available")
            chance = p.get("chance_of_playing_next_round", None)
            if pd.notna(chance):
                st.metric("Chance of playing", f"{int(chance)}%")
