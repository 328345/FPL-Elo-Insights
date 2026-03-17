import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from dashboard.data_loader import get_recent_form_data


def render(df: pd.DataFrame):
    st.header("Form & Trend Analysis")
    st.caption("Track player performance over recent gameweeks.")

    # Player selectors
    player_names = sorted(df["web_name"].dropna().unique())
    col1, col2, col3 = st.columns([2, 2, 1])

    with col1:
        player1 = st.selectbox("Player 1", player_names, index=0)
    with col2:
        player2 = st.selectbox("Player 2 (optional)", ["None"] + player_names, index=0)
    with col3:
        n_gws = st.slider("Gameweeks", 4, 15, 8)

    # Get player IDs
    p1_row = df[df["web_name"] == player1]
    if p1_row.empty:
        st.warning("Player not found.")
        return
    p1_id = int(p1_row.iloc[0]["id"])

    p1_form = get_recent_form_data(p1_id, n_gws)

    if p1_form.empty:
        st.warning(f"No gameweek data found for {player1}.")
        return

    # Points chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=p1_form["gw"], y=p1_form["event_points"],
        mode="lines+markers", name=f"{player1} - Points",
        line=dict(width=3),
    ))

    if player2 != "None":
        p2_row = df[df["web_name"] == player2]
        if not p2_row.empty:
            p2_id = int(p2_row.iloc[0]["id"])
            p2_form = get_recent_form_data(p2_id, n_gws)
            if not p2_form.empty:
                fig.add_trace(go.Scatter(
                    x=p2_form["gw"], y=p2_form["event_points"],
                    mode="lines+markers", name=f"{player2} - Points",
                    line=dict(width=3, dash="dash"),
                ))

    fig.update_layout(
        title="FPL Points per Gameweek",
        xaxis_title="Gameweek",
        yaxis_title="Points",
        height=400,
        xaxis=dict(dtick=1),
    )
    st.plotly_chart(fig, use_container_width=True)

    # xG / xA chart
    st.subheader("Expected Stats")
    metric_options = []
    if "expected_goals" in p1_form.columns:
        metric_options.append("expected_goals")
    if "expected_assists" in p1_form.columns:
        metric_options.append("expected_assists")
    if "expected_goal_involvements" in p1_form.columns:
        metric_options.append("expected_goal_involvements")

    if metric_options:
        selected_metrics = st.multiselect(
            "Metrics to plot",
            metric_options,
            default=metric_options[:2],
            format_func=lambda x: x.replace("expected_", "x").replace("_", " ").title(),
        )

        fig2 = go.Figure()
        colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]
        for i, metric in enumerate(selected_metrics):
            if metric in p1_form.columns:
                fig2.add_trace(go.Bar(
                    x=p1_form["gw"], y=p1_form[metric],
                    name=metric.replace("expected_", "x").replace("_", " ").title(),
                    marker_color=colors[i % len(colors)],
                    opacity=0.8,
                ))

        fig2.update_layout(
            xaxis_title="Gameweek",
            yaxis_title="Value",
            height=350,
            barmode="group",
            xaxis=dict(dtick=1),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Summary stats table
    st.subheader("Period Summary")
    summary_cols = {
        "event_points": "Pts",
        "minutes": "Mins",
        "goals_scored": "Goals",
        "assists": "Assists",
        "bonus": "Bonus",
        "expected_goals": "xG",
        "expected_assists": "xA",
        "clean_sheets": "CS",
    }
    available = {k: v for k, v in summary_cols.items() if k in p1_form.columns}
    if available:
        summary = p1_form[list(available.keys())].sum().round(2)
        summary.index = [available[c] for c in summary.index]
        st.dataframe(summary.to_frame(player1).T, use_container_width=True)
