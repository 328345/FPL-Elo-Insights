import streamlit as st
import pandas as pd
import plotly.express as px


def render(df: pd.DataFrame):
    st.header("Value Picks")
    st.caption("Best value players by position - points per million spent.")

    min_mins = st.slider("Minimum minutes played", 0, 2000, 450, step=90, key="value_min_mins")
    filtered = df[df["minutes"] >= min_mins].copy()

    if filtered.empty:
        st.warning("No players match the filter criteria.")
        return

    # Compute value metrics
    filtered["pts_per_m"] = (filtered["total_points"] / filtered["now_cost"]).round(2)
    filtered["form_per_m"] = (filtered["form"] / filtered["now_cost"]).round(2)

    metric = st.radio("Value metric", ["Points per Million", "Form per Million"], horizontal=True)
    val_col = "pts_per_m" if metric == "Points per Million" else "form_per_m"
    val_label = "Pts/m" if val_col == "pts_per_m" else "Form/m"

    # Top 10 per position
    positions = ["GKP", "DEF", "MID", "FWD"]
    cols = st.columns(2)

    for i, pos in enumerate(positions):
        pos_df = filtered[filtered["position"] == pos].nlargest(10, val_col)
        with cols[i % 2]:
            st.subheader(pos)
            if pos_df.empty:
                st.info(f"No {pos} players match filters.")
                continue
            display = pos_df[["web_name", "team_short_name", "now_cost", "total_points", "form", val_col]].copy()
            display.columns = ["Player", "Team", "Price", "Pts", "Form", val_label]
            display = display.reset_index(drop=True)
            display.index = display.index + 1
            st.dataframe(
                display,
                use_container_width=True,
                column_config={
                    "Price": st.column_config.NumberColumn(format="%.1fm"),
                    val_label: st.column_config.NumberColumn(format="%.2f"),
                },
            )

    # Scatter plot
    st.markdown("---")
    st.subheader("Price vs Points")

    scatter_df = filtered[filtered["position"].isin(positions)].copy()
    fig = px.scatter(
        scatter_df,
        x="now_cost",
        y="total_points",
        color="position",
        hover_name="web_name",
        hover_data={"team_short_name": True, "form": True, "now_cost": ":.1f"},
        labels={
            "now_cost": "Price (m)",
            "total_points": "Total Points",
            "position": "Position",
            "team_short_name": "Team",
        },
        category_orders={"position": positions},
    )
    fig.update_layout(height=500)
    # Add trend line
    fig.update_traces(marker=dict(size=8, opacity=0.7))
    st.plotly_chart(fig, use_container_width=True)
