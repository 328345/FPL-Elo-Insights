import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.data_loader import load_gameweek_summaries, load_players, get_current_gameweek


def render(df: pd.DataFrame, current_gw: int):
    st.header("Transfer Activity")

    # GW summary highlights
    gw_summaries = load_gameweek_summaries()
    players = load_players()

    # Find the most recent finished GW summary
    finished_gws = gw_summaries[gw_summaries["finished"] == True].sort_values("id", ascending=False)
    if not finished_gws.empty:
        latest_gw = finished_gws.iloc[0]

        def player_name(pid):
            if pd.isna(pid):
                return "N/A"
            row = players[players["player_id"] == int(pid)]
            return row.iloc[0]["web_name"] if not row.empty else f"ID:{int(pid)}"

        st.subheader(f"GW {int(latest_gw['id'])} Highlights")
        cols = st.columns(4)
        with cols[0]:
            st.metric("Most Captained", player_name(latest_gw.get("most_captained")))
        with cols[1]:
            st.metric("Most Transferred In", player_name(latest_gw.get("most_transferred_in")))
        with cols[2]:
            st.metric("Most Selected", player_name(latest_gw.get("most_selected")))
        with cols[3]:
            avg_score = latest_gw.get("average_entry_score", 0)
            st.metric("Average Score", f"{avg_score:.0f}" if pd.notna(avg_score) else "N/A")

    st.markdown("---")

    # Most transferred in
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Most Transferred In")
        if "transfers_in_event" in df.columns:
            top_in = df.nlargest(15, "transfers_in_event")[
                ["web_name", "team_short_name", "position", "now_cost", "transfers_in_event", "form"]
            ].copy()
            top_in.columns = ["Player", "Team", "Pos", "Price", "Transfers In", "Form"]
            top_in = top_in.reset_index(drop=True)
            top_in.index = top_in.index + 1

            fig_in = px.bar(
                top_in,
                y="Player",
                x="Transfers In",
                orientation="h",
                color="Form",
                color_continuous_scale="RdYlGn",
            )
            fig_in.update_layout(height=450, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_in, use_container_width=True)

    with col2:
        st.subheader("Most Transferred Out")
        if "transfers_out_event" in df.columns:
            top_out = df.nlargest(15, "transfers_out_event")[
                ["web_name", "team_short_name", "position", "now_cost", "transfers_out_event", "form"]
            ].copy()
            top_out.columns = ["Player", "Team", "Pos", "Price", "Transfers Out", "Form"]
            top_out = top_out.reset_index(drop=True)
            top_out.index = top_out.index + 1

            fig_out = px.bar(
                top_out,
                y="Player",
                x="Transfers Out",
                orientation="h",
                color="Form",
                color_continuous_scale="RdYlGn",
            )
            fig_out.update_layout(height=450, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_out, use_container_width=True)

    # Price changes
    st.markdown("---")
    st.subheader("Price Changes")

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("**Price Risers (This GW)**")
        if "cost_change_event" in df.columns:
            risers = df[df["cost_change_event"] > 0].nlargest(15, "cost_change_event")[
                ["web_name", "team_short_name", "now_cost", "cost_change_event", "selected_by_percent"]
            ].copy()
            risers.columns = ["Player", "Team", "Price", "Change", "Own%"]
            risers = risers.reset_index(drop=True)
            risers.index = risers.index + 1
            if not risers.empty:
                st.dataframe(risers, use_container_width=True, column_config={
                    "Price": st.column_config.NumberColumn(format="%.1fm"),
                    "Change": st.column_config.NumberColumn(format="+%.1f"),
                    "Own%": st.column_config.NumberColumn(format="%.1f%%"),
                })
            else:
                st.info("No price rises this gameweek.")

    with col4:
        st.markdown("**Price Fallers (This GW)**")
        if "cost_change_event" in df.columns:
            fallers = df[df["cost_change_event"] < 0].nsmallest(15, "cost_change_event")[
                ["web_name", "team_short_name", "now_cost", "cost_change_event", "selected_by_percent"]
            ].copy()
            fallers.columns = ["Player", "Team", "Price", "Change", "Own%"]
            fallers = fallers.reset_index(drop=True)
            fallers.index = fallers.index + 1
            if not fallers.empty:
                st.dataframe(fallers, use_container_width=True, column_config={
                    "Price": st.column_config.NumberColumn(format="%.1fm"),
                    "Change": st.column_config.NumberColumn(format="%.1f"),
                    "Own%": st.column_config.NumberColumn(format="%.1f%%"),
                })
            else:
                st.info("No price drops this gameweek.")

    # Season price changes
    st.markdown("---")
    st.subheader("Biggest Season Price Movers")
    if "cost_change_start" in df.columns:
        col5, col6 = st.columns(2)
        with col5:
            st.markdown("**Biggest Risers (Season)**")
            season_risers = df.nlargest(10, "cost_change_start")[
                ["web_name", "team_short_name", "now_cost", "cost_change_start", "total_points"]
            ].copy()
            season_risers.columns = ["Player", "Team", "Price", "Season Change", "Pts"]
            season_risers = season_risers.reset_index(drop=True)
            season_risers.index = season_risers.index + 1
            st.dataframe(season_risers, use_container_width=True, column_config={
                "Price": st.column_config.NumberColumn(format="%.1fm"),
                "Season Change": st.column_config.NumberColumn(format="+%.1f"),
            })

        with col6:
            st.markdown("**Biggest Fallers (Season)**")
            season_fallers = df.nsmallest(10, "cost_change_start")[
                ["web_name", "team_short_name", "now_cost", "cost_change_start", "total_points"]
            ].copy()
            season_fallers.columns = ["Player", "Team", "Price", "Season Change", "Pts"]
            season_fallers = season_fallers.reset_index(drop=True)
            season_fallers.index = season_fallers.index + 1
            st.dataframe(season_fallers, use_container_width=True, column_config={
                "Price": st.column_config.NumberColumn(format="%.1fm"),
                "Season Change": st.column_config.NumberColumn(format="%.1f"),
            })
