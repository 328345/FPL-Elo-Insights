import streamlit as st
import pandas as pd


DISPLAY_COLS = {
    "web_name": "Player",
    "position": "Pos",
    "team_short_name": "Team",
    "now_cost": "Price",
    "total_points": "Pts",
    "form": "Form",
    "points_per_game": "PPG",
    "expected_goals": "xG",
    "expected_assists": "xA",
    "expected_goal_involvements": "xGI",
    "bonus": "Bonus",
    "selected_by_percent": "Own%",
    "value_form": "Val(Form)",
    "value_season": "Val(Szn)",
    "ep_next": "EP Next",
    "minutes": "Mins",
    "goals_scored": "Goals",
    "assists": "Assists",
    "clean_sheets": "CS",
    "status": "Status",
}

SORT_PRESETS = {
    "By Form": ("form", False),
    "By Total Points": ("total_points", False),
    "By xGI": ("expected_goal_involvements", False),
    "By Value (Form)": ("value_form", False),
    "By Value (Season)": ("value_season", False),
    "By EP Next": ("ep_next", False),
    "By Price (Low to High)": ("now_cost", True),
    "By Price (High to Low)": ("now_cost", False),
    "By Ownership": ("selected_by_percent", False),
}


def render(df: pd.DataFrame):
    st.header("Player Rankings")

    col1, col2 = st.columns([2, 3])
    with col1:
        sort_preset = st.selectbox("Sort by", list(SORT_PRESETS.keys()), index=0)
    with col2:
        search = st.text_input("Search player", "")

    sort_col, ascending = SORT_PRESETS[sort_preset]

    available_cols = [c for c in DISPLAY_COLS if c in df.columns]
    display = df[available_cols].copy()
    display = display.rename(columns=DISPLAY_COLS)

    if search:
        display = display[display["Player"].str.contains(search, case=False, na=False)]

    mapped_sort = DISPLAY_COLS.get(sort_col, sort_col)
    if mapped_sort in display.columns:
        display = display.sort_values(mapped_sort, ascending=ascending, na_position="last")

    display = display.reset_index(drop=True)
    display.index = display.index + 1

    st.dataframe(
        display,
        use_container_width=True,
        height=700,
        column_config={
            "Price": st.column_config.NumberColumn(format="%.1fm"),
            "Own%": st.column_config.NumberColumn(format="%.1f%%"),
            "xG": st.column_config.NumberColumn(format="%.2f"),
            "xA": st.column_config.NumberColumn(format="%.2f"),
            "xGI": st.column_config.NumberColumn(format="%.2f"),
            "PPG": st.column_config.NumberColumn(format="%.1f"),
            "Form": st.column_config.NumberColumn(format="%.1f"),
            "Val(Form)": st.column_config.NumberColumn(format="%.1f"),
            "Val(Szn)": st.column_config.NumberColumn(format="%.1f"),
            "EP Next": st.column_config.NumberColumn(format="%.1f"),
        },
    )

    st.caption(f"Showing {len(display)} players")
