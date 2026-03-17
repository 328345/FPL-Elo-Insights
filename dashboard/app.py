import streamlit as st

st.set_page_config(
    page_title="FPL Transfer Insights",
    page_icon="\u26bd",
    layout="wide",
)

from dashboard.data_loader import get_master_player_df, get_current_gameweek

# Load master data
df = get_master_player_df()
current_gw = get_current_gameweek()

# --- Sidebar ---
st.sidebar.title("FPL Transfer Insights")
st.sidebar.caption(f"Season 2025-2026 | GW {current_gw}")

page = st.sidebar.radio(
    "Navigate",
    [
        "Player Rankings",
        "Fixture Difficulty",
        "Value Picks",
        "Form Trends",
        "Player Comparison",
        "Transfer Activity",
    ],
)

st.sidebar.markdown("---")
st.sidebar.subheader("Filters")

# Position filter
positions = sorted(df["position"].dropna().unique())
selected_positions = st.sidebar.multiselect("Position", positions, default=positions)

# Team filter
teams = sorted(df["team_short_name"].dropna().unique())
selected_teams = st.sidebar.multiselect("Team", teams, default=teams)

# Price range
min_cost = float(df["now_cost"].min())
max_cost = float(df["now_cost"].max())
price_range = st.sidebar.slider(
    "Price Range",
    min_value=min_cost,
    max_value=max_cost,
    value=(min_cost, max_cost),
    step=0.1,
    format="%.1fm",
)

# Min minutes
min_minutes = st.sidebar.slider("Min Minutes Played", 0, int(df["minutes"].max()), 0, step=90)

# Apply filters
filtered = df[
    (df["position"].isin(selected_positions))
    & (df["team_short_name"].isin(selected_teams))
    & (df["now_cost"] >= price_range[0])
    & (df["now_cost"] <= price_range[1])
    & (df["minutes"] >= min_minutes)
]

# --- Page Routing ---
if page == "Player Rankings":
    from dashboard.views.player_rankings import render
    render(filtered)
elif page == "Fixture Difficulty":
    from dashboard.views.fixture_difficulty import render
    render()
elif page == "Value Picks":
    from dashboard.views.value_picks import render
    render(filtered)
elif page == "Form Trends":
    from dashboard.views.form_trends import render
    render(df)
elif page == "Player Comparison":
    from dashboard.views.player_comparison import render
    render(df)
elif page == "Transfer Activity":
    from dashboard.views.transfer_activity import render
    render(filtered, current_gw)
