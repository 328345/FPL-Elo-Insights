import os
import pandas as pd
import streamlit as st

SEASON = "2025-2026"
BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", SEASON)


@st.cache_data(ttl=1800)
def load_players():
    return pd.read_csv(os.path.join(BASE_PATH, "players.csv"))


@st.cache_data(ttl=1800)
def load_teams():
    return pd.read_csv(os.path.join(BASE_PATH, "teams.csv"))


@st.cache_data(ttl=1800)
def load_playerstats():
    return pd.read_csv(os.path.join(BASE_PATH, "playerstats.csv"))


@st.cache_data(ttl=1800)
def load_gameweek_summaries():
    return pd.read_csv(os.path.join(BASE_PATH, "gameweek_summaries.csv"))


@st.cache_data(ttl=1800)
def get_current_gameweek():
    ps = load_playerstats()
    return int(ps["gw"].max())


@st.cache_data(ttl=1800)
def load_fixtures(gw):
    path = os.path.join(BASE_PATH, "By Gameweek", f"GW{gw}", "fixtures.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "tournament" in df.columns:
        df = df[df["tournament"] == "prem"]
    return df


@st.cache_data(ttl=1800)
def load_matches(gw):
    path = os.path.join(BASE_PATH, "By Gameweek", f"GW{gw}", "matches.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "tournament" in df.columns:
        df = df[df["tournament"] == "prem"]
    return df


@st.cache_data(ttl=1800)
def load_player_gameweek_stats(gw):
    path = os.path.join(BASE_PATH, "By Gameweek", f"GW{gw}", "player_gameweek_stats.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


@st.cache_data(ttl=1800)
def get_master_player_df():
    """Core function: latest-GW playerstats joined with players and teams."""
    ps = load_playerstats()
    players = load_players()
    teams = load_teams()

    current_gw = int(ps["gw"].max())
    latest = ps[ps["gw"] == current_gw].copy()

    # Join with players for position and team_code
    latest = latest.merge(
        players[["player_id", "position", "team_code"]],
        left_on="id",
        right_on="player_id",
        how="left",
    )

    # Join with teams for team name, short_name, elo
    latest = latest.merge(
        teams[["code", "name", "short_name", "elo"]].rename(
            columns={"name": "team_name", "short_name": "team_short_name", "elo": "team_elo"}
        ),
        left_on="team_code",
        right_on="code",
        how="left",
    )

    # Shorten position names
    pos_map = {"Goalkeeper": "GKP", "Defender": "DEF", "Midfielder": "MID", "Forward": "FWD"}
    latest["position"] = latest["position"].map(pos_map).fillna(latest["position"])

    return latest


@st.cache_data(ttl=1800)
def get_upcoming_fixtures(n_gws=6):
    """Returns a dict: {team_code: [{gw, opponent_short, is_home, elo_diff}, ...]}"""
    current_gw = get_current_gameweek()
    teams = load_teams()
    team_lookup = teams.set_index("code")[["short_name", "elo"]].to_dict("index")

    all_fixtures = []
    for gw in range(current_gw + 1, current_gw + n_gws + 1):
        fx = load_fixtures(gw)
        if fx.empty:
            continue
        for _, row in fx.iterrows():
            home_code = int(row["home_team"])
            away_code = int(row["away_team"])
            home_info = team_lookup.get(home_code, {})
            away_info = team_lookup.get(away_code, {})
            home_elo = home_info.get("elo", 1500)
            away_elo = away_info.get("elo", 1500)

            # Home team's fixture
            all_fixtures.append({
                "team_code": home_code,
                "team_short": home_info.get("short_name", "???"),
                "gw": gw,
                "opponent_short": away_info.get("short_name", "???"),
                "is_home": True,
                "elo_diff": home_elo - away_elo,
            })
            # Away team's fixture
            all_fixtures.append({
                "team_code": away_code,
                "team_short": away_info.get("short_name", "???"),
                "gw": gw,
                "opponent_short": home_info.get("short_name", "???"),
                "is_home": False,
                "elo_diff": away_elo - home_elo,
            })

    return pd.DataFrame(all_fixtures) if all_fixtures else pd.DataFrame()


@st.cache_data(ttl=1800)
def get_recent_form_data(player_id, n_gws=8):
    """Load discrete per-GW stats for a player over the last N gameweeks."""
    current_gw = get_current_gameweek()
    rows = []
    for gw in range(max(1, current_gw - n_gws + 1), current_gw + 1):
        df = load_player_gameweek_stats(gw)
        if df.empty:
            continue
        player_row = df[df["id"] == player_id]
        if not player_row.empty:
            row = player_row.iloc[0].to_dict()
            row["gw"] = gw
            rows.append(row)
    return pd.DataFrame(rows) if rows else pd.DataFrame()
