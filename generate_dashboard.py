"""
Generate a standalone FPL Transfer Dashboard HTML file.
Run: python3 generate_dashboard.py
Then open: dashboard.html in your browser.
"""
import os
import json
import pandas as pd
import numpy as np

SEASON = "2025-2026"
BASE_PATH = os.path.join("data", SEASON)
OUTPUT_FILE = "dashboard.html"


def load_data():
    players = pd.read_csv(os.path.join(BASE_PATH, "players.csv"))
    teams = pd.read_csv(os.path.join(BASE_PATH, "teams.csv"))
    ps = pd.read_csv(os.path.join(BASE_PATH, "playerstats.csv"))
    gw_summaries = pd.read_csv(os.path.join(BASE_PATH, "gameweek_summaries.csv"))

    current_gw = int(ps["gw"].max())

    # Master player DF
    latest = ps[ps["gw"] == current_gw].copy()
    latest = latest.merge(
        players[["player_id", "position", "team_code"]],
        left_on="id", right_on="player_id", how="left"
    )
    pos_map = {"Goalkeeper": "GKP", "Defender": "DEF", "Midfielder": "MID", "Forward": "FWD"}
    latest["position"] = latest["position"].map(pos_map).fillna(latest["position"])
    latest = latest.merge(
        teams[["code", "name", "short_name", "elo"]].rename(
            columns={"name": "team_name", "short_name": "team_short_name", "elo": "team_elo"}
        ),
        left_on="team_code", right_on="code", how="left"
    )
    latest["pts_per_m"] = (latest["total_points"] / latest["now_cost"]).round(2)

    # Fixtures for next 8 GWs
    team_lookup = teams.set_index("code")[["short_name", "elo"]].to_dict("index")
    fixture_rows = []
    for gw in range(current_gw + 1, current_gw + 9):
        path = os.path.join(BASE_PATH, "By Gameweek", f"GW{gw}", "fixtures.csv")
        if not os.path.exists(path):
            continue
        fx = pd.read_csv(path)
        if "tournament" in fx.columns:
            fx = fx[fx["tournament"] == "prem"]
        for _, row in fx.iterrows():
            try:
                h = int(row["home_team"])
                a = int(row["away_team"])
            except (ValueError, TypeError):
                continue
            h_info = team_lookup.get(h, {})
            a_info = team_lookup.get(a, {})
            h_elo = h_info.get("elo", 1500)
            a_elo = a_info.get("elo", 1500)
            fixture_rows.append({"team_code": h, "team_short": h_info.get("short_name", "?"),
                                  "gw": gw, "opponent": a_info.get("short_name", "?"),
                                  "is_home": True, "elo_diff": h_elo - a_elo})
            fixture_rows.append({"team_code": a, "team_short": a_info.get("short_name", "?"),
                                  "gw": gw, "opponent": h_info.get("short_name", "?"),
                                  "is_home": False, "elo_diff": a_elo - h_elo})

    # Per-GW data for last 10 GWs
    gw_data = {}
    for gw in range(max(1, current_gw - 9), current_gw + 1):
        path = os.path.join(BASE_PATH, "By Gameweek", f"GW{gw}", "player_gameweek_stats.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            gw_data[gw] = df[["id", "event_points", "minutes", "goals_scored", "assists",
                               "expected_goals", "expected_assists", "expected_goal_involvements",
                               "bonus", "clean_sheets"]].fillna(0).to_dict("records")

    # GW summary highlights
    finished = gw_summaries[gw_summaries["finished"] == True].sort_values("id", ascending=False)
    gw_highlight = {}
    if not finished.empty:
        row = finished.iloc[0]
        pid_map = players.set_index("player_id")["web_name"].to_dict()
        def pname(pid):
            try:
                return pid_map.get(int(pid), f"ID:{pid}")
            except Exception:
                return "N/A"
        gw_highlight = {
            "gw": int(row["id"]),
            "most_captained": pname(row.get("most_captained")),
            "most_transferred_in": pname(row.get("most_transferred_in")),
            "most_selected": pname(row.get("most_selected")),
            "avg_score": row.get("average_entry_score", 0),
        }

    # Clean player records
    keep_cols = ["id", "web_name", "position", "team_short_name", "team_elo",
                 "now_cost", "total_points", "form", "points_per_game", "minutes",
                 "goals_scored", "assists", "clean_sheets", "expected_goals",
                 "expected_assists", "expected_goal_involvements", "bonus", "bps",
                 "selected_by_percent", "value_form", "value_season", "ep_next",
                 "ict_index", "influence", "creativity", "threat",
                 "transfers_in_event", "transfers_out_event",
                 "cost_change_event", "cost_change_start",
                 "status", "news", "chance_of_playing_next_round", "pts_per_m"]
    available = [c for c in keep_cols if c in latest.columns]
    records = latest[available].replace({float("nan"): None, float("inf"): None, float("-inf"): None}).to_dict("records")

    return {
        "current_gw": current_gw,
        "players": records,
        "fixtures": fixture_rows,
        "gw_data": {str(k): v for k, v in gw_data.items()},
        "gw_highlight": gw_highlight,
    }


def generate_html(data):
    class NumpyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return super().default(obj)

    data_json = json.dumps(data, cls=NumpyEncoder, allow_nan=False)
    current_gw = data["current_gw"]

    # Use plain string (no f-string) to avoid conflicts with JS object literals.
    # Inject Python values via .replace() at the end.
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FPL Transfer Insights — GW__CURRENT_GW__</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0e1117; --surface: #1a1d27; --surface2: #22263a;
    --text: #e8eaf6; --text2: #9ea3c0; --accent: #6c63ff;
    --green: #4caf50; --red: #f44336; --yellow: #ffeb3b; --orange: #ff9800;
    --border: #2e3356;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }
  #app { display: flex; height: 100vh; overflow: hidden; }

  /* Sidebar */
  #sidebar { width: 220px; min-width: 220px; background: var(--surface); border-right: 1px solid var(--border); display: flex; flex-direction: column; overflow-y: auto; }
  #sidebar h1 { padding: 18px 16px 4px; font-size: 15px; font-weight: 700; color: var(--accent); }
  #sidebar .gw-badge { margin: 0 16px 14px; font-size: 11px; color: var(--text2); background: var(--surface2); padding: 3px 8px; border-radius: 4px; display: inline-block; }
  .nav-item { padding: 10px 16px; cursor: pointer; font-size: 13px; color: var(--text2); border-left: 3px solid transparent; transition: all .15s; }
  .nav-item:hover { background: var(--surface2); color: var(--text); }
  .nav-item.active { color: var(--accent); border-left-color: var(--accent); background: var(--surface2); font-weight: 600; }
  #sidebar hr { border: none; border-top: 1px solid var(--border); margin: 8px 0; }
  #sidebar .filter-label { padding: 8px 16px 4px; font-size: 11px; font-weight: 600; color: var(--text2); text-transform: uppercase; letter-spacing: .5px; }
  .filter-group { padding: 0 16px 10px; }
  .filter-group select, .filter-group input[type=range] { width: 100%; margin-top: 4px; }
  .filter-group select { background: var(--surface2); color: var(--text); border: 1px solid var(--border); border-radius: 4px; padding: 5px; font-size: 12px; }
  .filter-group select[multiple] { height: 80px; }
  .range-val { font-size: 11px; color: var(--text2); text-align: right; margin-top: 2px; }

  /* Main content */
  #main { flex: 1; overflow-y: auto; padding: 24px; }
  .page { display: none; }
  .page.active { display: block; }
  h2 { font-size: 20px; font-weight: 700; margin-bottom: 6px; }
  .subtitle { color: var(--text2); font-size: 13px; margin-bottom: 20px; }

  /* Tables */
  .tbl-wrap { overflow-x: auto; border-radius: 8px; border: 1px solid var(--border); }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  thead th { background: var(--surface2); padding: 9px 10px; text-align: left; font-size: 11px; font-weight: 600; color: var(--text2); text-transform: uppercase; cursor: pointer; white-space: nowrap; user-select: none; }
  thead th:hover { color: var(--accent); }
  tbody tr { border-top: 1px solid var(--border); }
  tbody tr:hover { background: var(--surface2); }
  tbody td { padding: 8px 10px; white-space: nowrap; }
  .sort-asc::after { content: ' ↑'; color: var(--accent); }
  .sort-desc::after { content: ' ↓'; color: var(--accent); }

  /* Status badges */
  .badge { display: inline-block; padding: 2px 7px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .badge-a { background: #1b3a1f; color: #4caf50; }
  .badge-i { background: #3a1b1b; color: #f44336; }
  .badge-d { background: #3a2e1b; color: #ff9800; }
  .badge-u { background: #2a2a2a; color: #9ea3c0; }

  /* Controls row */
  .controls { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 16px; align-items: center; }
  .controls input[type=text] { background: var(--surface); border: 1px solid var(--border); color: var(--text); padding: 7px 12px; border-radius: 6px; font-size: 13px; width: 200px; }
  .controls select { background: var(--surface); border: 1px solid var(--border); color: var(--text); padding: 7px 10px; border-radius: 6px; font-size: 13px; }
  .controls label { font-size: 12px; color: var(--text2); }

  /* Metrics cards */
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
  .card .card-label { font-size: 11px; color: var(--text2); text-transform: uppercase; letter-spacing: .5px; margin-bottom: 6px; }
  .card .card-value { font-size: 20px; font-weight: 700; color: var(--accent); }
  .card .card-sub { font-size: 12px; color: var(--text2); margin-top: 2px; }

  /* Fixture grid */
  #fixture-grid { overflow-x: auto; }
  .fx-table { border-collapse: collapse; font-size: 13px; width: 100%; }
  .fx-table th { background: var(--surface2); padding: 8px 12px; text-align: center; font-size: 11px; font-weight: 600; color: var(--text2); border: 1px solid var(--border); }
  .fx-table th:first-child { text-align: left; }
  .fx-table td { padding: 7px 10px; border: 1px solid var(--border); text-align: center; font-size: 12px; font-weight: 500; }
  .fx-table td:first-child { text-align: left; font-weight: 700; }
  .legend { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
  .legend-item { display: flex; align-items: center; gap: 4px; font-size: 11px; }
  .legend-swatch { width: 14px; height: 14px; border-radius: 3px; }

  /* Chart containers */
  .chart-wrap { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 16px; position: relative; }
  .chart-wrap h3 { font-size: 14px; font-weight: 600; margin-bottom: 12px; }
  .chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  @media (max-width: 800px) { .chart-row { grid-template-columns: 1fr; } }

  /* Radar */
  .radar-wrap { max-width: 420px; margin: 0 auto; }

  /* Player selector */
  .player-sel-group { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 16px; }
  .player-sel-group > div { display: flex; flex-direction: column; gap: 4px; }
  .player-sel-group label { font-size: 11px; color: var(--text2); }
  .player-sel-group select { background: var(--surface); border: 1px solid var(--border); color: var(--text); padding: 7px 10px; border-radius: 6px; font-size: 13px; min-width: 180px; }

  /* Comparison table */
  .cmp-table td:first-child { color: var(--text2); font-size: 12px; }
  .cmp-table td:not(:first-child) { font-weight: 600; }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: var(--surface); }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

  /* Positive / negative numbers */
  .pos { color: #4caf50; } .neg { color: #f44336; }

  /* Range slider */
  input[type=range] { accent-color: var(--accent); }

  .tabs { display: flex; gap: 4px; margin-bottom: 16px; flex-wrap: wrap; }
  .tab { padding: 6px 14px; border-radius: 6px; font-size: 12px; cursor: pointer; background: var(--surface2); color: var(--text2); border: 1px solid var(--border); }
  .tab.active { background: var(--accent); color: white; border-color: var(--accent); }
</style>
</head>
<body>
<div id="app">
  <div id="sidebar">
    <h1>⚽ FPL Insights</h1>
    <span class="gw-badge">GW<span id="gw-badge">__CURRENT_GW__</span> · 2025/26</span>
    <nav>
      <div class="nav-item active" onclick="navigate('rankings')">📊 Player Rankings</div>
      <div class="nav-item" onclick="navigate('fixtures')">📅 Fixture Difficulty</div>
      <div class="nav-item" onclick="navigate('value')">💰 Value Picks</div>
      <div class="nav-item" onclick="navigate('form')">📈 Form Trends</div>
      <div class="nav-item" onclick="navigate('compare')">⚖️ Compare Players</div>
      <div class="nav-item" onclick="navigate('transfers')">🔄 Transfer Activity</div>
    </nav>
    <hr>
    <div class="filter-label">Filters</div>
    <div class="filter-group">
      <label style="font-size:12px;color:var(--text2)">Position</label>
      <select id="f-pos" multiple onchange="applyFilters()">
        <option value="GKP" selected>Goalkeeper</option>
        <option value="DEF" selected>Defender</option>
        <option value="MID" selected>Midfielder</option>
        <option value="FWD" selected>Forward</option>
      </select>
    </div>
    <div class="filter-group" id="team-filter-wrap">
      <label style="font-size:12px;color:var(--text2)">Team</label>
      <select id="f-team" multiple onchange="applyFilters()"></select>
    </div>
    <div class="filter-group">
      <label style="font-size:12px;color:var(--text2)">Max Price: <span id="f-price-val">15.0</span>m</label>
      <input type="range" id="f-price" min="3" max="15" step="0.1" value="15" oninput="document.getElementById('f-price-val').textContent=parseFloat(this.value).toFixed(1);applyFilters()">
    </div>
    <div class="filter-group">
      <label style="font-size:12px;color:var(--text2)">Min Minutes: <span id="f-mins-val">0</span></label>
      <input type="range" id="f-mins" min="0" max="2700" step="90" value="0" oninput="document.getElementById('f-mins-val').textContent=this.value;applyFilters()">
    </div>
  </div>

  <div id="main">
    <!-- RANKINGS -->
    <div class="page active" id="page-rankings">
      <h2>Player Rankings</h2>
      <p class="subtitle">All players for the current season. Click column headers to sort.</p>
      <div class="controls">
        <input type="text" id="search-player" placeholder="Search player..." oninput="renderRankings()">
        <select id="sort-preset" onchange="applyPreset()">
          <option value="form,desc">Sort: Form</option>
          <option value="total_points,desc">Sort: Total Points</option>
          <option value="expected_goal_involvements,desc">Sort: xGI</option>
          <option value="pts_per_m,desc">Sort: Value (Pts/m)</option>
          <option value="value_season,desc">Sort: Value Season</option>
          <option value="ep_next,desc">Sort: EP Next</option>
          <option value="now_cost,asc">Sort: Price ↑</option>
          <option value="selected_by_percent,desc">Sort: Ownership</option>
        </select>
      </div>
      <div class="tbl-wrap">
        <table id="rankings-table">
          <thead>
            <tr>
              <th onclick="sortTable('web_name','asc')" id="th-web_name">Player</th>
              <th onclick="sortTable('position','asc')" id="th-position">Pos</th>
              <th onclick="sortTable('team_short_name','asc')" id="th-team_short_name">Team</th>
              <th onclick="sortTable('now_cost','desc')" id="th-now_cost">Price</th>
              <th onclick="sortTable('total_points','desc')" id="th-total_points">Pts</th>
              <th onclick="sortTable('form','desc')" id="th-form">Form</th>
              <th onclick="sortTable('points_per_game','desc')" id="th-points_per_game">PPG</th>
              <th onclick="sortTable('expected_goals','desc')" id="th-expected_goals">xG</th>
              <th onclick="sortTable('expected_assists','desc')" id="th-expected_assists">xA</th>
              <th onclick="sortTable('expected_goal_involvements','desc')" id="th-expected_goal_involvements">xGI</th>
              <th onclick="sortTable('bonus','desc')" id="th-bonus">Bonus</th>
              <th onclick="sortTable('selected_by_percent','desc')" id="th-selected_by_percent">Own%</th>
              <th onclick="sortTable('pts_per_m','desc')" id="th-pts_per_m">Pts/m</th>
              <th onclick="sortTable('ep_next','desc')" id="th-ep_next">EP Next</th>
              <th onclick="sortTable('minutes','desc')" id="th-minutes">Mins</th>
              <th onclick="sortTable('status','asc')" id="th-status">Status</th>
            </tr>
          </thead>
          <tbody id="rankings-body"></tbody>
        </table>
      </div>
      <p id="rankings-count" style="font-size:12px;color:var(--text2);margin-top:8px;"></p>
    </div>

    <!-- FIXTURES -->
    <div class="page" id="page-fixtures">
      <h2>Fixture Difficulty</h2>
      <p class="subtitle">Upcoming Premier League fixtures rated by Elo strength difference.</p>
      <div class="controls">
        <label>Gameweeks to show:
          <select id="fx-gws" onchange="renderFixtures()">
            <option value="4">4 GWs</option>
            <option value="6" selected>6 GWs</option>
            <option value="8">8 GWs</option>
          </select>
        </label>
        <label><input type="checkbox" id="fx-prem-only" checked onchange="renderFixtures()"> Premier League only</label>
      </div>
      <div id="fixture-grid"></div>
      <div class="legend">
        <div class="legend-item"><div class="legend-swatch" style="background:#1b7a2b"></div> Very Easy (&gt;150)</div>
        <div class="legend-item"><div class="legend-swatch" style="background:#4caf50"></div> Easy (75–150)</div>
        <div class="legend-item"><div class="legend-swatch" style="background:#8bc34a"></div> Fairly Easy (25–75)</div>
        <div class="legend-item"><div class="legend-swatch" style="background:#ffeb3b"></div> Neutral (±25)</div>
        <div class="legend-item"><div class="legend-swatch" style="background:#ff9800"></div> Tough (-25 to -75)</div>
        <div class="legend-item"><div class="legend-swatch" style="background:#f44336"></div> Hard (-75 to -150)</div>
        <div class="legend-item"><div class="legend-swatch" style="background:#b71c1c"></div> Very Hard (&lt;-150)</div>
      </div>
    </div>

    <!-- VALUE -->
    <div class="page" id="page-value">
      <h2>Value Picks</h2>
      <p class="subtitle">Best points-per-million players by position.</p>
      <div class="controls">
        <label>Min minutes: <input type="number" id="val-mins" value="450" min="0" step="90" style="width:80px;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:5px 8px;border-radius:4px;" oninput="renderValue()"></label>
        <div class="tabs" id="value-metric-tabs">
          <div class="tab active" onclick="setValueMetric('pts_per_m',this)">Pts per Million</div>
          <div class="tab" onclick="setValueMetric('form_per_m',this)">Form per Million</div>
        </div>
      </div>
      <div class="chart-row" id="value-tables"></div>
      <div class="chart-wrap" style="margin-top:16px;">
        <h3>Price vs Total Points</h3>
        <canvas id="scatter-chart" height="350"></canvas>
      </div>
    </div>

    <!-- FORM -->
    <div class="page" id="page-form">
      <h2>Form Trends</h2>
      <p class="subtitle">Gameweek-by-gameweek performance over recent weeks.</p>
      <div class="player-sel-group">
        <div><label>Player 1</label><select id="form-p1" onchange="renderForm()"></select></div>
        <div><label>Player 2 (optional)</label><select id="form-p2" onchange="renderForm()"><option value="">— None —</option></select></div>
        <div><label>Gameweeks</label>
          <select id="form-gws" onchange="renderForm()">
            <option value="5">5 GWs</option>
            <option value="8" selected>8 GWs</option>
            <option value="10">10 GWs</option>
          </select>
        </div>
      </div>
      <div class="chart-wrap">
        <h3>FPL Points per Gameweek</h3>
        <canvas id="form-points-chart" height="280"></canvas>
      </div>
      <div class="chart-wrap">
        <h3>Expected Goals &amp; Assists per Gameweek</h3>
        <canvas id="form-xg-chart" height="250"></canvas>
      </div>
      <div id="form-summary"></div>
    </div>

    <!-- COMPARE -->
    <div class="page" id="page-compare">
      <h2>Compare Players</h2>
      <p class="subtitle">Side-by-side stats and radar chart comparison.</p>
      <div class="player-sel-group">
        <div><label>Player 1</label><select id="cmp-p1" onchange="renderCompare()"></select></div>
        <div><label>Player 2</label><select id="cmp-p2" onchange="renderCompare()"></select></div>
        <div><label>Player 3 (optional)</label><select id="cmp-p3" onchange="renderCompare()"><option value="">— None —</option></select></div>
      </div>
      <div class="chart-row">
        <div class="chart-wrap">
          <h3>Radar Comparison</h3>
          <div class="radar-wrap"><canvas id="radar-chart"></canvas></div>
        </div>
        <div class="tbl-wrap" style="max-height:500px;overflow-y:auto;">
          <table id="cmp-table" class="cmp-table">
            <thead><tr id="cmp-thead"></tr></thead>
            <tbody id="cmp-tbody"></tbody>
          </table>
        </div>
      </div>
      <div id="cmp-info-cards" class="cards" style="margin-top:16px;"></div>
    </div>

    <!-- TRANSFERS -->
    <div class="page" id="page-transfers">
      <h2>Transfer Activity</h2>
      <p class="subtitle">Most moved players and price changes this gameweek.</p>
      <div class="cards" id="transfer-highlights"></div>
      <div class="chart-row">
        <div class="chart-wrap">
          <h3>Most Transferred In</h3>
          <canvas id="tin-chart" height="320"></canvas>
        </div>
        <div class="chart-wrap">
          <h3>Most Transferred Out</h3>
          <canvas id="tout-chart" height="320"></canvas>
        </div>
      </div>
      <div class="chart-row" style="margin-top:16px;">
        <div>
          <h3 style="font-size:14px;font-weight:600;margin-bottom:8px;">Price Risers (This GW)</h3>
          <div class="tbl-wrap"><table id="risers-table"><thead><tr><th>Player</th><th>Team</th><th>Price</th><th>Change</th><th>Own%</th></tr></thead><tbody id="risers-body"></tbody></table></div>
        </div>
        <div>
          <h3 style="font-size:14px;font-weight:600;margin-bottom:8px;">Price Fallers (This GW)</h3>
          <div class="tbl-wrap"><table id="fallers-table"><thead><tr><th>Player</th><th>Team</th><th>Price</th><th>Change</th><th>Own%</th></tr></thead><tbody id="fallers-body"></tbody></table></div>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
const RAW_DATA = __DATA_JSON__;
const DATA = RAW_DATA.players;
const FIXTURES = RAW_DATA.fixtures;
const GW_DATA = RAW_DATA.gw_data;
const GW_HIGHLIGHT = RAW_DATA.gw_highlight;
const CURRENT_GW = RAW_DATA.current_gw;

let filteredData = [...DATA];
let sortKey = 'form';
let sortDir = 'desc';
let valueMetric = 'pts_per_m';

// ---- Shared chart instances (destroy before recreate) ----
const charts = {};
function getOrDestroyChart(id) {
  if (charts[id]) { charts[id].destroy(); delete charts[id]; }
}

// ---- Navigation ----
function navigate(page) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('page-' + page).classList.add('active');
  event.currentTarget.classList.add('active');
  if (page === 'rankings') renderRankings();
  if (page === 'fixtures') renderFixtures();
  if (page === 'value') renderValue();
  if (page === 'form') renderForm();
  if (page === 'compare') renderCompare();
  if (page === 'transfers') renderTransfers();
}

// ---- Filters ----
function applyFilters() {
  const pos = Array.from(document.getElementById('f-pos').selectedOptions).map(o => o.value);
  const teams = Array.from(document.getElementById('f-team').selectedOptions).map(o => o.value);
  const maxPrice = parseFloat(document.getElementById('f-price').value);
  const minMins = parseInt(document.getElementById('f-mins').value);
  filteredData = DATA.filter(p =>
    pos.includes(p.position) &&
    (teams.length === 0 || teams.includes(p.team_short_name)) &&
    (p.now_cost || 0) <= maxPrice &&
    (p.minutes || 0) >= minMins
  );
  renderRankings();
  renderValue();
}

function initTeamFilter() {
  const teams = [...new Set(DATA.map(p => p.team_short_name).filter(Boolean))].sort();
  const sel = document.getElementById('f-team');
  sel.innerHTML = teams.map(t => `<option value="${t}" selected>${t}</option>`).join('');
}

// ---- Rankings ----
function sortTable(key, defaultDir) {
  if (sortKey === key) {
    sortDir = sortDir === 'asc' ? 'desc' : 'asc';
  } else {
    sortKey = key;
    sortDir = defaultDir || 'desc';
  }
  document.querySelectorAll('thead th').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
  });
  const th = document.getElementById('th-' + key);
  if (th) th.classList.add(sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
  renderRankings();
}

function applyPreset() {
  const [k, d] = document.getElementById('sort-preset').value.split(',');
  sortKey = k; sortDir = d;
  renderRankings();
}

function renderRankings() {
  const search = (document.getElementById('search-player').value || '').toLowerCase();
  let rows = filteredData.filter(p => !search || (p.web_name || '').toLowerCase().includes(search));
  rows.sort((a, b) => {
    let av = a[sortKey], bv = b[sortKey];
    if (av === null || av === undefined) av = sortDir === 'asc' ? Infinity : -Infinity;
    if (bv === null || bv === undefined) bv = sortDir === 'asc' ? Infinity : -Infinity;
    return sortDir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
  });

  const statBadge = s => {
    const map = {a:'badge-a',i:'badge-i',d:'badge-d',u:'badge-u'};
    const label = {a:'✓',i:'Inj',d:'Dbt',u:'N/A'};
    return `<span class="badge ${map[s]||'badge-u'}">${label[s]||s}</span>`;
  };

  document.getElementById('rankings-body').innerHTML = rows.map(p => `
    <tr>
      <td><b>${p.web_name||''}</b></td>
      <td>${p.position||''}</td>
      <td>${p.team_short_name||''}</td>
      <td>${fmt(p.now_cost,1)}m</td>
      <td><b>${p.total_points||0}</b></td>
      <td>${fmt(p.form,1)}</td>
      <td>${fmt(p.points_per_game,1)}</td>
      <td>${fmt(p.expected_goals,2)}</td>
      <td>${fmt(p.expected_assists,2)}</td>
      <td>${fmt(p.expected_goal_involvements,2)}</td>
      <td>${p.bonus||0}</td>
      <td>${fmt(p.selected_by_percent,1)}%</td>
      <td>${fmt(p.pts_per_m,2)}</td>
      <td>${fmt(p.ep_next,1)}</td>
      <td>${p.minutes||0}</td>
      <td>${statBadge(p.status)}</td>
    </tr>`).join('');
  document.getElementById('rankings-count').textContent = `Showing ${rows.length} players`;
}

// ---- Fixtures ----
function eloColor(diff) {
  if (diff > 150) return ['#1b7a2b','#fff'];
  if (diff > 75)  return ['#4caf50','#000'];
  if (diff > 25)  return ['#8bc34a','#000'];
  if (diff > -25) return ['#ffeb3b','#000'];
  if (diff > -75) return ['#ff9800','#000'];
  if (diff > -150)return ['#f44336','#fff'];
  return ['#b71c1c','#fff'];
}

function renderFixtures() {
  const n = parseInt(document.getElementById('fx-gws').value);
  const gws = [...new Set(FIXTURES.map(f => f.gw))].sort((a,b)=>a-b).slice(0,n);
  const teams = [...new Set(FIXTURES.map(f => f.team_short))].sort();

  let html = '<table class="fx-table"><thead><tr><th>Team</th>' +
    gws.map(g => `<th>GW${g}</th>`).join('') + '</tr></thead><tbody>';

  for (const team of teams) {
    html += `<tr><td>${team}</td>`;
    for (const gw of gws) {
      const fixtures = FIXTURES.filter(f => f.team_short === team && f.gw === gw);
      if (!fixtures.length) {
        html += `<td style="background:#1a1d27;color:#444">—</td>`;
      } else {
        const avgDiff = fixtures.reduce((s,f) => s + f.elo_diff, 0) / fixtures.length;
        const [bg, fg] = eloColor(avgDiff);
        const text = fixtures.map(f => `${f.opponent} (${f.is_home ? 'H' : 'A'})`).join('<br>');
        html += `<td style="background:${bg};color:${fg}">${text}</td>`;
      }
    }
    html += '</tr>';
  }
  html += '</tbody></table>';
  document.getElementById('fixture-grid').innerHTML = html;
}

// ---- Value ----
function setValueMetric(m, el) {
  valueMetric = m;
  document.querySelectorAll('#value-metric-tabs .tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  renderValue();
}

let scatterChart = null;
function renderValue() {
  const minMins = parseInt(document.getElementById('val-mins').value) || 0;
  const metric = valueMetric;

  let pool = filteredData.filter(p => (p.minutes||0) >= minMins).map(p => ({
    ...p,
    form_per_m: p.now_cost ? ((p.form||0) / p.now_cost) : 0
  }));

  const positions = ['GKP','DEF','MID','FWD'];
  const metricLabel = metric === 'pts_per_m' ? 'Pts/m' : 'Form/m';

  let tablesHtml = '';
  for (const pos of positions) {
    const posPlayers = pool.filter(p => p.position === pos)
      .sort((a,b) => (b[metric]||0) - (a[metric]||0)).slice(0,10);
    tablesHtml += `
      <div>
        <h3 style="font-size:14px;font-weight:600;margin-bottom:8px;">${pos}</h3>
        <div class="tbl-wrap">
          <table>
            <thead><tr><th>Player</th><th>Team</th><th>Price</th><th>Pts</th><th>${metricLabel}</th></tr></thead>
            <tbody>${posPlayers.map(p=>`<tr>
              <td><b>${p.web_name}</b></td>
              <td>${p.team_short_name||''}</td>
              <td>${fmt(p.now_cost,1)}m</td>
              <td>${p.total_points||0}</td>
              <td><b>${fmt(p[metric],2)}</b></td>
            </tr>`).join('')}</tbody>
          </table>
        </div>
      </div>`;
  }
  document.getElementById('value-tables').innerHTML = tablesHtml;

  // Scatter
  getOrDestroyChart('scatter');
  const posColors = {GKP:'#9c27b0',DEF:'#2196f3',MID:'#4caf50',FWD:'#ff9800'};
  const datasets = positions.map(pos => ({
    label: pos,
    data: pool.filter(p=>p.position===pos).map(p=>({x:p.now_cost,y:p.total_points,name:p.web_name,team:p.team_short_name})),
    backgroundColor: posColors[pos] + 'cc',
    pointRadius: 5,
  }));

  const ctx = document.getElementById('scatter-chart').getContext('2d');
  charts['scatter'] = new Chart(ctx, {
    type: 'scatter',
    data: { datasets },
    options: {
      responsive: true,
      plugins: {
        tooltip: { callbacks: { label: ctx => `${ctx.raw.name} (${ctx.raw.team}) — ${fmt(ctx.parsed.x,1)}m · ${ctx.parsed.y}pts` } },
        legend: { labels: { color: '#e8eaf6' } }
      },
      scales: {
        x: { title: { display:true, text:'Price (m)', color:'#9ea3c0' }, ticks: { color:'#9ea3c0' }, grid: { color:'#2e3356' } },
        y: { title: { display:true, text:'Total Points', color:'#9ea3c0' }, ticks: { color:'#9ea3c0' }, grid: { color:'#2e3356' } }
      }
    }
  });
}

// ---- Form Trends ----
function renderForm() {
  const p1Name = document.getElementById('form-p1').value;
  const p2Name = document.getElementById('form-p2').value;
  const n = parseInt(document.getElementById('form-gws').value);

  const p1 = DATA.find(p => p.web_name === p1Name);
  if (!p1) return;

  const gws = Object.keys(GW_DATA).map(Number).sort((a,b)=>a-b).slice(-n);

  function getPlayerGwData(playerId) {
    return gws.map(gw => {
      const rows = GW_DATA[String(gw)] || [];
      return rows.find(r => r.id === playerId) || null;
    });
  }

  const p1Rows = getPlayerGwData(p1.id);

  // Points chart
  getOrDestroyChart('form-points');
  const ptCtx = document.getElementById('form-points-chart').getContext('2d');
  const datasets = [{
    label: p1.web_name,
    data: p1Rows.map(r => r ? r.event_points : null),
    borderColor: '#6c63ff', backgroundColor: '#6c63ff33',
    borderWidth: 2.5, pointRadius: 4, tension: 0.3, spanGaps: true,
  }];

  if (p2Name) {
    const p2 = DATA.find(p => p.web_name === p2Name);
    if (p2) {
      const p2Rows = getPlayerGwData(p2.id);
      datasets.push({
        label: p2.web_name,
        data: p2Rows.map(r => r ? r.event_points : null),
        borderColor: '#ff9800', backgroundColor: '#ff980033',
        borderWidth: 2.5, pointRadius: 4, tension: 0.3, borderDash: [5,3], spanGaps: true,
      });
    }
  }

  charts['form-points'] = new Chart(ptCtx, {
    type: 'line',
    data: { labels: gws.map(g=>`GW${g}`), datasets },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color:'#e8eaf6' } } },
      scales: {
        x: { ticks: { color:'#9ea3c0' }, grid: { color:'#2e3356' } },
        y: { ticks: { color:'#9ea3c0' }, grid: { color:'#2e3356' }, beginAtZero:true }
      }
    }
  });

  // xG/xA chart
  getOrDestroyChart('form-xg');
  const xgCtx = document.getElementById('form-xg-chart').getContext('2d');
  charts['form-xg'] = new Chart(xgCtx, {
    type: 'bar',
    data: {
      labels: gws.map(g=>`GW${g}`),
      datasets: [
        { label: `${p1.web_name} xG`, data: p1Rows.map(r=>r?r.expected_goals:null), backgroundColor:'#4caf5099', borderColor:'#4caf50', borderWidth:1 },
        { label: `${p1.web_name} xA`, data: p1Rows.map(r=>r?r.expected_assists:null), backgroundColor:'#2196f399', borderColor:'#2196f3', borderWidth:1 },
      ]
    },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color:'#e8eaf6' } } },
      scales: {
        x: { ticks: { color:'#9ea3c0' }, grid: { color:'#2e3356' } },
        y: { ticks: { color:'#9ea3c0' }, grid: { color:'#2e3356' }, beginAtZero:true }
      }
    }
  });

  // Summary
  const sum = (arr, key) => arr.filter(Boolean).reduce((s,r)=>s+(r[key]||0),0);
  document.getElementById('form-summary').innerHTML = `
    <div class="cards" style="margin-top:12px;">
      <div class="card"><div class="card-label">Total Points</div><div class="card-value">${sum(p1Rows,'event_points')}</div><div class="card-sub">last ${n} GWs</div></div>
      <div class="card"><div class="card-label">Goals</div><div class="card-value">${sum(p1Rows,'goals_scored')}</div></div>
      <div class="card"><div class="card-label">Assists</div><div class="card-value">${sum(p1Rows,'assists')}</div></div>
      <div class="card"><div class="card-label">xG</div><div class="card-value">${fmt(sum(p1Rows,'expected_goals'),2)}</div></div>
      <div class="card"><div class="card-label">xA</div><div class="card-value">${fmt(sum(p1Rows,'expected_assists'),2)}</div></div>
      <div class="card"><div class="card-label">Bonus Pts</div><div class="card-value">${sum(p1Rows,'bonus')}</div></div>
    </div>`;
}

// ---- Compare ----
const RADAR_KEYS = ['form','points_per_game','expected_goal_involvements','bonus','ict_index','value_season'];
const RADAR_LABELS = ['Form','PPG','xGI','Bonus','ICT','Value'];
const CMP_METRICS = [
  ['total_points','Total Points'],['form','Form'],['points_per_game','PPG'],
  ['now_cost','Price (m)'],['expected_goals','xG'],['expected_assists','xA'],
  ['expected_goal_involvements','xGI'],['bonus','Bonus'],['bps','BPS'],
  ['ict_index','ICT Index'],['influence','Influence'],['creativity','Creativity'],
  ['threat','Threat'],['selected_by_percent','Ownership %'],['minutes','Minutes'],
  ['goals_scored','Goals'],['assists','Assists'],['clean_sheets','Clean Sheets'],
  ['value_season','Value (Season)'],['ep_next','EP Next'],
];

function renderCompare() {
  const names = ['cmp-p1','cmp-p2','cmp-p3'].map(id => document.getElementById(id).value).filter(Boolean);
  const players = names.map(n => DATA.find(p => p.web_name === n)).filter(Boolean);
  if (players.length < 2) return;

  // Radar
  getOrDestroyChart('radar');
  const mins = {}, maxs = {};
  RADAR_KEYS.forEach(k => {
    const vals = DATA.map(p => p[k]||0);
    mins[k] = Math.min(...vals);
    maxs[k] = Math.max(...vals);
  });
  const norm = (v, k) => maxs[k] > mins[k] ? ((v-mins[k])/(maxs[k]-mins[k]))*100 : 50;
  const colors = ['#6c63ff','#ff9800','#4caf50'];

  const radarCtx = document.getElementById('radar-chart').getContext('2d');
  charts['radar'] = new Chart(radarCtx, {
    type: 'radar',
    data: {
      labels: RADAR_LABELS,
      datasets: players.map((p,i) => ({
        label: p.web_name,
        data: RADAR_KEYS.map(k => fmt(norm(p[k]||0, k), 1)),
        borderColor: colors[i], backgroundColor: colors[i]+'33', borderWidth:2, pointRadius:3
      }))
    },
    options: {
      responsive: true,
      scales: { r: { min:0, max:100, ticks: { display:false }, grid: { color:'#2e3356' }, pointLabels: { color:'#9ea3c0', font: {size:12} } } },
      plugins: { legend: { labels: { color:'#e8eaf6' } } }
    }
  });

  // Table
  const thead = document.getElementById('cmp-thead');
  const tbody = document.getElementById('cmp-tbody');
  thead.innerHTML = '<th>Metric</th>' + players.map(p=>`<th>${p.web_name}</th>`).join('');
  tbody.innerHTML = CMP_METRICS.map(([k,label]) => `
    <tr>
      <td>${label}</td>
      ${players.map(p => `<td>${fmt(p[k],2)}</td>`).join('')}
    </tr>`).join('');

  // Info cards
  document.getElementById('cmp-info-cards').innerHTML = players.map((p,i) => `
    <div class="card" style="border-left:3px solid ${colors[i]}">
      <div class="card-label">${p.web_name}</div>
      <div class="card-value" style="font-size:14px;">${p.position} | ${p.team_short_name} | ${fmt(p.now_cost,1)}m</div>
      <div class="card-sub" style="margin-top:6px;">${p.status === 'a' ? '✅ Available' : `⚠️ ${p.news||p.status}`}</div>
      ${p.chance_of_playing_next_round != null ? `<div class="card-sub">Chance: ${p.chance_of_playing_next_round}%</div>` : ''}
    </div>`).join('');
}

// ---- Transfers ----
function renderTransfers() {
  // Highlights
  if (GW_HIGHLIGHT.gw) {
    document.getElementById('transfer-highlights').innerHTML = `
      <div class="card"><div class="card-label">GW${GW_HIGHLIGHT.gw} Highlights</div></div>
      <div class="card"><div class="card-label">Most Captained</div><div class="card-value" style="font-size:15px;">${GW_HIGHLIGHT.most_captained}</div></div>
      <div class="card"><div class="card-label">Most Transferred In</div><div class="card-value" style="font-size:15px;">${GW_HIGHLIGHT.most_transferred_in}</div></div>
      <div class="card"><div class="card-label">Most Selected</div><div class="card-value" style="font-size:15px;">${GW_HIGHLIGHT.most_selected}</div></div>
      <div class="card"><div class="card-label">Avg Score</div><div class="card-value">${fmt(GW_HIGHLIGHT.avg_score,0)}</div></div>`;
  }

  const topIn = [...filteredData].sort((a,b)=>(b.transfers_in_event||0)-(a.transfers_in_event||0)).slice(0,15);
  const topOut = [...filteredData].sort((a,b)=>(b.transfers_out_event||0)-(a.transfers_out_event||0)).slice(0,15);

  const formColor = f => {
    const v = parseFloat(f)||0;
    if (v >= 7) return '#4caf50';
    if (v >= 4) return '#ffeb3b';
    return '#f44336';
  };

  getOrDestroyChart('tin');
  const tinCtx = document.getElementById('tin-chart').getContext('2d');
  charts['tin'] = new Chart(tinCtx, {
    type: 'bar',
    data: {
      labels: topIn.map(p=>p.web_name),
      datasets: [{ label:'Transfers In', data: topIn.map(p=>p.transfers_in_event||0),
        backgroundColor: topIn.map(p=>formColor(p.form)), borderWidth:0 }]
    },
    options: {
      indexAxis:'y', responsive:true,
      plugins: { legend: { display:false } },
      scales: {
        x: { ticks:{color:'#9ea3c0'}, grid:{color:'#2e3356'} },
        y: { ticks:{color:'#e8eaf6',font:{size:11}}, grid:{display:false} }
      }
    }
  });

  getOrDestroyChart('tout');
  const toutCtx = document.getElementById('tout-chart').getContext('2d');
  charts['tout'] = new Chart(toutCtx, {
    type: 'bar',
    data: {
      labels: topOut.map(p=>p.web_name),
      datasets: [{ label:'Transfers Out', data: topOut.map(p=>p.transfers_out_event||0),
        backgroundColor: topOut.map(p=>formColor(p.form)), borderWidth:0 }]
    },
    options: {
      indexAxis:'y', responsive:true,
      plugins: { legend: { display:false } },
      scales: {
        x: { ticks:{color:'#9ea3c0'}, grid:{color:'#2e3356'} },
        y: { ticks:{color:'#e8eaf6',font:{size:11}}, grid:{display:false} }
      }
    }
  });

  // Price tables
  const risers = filteredData.filter(p=>(p.cost_change_event||0)>0).sort((a,b)=>b.cost_change_event-a.cost_change_event).slice(0,15);
  const fallers = filteredData.filter(p=>(p.cost_change_event||0)<0).sort((a,b)=>a.cost_change_event-b.cost_change_event).slice(0,15);

  document.getElementById('risers-body').innerHTML = risers.map(p=>`<tr>
    <td><b>${p.web_name}</b></td><td>${p.team_short_name}</td>
    <td>${fmt(p.now_cost,1)}m</td>
    <td class="pos">+${fmt(p.cost_change_event,1)}</td>
    <td>${fmt(p.selected_by_percent,1)}%</td></tr>`).join('') || '<tr><td colspan="5" style="color:var(--text2);text-align:center">No risers</td></tr>';

  document.getElementById('fallers-body').innerHTML = fallers.map(p=>`<tr>
    <td><b>${p.web_name}</b></td><td>${p.team_short_name}</td>
    <td>${fmt(p.now_cost,1)}m</td>
    <td class="neg">${fmt(p.cost_change_event,1)}</td>
    <td>${fmt(p.selected_by_percent,1)}%</td></tr>`).join('') || '<tr><td colspan="5" style="color:var(--text2);text-align:center">No fallers</td></tr>';
}

// ---- Helpers ----
function fmt(v, dp=2) {
  if (v === null || v === undefined) return '—';
  return parseFloat(v).toFixed(dp);
}

function populatePlayerSelects() {
  const names = [...new Set(DATA.map(p=>p.web_name))].sort();
  const opts = names.map(n=>`<option value="${n}">${n}</option>`).join('');
  const noneOpt = '<option value="">— None —</option>';
  ['form-p1','form-p2','cmp-p1','cmp-p2','cmp-p3'].forEach(id => {
    const el = document.getElementById(id);
    const hasNone = id === 'form-p2' || id === 'cmp-p3';
    el.innerHTML = (hasNone ? noneOpt : '') + opts;
  });
  // Set different defaults for compare
  const p2el = document.getElementById('cmp-p2');
  if (p2el.options.length > 1) p2el.selectedIndex = 1;
}

// ---- Init ----
initTeamFilter();
populatePlayerSelects();
applyFilters();
renderFixtures();
renderTransfers();
</script>
</body>
</html>"""
    html = html.replace("__DATA_JSON__", data_json)
    html = html.replace("__CURRENT_GW__", str(current_gw))
    return html


if __name__ == "__main__":
    print("Loading data...")
    data = load_data()
    print(f"  {len(data['players'])} players, GW {data['current_gw']}, {len(data['fixtures'])} fixture entries")
    print("Generating HTML...")
    html = generate_html(data)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"Done! Saved to {OUTPUT_FILE} ({size_kb:.0f} KB)")
    print(f"Open dashboard.html in your browser to view the dashboard.")
