import streamlit as st
import pandas as pd
from datetime import datetime

# Load data
df = pd.read_csv(r"C:\Users\yamin\OneDrive\cricket_dataset\data\cleaned_data.csv", parse_dates=["DATE"])

# Preprocess
df["WINNER"] = df["WINNER"].fillna("No Result")
teams = sorted(set(df["HOME TEAM"]) | set(df["AWAY TEAM"]))

# Set page config
st.set_page_config(page_title="CWC19 Dashboard", layout="wide", initial_sidebar_state="expanded")

# Global filters
st.sidebar.header("Filters")
selected_team = st.sidebar.selectbox("Team", ["All"] + teams)
match_stages = df["MATCH STAGE"].unique()
selected_stage = st.sidebar.selectbox("Match Stage", ["All"] + list(match_stages))
date_range = st.sidebar.date_input(
    "Date Range",
    [df["DATE"].min().date(), df["DATE"].max().date()],
    min_value=df["DATE"].min().date(),
    max_value=df["DATE"].max().date()
)

# Apply filters
filtered_df = df.copy()
if selected_team != "All":
    filtered_df = filtered_df[(filtered_df["HOME TEAM"] == selected_team) | (filtered_df["AWAY TEAM"] == selected_team)]

if selected_stage != "All":
    filtered_df = filtered_df[filtered_df["MATCH STAGE"] == selected_stage]

filtered_df = filtered_df[(filtered_df["DATE"].dt.date >= date_range[0]) & (filtered_df["DATE"].dt.date <= date_range[1])]

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["🏟 Overview", "⚔️ Teams", "📅 Matches", "📍 Grounds"])

# --- Overview Tab ---
with tab1:
    st.markdown("## 🏆 CWC19 Overview")
    col1, col2, col3, col4 = st.columns(4)

    total_matches = len(filtered_df)
    total_runs = filtered_df["TOTAL RUNS"].sum()
    washouts = (filtered_df["WINNER"] == "No Result").sum()
    total_wins = total_matches - washouts

    col1.metric("Total Matches", total_matches)
    col2.metric("Total Wins", total_wins)
    col3.metric("Washouts", washouts)
    col4.metric("Total Runs", f"{total_runs:,}")

# --- Teams Tab ---
with tab2:
    st.markdown("## 📊 Most Wins by Team")
    wins_df = filtered_df[filtered_df["WINNER"] != "No Result"]
    win_counts = wins_df["WINNER"].value_counts().reset_index()
    win_counts.columns = ["Team", "Wins"]
    st.bar_chart(win_counts.set_index("Team"))

# --- Matches Tab ---
with tab3:
    st.markdown("## 📅 Match Table")
    st.dataframe(filtered_df.sort_values("DATE")[[
        "DATE", "HOME TEAM", "AWAY TEAM", "WINNER", "PLAYER OF THE MATCH", "TOTAL RUNS"
    ]].reset_index(drop=True))

# --- Grounds Tab ---
with tab4:
    st.markdown("## 🏟️ Matches by Stadium")
    stadium_counts = filtered_df["STADIUM NAME"].value_counts().reset_index()
    stadium_counts.columns = ["Stadium", "Matches"]
    st.bar_chart(stadium_counts.set_index("Stadium"))
