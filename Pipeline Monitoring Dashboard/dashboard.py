import streamlit as st
import pandas as pd

st.title("Pipeline Monitoring Dashboard")


data = [
    ["orders", 1, "success", "2026-05-01 10:00:00", "2026-05-01 10:05:00"],
    ["orders", 2, "failed",  "2026-05-01 11:00:00", "2026-05-01 11:03:00"],
    ["orders", 3, "success", "2026-05-02 09:00:00", "2026-05-02 09:04:00"],
    ["payments", 1, "success", "2026-05-01 10:00:00", "2026-05-01 10:06:00"],
    ["payments", 2, "success", "2026-05-02 12:00:00", "2026-05-02 12:07:00"],
    ["payments", 3, "failed",  "2026-05-03 14:00:00", "2026-05-03 14:02:00"],
]

df = pd.DataFrame(data, columns=[
    "pipeline", "run_id", "status", "start_time", "end_time"
])


df["start_time"] = pd.to_datetime(df["start_time"])
df["end_time"] = pd.to_datetime(df["end_time"])


df["runtime_sec"] = (df["end_time"] - df["start_time"]).dt.total_seconds()


total_runs = len(df)
failures = (df["status"] == "failed").sum()
avg_runtime = round(df["runtime_sec"].mean(), 2)

st.subheader("Metrics")

col1, col2, col3 = st.columns(3)
col1.metric("Total Runs", total_runs)
col2.metric("Failures", failures)
col3.metric("Average Runtime (sec)", avg_runtime)


st.subheader("Pipeline Data")
st.dataframe(df)


summary = df.groupby("pipeline").agg(
    total_runs=("run_id", "count"),
    failures=("status", lambda x: (x == "failed").sum()),
    avg_runtime=("runtime_sec", "mean")
).reset_index()

st.subheader("Pipeline Summary")
st.dataframe(summary)