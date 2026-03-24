# ----------------------------
# RAW DATA
# ----------------------------
st.markdown("### Historical Event Table")

table_view_mode = st.radio(
    "Table View",
    ["Warning + High Risk Only", "All Records"],
    horizontal=True
)

if table_view_mode == "Warning + High Risk Only":
    display_df = hist_df[hist_df["status"].isin(["WARNING", "HIGH RISK"])].copy()
else:
    display_df = hist_df.copy()

if not display_df.empty:
    display_df["created_at"] = display_df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

    display_columns = [
        "created_at",
        "line_key",
        "process_name",
        "risk",
        "status",
        "action",
        "reasons",
        "helmet",
        "distance",
        "vibration",
        "temperature"
    ]

    display_df = display_df[display_columns].sort_values(
        by=["created_at", "risk"],
        ascending=[False, False]
    )

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    csv_data = to_csv_bytes(display_df)
    st.download_button(
        "Download Filtered CSV",
        data=csv_data,
        file_name=f"smartsafe_events_{selected_history_line.replace(' ', '_').lower()}_{history_days}d.csv",
        mime="text/csv"
    )
else:
    st.info("No records found for the selected filter.")
