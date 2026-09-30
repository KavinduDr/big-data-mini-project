import os
import time
from datetime import datetime
import pandas as pd
import requests
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Ride-Hailing Fleet Operations | Lambda Architecture",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E293B;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #64748B;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    .unprofitable-badge {
        background-color: #FEE2E2;
        color: #991B1B;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
    .profitable-badge {
        background-color: #DCFCE7;
        color: #166534;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

def fetch_api(endpoint):
    """Fetch from API with fallback between localhost and api container host."""
    urls = [f"{API_BASE_URL}{endpoint}"]
    if "api:8000" in API_BASE_URL:
        urls.append(f"http://localhost:8000{endpoint}")
    elif "localhost:8000" in API_BASE_URL:
        urls.append(f"http://127.0.0.1:8000{endpoint}")

    last_err = None
    for url in urls:
        try:
            res = requests.get(url, timeout=3)
            if res.status_code == 200:
                return res.json()
        except Exception as e:
            last_err = e
    raise last_err or Exception(f"Failed to fetch {endpoint}")

st.sidebar.title("🎛️ Control Panel")
auto_refresh = st.sidebar.checkbox("Auto-refresh (every 5s)", value=True)
st.sidebar.markdown("---")
st.sidebar.info("""
**Architecture:** Lambda Architecture  
**Stream Engine:** Spark / Kafka  
**Batch Engine:** Airflow DAG  
**Serving Layer:** PostgreSQL / SQLite  
**Simulated Clock:** 1 Day = 5 Minutes
""")

if st.sidebar.button("Manual Refresh"):
    st.rerun()

st.markdown('<div class="main-header">🚗 Ride-Hailing Fleet Operations Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Live Telemetry & Daily Expense Reconciliation • EC 8203 Big Data Engineering</div>', unsafe_allow_html=True)

# Fetch data from serving API
try:
    health_res = fetch_api("/health")
    util_res = fetch_api("/api/fleet/realtime-utilization")
    alerts_res = fetch_api("/api/fleet/alerts?limit=20")
    profit_res = fetch_api("/api/fleet/daily-profitability")
    api_online = True
except Exception as e:
    api_online = False
    st.error(f"Cannot connect to Serving API at {API_BASE_URL}. Ensure pipeline is running. Error: {e}")

if api_online:
    # Top KPI Metrics Bar
    summary = util_res.get("summary", {})
    profit_data = profit_res.get("reconciliation_records", [])
    unprofitable_count = profit_res.get("unprofitable_vehicles_count", 0)
    total_net_profit = profit_res.get("total_fleet_net_profit", 0.0)

    kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
    with kpi1:
        st.metric(label="Active Vehicles (On Trip)", value=summary.get("total_active_on_trip", 0))
    with kpi2:
        st.metric(label="Enroute Vehicles", value=summary.get("total_enroute", 0))
    with kpi3:
        st.metric(label="Fleet Idle Ratio", value=f"{summary.get('overall_idle_ratio_pct', 0)}%")
    with kpi4:
        st.metric(label="Live Stream Earnings", value=f"${summary.get('total_realtime_earnings', 0):,.2f}")
    with kpi5:
        st.metric(
            label="Unprofitable Vehicles", 
            value=unprofitable_count, 
            delta=f"${total_net_profit:,.2f} Net",
            delta_color="normal" if total_net_profit >= 0 else "inverse"
        )
    with kpi6:
        # Observability: freshness of the speed layer (see GET /health).
        ingest_lag = health_res.get("ingest_lag_seconds")
        st.metric(
            label="Telemetry Lag",
            value="n/a" if ingest_lag is None else f"{ingest_lag:,.1f}s",
            delta=health_res.get("status"),
            delta_color="normal" if health_res.get("data_fresh") else "inverse"
        )

    st.markdown("---")

    # Tabs for Organization
    tab_live, tab_profit, tab_alerts, tab_arch = st.tabs([
        "⚡ Real-Time Utilization", 
        "📊 Daily Profitability Reconciliation", 
        "🚨 Threshold Alerts", 
        "🏛️ Lambda Architecture Specs"
    ])

    with tab_live:
        st.subheader("Live Fleet Activity & Earnings by Zone")
        zones = util_res.get("zones", [])
        if zones:
            df_zones = pd.DataFrame(zones)
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                fig_zone_earnings = px.bar(
                    df_zones,
                    x="grid_zone",
                    y="zone_earnings",
                    color="grid_zone",
                    title="Real-Time Earnings by Zone ($)",
                    labels={"zone_earnings": "Earnings ($)", "grid_zone": "Zone"},
                    text_auto=True
                )
                fig_zone_earnings.update_layout(showlegend=False)
                st.plotly_chart(fig_zone_earnings, use_container_width=True)

            with col_chart2:
                fig_vehicles = px.bar(
                    df_zones,
                    x="grid_zone",
                    y=["active_vehicles", "enroute_vehicles", "idle_vehicles"],
                    title="Vehicle Status Distribution by Zone",
                    labels={"value": "Vehicle Count", "variable": "Status"},
                    barmode="group"
                )
                st.plotly_chart(fig_vehicles, use_container_width=True)

            st.dataframe(
                df_zones[["grid_zone", "active_vehicles", "idle_vehicles", "idle_ratio_pct", "total_trips", "zone_earnings", "avg_speed"]],
                use_container_width=True
            )
        else:
            st.info("Awaiting initial streaming window aggregation from Speed Layer...")

    with tab_profit:
        st.subheader("Batch Reconciliation: Yesterday's Revenue vs. Fuel & Maintenance Expenses")
        st.caption("Business Question: Which vehicles are becoming unprofitable once fuel/maintenance costs are factored in?")
        
        if profit_data:
            df_profit = pd.DataFrame(profit_data)
            
            # Highlight unprofitable vehicles
            fig_profit = go.Figure()
            colors = ['#EF4444' if x else '#10B981' for x in df_profit['is_unprofitable']]
            
            fig_profit.add_trace(go.Bar(
                x=df_profit['vehicle_id'],
                y=df_profit['net_profit'],
                marker_color=colors,
                name="Net Profit ($)"
            ))
            fig_profit.update_layout(
                title="Per-Vehicle Net Profitability ($)",
                xaxis_title="Vehicle ID",
                yaxis_title="Net Profit / Loss ($)",
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig_profit, use_container_width=True)

            col_cost1, col_cost2 = st.columns(2)
            with col_cost1:
                fig_breakdown = px.bar(
                    df_profit,
                    x="vehicle_id",
                    y=["fuel_cost", "maintenance_cost"],
                    title="Expense Breakdown: Fuel vs Maintenance Costs",
                    labels={"value": "Cost ($)", "variable": "Expense Type"}
                )
                fig_breakdown.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_breakdown, use_container_width=True)

            with col_cost2:
                fig_scatter = px.scatter(
                    df_profit,
                    x="gross_earnings",
                    y="total_expenses",
                    color="is_unprofitable",
                    hover_data=["vehicle_id", "recommendation"],
                    title="Revenue vs. Cost Correlation",
                    labels={"gross_earnings": "Gross Revenue ($)", "total_expenses": "Total Expenses ($)"}
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

            # Detailed Table
            st.write("### Consolidated Per-Vehicle Reconciliation Table")
            desired_cols = ["simulated_date", "vehicle_id", "total_trips", "gross_earnings", "fuel_cost", "maintenance_cost", "total_expenses", "net_profit", "profit_margin_pct", "is_unprofitable", "recommendation"]
            # Guard against schema drift between the API payload and the UI.
            display_cols = [col for col in desired_cols if col in df_profit.columns]
            st.dataframe(df_profit[display_cols], use_container_width=True)
        else:
            st.info("Batch layer reconciliation in progress or awaiting first daily drop from Airflow DAG.")

    with tab_alerts:
        st.subheader("Observability & Threshold-based Alerts")
        alerts = alerts_res.get("alerts", [])
        if alerts:
            df_alerts = pd.DataFrame(alerts)
            st.write(f"**Total Active Alerts Detected:** {len(alerts)}")
            st.dataframe(
                df_alerts[["alert_timestamp", "vehicle_id", "driver_id", "grid_zone", "alert_type", "idle_duration_sec", "alert_message"]],
                use_container_width=True
            )
        else:
            st.success("No excessive idle or operational threshold violations detected in current window.")

    with tab_arch:
        st.subheader("Lambda Architecture & System Observability Specs")
        st.markdown("""
        | Architecture Layer | Technology | Role in Ride-Hailing Operations |
        | :--- | :--- | :--- |
        | **Ingestion** | Apache Kafka | Ingests continuous GPS & fare events with vehicle partition keys |
        | **Speed Layer** | PySpark / Streaming Engine | Computes live zone aggregations & triggers idle alerts (<10s latency) |
        | **Batch Layer** | Apache Airflow + Python/Parquet | Reconciles daily garage/fuel costs against fares into Parquet Data Lake |
        | **Serving Layer** | PostgreSQL | Holds queryable real-time metrics, historical views, and unified queries |
        | **Observability** | Structured Logging & Health APIs | Monitors pipeline lag, alert rates, and service uptime |
        """)
        st.json(health_res)

if auto_refresh and api_online:
    time.sleep(5)
    st.rerun()
