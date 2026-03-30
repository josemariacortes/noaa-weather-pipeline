import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="NOAA Weather Lakehouse", layout="wide")
st.title("🌍 NOAA Global Historical Climatology Network")
st.caption("**Lakehouse Medallion** — Bronze (dlt) → Silver (dbt) → Gold")

# Conexión DuckDB
conn = duckdb.connect("noaa_weather.duckdb")

# ==================== AÑOS DISPONIBLES (dinámico) ====================
st.sidebar.header("📅 Selección de año")
available_years = conn.execute("""
    SELECT DISTINCT year(date) as year 
    FROM main_gold.gold_fact_observations 
    ORDER BY year DESC
""").df()

if available_years.empty:
    st.error("No hay datos en gold_fact_observations. Ejecuta primero el pipeline.")
    st.stop()

year_options = available_years['year'].tolist()
year = st.sidebar.selectbox("Año", options=year_options, index=0)

# Botón para recargar datos
if st.sidebar.button("🔄 Recargar datos"):
    st.rerun()

# Carga de datos
df_fact = conn.execute(f"""
    SELECT * FROM main_gold.gold_fact_observations 
    WHERE year(date) = {year}
""").df()

df_station = conn.execute("SELECT * FROM main_gold.gold_dim_station_enriched").df()
df_country = conn.execute("SELECT * FROM main_gold.gold_dim_country_enriched").df()

# KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric("Estaciones", f"{df_station['station_id'].nunique():,}")
col2.metric("Observaciones", f"{len(df_fact):,}")
col3.metric("Temperatura media", f"{df_fact['tavg_c'].mean():.1f} °C" if not df_fact.empty else "N/D")
col4.metric("Precipitación total", f"{df_fact['prcp_mm'].sum():.0f} mm" if not df_fact.empty else "N/D")

# Pestañas
tab1, tab2, tab3, tab4 = st.tabs(["📈 Evolución Temporal", "🌍 Mapa de Estaciones", "🌡️ Análisis por País", "📊 Top Estaciones"])

with tab1:
    st.subheader("Evolución diaria")
    daily = df_fact.groupby('date').agg({'tavg_c':'mean', 'prcp_mm':'sum'}).reset_index()
    fig = px.line(daily, x='date', y=['tavg_c', 'prcp_mm'])
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Mapa global de estaciones")
    fig_map = px.scatter_mapbox(
        df_station.head(5000), lat="latitude", lon="longitude",
        hover_name="station_name", hover_data=["country_name", "elevation"],
        color="elevation", zoom=1, height=600,
        mapbox_style="carto-positron"
    )
    st.plotly_chart(fig_map, use_container_width=True)

with tab3:
    st.subheader("Estaciones por país")
    fig_country = px.bar(df_country.head(15), x='country_name', y='num_stations')
    st.plotly_chart(fig_country, use_container_width=True)

with tab4:
    st.subheader("Top 10 estaciones")
    top = df_station.nlargest(10, 'observation_days')
    st.dataframe(top[['station_name', 'country_name', 'observation_days', 'elevation']], use_container_width=True)

st.markdown("---")
st.caption(f"""
**Arquitectura**: Lakehouse Medallion (Bronze → Silver → Gold)  
**Plataformas de mercado**: Databricks Lakehouse, Snowflake, BigQuery  
**SGBD distribuidas**: CockroachDB (NewSQL), MongoDB, Neo4j, HBase
""")