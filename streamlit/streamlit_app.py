"""
Executive Dashboard - Food Truck Business Analytics
Snowflake Streamlit Application
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Executive Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Get Snowflake session
session = get_active_session()

# Custom CSS for professional styling
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMetric {
        background-color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)

# Title
st.markdown('<h1 class="main-header">🚚 Executive Dashboard</h1>', unsafe_allow_html=True)
st.markdown("### Real-Time Business Intelligence & Analytics")

# Sidebar filters
st.sidebar.header("📅 Filters")

# Date range filter
date_range = st.sidebar.selectbox(
    "Time Period",
    ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Last Year", "All Time"],
    index=1
)

# Map date range to SQL
date_filters = {
    "Last 7 Days": "AND ORDER_TS >= DATEADD(day, -7, CURRENT_DATE())",
    "Last 30 Days": "AND ORDER_TS >= DATEADD(day, -30, CURRENT_DATE())",
    "Last 90 Days": "AND ORDER_TS >= DATEADD(day, -90, CURRENT_DATE())",
    "Last Year": "AND ORDER_TS >= DATEADD(year, -1, CURRENT_DATE())",
    "All Time": ""
}
date_filter = date_filters[date_range]

# Get location filter options
locations_query = """
SELECT DISTINCT c.CITY, co.COUNTRY
FROM DEV_DB.INT.ORDER_HEADER_FACT o
JOIN DEV_DB.INT.LOCATION_DIM l ON o.LOCATION_ID = l.LOCATION_ID
JOIN DEV_DB.INT.CITY_DIM c ON l.CITY_ID = c.CITY_ID
JOIN DEV_DB.INT.COUNTRY_DIM co ON l.COUNTRY_ID = co.COUNTRY_ID
WHERE 1=1 {}
ORDER BY co.COUNTRY, c.CITY
""".format(date_filter)

locations_df = session.sql(locations_query).to_pandas()
locations_list = ["All Locations"] + [f"{row['CITY']}, {row['COUNTRY']}" for _, row in locations_df.iterrows()]

selected_location = st.sidebar.selectbox("Location", locations_list)

# Build location filter
if selected_location != "All Locations":
    city, country = selected_location.split(", ")
    location_filter = f"AND c.CITY = '{city}' AND co.COUNTRY = '{country}'"
else:
    location_filter = ""

st.sidebar.markdown("---")
st.sidebar.info("💡 **Tip**: Use filters to drill down into specific time periods and locations.")

# ===== KEY METRICS =====
st.header("📈 Key Performance Indicators")

kpi_query = f"""
SELECT 
    COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS,
    ROUND(SUM(o.ORDER_TOTAL), 2) as TOTAL_REVENUE,
    ROUND(AVG(o.ORDER_TOTAL), 2) as AVG_ORDER_VALUE,
    COUNT(DISTINCT o.CUSTOMER_ID) as UNIQUE_CUSTOMERS,
    COUNT(DISTINCT o.LOCATION_ID) as ACTIVE_LOCATIONS,
    COUNT(DISTINCT o.TRUCK_ID) as ACTIVE_TRUCKS
FROM DEV_DB.INT.ORDER_HEADER_FACT o
JOIN DEV_DB.INT.LOCATION_DIM l ON o.LOCATION_ID = l.LOCATION_ID
JOIN DEV_DB.INT.CITY_DIM c ON l.CITY_ID = c.CITY_ID
JOIN DEV_DB.INT.COUNTRY_DIM co ON l.COUNTRY_ID = co.COUNTRY_ID
WHERE 1=1 {date_filter} {location_filter}
"""

kpi_df = session.sql(kpi_query).to_pandas()

col1, col2, col3 = st.columns(3)
col4, col5, col6 = st.columns(3)

with col1:
    st.metric(
        "💰 Total Revenue",
        f"${kpi_df['TOTAL_REVENUE'][0]:,.0f}",
        help="Total revenue for selected period"
    )

with col2:
    st.metric(
        "🛒 Total Orders",
        f"{kpi_df['TOTAL_ORDERS'][0]:,.0f}",
        help="Number of orders placed"
    )

with col3:
    st.metric(
        "📊 Avg Order Value",
        f"${kpi_df['AVG_ORDER_VALUE'][0]:,.2f}",
        help="Average revenue per order"
    )

with col4:
    st.metric(
        "👥 Unique Customers",
        f"{kpi_df['UNIQUE_CUSTOMERS'][0]:,.0f}",
        help="Number of unique customers"
    )

with col5:
    st.metric(
        "📍 Active Locations",
        f"{kpi_df['ACTIVE_LOCATIONS'][0]:,.0f}",
        help="Number of active locations"
    )

with col6:
    st.metric(
        "🚚 Active Trucks",
        f"{kpi_df['ACTIVE_TRUCKS'][0]:,.0f}",
        help="Number of trucks in operation"
    )

st.markdown("---")

# ===== REVENUE TRENDS =====
col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Revenue Trend Over Time")
    
    revenue_trend_query = f"""
    SELECT 
        DATE_TRUNC('day', o.ORDER_TS) as ORDER_DATE,
        ROUND(SUM(o.ORDER_TOTAL), 2) as DAILY_REVENUE,
        COUNT(DISTINCT o.ORDER_ID) as DAILY_ORDERS
    FROM DEV_DB.INT.ORDER_HEADER_FACT o
    JOIN DEV_DB.INT.LOCATION_DIM l ON o.LOCATION_ID = l.LOCATION_ID
    JOIN DEV_DB.INT.CITY_DIM c ON l.CITY_ID = c.CITY_ID
    JOIN DEV_DB.INT.COUNTRY_DIM co ON l.COUNTRY_ID = co.COUNTRY_ID
    WHERE 1=1 {date_filter} {location_filter}
    GROUP BY DATE_TRUNC('day', o.ORDER_TS)
    ORDER BY ORDER_DATE
    """
    
    revenue_trend_df = session.sql(revenue_trend_query).to_pandas()
    
    if not revenue_trend_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=revenue_trend_df['ORDER_DATE'],
            y=revenue_trend_df['DAILY_REVENUE'],
            mode='lines+markers',
            name='Revenue',
            line=dict(color='#1f77b4', width=3),
            fill='tozeroy',
            fillcolor='rgba(31, 119, 180, 0.2)'
        ))
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Revenue ($)",
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for selected filters")

with col2:
    st.subheader("🏆 Top 10 Menu Items by Revenue")
    
    top_items_query = f"""
    SELECT 
        m.MENU_ITEM_NAME,
        ROUND(SUM(d.TOTAL_REVENUE_USD), 2) as TOTAL_REVENUE,
        SUM(d.QUANTITY) as TOTAL_QUANTITY
    FROM DEV_DB.INT.ORDER_DETAIL_FACT d
    JOIN DEV_DB.INT.ORDER_HEADER_FACT o ON d.ORDER_ID = o.ORDER_ID
    JOIN DEV_DB.INT.MENU_ITEM_DIM m ON d.MENU_ITEM_ID = m.MENU_ITEM_ID
    JOIN DEV_DB.INT.LOCATION_DIM l ON o.LOCATION_ID = l.LOCATION_ID
    JOIN DEV_DB.INT.CITY_DIM c ON l.CITY_ID = c.CITY_ID
    JOIN DEV_DB.INT.COUNTRY_DIM co ON l.COUNTRY_ID = co.COUNTRY_ID
    WHERE 1=1 {date_filter} {location_filter}
    GROUP BY m.MENU_ITEM_NAME
    ORDER BY TOTAL_REVENUE DESC
    LIMIT 10
    """
    
    top_items_df = session.sql(top_items_query).to_pandas()
    
    if not top_items_df.empty:
        fig = px.bar(
            top_items_df,
            x='TOTAL_REVENUE',
            y='MENU_ITEM_NAME',
            orientation='h',
            color='TOTAL_REVENUE',
            color_continuous_scale='Blues',
            labels={'TOTAL_REVENUE': 'Revenue ($)', 'MENU_ITEM_NAME': 'Menu Item'}
        )
        fig.update_layout(
            showlegend=False,
            height=400,
            yaxis={'categoryorder': 'total ascending'}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for selected filters")

st.markdown("---")

# ===== GEOGRAPHIC ANALYSIS =====
col1, col2 = st.columns(2)

with col1:
    st.subheader("🌍 Revenue by Country")
    
    country_query = f"""
    SELECT 
        co.COUNTRY,
        ROUND(SUM(o.ORDER_TOTAL), 2) as TOTAL_REVENUE,
        COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS
    FROM DEV_DB.INT.ORDER_HEADER_FACT o
    JOIN DEV_DB.INT.LOCATION_DIM l ON o.LOCATION_ID = l.LOCATION_ID
    JOIN DEV_DB.INT.CITY_DIM c ON l.CITY_ID = c.CITY_ID
    JOIN DEV_DB.INT.COUNTRY_DIM co ON l.COUNTRY_ID = co.COUNTRY_ID
    WHERE 1=1 {date_filter} {location_filter}
    GROUP BY co.COUNTRY
    ORDER BY TOTAL_REVENUE DESC
    """
    
    country_df = session.sql(country_query).to_pandas()
    
    if not country_df.empty:
        fig = px.pie(
            country_df,
            values='TOTAL_REVENUE',
            names='COUNTRY',
            title='Revenue Distribution by Country',
            color_discrete_sequence=px.colors.sequential.Blues_r
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for selected filters")

with col2:
    st.subheader("📍 Top 10 Cities by Revenue")
    
    city_query = f"""
    SELECT 
        c.CITY,
        co.COUNTRY,
        ROUND(SUM(o.ORDER_TOTAL), 2) as TOTAL_REVENUE,
        COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS
    FROM DEV_DB.INT.ORDER_HEADER_FACT o
    JOIN DEV_DB.INT.LOCATION_DIM l ON o.LOCATION_ID = l.LOCATION_ID
    JOIN DEV_DB.INT.CITY_DIM c ON l.CITY_ID = c.CITY_ID
    JOIN DEV_DB.INT.COUNTRY_DIM co ON l.COUNTRY_ID = co.COUNTRY_ID
    WHERE 1=1 {date_filter} {location_filter}
    GROUP BY c.CITY, co.COUNTRY
    ORDER BY TOTAL_REVENUE DESC
    LIMIT 10
    """
    
    city_df = session.sql(city_query).to_pandas()
    
    if not city_df.empty:
        city_df['LOCATION'] = city_df['CITY'] + ', ' + city_df['COUNTRY']
        fig = px.bar(
            city_df,
            x='LOCATION',
            y='TOTAL_REVENUE',
            color='TOTAL_REVENUE',
            color_continuous_scale='Viridis',
            labels={'TOTAL_REVENUE': 'Revenue ($)', 'LOCATION': 'City'}
        )
        fig.update_layout(
            showlegend=False,
            height=400,
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for selected filters")

st.markdown("---")

# ===== OPERATIONAL INSIGHTS =====
col1, col2 = st.columns(2)

with col1:
    st.subheader("⏰ Orders by Hour of Day")
    
    hourly_query = f"""
    SELECT 
        HOUR(o.ORDER_TS) as HOUR_OF_DAY,
        COUNT(DISTINCT o.ORDER_ID) as ORDER_COUNT,
        ROUND(AVG(o.ORDER_TOTAL), 2) as AVG_ORDER_VALUE
    FROM DEV_DB.INT.ORDER_HEADER_FACT o
    JOIN DEV_DB.INT.LOCATION_DIM l ON o.LOCATION_ID = l.LOCATION_ID
    JOIN DEV_DB.INT.CITY_DIM c ON l.CITY_ID = c.CITY_ID
    JOIN DEV_DB.INT.COUNTRY_DIM co ON l.COUNTRY_ID = co.COUNTRY_ID
    WHERE 1=1 {date_filter} {location_filter}
    GROUP BY HOUR(o.ORDER_TS)
    ORDER BY HOUR_OF_DAY
    """
    
    hourly_df = session.sql(hourly_query).to_pandas()
    
    if not hourly_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=hourly_df['HOUR_OF_DAY'],
            y=hourly_df['ORDER_COUNT'],
            name='Orders',
            marker_color='lightblue'
        ))
        fig.update_layout(
            xaxis_title="Hour of Day",
            yaxis_title="Number of Orders",
            height=400,
            xaxis=dict(tickmode='linear', tick0=0, dtick=1)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for selected filters")

with col2:
    st.subheader("📦 Order Channel Distribution")
    
    channel_query = f"""
    SELECT 
        COALESCE(o.ORDER_CHANNEL, 'Unknown') as CHANNEL,
        COUNT(DISTINCT o.ORDER_ID) as ORDER_COUNT,
        ROUND(SUM(o.ORDER_TOTAL), 2) as TOTAL_REVENUE
    FROM DEV_DB.INT.ORDER_HEADER_FACT o
    JOIN DEV_DB.INT.LOCATION_DIM l ON o.LOCATION_ID = l.LOCATION_ID
    JOIN DEV_DB.INT.CITY_DIM c ON l.CITY_ID = c.CITY_ID
    JOIN DEV_DB.INT.COUNTRY_DIM co ON l.COUNTRY_ID = co.COUNTRY_ID
    WHERE 1=1 {date_filter} {location_filter}
    GROUP BY COALESCE(o.ORDER_CHANNEL, 'Unknown')
    ORDER BY ORDER_COUNT DESC
    """
    
    channel_df = session.sql(channel_query).to_pandas()
    
    if not channel_df.empty:
        fig = px.pie(
            channel_df,
            values='ORDER_COUNT',
            names='CHANNEL',
            title='Orders by Channel',
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for selected filters")

st.markdown("---")

# ===== DETAILED DATA TABLE =====
with st.expander("📊 View Detailed Order Data", expanded=False):
    st.subheader("Recent Orders")
    
    detailed_query = f"""
    SELECT 
        o.ORDER_ID,
        o.ORDER_TS,
        ci.CITY,
        co.COUNTRY,
        o.ORDER_CHANNEL,
        o.ORDER_TOTAL,
        cu.FIRST_NAME || ' ' || cu.LAST_NAME as CUSTOMER_NAME
    FROM DEV_DB.INT.ORDER_HEADER_FACT o
    JOIN DEV_DB.INT.LOCATION_DIM l ON o.LOCATION_ID = l.LOCATION_ID
    JOIN DEV_DB.INT.CITY_DIM ci ON l.CITY_ID = ci.CITY_ID
    JOIN DEV_DB.INT.COUNTRY_DIM co ON l.COUNTRY_ID = co.COUNTRY_ID
    LEFT JOIN DEV_DB.INT.CUSTOMER_DIM cu ON o.CUSTOMER_ID = cu.CUSTOMER_ID
    WHERE 1=1 {date_filter} {location_filter}
    ORDER BY o.ORDER_TS DESC
    LIMIT 100
    """
    
    detailed_df = session.sql(detailed_query).to_pandas()
    
    if not detailed_df.empty:
        st.dataframe(
            detailed_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Download button
        csv = detailed_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Data as CSV",
            data=csv,
            file_name=f"orders_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No data available for selected filters")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 2rem;'>
        <p>📊 Executive Dashboard | Powered by Snowflake & Streamlit</p>
        <p>Last Updated: {}</p>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    unsafe_allow_html=True
)

