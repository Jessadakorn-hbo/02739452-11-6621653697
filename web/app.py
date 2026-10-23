import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Thai Ad & Marketing News", page_icon="📰", layout="wide")


@st.cache_resource
def engine():
    return create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)


@st.cache_data(ttl=30)
def q(sql: str) -> pd.DataFrame:
    with engine().connect() as c:           # reads PostgreSQL directly
        return pd.read_sql(text(sql), c)


st.title("📰 Thai Advertising & Marketing News")
st.caption("Marketing Oops · Positioning · Brand Inside → Airflow → PostgreSQL → this dashboard")

tab_dash, tab_status = st.tabs(["Dashboard", "Pipeline status"])

# ---------------------------------------------------------------- dashboard
with tab_dash:
    try:
        df = q("SELECT url, title, published_date, category, source, brand FROM ad_news")
    except Exception:
        df = None
        st.warning("ยังไม่มีตาราง ad_news — เปิด Airflow (localhost:8080) แล้ว trigger DAG ad_news_pipeline ก่อน")

    if df is not None and df.empty:
        st.info("ตารางว่าง — รอ DAG รันเสร็จ แล้วกด refresh")
    elif df is not None:
        df["published_date"] = pd.to_datetime(df["published_date"])

        with st.sidebar:
            st.header("Filters")
            sources = st.multiselect("Source", sorted(df["source"].unique()), default=sorted(df["source"].unique()))
            cats = st.multiselect("Category", sorted(df["category"].dropna().unique()))
            brands = st.multiselect("Brand", sorted(df["brand"].dropna().unique()))
            kw = st.text_input("Keyword in title")
            dmin, dmax = df["published_date"].min(), df["published_date"].max()
            if pd.notna(dmin):
                rng = st.date_input("Published between", (dmin.date(), dmax.date()),
                                    min_value=dmin.date(), max_value=dmax.date())
            else:
                rng = None

        f = df[df["source"].isin(sources)]
        if cats:
            f = f[f["category"].isin(cats)]
        if brands:
            f = f[f["brand"].isin(brands)]
        if kw:
            f = f[f["title"].str.contains(kw, case=False, na=False)]
        if rng is not None and len(rng) == 2:
            f = f[f["published_date"].isna() |
                  f["published_date"].between(pd.Timestamp(rng[0]), pd.Timestamp(rng[1]))]

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Articles", f"{len(f):,}")
        c2.metric("Sources", f["source"].nunique())
        c3.metric("Brands detected", f["brand"].nunique())
        c4.metric("Brand coverage", f"{(f['brand'].notna().mean() * 100 if len(f) else 0):.0f}%")
        latest = f["published_date"].max()
        c5.metric("Latest article", latest.strftime("%d %b %Y") if pd.notna(latest) else "-")

        left, right = st.columns(2)
        with left:
            by_day = f.dropna(subset=["published_date"]).groupby([f["published_date"].dt.date, "source"]).size() \
                .reset_index(name="articles").rename(columns={"published_date": "date"})
            if not by_day.empty:
                st.plotly_chart(px.bar(by_day, x="date", y="articles", color="source",
                                       title="Articles per day by source"), width="stretch")
        with right:
            top = f["brand"].value_counts().head(10).reset_index()
            top.columns = ["brand", "articles"]
            if not top.empty:
                st.plotly_chart(px.bar(top.sort_values("articles"), x="articles", y="brand", orientation="h",
                                       title="Top 10 brands mentioned"), width="stretch")

        cat = f["category"].value_counts().head(12).reset_index()
        cat.columns = ["category", "articles"]
        if not cat.empty:
            st.plotly_chart(px.bar(cat, x="category", y="articles", color="category",
                                   title="Articles by category"), width="stretch")

        st.subheader("Articles")
        st.dataframe(
            f.sort_values("published_date", ascending=False)[["published_date", "source", "category", "brand", "title", "url"]],
            column_config={"url": st.column_config.LinkColumn("Link", display_text="open"),
                           "published_date": st.column_config.DateColumn("Date")},
            hide_index=True, width="stretch",
        )

# ---------------------------------------------------------------- pipeline status (bonus)
with tab_status:
    try:
        total = q("SELECT COUNT(*) AS n, MAX(updated_at) AS last_update FROM ad_news")
        rejects = q("SELECT COUNT(*) AS n FROM ad_news_rejects")
        a, b, c = st.columns(3)
        a.metric("Rows in ad_news", int(total["n"][0]))
        b.metric("Last DB update", str(total["last_update"][0])[:19] if total["last_update"][0] is not None else "-")
        c.metric("Quarantined batches", int(rejects["n"][0]))

        st.subheader("Latest status per source")
        latest = q("""
            SELECT DISTINCT ON (source) source, status, rows_count, detail, finished_at
            FROM pipeline_runs WHERE task LIKE 'extract_%' ORDER BY source, finished_at DESC""")
        st.dataframe(latest, hide_index=True, width="stretch")

        st.subheader("Recent pipeline events")
        st.dataframe(q("SELECT finished_at, run_id, task, source, status, rows_count, detail "
                       "FROM pipeline_runs ORDER BY finished_at DESC LIMIT 40"), hide_index=True, width="stretch")
    except Exception:
        st.info("ยังไม่มีข้อมูล pipeline_runs — รัน DAG อย่างน้อย 1 ครั้งก่อน")
