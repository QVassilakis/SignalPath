import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import json
from collections import Counter
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="SkillLens | Job Market Intelligence",
    page_icon="🔭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Auth ──────────────────────────────────────────────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown("### 🔭 SkillLens")
    st.markdown("Job Market Intelligence for Career Coaches")
    pwd = st.text_input("Access code", type="password")
    if st.button("Enter"):
        if pwd == os.getenv("APP_PASSWORD", "skilllens2024"):
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password")
    st.stop()

# ── Database ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    password = quote_plus(os.getenv("DBPASS", ""))
    url = f"postgresql://{os.getenv('DBUSER')}:{password}@{os.getenv('DBHOST')}:5432/postgres"
    return create_engine(url)

@st.cache_data(ttl=3600)
def load_jobs(roles=None, locations=None, days_back=90):
    role_filter = ""
    loc_filter = ""
    date_filter = f"AND scrape_datetime::timestamptz >= NOW() - INTERVAL '{days_back} days'" if days_back < 365 else ""
    if roles:
        role_filter = "AND (" + " OR ".join([f"title ILIKE '%{r}%'" for r in roles]) + ")"
    if locations:
        loc_filter = "AND (" + " OR ".join([f"location ILIKE '%{l}%'" for l in locations]) + ")"
    query = f"""
        SELECT
            id, title, company, location,
            url AS job_url,
            skills_analysis->>'tools' AS tools,
            skills_analysis->>'domain_knowledge' AS domain_knowledge,
            skills_analysis->>'functional_skills' AS functional_abilities,
            skills_analysis->>'behavioral_skills' AS behavioral_abilities,
            enrichment_status, scrape_datetime, job_category, is_remote
        FROM public.jobs_master_v2
        WHERE enrichment_status = 'done'
        {role_filter} {loc_filter} {date_filter}
        LIMIT 2000
    """
    try:
        with get_engine().connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            cols = list(result.keys())
            return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_skill_list(val):
    if not val or isinstance(val, float):
        return []
    if isinstance(val, list):
        return val
    try:
        parsed = json.loads(val)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        if isinstance(val, str) and val.strip():
            clean = val.replace("[","").replace("]","").replace('"',"").replace("'","")
            return [s.strip() for s in clean.split(",") if s.strip()]
    return []

def skill_counts(df, column, top_n=20):
    all_skills = []
    for val in df[column].dropna():
        all_skills.extend(parse_skill_list(val))
    counts = Counter(all_skills)
    result = pd.DataFrame(counts.most_common(top_n), columns=["Skill", "Count"])
    if len(df) > 0 and not result.empty:
        result["% of Jobs"] = (result["Count"] / len(df) * 100).round(1)
    return result

COLORS = {
    "tools": "#4f98a3",
    "functional_abilities": "#a86fdf",
    "domain_knowledge": "#fdab43",
    "behavioral_abilities": "#6daa45"
}

def bar_chart(df_counts, color, title):
    fig = px.bar(
        df_counts.head(15).sort_values("Count"),
        x="Count", y="Skill", orientation="h",
        text="% of Jobs", color_discrete_sequence=[color]
    )
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(
        paper_bgcolor="#0e0e0c", plot_bgcolor="#0e0e0c",
        font_color="#7a7870", title=title, title_font_color="#e8e6e0",
        xaxis=dict(gridcolor="#262523"),
        yaxis=dict(gridcolor="rgba(0,0,0,0)", color="#e8e6e0"),
        margin=dict(l=10, r=60, t=40, b=10), height=420
    )
    return fig

# ── Constants ─────────────────────────────────────────────────────────────────
ROLES = [
    "Data Analyst", "Data Scientist", "Analytics Engineer",
    "Machine Learning Engineer", "Quantitative Researcher",
    "Software Engineer", "Data Engineer"
]
LOCATIONS = [
    "New York", "San Francisco", "Seattle", "Austin",
    "Boston", "Chicago", "Los Angeles", "Remote"
]
col_map = {
    "Tools & Tech": "tools",
    "Functional Skills": "functional_abilities",
    "Domain Knowledge": "domain_knowledge",
    "Behavioral Traits": "behavioral_abilities"
}

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🔭 SkillLens")
    selected_roles = st.multiselect("Target Roles", ROLES, default=["Data Analyst", "Data Scientist"])
    selected_locs = st.multiselect("Locations", LOCATIONS, default=[])
    days_back = st.select_slider(
        "Data freshness",
        options=[30, 60, 90, 180, 365],
        value=90,
        format_func=lambda x: f"Last {x} days" if x < 365 else "All time"
    )
    st.divider()
    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Main Header ───────────────────────────────────────────────────────────────
st.markdown("## 🔭 SkillLens — Job Market Intelligence")

if not selected_roles:
    st.info("Select at least one role to get started.")
    st.stop()

with st.spinner("Loading job data..."):
    df = load_jobs(selected_roles, selected_locs if selected_locs else None, days_back)

if df.empty:
    st.warning("No enriched jobs found. Try broadening your filters.")
    st.stop()

# ── Metric Cards ──────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
top_tool_df = skill_counts(df, "tools")
top_tool = top_tool_df.iloc[0]["Skill"] if not top_tool_df.empty else "N/A"
top_tool_pct = top_tool_df.iloc[0]["% of Jobs"] if not top_tool_df.empty else 0
remote_pct = round(int(df["is_remote"].sum()) / len(df) * 100, 1) if "is_remote" in df.columns else 0

for col, label, val, sub in [
    (k1, "Jobs Analyzed", f"{len(df):,}", ", ".join(selected_roles[:2])),
    (k2, "Companies", f"{df['company'].nunique():,}", "unique hiring orgs"),
    (k3, "Remote Roles", f"{remote_pct}%", "of filtered postings"),
    (k4, "#1 Tool", top_tool, f"in {top_tool_pct}% of postings"),
]:
    with col:
        st.metric(label=label, value=val, help=sub)

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Skill Frequency", "🗂️ 4-Layer Taxonomy",
    "⚖️ Role Comparison", "🎯 Gap Analysis", "🔍 Raw Jobs"
])

# Tab 1 — Skill Frequency
with tab1:
    st.markdown("#### Most In-Demand Skills")
    layer = st.radio("Skill layer", list(col_map.keys()), horizontal=True)
    col = col_map[layer]
    counts = skill_counts(df, col)
    if not counts.empty:
        st.plotly_chart(bar_chart(counts, COLORS[col], f"Top {layer}"), use_container_width=True)
        with st.expander("View full table"):
            st.dataframe(counts, use_container_width=True, hide_index=True)
    else:
        st.info("No data for this layer yet.")

# Tab 2 — 4-Layer Taxonomy
with tab2:
    st.markdown("#### The 4-Layer Skill Taxonomy")
    layers_info = [
        ("🔧 Tools & Tech", "tools", "#4f98a3", "Software, languages, platforms"),
        ("⚙️ Functional Skills", "functional_abilities", "#a86fdf", "What you actually do"),
        ("🏢 Domain Knowledge", "domain_knowledge", "#fdab43", "Industry context"),
        ("🧠 Behavioral Traits", "behavioral_abilities", "#6daa45", "How you work"),
    ]
    for label, col, color, desc in layers_info:
        counts = skill_counts(df, col, top_n=10)
        if counts.empty:
            continue
        with st.expander(f"{label} — {desc}", expanded=True):
            st.plotly_chart(bar_chart(counts, color, label), use_container_width=True)

# Tab 3 — Role Comparison
with tab3:
    st.markdown("#### Side-by-Side Role Comparison")
    c1, c2 = st.columns(2)
    with c1:
        role_a = st.selectbox("Role A", ROLES, index=0)
    with c2:
        role_b = st.selectbox("Role B", ROLES, index=1)
    compare_layer = st.radio("Compare", list(col_map.keys()), horizontal=True, key="cl")
    compare_col = col_map[compare_layer]
    with st.spinner("Loading..."):
        df_a = load_jobs([role_a], None, days_back)
        df_b = load_jobs([role_b], None, days_back)
    if not df_a.empty and not df_b.empty:
        ca = skill_counts(df_a, compare_col, 12).set_index("Skill")["Count"].to_dict()
        cb = skill_counts(df_b, compare_col, 12).set_index("Skill")["Count"].to_dict()
        top = list(set(list(ca.keys()) + list(cb.keys())))[:20]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name=role_a, y=top, x=[ca.get(s, 0) for s in top],
            orientation="h", marker_color="#4f98a3", opacity=0.85
        ))
        fig.add_trace(go.Bar(
            name=role_b, y=top, x=[cb.get(s, 0) for s in top],
            orientation="h", marker_color="#a86fdf", opacity=0.85
        ))
        fig.update_layout(
            barmode="group", paper_bgcolor="#0e0e0c", plot_bgcolor="#0e0e0c",
            font_color="#7a7870", legend_font_color="#e8e6e0",
            xaxis=dict(gridcolor="#262523"), yaxis=dict(color="#e8e6e0"),
            margin=dict(l=10, r=20, t=20, b=10), height=500
        )
        st.plotly_chart(fig, use_container_width=True)
        st.divider()
        cl, cr = st.columns(2)
        with cl:
            st.markdown(f"**{role_a} only**")
            for s in [s for s in ca if s not in cb and ca[s] > 2][:8]:
                st.markdown(f"- {s}")
        with cr:
            st.markdown(f"**{role_b} only**")
            for s in [s for s in cb if s not in ca and cb[s] > 2][:8]:
                st.markdown(f"- {s}")
    else:
        st.warning("Not enough data for one or both roles.")

# Tab 4 — Gap Analysis
with tab4:
    st.markdown("#### Client Skill Gap Analysis")
    target_role = st.selectbox("Target Role", ROLES, key="gap_role")
    client_input = st.text_area(
        "Client's current skills (comma-separated)",
        placeholder="e.g. Python, SQL, Excel, Communication",
        height=80
    )
    if client_input:
        client_skills = {s.strip().lower() for s in client_input.split(",") if s.strip()}
        with st.spinner("Analyzing..."):
            df_target = load_jobs([target_role], None, days_back)
        if not df_target.empty:
            market = {}
            for ln, c in [
                ("Tools", "tools"), ("Functional", "functional_abilities"),
                ("Domain", "domain_knowledge"), ("Behavioral", "behavioral_abilities")
            ]:
                for _, row in skill_counts(df_target, c).iterrows():
                    market[row["Skill"]] = {"pct": row["% of Jobs"], "layer": ln}
            have = [(s, i["pct"], i["layer"]) for s, i in sorted(market.items(), key=lambda x: -x[1]["pct"]) if s.lower() in client_skills]
            need = [(s, i["pct"], i["layer"]) for s, i in sorted(market.items(), key=lambda x: -x[1]["pct"]) if s.lower() not in client_skills]
            st.markdown(f"**{len(have)} matched · {len(need[:15])} gaps identified**")
            ch, cn = st.columns(2)
            with ch:
                st.markdown("##### ✅ Skills You Have")
                for skill, pct, layer in have:
                    st.markdown(f"- **{skill}** · {layer} · {pct}%")
            with cn:
                st.markdown("##### 🎯 Priority Gaps")
                for skill, pct, layer in need[:15]:
                    st.markdown(f"- **{skill}** · {layer} · {pct}%")
        else:
            st.warning("No data found for this role.")
    else:
        st.info("Enter your client's skills above.")

# Tab 5 — Raw Jobs
with tab5:
    st.markdown("#### Raw Job Postings")
    search = st.text_input("Search", placeholder="Filter by title, company, or location...")
    display = df.copy()
    if search:
        mask = (
            display["title"].str.contains(search, case=False, na=False) |
            display["company"].str.contains(search, case=False, na=False) |
            display["location"].str.contains(search, case=False, na=False)
        )
        display = display[mask]
    show_cols = [c for c in ["title", "company", "location", "job_category", "is_remote", "scrape_datetime", "job_url"] if c in display.columns]
    st.markdown(f"**{len(display):,} jobs**")
    st.dataframe(display[show_cols].head(500), use_container_width=True, hide_index=True)