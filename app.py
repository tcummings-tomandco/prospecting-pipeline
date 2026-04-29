"""
Prospecting Pipeline — Streamlit UI

A simple web app for enriching company lists with Apollo contacts and
pushing them to Pipedrive as Organizations, Persons, and Leads.

Run locally:  streamlit run app.py
"""
import io
import os
import tempfile
import time
import traceback
from functools import partial
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import streamlit as st

from apollo_enrich import enrich_companies, save_enriched_excel, ROLE_PROFILES, DEFAULT_PROFILE
from pipedrive_push import push_to_pipedrive, prepare_contacts
from job_state import (
    create_job, get_job, update_progress, run_in_background, clear_job, list_active_jobs,
)


# --- Page setup ---
st.set_page_config(
    page_title="Enrich Contacts",
    page_icon="🎯",
    layout="wide",
)

st.markdown("""
<style>
  .stMetric { background: #f5f7fa; padding: 16px; border-radius: 8px; border: 1px solid #e1e4e8; }
  .stDownloadButton button { width: 100%; }
</style>
""", unsafe_allow_html=True)


# --- Session state ---
if "enriched_df" not in st.session_state:
    st.session_state.enriched_df = None
if "enriched_filename" not in st.session_state:
    st.session_state.enriched_filename = None
if "push_stats" not in st.session_state:
    st.session_state.push_stats = None
if "category" not in st.session_state:
    st.session_state.category = ""
if "enrich_job_id" not in st.session_state:
    st.session_state.enrich_job_id = None
if "push_job_id" not in st.session_state:
    st.session_state.push_job_id = None

# If browser was refreshed mid-run, recover any active job
if st.session_state.enrich_job_id is None:
    active = list_active_jobs(kind="enrich")
    if active:
        st.session_state.enrich_job_id = active[0]
if st.session_state.push_job_id is None:
    active = list_active_jobs(kind="push")
    if active:
        st.session_state.push_job_id = active[0]


# --- Sidebar: status of keys ---
st.sidebar.header("🔑 API Status")
apollo_ok = bool(os.environ.get("APOLLO_API_KEY"))
pipedrive_ok = bool(os.environ.get("PIPEDRIVE_API_TOKEN")) and bool(os.environ.get("PIPEDRIVE_DOMAIN"))

st.sidebar.markdown(f"**Apollo**: {'✅ Connected' if apollo_ok else '❌ Not configured'}")
st.sidebar.markdown(f"**Pipedrive**: {'✅ Connected' if pipedrive_ok else '❌ Not configured'}")

if not apollo_ok or not pipedrive_ok:
    st.sidebar.warning("Set API keys in the `.env` file or environment before running.")

st.sidebar.divider()
st.sidebar.markdown("### ℹ️ Workflow")
st.sidebar.markdown("""
1. **Upload** company list (.xlsx)
2. **Enrich** with Apollo contacts
3. **Review** the enriched data
4. **Push** to Pipedrive with label
""")


# --- Main header ---
st.title("🎯 Enrich Contacts")
st.caption("Enrich company lists with Apollo and push contacts to Pipedrive.")


# --- Tabs ---
tab_enrich, tab_push = st.tabs(["**1. Enrich**", "**2. Push to Pipedrive**"])


# ==================== ENRICH TAB ====================
with tab_enrich:
    st.subheader("Upload Company List")

    col1, col2 = st.columns([2, 1])

    with col1:
        uploaded = st.file_uploader(
            "Excel file with 'Company' and 'Website' columns",
            type=["xlsx"],
            key="upload_enrich",
        )

    with col2:
        category_input = st.text_input(
            "Category label",
            value=st.session_state.category,
            placeholder="e.g. Furniture",
            help="Becomes the label on Organizations, Persons and Leads in Pipedrive.",
        )
        if category_input:
            st.session_state.category = category_input

    if uploaded:
        try:
            preview_df = pd.read_excel(uploaded)

            # Validate columns
            cols_lower = [c.lower().strip() for c in preview_df.columns]
            has_company = any(c in cols_lower for c in ["company", "company name", "name", "organisation", "organization"])
            has_website = any(c in cols_lower for c in ["website", "domain", "url", "web", "site"])

            if not has_company or not has_website:
                st.error(f"File needs a 'Company' and 'Website' column. Found: {list(preview_df.columns)}")
                preview_df = None
            else:
                st.success(f"Loaded **{len(preview_df)} companies** from {uploaded.name}")

                with st.expander("Preview input", expanded=False):
                    st.dataframe(preview_df, use_container_width=True, height=200)

            # Reset pointer for re-reading
            uploaded.seek(0)
        except Exception as e:
            st.error(f"Couldn't read file: {e}")
            preview_df = None
    else:
        preview_df = None

    st.divider()

    # --- Role profile selector ---
    st.markdown("### 🎯 Who to target")

    profile_names = list(ROLE_PROFILES.keys())
    selected_profile = st.selectbox(
        "Role profile",
        profile_names,
        index=profile_names.index(DEFAULT_PROFILE),
        help="Choose which job titles to search for. The script tries 'primary' titles first, then falls back to broader 'fallback' titles if no contacts found.",
    )

    profile_data = ROLE_PROFILES[selected_profile]
    st.caption(profile_data["description"])

    # Allow customising the titles
    customise = st.checkbox("Customise titles for this run", value=False)

    if customise:
        col_p, col_f = st.columns(2)
        with col_p:
            primary_text = st.text_area(
                "Primary titles (one per line)",
                value="\n".join(profile_data["primary"]),
                height=180,
                help="Tried first. Most specific to your target role.",
            )
        with col_f:
            fallback_text = st.text_area(
                "Fallback titles (one per line)",
                value="\n".join(profile_data["fallback"]),
                height=180,
                help="Used only if no contacts found with primary titles.",
            )
        custom_primary = [t.strip() for t in primary_text.splitlines() if t.strip()]
        custom_fallback = [t.strip() for t in fallback_text.splitlines() if t.strip()]
    else:
        custom_primary = None
        custom_fallback = None
        with st.expander("View titles for this profile"):
            cP, cF = st.columns(2)
            with cP:
                st.markdown("**Primary**")
                for t in profile_data["primary"]:
                    st.text(f"• {t}")
            with cF:
                st.markdown("**Fallback**")
                for t in profile_data["fallback"]:
                    st.text(f"• {t}")

    st.divider()

    # --- Enrich button + background job handling ---
    active_job_id = st.session_state.enrich_job_id
    active_job = get_job(active_job_id) if active_job_id else None
    has_active_job = active_job and active_job["status"] in ("pending", "running")

    enrich_disabled = (
        has_active_job
        or not (uploaded and category_input and apollo_ok and preview_df is not None)
    )

    btn_label = "🚀 Enrich with Apollo"
    if has_active_job:
        btn_label = "⏳ Enrichment in progress..."

    if st.button(btn_label, type="primary", disabled=enrich_disabled, use_container_width=True):
        # Snapshot inputs before launching the thread (Streamlit objects can't be used in workers)
        uploaded.seek(0)
        df = pd.read_excel(uploaded)
        filename = uploaded.name.replace(".xlsx", " - Enriched.xlsx")
        st.session_state.enriched_filename = filename

        job_id = create_job(kind="enrich")
        st.session_state.enrich_job_id = job_id

        def worker(_df=df, _category=category_input, _profile=selected_profile,
                   _primary=custom_primary, _fallback=custom_fallback, _job_id=job_id):
            def on_progress(idx, total, company, message):
                update_progress(_job_id, idx, total, company, message)
            return enrich_companies(
                _df, _category,
                on_progress=on_progress,
                profile_name=_profile,
                primary_titles=_primary,
                fallback_titles=_fallback,
            )

        run_in_background(job_id, worker)
        st.rerun()

    # Show progress for active job
    if has_active_job:
        st.info("✨ Enrichment running in the background — you can switch tabs and come back. Progress updates every few seconds.")
        pct = active_job["progress"] / active_job["total"] if active_job["total"] else 0.0
        progress_text = f"[{active_job['progress']}/{active_job['total']}] {active_job['current']}" if active_job["total"] else "Starting..."
        st.progress(min(pct, 1.0), text=progress_text)
        if active_job["messages"]:
            st.code("\n".join(active_job["messages"][-10:]), language=None)
        # Auto-refresh every 3 seconds while running
        time.sleep(3)
        st.rerun()

    # Handle finished jobs
    if active_job and active_job["status"] == "complete" and active_job["result"] is not None:
        if st.session_state.enriched_df is None or len(st.session_state.enriched_df) != len(active_job["result"]):
            st.session_state.enriched_df = active_job["result"]
            st.success(f"✅ Enrichment complete — {len(active_job['result'])} rows")
            # Clear job from memory once we've consumed it
            clear_job(active_job_id)
            st.session_state.enrich_job_id = None
            st.rerun()

    if active_job and active_job["status"] == "failed":
        st.error(f"Enrichment failed: {active_job['error']}")
        if st.button("Dismiss error"):
            clear_job(active_job_id)
            st.session_state.enrich_job_id = None
            st.rerun()

    # --- Enriched summary ---
    if st.session_state.enriched_df is not None:
        st.divider()
        st.subheader("📊 Enrichment Summary")

        df = st.session_state.enriched_df
        total_rows = len(df)
        companies = df["Company"].nunique()
        with_email = df[df["Email"].notna() & (df["Email"] != "")].shape[0]
        verified = (df["Email Status"] == "verified").sum() if "Email Status" in df.columns else 0
        no_contacts = df[df["Search Type"] == "none found"].shape[0] if "Search Type" in df.columns else 0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Companies", companies)
        c2.metric("Total contacts", total_rows)
        c3.metric("With email", with_email)
        c4.metric("Verified", verified)
        c5.metric("No contacts", no_contacts)

        st.subheader("📋 Enriched Contacts")
        st.dataframe(df, use_container_width=True, height=400)

        # Download button
        tmp = io.BytesIO()
        try:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tf:
                save_enriched_excel(df, tf.name)
                with open(tf.name, "rb") as fh:
                    tmp.write(fh.read())
            os.unlink(tf.name)
        except Exception:
            df.to_excel(tmp, index=False)
        tmp.seek(0)

        st.download_button(
            "⬇️ Download enriched Excel",
            data=tmp.getvalue(),
            file_name=st.session_state.enriched_filename or "Enriched.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


# ==================== PUSH TAB ====================
with tab_push:
    st.subheader("Push to Pipedrive")

    # Source: either the in-session enriched df or a freshly uploaded file
    source = st.radio(
        "Source",
        ["Use enrichment from this session", "Upload an enriched Excel file"],
        horizontal=True,
    )

    push_df = None
    push_category = st.session_state.category

    if source == "Use enrichment from this session":
        if st.session_state.enriched_df is not None:
            push_df = st.session_state.enriched_df
            st.info(f"Using enriched data from this session ({len(push_df)} rows)")
        else:
            st.warning("No enrichment in this session yet — run enrichment in tab 1 first, or upload a file.")
    else:
        uploaded_push = st.file_uploader(
            "Enriched Excel file",
            type=["xlsx"],
            key="upload_push",
        )
        if uploaded_push:
            try:
                push_df = pd.read_excel(uploaded_push)
                st.success(f"Loaded {len(push_df)} rows from {uploaded_push.name}")
            except Exception as e:
                st.error(f"Couldn't read file: {e}")

    if push_df is not None:
        push_category = st.text_input(
            "Category label",
            value=push_category or (push_df["Category"].iloc[0] if "Category" in push_df.columns else ""),
            help="Applied as a label on Organizations, Persons and Leads.",
        )

        # Preview of what will be pushed
        contacts, has_split = prepare_contacts(push_df)
        if contacts.empty:
            st.warning("No contacts with emails in this file — nothing to push.")
        else:
            grouped = contacts.groupby("Company")

            st.divider()
            st.markdown("### Preview")
            c1, c2, c3 = st.columns(3)
            c1.metric("Organizations", len(grouped))
            c2.metric("Persons", len(contacts))
            c3.metric("Leads", len(grouped))

            with st.expander("Preview by company", expanded=False):
                preview_lines = []
                for company, rows in grouped:
                    preview_lines.append(f"**{company}** — Lead: '{company}' [{push_category}]")
                    for _, row in rows.iterrows():
                        if has_split:
                            name = f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip()
                        else:
                            name = row.get("Contact Name", "")
                        title = row.get("Title", "")
                        if pd.isna(title): title = ""
                        preview_lines.append(f"  • {name} ({title}) — {row['Email']}")
                st.markdown("\n".join(preview_lines))

            st.divider()

            push_active_id = st.session_state.push_job_id
            push_active = get_job(push_active_id) if push_active_id else None
            push_in_progress = push_active and push_active["status"] in ("pending", "running")

            push_disabled = push_in_progress or not (pipedrive_ok and push_category)
            push_btn_label = "📤 Push to Pipedrive"
            if push_in_progress:
                push_btn_label = "⏳ Push in progress..."

            if st.button(push_btn_label, type="primary", disabled=push_disabled, use_container_width=True):
                # Snapshot inputs
                df_snap = push_df.copy()
                cat_snap = push_category

                job_id = create_job(kind="push")
                st.session_state.push_job_id = job_id

                def push_worker(_df=df_snap, _cat=cat_snap, _job_id=job_id):
                    def on_progress(i, total, msg):
                        update_progress(_job_id, i, total, "", msg)
                    return push_to_pipedrive(_df, _cat, on_progress=on_progress)

                run_in_background(job_id, push_worker)
                st.rerun()

            if push_in_progress:
                st.info("✨ Push running in the background — you can switch tabs and come back.")
                pct = push_active["progress"] / push_active["total"] if push_active["total"] else 0.0
                progress_text = push_active["messages"][-1] if push_active["messages"] else "Starting..."
                st.progress(min(pct, 1.0), text=progress_text)
                if push_active["messages"]:
                    st.code("\n".join(push_active["messages"][-10:]), language=None)
                time.sleep(3)
                st.rerun()

            if push_active and push_active["status"] == "complete" and push_active["result"] is not None:
                st.session_state.push_stats = push_active["result"]
                st.success("✅ Push complete")
                clear_job(push_active_id)
                st.session_state.push_job_id = None
                st.rerun()

            if push_active and push_active["status"] == "failed":
                st.error(f"Push failed: {push_active['error']}")
                if st.button("Dismiss push error"):
                    clear_job(push_active_id)
                    st.session_state.push_job_id = None
                    st.rerun()

    # --- Push summary ---
    if st.session_state.push_stats is not None:
        st.divider()
        st.subheader("📊 Push Summary")

        s = st.session_state.push_stats
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("🏢 Orgs created", s.get("orgs_created", 0))
            st.metric("🔁 Orgs existing", s.get("orgs_existing", 0))
        with c2:
            st.metric("👤 Persons created", s.get("persons_created", 0))
            st.metric("🔁 Persons updated", s.get("persons_existing", 0))
        with c3:
            st.metric("🎯 Leads created", s.get("leads_created", 0))
            st.metric("🔁 Leads existing", s.get("leads_existing", 0))
            st.metric("❌ Errors", s.get("errors", 0))

        if s.get("messages"):
            with st.expander("Errors and warnings"):
                for m in s["messages"]:
                    st.text(m)
