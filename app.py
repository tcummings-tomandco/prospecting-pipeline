"""
Prospecting Pipeline — Streamlit UI

A simple web app for enriching company lists with Apollo contacts and
pushing them to Pipedrive as Organizations, Persons, and Leads.

Run locally:  streamlit run app.py
"""
import io
import os
import tempfile
import traceback
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import streamlit as st

from apollo_enrich import enrich_companies, save_enriched_excel
from pipedrive_push import push_to_pipedrive, prepare_contacts


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

    # --- Enrich button ---
    enrich_disabled = not (uploaded and category_input and apollo_ok and preview_df is not None)
    if st.button("🚀 Enrich with Apollo", type="primary", disabled=enrich_disabled, use_container_width=True):
        progress_bar = st.progress(0.0, text="Starting...")
        log_area = st.empty()
        log_messages = []

        def on_progress(idx, total, company, message):
            pct = (idx + 1) / total if total else 1.0
            progress_bar.progress(min(pct, 1.0), text=f"[{idx+1}/{total}] {company}")
            log_messages.append(message)
            # Show last 10 messages
            log_area.code("\n".join(log_messages[-10:]), language=None)

        try:
            uploaded.seek(0)
            df = pd.read_excel(uploaded)
            result_df = enrich_companies(df, category_input, on_progress=on_progress)
            st.session_state.enriched_df = result_df
            st.session_state.enriched_filename = uploaded.name.replace(".xlsx", " - Enriched.xlsx")
            progress_bar.progress(1.0, text="Done!")
            st.success(f"✅ Enrichment complete — {len(result_df)} rows")
        except Exception as e:
            st.error(f"Enrichment failed: {e}")
            st.code(traceback.format_exc())

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

            push_disabled = not (pipedrive_ok and push_category)
            if st.button("📤 Push to Pipedrive", type="primary", disabled=push_disabled, use_container_width=True):
                progress_bar = st.progress(0.0, text="Starting...")
                log_area = st.empty()
                log_messages = []

                def on_progress(i, total, msg):
                    pct = (i + 1) / total if total else 1.0
                    progress_bar.progress(min(pct, 1.0), text=msg)
                    log_messages.append(msg)
                    log_area.code("\n".join(log_messages[-10:]), language=None)

                try:
                    stats = push_to_pipedrive(push_df, push_category, on_progress=on_progress)
                    st.session_state.push_stats = stats
                    progress_bar.progress(1.0, text="Done!")
                    st.success("✅ Push complete")
                except Exception as e:
                    st.error(f"Push failed: {e}")
                    st.code(traceback.format_exc())

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
            st.metric("❌ Errors", s.get("errors", 0))

        if s.get("messages"):
            with st.expander("Errors and warnings"):
                for m in s["messages"]:
                    st.text(m)
