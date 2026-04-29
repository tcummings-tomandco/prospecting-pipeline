import argparse, os, sys, time, json, requests, pandas as pd
from dotenv import load_dotenv
load_dotenv()
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

BASE = "https://api.apollo.io/api/v1"

PRIMARY_TITLES = [
    "ecommerce manager", "ecommerce director", "head of ecommerce",
    "vp ecommerce", "director of ecommerce", "digital director",
    "head of digital", "director of digital", "chief digital officer",
    "chief marketing officer", "head of online", "online director",
    "vp digital", "director of online sales",
]

FALLBACK_TITLES = [
    "managing director", "owner", "founder", "ceo", "general manager",
    "marketing manager", "head of marketing", "marketing director",
    "commercial director", "operations director", "sales director",
]

SENIORITIES = ["owner", "founder", "c_suite", "vp", "director", "manager"]


def _headers():
    key = os.environ.get("APOLLO_API_KEY", "")
    if not key:
        raise RuntimeError("APOLLO_API_KEY not set. Add it to your .env file or environment.")
    return {"x-api-key": key, "Content-Type": "application/json"}


def clean_domain(website):
    """Extract first clean domain from website field."""
    if pd.isna(website):
        return None
    w = str(website).strip()
    for prefix in ["https://www.", "http://www.", "https://", "http://", "www."]:
        w = w.replace(prefix, "")
    w = w.split("/")[0].strip()
    for sep in [" / ", " /", "/ ", " "]:
        if sep in w:
            w = w.split(sep)[0].strip()
            break
    if "(" in w:
        w = w.split("(")[0].strip()
    return w if w and "." in w else None


def search_people(domain):
    """Search Apollo for ecommerce decision-makers at a domain, with fallback."""
    payload = {
        "q_organization_domains_list": [domain],
        "person_titles": PRIMARY_TITLES,
        "person_seniorities": SENIORITIES,
        "page": 1,
        "per_page": 10,
    }
    try:
        r = requests.post(f"{BASE}/mixed_people/api_search", headers=_headers(), json=payload)
        r.raise_for_status()
        people = r.json().get("people", [])
        if people:
            return people, "primary"

        time.sleep(0.3)
        payload["person_titles"] = FALLBACK_TITLES
        r = requests.post(f"{BASE}/mixed_people/api_search", headers=_headers(), json=payload)
        r.raise_for_status()
        return r.json().get("people", []), "fallback"
    except requests.exceptions.RequestException as e:
        print(f"[APOLLO SEARCH] error for {domain}: {e}", flush=True)
        return [], f"error: {e}"


def enrich_person(person):
    """Enrich a person to get their email and full name."""
    payload = {"id": person["id"], "reveal_personal_emails": True}
    try:
        r = requests.post(f"{BASE}/people/match", headers=_headers(), json=payload)
        r.raise_for_status()
        p = r.json().get("person", {})
        return {
            "first_name": p.get("first_name", person.get("first_name", "")),
            "last_name": p.get("last_name", person.get("last_name", "")),
            "email": p.get("email", ""),
            "email_status": p.get("email_status", ""),
            "phone": (p.get("phone_numbers") or [{}])[0].get("sanitized_number", "") if p.get("phone_numbers") else "",
            "linkedin": p.get("linkedin_url", ""),
        }
    except requests.exceptions.RequestException as e:
        print(f"[APOLLO ENRICH] error for {person.get('id', 'unknown')}: {e}", flush=True)
        return {"first_name": person.get("first_name", ""), "last_name": person.get("last_name", ""),
                "email": "", "email_status": "", "phone": "", "linkedin": ""}


def _find_column(df, candidates):
    """Find the first matching column name (case-insensitive)."""
    col_map = {c.lower().strip(): c for c in df.columns}
    for name in candidates:
        if name.lower() in col_map:
            return col_map[name.lower()]
    return None


def enrich_companies(df, category, on_progress=None, contacts_per_company=3):
    """Enrich a DataFrame of companies. Calls on_progress(idx, total, company, message) per step.
    Returns a DataFrame of results.
    """
    # Flexible column matching
    company_col = _find_column(df, ["Company", "Company Name", "Name", "Organisation", "Organization"])
    website_col = _find_column(df, ["Website", "Domain", "URL", "Web", "Site"])

    if not company_col:
        raise ValueError(f"No 'Company' column found. Columns in file: {list(df.columns)}")
    if not website_col:
        raise ValueError(f"No 'Website' column found. Columns in file: {list(df.columns)}")

    results = []
    total = len(df)

    def log(idx, company, message):
        if on_progress:
            on_progress(idx, total, company, message)

    for idx, row in df.iterrows():
        company = row[company_col]
        domain = clean_domain(row[website_col])
        log(idx, company, f"Processing {company} ({domain or 'no domain'})")

        if not domain:
            results.append({
                "Company": company, "Website": row[website_col],
                "First Name": "", "Last Name": "", "Title": "", "Email": "",
                "Email Status": "", "Phone": "", "LinkedIn": "",
                "Search Type": "no domain", "Category": category,
            })
            continue

        people, search_type = search_people(domain)
        time.sleep(0.3)

        if not people:
            log(idx, company, f"  No contacts found for {company}")
            results.append({
                "Company": company, "Website": row[website_col],
                "First Name": "", "Last Name": "", "Title": "", "Email": "",
                "Email Status": "", "Phone": "", "LinkedIn": "",
                "Search Type": "none found", "Category": category,
            })
            continue

        label = "ecommerce" if search_type == "primary" else "fallback"
        log(idx, company, f"  Found {len(people)} via {label}, enriching top {contacts_per_company}...")

        for person in people[:contacts_per_company]:
            title = person.get("title", "")
            enriched = enrich_person(person)
            time.sleep(0.3)

            first = enriched["first_name"]
            last = enriched["last_name"]
            results.append({
                "Company": company,
                "Website": row[website_col],
                "First Name": first,
                "Last Name": last,
                "Title": title,
                "Email": enriched["email"],
                "Email Status": enriched["email_status"],
                "Phone": enriched["phone"],
                "LinkedIn": enriched["linkedin"],
                "Search Type": search_type,
                "Category": category,
            })
            log(idx, company, f"    {first} {last} - {title} - {enriched['email']}")

    return pd.DataFrame(results)


def save_enriched_excel(df, output_file):
    """Save enriched DataFrame to a formatted Excel file."""
    df.to_excel(output_file, index=False, sheet_name="Enriched Contacts")

    wb = load_workbook(output_file)
    ws = wb.active

    header_font = Font(name="Arial", bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill("solid", fgColor="2F5496")
    cell_font = Font(name="Arial", size=10)
    thin_border = Border(
        left=Side(style="thin", color="D9D9D9"),
        right=Side(style="thin", color="D9D9D9"),
        top=Side(style="thin", color="D9D9D9"),
        bottom=Side(style="thin", color="D9D9D9"),
    )

    for cell in ws[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")
        cell.border = thin_border

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.font = cell_font
            cell.border = thin_border

    widths = {"A": 28, "B": 35, "C": 18, "D": 18, "E": 35, "F": 35, "G": 14, "H": 18, "I": 40, "J": 16, "K": 16}
    for col, w in widths.items():
        ws.column_dimensions[col].width = w

    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"
    wb.save(output_file)


def main():
    parser = argparse.ArgumentParser(description="Enrich company list with Apollo contacts")
    parser.add_argument("input_file", help="Excel file with Company and Website columns")
    parser.add_argument("--category", "-c", default=None,
                        help="Category label (e.g. 'Furniture'). Defaults to filename stem.")
    args = parser.parse_args()

    input_file = args.input_file
    category = args.category or os.path.splitext(os.path.basename(input_file))[0].replace(" Companies", "")
    output_file = input_file.replace(".xlsx", " - Enriched.xlsx")

    df = pd.read_excel(input_file)
    print(f"Loaded {len(df)} companies from {input_file}")
    print(f"Category: {category}\n")

    def on_progress(idx, total, company, message):
        print(f"[{idx+1}/{total}] {message}")

    result_df = enrich_companies(df, category, on_progress=on_progress)
    save_enriched_excel(result_df, output_file)

    print(f"\nDone! {len(result_df)} rows written to {output_file}")
    verified = (result_df["Email Status"] == "verified").sum()
    print(f"Verified emails: {verified}")


if __name__ == "__main__":
    main()
