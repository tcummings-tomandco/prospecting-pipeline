import argparse, os, sys, time, requests, pandas as pd
from dotenv import load_dotenv
load_dotenv()


def _config():
    """Lazy-load Pipedrive config. Raises if env vars missing."""
    token = os.environ.get("PIPEDRIVE_API_TOKEN", "")
    domain = os.environ.get("PIPEDRIVE_DOMAIN", "")
    if not token:
        raise RuntimeError("PIPEDRIVE_API_TOKEN not set. Add it to your .env file.")
    if not domain:
        raise RuntimeError("PIPEDRIVE_DOMAIN not set. Add it to your .env file.")
    return {
        "token": token,
        "base_v1": f"https://{domain}.pipedrive.com/api/v1",
        "params": {"api_token": token},
    }


def api_get(url, params=None):
    cfg = _config()
    p = {**cfg["params"], **(params or {})}
    r = requests.get(url, params=p)
    r.raise_for_status()
    return r.json()


def api_post(url, data):
    cfg = _config()
    r = requests.post(url, params=cfg["params"], json=data)
    r.raise_for_status()
    return r.json()


def api_put(url, data):
    cfg = _config()
    r = requests.put(url, params=cfg["params"], json=data)
    r.raise_for_status()
    return r.json()


# --- Label management ---

def get_or_create_lead_label(name):
    cfg = _config()
    data = api_get(f"{cfg['base_v1']}/leadLabels")
    for label in data.get("data", []) or []:
        if label["name"].lower() == name.lower():
            return label["id"]
    result = api_post(f"{cfg['base_v1']}/leadLabels", {"name": name, "color": "blue"})
    return result["data"]["id"]


def _get_or_create_field_label(entity, name):
    """Get or create a label option on the built-in 'label' field for persons or organizations."""
    cfg = _config()
    fields = api_get(f"{cfg['base_v1']}/{entity}Fields")
    label_field = None
    for f in fields.get("data", []) or []:
        if f.get("key") == "label":
            label_field = f
            break
    if not label_field:
        return None

    for option in label_field.get("options", []) or []:
        if option["label"].lower() == name.lower():
            return option["id"]

    existing = label_field.get("options", []) or []
    updated = api_put(
        f"{cfg['base_v1']}/{entity}Fields/{label_field['id']}",
        {"options": existing + [{"label": name}]},
    )
    for option in updated.get("data", {}).get("options", []) or []:
        if option["label"].lower() == name.lower():
            return option["id"]
    return None


def get_or_create_person_label(name):
    return _get_or_create_field_label("person", name)


def get_or_create_org_label(name):
    return _get_or_create_field_label("organization", name)


# --- Field key discovery ---

def get_person_field_keys():
    cfg = _config()
    fields = api_get(f"{cfg['base_v1']}/personFields")
    keys = {"job_title": "job_title", "linkedin": None}
    for f in fields.get("data", []) or []:
        name = f.get("name", "").lower()
        key = f.get("key", "")
        if name == "job title" or key == "job_title":
            keys["job_title"] = key
        if "linkedin" in name:
            keys["linkedin"] = key
    return keys


def ensure_linkedin_field():
    cfg = _config()
    fields = api_get(f"{cfg['base_v1']}/personFields")
    for f in fields.get("data", []) or []:
        if "linkedin" in f.get("name", "").lower():
            return f["key"]
    result = api_post(f"{cfg['base_v1']}/personFields", {"name": "LinkedIn", "field_type": "varchar"})
    return result.get("data", {}).get("key")


# --- Deduplication ---

def search_org(name):
    cfg = _config()
    data = api_get(f"{cfg['base_v1']}/organizations/search", {"term": name, "limit": 5})
    for item in (data.get("data", {}) or {}).get("items", []) or []:
        if item.get("item", {}).get("name", "").lower() == name.lower():
            return item["item"]["id"]
    return None


def search_person_by_email(email):
    if not email:
        return None
    cfg = _config()
    data = api_get(f"{cfg['base_v1']}/persons/search", {"term": email, "fields": "email", "limit": 5})
    for item in (data.get("data", {}) or {}).get("items", []) or []:
        for e in item.get("item", {}).get("emails", []) or []:
            if e.lower() == email.lower():
                return item["item"]["id"]
    return None


def search_lead_for_org(org_id, title):
    """Check if an open lead already exists for this org with matching title."""
    if not org_id:
        return None
    cfg = _config()
    try:
        data = api_get(f"{cfg['base_v1']}/leads", {"organization_id": org_id, "archived_status": "not_archived"})
        for lead in data.get("data", []) or []:
            if lead.get("title", "").lower() == title.lower():
                return lead["id"]
    except Exception:
        pass
    return None


# --- Create / update ---

def create_org(name, org_label_id):
    cfg = _config()
    payload = {"name": name}
    if org_label_id:
        payload["label_ids"] = [org_label_id]
    return api_post(f"{cfg['base_v1']}/organizations", payload).get("data", {}).get("id")


def create_person(first_name, last_name, email, phone, title, org_id, person_label_id=None, field_keys=None, linkedin=""):
    cfg = _config()
    fk = field_keys or {}
    name = f"{first_name} {last_name}".strip() or first_name
    payload = {"name": name, "org_id": org_id}
    if email:
        payload["email"] = [{"value": email, "primary": True, "label": "work"}]
    if phone:
        payload["phone"] = [{"value": phone, "primary": True, "label": "work"}]
    if title:
        payload[fk.get("job_title", "job_title")] = title
    if linkedin and fk.get("linkedin"):
        payload[fk["linkedin"]] = linkedin
    if person_label_id:
        payload["label_ids"] = [person_label_id]
    return api_post(f"{cfg['base_v1']}/persons", payload).get("data", {}).get("id")


def update_person(person_id, title=None, person_label_id=None, field_keys=None, linkedin=""):
    cfg = _config()
    fk = field_keys or {}
    payload = {}
    if title:
        payload[fk.get("job_title", "job_title")] = title
    if linkedin and fk.get("linkedin"):
        payload[fk["linkedin"]] = linkedin
    if person_label_id:
        payload["label_ids"] = [person_label_id]
    if not payload:
        return
    return api_put(f"{cfg['base_v1']}/persons/{person_id}", payload)


def create_lead(title, person_id, org_id, lead_label_id):
    cfg = _config()
    payload = {"title": title, "person_id": person_id, "organization_id": org_id}
    if lead_label_id:
        payload["label_ids"] = [lead_label_id]
    return api_post(f"{cfg['base_v1']}/leads", payload).get("data", {}).get("id")


# --- High-level orchestration ---

def _clean(v):
    return "" if pd.isna(v) else v


def prepare_contacts(df):
    """Filter enriched DataFrame to rows with contact name and email."""
    has_split = "First Name" in df.columns
    if has_split:
        contacts = df[df["First Name"].notna() & (df["First Name"] != "")]
    else:
        contacts = df[df["Contact Name"].notna() & (df["Contact Name"] != "")]
    contacts = contacts[contacts["Email"].notna() & (contacts["Email"] != "")]
    return contacts, has_split


def push_to_pipedrive(df, category, on_progress=None):
    """Push enriched contacts to Pipedrive. Returns stats dict.
    Calls on_progress(current, total, message) at each step.
    """
    contacts, has_split = prepare_contacts(df)
    if contacts.empty:
        return {"orgs_created": 0, "orgs_existing": 0, "persons_created": 0,
                "persons_existing": 0, "leads_created": 0, "errors": 0,
                "messages": ["No contacts with emails to push."]}

    grouped = list(contacts.groupby("Company"))
    total = len(grouped)

    def log(i, msg):
        if on_progress:
            on_progress(i, total, msg)

    log(0, "Setting up labels...")
    lead_label_id = get_or_create_lead_label(category)
    org_label_id = get_or_create_org_label(category)
    person_label_id = get_or_create_person_label(category)
    field_keys = get_person_field_keys()
    field_keys["linkedin"] = ensure_linkedin_field()

    stats = {"orgs_created": 0, "orgs_existing": 0, "persons_created": 0,
             "persons_existing": 0, "leads_created": 0, "leads_existing": 0,
             "errors": 0, "messages": []}

    for comp_idx, (company, rows) in enumerate(grouped):
        log(comp_idx, f"[{comp_idx+1}/{total}] {company}")

        try:
            # Organization
            org_id = search_org(company)
            if org_id:
                stats["orgs_existing"] += 1
            else:
                org_id = create_org(company, org_label_id)
                stats["orgs_created"] += 1
            time.sleep(0.2)

            # Persons
            first_person_id = None
            for _, row in rows.iterrows():
                if has_split:
                    first_name = _clean(row.get("First Name", ""))
                    last_name = _clean(row.get("Last Name", ""))
                else:
                    parts = str(row["Contact Name"]).split(" ", 1)
                    first_name = parts[0]
                    last_name = parts[1] if len(parts) > 1 else ""

                email = _clean(row.get("Email", ""))
                phone = _clean(row.get("Phone", ""))
                title = _clean(row.get("Title", ""))
                linkedin = _clean(row.get("LinkedIn", ""))

                person_id = search_person_by_email(email)
                if person_id:
                    update_person(person_id, title=title, person_label_id=person_label_id,
                                  field_keys=field_keys, linkedin=linkedin)
                    stats["persons_existing"] += 1
                else:
                    person_id = create_person(first_name, last_name, email, phone, title, org_id,
                                              person_label_id, field_keys, linkedin)
                    stats["persons_created"] += 1
                if not first_person_id:
                    first_person_id = person_id
                time.sleep(0.2)

            # Lead — dedupe by org + title to avoid creating duplicates on re-run
            existing_lead = search_lead_for_org(org_id, company)
            if existing_lead:
                stats["leads_existing"] += 1
            else:
                create_lead(company, first_person_id, org_id, lead_label_id)
                stats["leads_created"] += 1
            time.sleep(0.2)

        except Exception as e:
            import traceback
            msg = f"ERROR on {company}: {e}"
            stats["errors"] += 1
            stats["messages"].append(msg)
            log(comp_idx, msg)
            # Also print to stdout so it shows up in Cloud Run logs
            print(f"[PIPEDRIVE PUSH] {msg}", flush=True)
            print(traceback.format_exc(), flush=True)

    return stats


def main():
    parser = argparse.ArgumentParser(description="Push enriched contacts to Pipedrive")
    parser.add_argument("enriched_file", help="Enriched Excel file from apollo_enrich.py")
    parser.add_argument("--category", "-c", default=None,
                        help="Category label (e.g. 'Furniture'). Overrides category from file.")
    parser.add_argument("--dry-run", action="store_true", help="Preview what would be pushed")
    args = parser.parse_args()

    df = pd.read_excel(args.enriched_file)
    print(f"Loaded {len(df)} rows from {args.enriched_file}\n")

    contacts, has_split = prepare_contacts(df)
    if contacts.empty:
        print("No contacts with emails to push.")
        return

    category = args.category or (contacts["Category"].iloc[0] if "Category" in contacts.columns else "Prospect")
    grouped = list(contacts.groupby("Company"))
    print(f"Category: {category}")
    print(f"Companies: {len(grouped)}, Contacts: {len(contacts)}\n")

    if args.dry_run:
        print("=== DRY RUN ===\n")
        for company, rows in grouped:
            print(f"  Org: {company}")
            print(f"  Lead: '{company}' [label: {category}]")
            for _, row in rows.iterrows():
                if has_split:
                    first = row.get("First Name", "")
                    last = row.get("Last Name", "")
                    display = f"{first} {last}".strip()
                else:
                    display = row["Contact Name"]
                title = "" if pd.isna(row.get("Title", "")) else row.get("Title", "")
                print(f"    Person: {display} ({title}) - {row['Email']}")
            print()
        return

    def on_progress(i, total, msg):
        print(msg)

    stats = push_to_pipedrive(df, category, on_progress=on_progress)

    print(f"\n{'='*50}")
    print(f"Orgs created:     {stats['orgs_created']}")
    print(f"Orgs existing:    {stats['orgs_existing']}")
    print(f"Persons created:  {stats['persons_created']}")
    print(f"Persons existing: {stats['persons_existing']}")
    print(f"Leads created:    {stats['leads_created']}")
    print(f"Errors:           {stats['errors']}")


if __name__ == "__main__":
    main()
