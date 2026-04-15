# Prospecting Pipeline

Enrich company lists with Apollo contacts and push them to Pipedrive as Organizations, Persons, and Leads — all through a simple web UI.

## 🚀 Quick Start (Web App)

**Double-click `Launch App.command`** — that's it. The app opens in your browser.

First time only:

```bash
pip3 install -r requirements.txt --break-system-packages
```

Then set up your API keys in `.env` (copy from `.env.example`):

```
APOLLO_API_KEY=your-apollo-api-key
PIPEDRIVE_API_TOKEN=your-pipedrive-api-token
PIPEDRIVE_DOMAIN=yourcompany
```

## 🖥 The App

Open a browser to `http://localhost:8501` after launching. Two tabs:

### Tab 1 — Enrich
- Upload an Excel file with `Company` and `Website` columns
- Set a category label (e.g. "Furniture", "Sports & Leisure")
- Click **Enrich with Apollo** — live progress, summary cards, downloadable output

### Tab 2 — Push to Pipedrive
- Use the enriched data from this session, or upload a previously enriched file
- Preview what will be pushed (orgs, persons, leads)
- Click **Push to Pipedrive** — live progress, summary cards of created/updated records

## 📁 Files

| File | Purpose |
|------|---------|
| `app.py` | Streamlit UI |
| `apollo_enrich.py` | Apollo enrichment (also runs standalone via CLI) |
| `pipedrive_push.py` | Pipedrive push (also runs standalone via CLI) |
| `Launch App.command` | Double-click launcher for the web app |
| `.env` | Your API keys (not committed to git) |
| `.env.example` | Template for setting up `.env` |
| `requirements.txt` | Python dependencies |

## ⚙️ What Gets Created in Pipedrive

For each company with enriched contacts:
- **1 Organization** (deduplicated by name)
- **1 Person per contact** (deduplicated by email, linked to the Org)
  - Includes job title, LinkedIn URL, phone, work email
- **1 Lead** (linked to the Org and first contact)

All three (Org, Person, Lead) get a **category label** (e.g. "Furniture") for filtering.

On first run, the app auto-creates:
- Labels on Organizations, Persons, and Leads
- A custom "LinkedIn" field on Persons (if it doesn't already exist)

## 🔁 Re-running Is Safe

- Orgs deduplicated by name
- Persons deduplicated by email — existing ones get their job title, LinkedIn, and label updated
- New leads are created each run — delete old ones in Pipedrive if re-running

## 💻 CLI (Advanced)

The scripts also run from the terminal:

```bash
python3 apollo_enrich.py "Furniture Companies.xlsx" --category "Furniture"
python3 pipedrive_push.py "Furniture Companies - Enriched.xlsx" --category "Furniture" --dry-run
python3 pipedrive_push.py "Furniture Companies - Enriched.xlsx" --category "Furniture"
```

## 💳 Apollo Credits

- People Search: free (no credits)
- People Enrichment: ~1 credit per contact (up to 3 per company)
- Don't re-run enrichment unnecessarily — it costs credits

## 🌐 Deploying as a Shared URL (Optional)

To host this for your whole team:

1. Push the repo to GitHub (make sure `.env` is in `.gitignore`)
2. Go to [share.streamlit.io](https://share.streamlit.io/) and connect the repo
3. In **App settings → Secrets**, add your API keys:
   ```
   APOLLO_API_KEY = "your-key"
   PIPEDRIVE_API_TOKEN = "your-token"
   PIPEDRIVE_DOMAIN = "yourcompany"
   ```
4. Deploy — share the URL with your team

## 🔒 Security Notes

- `.env` contains real keys and is excluded from git
- `.env.example` has placeholders — safe to commit
- Never share `.env` directly — share `.env.example` and let each person fill in their own
