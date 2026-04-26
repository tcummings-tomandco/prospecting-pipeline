# Deploying to Firebase + Cloud Run

This deploys the Streamlit app to **Cloud Run** (Python container) and serves it via **Firebase Hosting** at `https://enrich-contacts.web.app`. All within the existing Firebase Blaze project.

## One-Time Setup

### 1. Install CLI tools

```bash
brew install --cask google-cloud-sdk
npm install -g firebase-tools
```

### 2. Log in

```bash
gcloud auth login
firebase login
```

### 3. Set the active project

```bash
gcloud config set project enrich-contacts
```

### 4. Enable required Google Cloud APIs

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com
```

## Deploy

### Step 1 — Deploy the app to Cloud Run

From the project root:

```bash
gcloud run deploy enrich-contacts \
  --source . \
  --region europe-west1 \
  --allow-unauthenticated \
  --set-env-vars "APOLLO_API_KEY=yV9-CzIP1Iq86SdmmFyx9Q,PIPEDRIVE_API_TOKEN=1cf909fd36842a0266e95369000233a172b21fcc,PIPEDRIVE_DOMAIN=tomcoltd"
```

This:
- Builds the Docker container from the `Dockerfile`
- Deploys to Cloud Run as service `enrich-contacts` in `europe-west1`
- Sets the API keys as environment variables
- Returns a URL like `https://enrich-contacts-xxxxxx.a.run.app` — you can already use this URL

### Step 2 — Connect Firebase Hosting

```bash
firebase deploy --only hosting
```

After this, your app is live at:
- `https://enrich-contacts.web.app`
- `https://enrich-contacts.firebaseapp.com`

## Updating the App

After making code changes and pushing to GitHub:

```bash
gcloud run deploy enrich-contacts --source . --region europe-west1
```

Firebase Hosting automatically routes to the new revision — no need to redeploy hosting.

## Updating API Keys

```bash
gcloud run services update enrich-contacts \
  --region europe-west1 \
  --set-env-vars "APOLLO_API_KEY=new-key,PIPEDRIVE_API_TOKEN=new-token,PIPEDRIVE_DOMAIN=tomcoltd"
```

## Cost

- **Cloud Run**: scales to zero when idle. Pay only for actual request time. For light internal team use, expect <£5/month, often free.
- **Firebase Hosting**: free tier covers everything you'll use.

## Restricting Access (Optional)

By default the app is public (anyone with the URL can use it). To restrict to your team:

**Option A — Remove public access:**
```bash
gcloud run services update enrich-contacts \
  --region europe-west1 \
  --no-allow-unauthenticated
```
Then grant specific Google accounts access via IAM in the Cloud Run console.

**Option B — Add Firebase Authentication** to the app (requires code changes).
