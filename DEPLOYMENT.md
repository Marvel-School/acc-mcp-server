# ACC MCP Server — Deployment & Operations Reference

Last updated: April 28, 2026

This document describes the deployed environments, their components, and
how to operate them. Anyone joining the project should read this first.

---

## Architecture Overview

The ACC MCP Server is a Python FastMCP application that bridges Microsoft
Copilot Studio (and Claude Desktop) to Autodesk Construction Cloud. It
exposes three MCP endpoints, each serving a category of tools, mounted on
a single Starlette app behind API key authentication.
                Copilot Studio Orchestrator
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   ACC Navigator      ACC Access & Admin   ACC BIM & Model
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
            Custom Connectors (Power Platform)
                             │
                             ▼
          autodesk-agent-prod.azurewebsites.net
                             │
                             ▼
              Autodesk Construction Cloud APIs

---

## Environments

Two independent environments run the same codebase from different git
branches. Each has its own Azure App Service, its own API key, and its
own Copilot Studio configuration where applicable.

| Environment | Branch | App Service URL | Purpose |
|---|---|---|---|
| Production | `main` | https://autodesk-agent-prod.azurewebsites.net | Real users via Copilot Studio |
| Development | `dev` | https://autodesk-agent-dev.azurewebsites.net | Pre-production testing |

A third **experimental** environment is planned for 3LO and Microsoft
Entra ID integration work. See the Roadmap section for details.

### Production
- **18 tools** across three MCP endpoints
- **2-legged OAuth only** — service account credentials
- Used by: Copilot Studio agents (real users)
- Stability: only proven changes from `dev` are merged here

### Development
- **18 tools** identical to production
- **2-legged OAuth only**
- Used by: developer testing before promoting to production
- Stability: should always be working — this is "next week's prod"

---

## Production Copilot Studio Setup

### Agents (4 total)

| Agent Name | Role |
|---|---|
| Autodesk Construction Helper Prod | Orchestrator — routes user requests to specialists |
| ACC Navigator Prod | Navigation specialist (hubs, projects, folders, files) |
| ACC Access & Admin Prod | Access management specialist (members, permissions, folder admin) |
| ACC BIM & Model Expert Prod | BIM specialist (file translation, element counts) |

### Custom Connectors (3 total)

| Connector | MCP Endpoint | Tools |
|---|---|---|
| acc-mcp-nav-prod | /mcp/nav/ | list_hubs, list_projects, find_project, list_top_folders, list_folder_contents |
| acc-mcp-admin-prod | /mcp/admin/ | create_project, list_project_users, add_user, audit_hub_users, check_project_permissions, find_user_projects, apply_folder_template, delete_folder |
| acc-mcp-bim-prod | /mcp/bim/ | inspect_file, reprocess_file, count_elements |

All three connectors point to autodesk-agent-prod.azurewebsites.net and
use the production MCP API key for authentication.

### Configuration Notes

- Generative orchestration enabled on the orchestrator
- **Work IQ disabled on all 4 agents** — leaving this on causes random "No information was found" failures
- Content moderation set to **Medium** — High triggers false positives on legitimate construction terminology
- Routing topics use phrase triggers (`The agent chooses`) on the orchestrator
- Hardcoded loading messages are configured per route (Navigator: "⏳ Searching your Autodesk hubs...", Admin: "⏳ Connecting to Autodesk — retrieving access data...", BIM: "⏳ Connecting to the Model Derivative API...")

### Publishing

Published to Microsoft Teams. End users access the agent through the
Teams channel within the TBI tenant.

---

## Secrets Management

All secrets are stored as **Azure App Service environment variables** on
each App Service. They are never committed to the repository, never
included in Docker images, and never shared in chat or screenshots.

Each environment has independent values for:

- `MCP_API_KEY` — the X-API-Key header value required for all MCP traffic
- `APS_CLIENT_ID` and `APS_CLIENT_SECRET` — Autodesk APS app credentials
- `ACC_ADMIN_ID` — Autodesk admin user UUID for service account operations
- `ALLOWED_ORIGINS` — CORS allow-list for browser-based MCP clients

Personal copies of all keys are stored in the team password manager
under entries named:
- `acc-mcp-server prod`
- `acc-mcp-server dev`

To rotate any key, update the value in Azure → restart the App Service →
update the corresponding consumer (Copilot Studio connector for prod, or
Claude Desktop config for dev).

---

## Autodesk APS Configuration

A single APS app named "Copilot(Dev)" backs both environments.

- **Application Type:** Traditional Web App
- **Grant Type:** Authorization Code and Client Credentials (supports both 2LO and 3LO)
- **Callback URLs registered:**
  - https://autodesk-agent-dev.azurewebsites.net/callback
  - https://autodesk-agent-prod.azurewebsites.net/callback

All necessary APIs are enabled on the app: Data Management, Autodesk
Construction Cloud, Model Derivative, BIM 360 Account Administration.

---

## Deployment Pipeline

GitHub Actions deploys each branch to its corresponding App Service:

| Workflow | Trigger | Target |
|---|---|---|
| .github/workflows/main_autodesk-agent-prod.yml | push to `main` | autodesk-agent-prod |
| .github/workflows/deploy_dev.yml | push to `dev` | autodesk-agent-dev |

All workflows are SHA-pinned for reproducibility and use concurrency
guards to prevent overlapping deploys.

### Promotion flow
dev ──(merge --no-ff)──► main

Never push directly to `main`. All production changes flow through `dev`
first for verification.

---

## Health Checks

Each environment exposes a `/health` endpoint that returns 200 OK with
`{"status":"ok"}` when the service is healthy. These are pinged by Azure
load balancers automatically and can be used for manual verification.
https://autodesk-agent-prod.azurewebsites.net/health
https://autodesk-agent-dev.azurewebsites.net/health

Each MCP endpoint also responds to bare GETs (without trailing slash and
without API key) with a status JSON, useful for connector health checks:
https://autodesk-agent-prod.azurewebsites.net/mcp/nav
https://autodesk-agent-prod.azurewebsites.net/mcp/admin
https://autodesk-agent-prod.azurewebsites.net/mcp/bim

---

## Logging

Both App Services use a human-readable text log format. Logs are
viewable in real time via:
Azure Portal → autodesk-agent-{env} → Log stream

Log format:
HH:MM:SS LEVEL [request_id] logger_name | message

Each tool call generates a correlated set of log lines sharing a single
request ID, making it easy to trace a request end-to-end:
[abc12345] main   | TOOL list_hubs | params: {}
[abc12345] auth       | TOKEN cache hit (expires in 3120s)
[abc12345] api        | API GET /project/v1/hubs | 200 | 0.84s
[abc12345] main   | TOOL list_hubs | completed in 0.91s

To grep all log lines for a single request, search by the request ID.

---

## Common Operational Tasks

### Restart an App Service
Azure Portal → autodesk-agent-{env} → Overview → Restart. Wait 60 seconds
and verify /health returns 200.

### Rotate an API key
1. Generate new key: `python -c "import secrets; print(secrets.token_hex(32))"`
2. Update `MCP_API_KEY` in Azure environment variables
3. Apply (App Service restarts automatically)
4. Update consumers:
   - **Production:** update each of the three Power Platform custom connectors → Security → API key
   - **Development:** update Claude Desktop config and restart Claude Desktop
5. Update password manager entry

### Deploy a hotfix to production
1. Branch from `main` (do not commit directly to main)
2. Make the fix on a hotfix branch
3. Test against `autodesk-agent-dev` first by merging to dev
4. Once verified on dev, merge to main
5. Watch GitHub Actions for green deploy
6. Verify /health on prod
7. Smoke test via Copilot Studio

### Roll back production
git checkout main
git reset --hard HEAD~1
git push --force-with-lease origin main
Watch GitHub Actions, then verify /health.

---

## Known Limitations

- **3LO does not work in Copilot Studio.** The session model is
  incompatible with how MCP sessions persist across requests. Production
  users authenticate via the service account; admin actions are
  attributed to the service account in the ACC audit log. Migration to
  Microsoft Entra ID integration is on the roadmap.

- **The 3D viewer (preview_model tool) only works in Claude Desktop.**
  Copilot Studio cannot render the inline viewer. Production users see a
  fallback message if they attempt to use this tool.

- **`replicate_folders` is sequential.** One HTTP call per folder.
  Parallelization deferred — typical folder structures are small enough
  that this is acceptable.

---

## Support and Escalation

| Issue Type | First Action | Escalation |
|---|---|---|
| Production agent returning errors | Check Azure log stream for autodesk-agent-prod | Marvel Tiyjudy |
| Copilot Studio agent not routing | Verify Work IQ is disabled on all 4 prod agents | Marvel Tiyjudy |
| Authentication failures | Verify MCP_API_KEY matches between Azure and Power Platform | Marvel Tiyjudy |
| Autodesk API errors | Check APS app status at https://aps.autodesk.com | Autodesk support |
| Azure infrastructure issues | Azure Portal → autodesk-agent-prod → Diagnose and solve problems | TBI IT |

---

## Roadmap

Items planned but not yet implemented:

### Experimental Environment

A third Azure App Service (`autodesk-agent-experimental`) on its own
`experimental` git branch will host risky features that aren't ready for
production. Initial focus: 3-legged OAuth (3LO) and Microsoft Entra ID
integration. The current `experimental` git branch already contains
working 3LO code (21 tools) — it just needs an App Service to deploy to.

### 3LO and Entra ID Integration

- **Persistent token storage** via Azure Table Storage (no more in-memory dicts)
- **Microsoft Entra ID integration** for proper user attribution in Copilot Studio
- **Hybrid auth routing** — internal TBI users via Entra SSO, external collaborators via 3LO

### Other planned improvements

- **Bidirectional visual overrides** in Claude Desktop viewer (isolation commands)
- **Parallelize `replicate_folders`** for very large folder structures