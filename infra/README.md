# Kardiaflow Infrastructure Deployment Guide

This folder contains the infrastructure-as-code (IaC) scripts for deploying and
tearing down the minimal Kardiaflow development environment in Azure using Bicep
and the Azure CLI.



### What It Deploys

- Azure Resource Group (`kardia-rg-dev`)
- Azure Data Lake Storage Gen2 account (`kardiaadlsdemo`) with container (`raw`)
- Azure Databricks Workspace (`kardia-dbx`) with managed resource group

Designed for local development and demos. No NAT Gateway, no Unity Catalog, no VNet injection, no Key Vault.



## Deploy Instructions

Run these from your local terminal in the project root. Make sure you're logged into the correct Azure subscription.

**1. Load environment variables from .env**

```bash
source infra/.env
```

---

**2. Create the Azure resource group**

```bash
az group create --name "$RG" --location eastus
```

---

**3.  Deploy infrastructure with Bicep (Databricks + ADLS)**

```bash
az deployment group create \
  --resource-group "$RG" \
  --template-file infra/bicep/deploy.bicep
  --name "$DEPLOY"
```

---

**4. Generate a Databricks Personal Access Token (PAT)**

(Databricks UI → Settings → Developer → Generate New Token)

---

**5. Add your PAT to.env as `DATABRICKS_PAT=...`**

---

**6. Run gen_sas.sh to auto-generate and store the ADLS SAS token in Databricks**

```bash
infra/deploy/gen_sas.sh
```

---

**7. Build and push the kflow wheel to the workspace**

This will build the wheel using pyproject.toml and upload it to /Workspace/Shared/libs/ in the Databricks Workspace.

```bash
infra/deploy/build_push_kflow.sh
```

---

**8. Attach the wheel to your Databricks cluster**

(Compute → Cluster → Libraries → Install → /Shared/libs/kflow-0.1.0-py3-none-any.whl)

---

**9. Tear down all provisioned resources safely**

```bash
./infra/deploy/teardown.sh
```

The teardown script script will:

- Delete the Databricks workspace (which deletes the managed RG too)
- Delete the main resource group (kardia-rg-dev)
- Print a confirmation message
- Resources will disappear over the next 2–5 minutes.

---

### Dry-Run Deployment

To preview what the deployment will do without actually creating resources:

```bash
az deployment group what-if \
  --resource-group "$RG" \
  --template-file infra/bicep/deploy.bicep \
  --name "$DEPLOY"
```