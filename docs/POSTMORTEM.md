# POSTMORTEM.md

## Incident Summary

In May 2025, an early version of the Kardiaflow project was deployed to Azure as part of a simulated healthcare data engineering pipeline. This version provisioned persistent infrastructure including Azure Data Lake Storage Gen2 (ADLS), Azure Data Factory (ADF), Databricks, and network-bound services such as a Self-Hosted Integration Runtime (SHIR). 

Over the course of several development iterations, the project accrued over **$250 USD in unexpected Azure charges**—despite only handling synthetic CSV files and running short-term tests.

---

## Root Causes

### 1. ADLS Gen2 Transaction Costs
- Frequent partitioned overwrite operations triggered **excessive write transactions**, especially during PySpark `overwrite` writes to partitioned directories.
- ADLS billed **per operation**, not per GB, leading to hundreds of dollars in transaction charges even on tiny 
  datasets.

### 2. NAT Gateway Idle Billing (via SHIR)
- Creating a SHIR VM to support ADF resulted in the **silent deployment of NAT Gateways** for outbound traffic.
- NATs billed per hour whether traffic existed or not. After just a few days, idle charges exceeded **$20**.

### 3. Missing Teardown Controls
- No teardown automation was in place. Forgotten or stranded RGs and workspaces quietly continued incurring costs.

### 4. Lack of Budget Alerts
- No Azure cost budget or alerts were configured. There was no early warning before costs escalated.

---

## Impact

| Area                  | Outcome                                     |
|-----------------------|---------------------------------------------|
| Azure billing         | ~$250 in unexpected charges across services |
| Resource hygiene      | Multiple RGs required manual cleanup        |
| Time loss             | ~8 hours spent with Azure Support           |
| Project progress      | Paused and reset to avoid further cost risk |

---

## Resolution

I destroyed all existing RGs and terminated the Azure subscription to prevent further leakage. I saved all scripts and synthetic datasets locally. A new Azure subscription was created with zero resources provisioned.

I then rearchitected Kardiaflow from the ground up with **cost hygiene and teardown safety as first-class goals**.

---

## Redesign Strategy: “Safe Mode Data Engineering”

The new Kardiaflow project is designed around several non-negotiable constraints:

| Constraint                        | Implementation                                                                 |
|----------------------------------|--------------------------------------------------------------------------------|
| Fully disposable infrastructure  | All resources exist inside a single Azure RG, created and destroyed per run    |
| Bicep-defined IaC                | Bicep script provisions only essential services (RG, ADF, Databricks, Key Vault)|
| Cost alerting                    | Budget cap ($2 soft limit) with email/SMS alerts                              |
| No cost-trap services            | No ADLS, no SHIR, no VNet, no UC, no Access Connector                          |
| DBFS-only data staging           | Synthetic CSVs loaded directly into Databricks FileStore (no ADLS transactions)|
| Single-pass Spark jobs           | 1-node cluster, 10-min termination, <1¢ per job run                            |
| GitHub Actions CI/CD             | One-click end-to-end demo run + teardown + cost audit                         |
| Cost snapshot after each run     | `az consumption usage list > run_cost.json` stored per pipeline execution     |

---

## Outcome

This postmortem and the resulting architecture show not just the ability to build data engineering systems—but the ability to run them safely in cloud environments where **cost is a risk surface**.

Kardiaflow now simulates:
- Healthcare-grade ingestion and PHI masking
- Full infra-as-code provisioning
- Safe teardown
- Cost transparency
- CI-driven reproducibility

All within a repeatable, sub-$1 budget envelope.

---

## Lessons Learned

| Lesson | Takeaway |
|--------|----------|
| Cost control must be proactive | Azure’s billing model requires defensive defaults and budgeting before deploying. |
| Infra must be disposable | If you can’t safely delete it, you don’t own it — it owns you. |
| Simulations can leak | Even synthetic data + eval services can trigger real billing consequences. |
| Design from failure | A failed pipeline is more valuable than a safe one that never taught you anything. |

---

**Author**: Matthew Tripodi  
**Date**: June 2025  
