# demo-ontology-energy

Sample **medallion (bronze / silver / gold)** analytics project for the Schneider
Electric energy domain, deployed to **Microsoft Fabric** from CI/CD using a
**service principal** (non-interactive auth). Secrets are managed by GitHub Actions
and never stored in the repository.

## Architecture

```mermaid
flowchart LR
    NB1[nb_seed_dimensions] --> B
    NB2[nb_seed_facts] --> B
    B[🥉 lh_bronze<br/>raw, as-ingested] -->|pl_bronze_to_silver| S[🥈 lh_silver<br/>cleaned, conformed]
    S -->|pl_silver_to_gold| G[🥇 lh_gold<br/>aggregated KPIs]
    G --> BI[Power BI / analytics]
```

Data is **generated** by manually-run notebooks and **transformed** by data
pipelines — all running inside Fabric. The structure (lakehouses, empty tables,
notebooks and pipelines) is deployed from CI/CD with a service principal.

Each medallion layer is a separate Fabric **lakehouse**:

| Lakehouse | Layer | Role |
|-----------|-------|------|
| `lh_bronze` | Bronze | Raw source extracts, as-ingested (string-typed + ingest metadata) |
| `lh_silver` | Silver | Cleaned, type-cast, deduplicated, conformed star model |
| `lh_gold`   | Gold   | Business-ready aggregates & KPIs for reporting |

## Data model

### 🥉 Bronze — raw landing zone
Raw tables mirror the source extracts. **All business columns are `string`**, plus
ingestion metadata (`ingested_at`, `source_file`). Immutable and reprocessable.

| Table | Business columns |
|-------|------------------|
| `raw_site` | site_id, site_name, country, region, latitude, longitude, site_type, contracted_power_kw, commissioned_date |
| `raw_device` | device_id, site_id, device_type, model, measurement_unit, install_location, is_active |
| `raw_meter_readings` | reading_id, device_id, site_id, timestamp, active_power_kw, energy_kwh, voltage_v, current_a, power_factor |
| `raw_billing` | cost_id, site_id, billing_period, energy_consumed_kwh, peak_demand_kw, tariff_rate, energy_cost, co2_emissions_kg, currency |

### 🥈 Silver — conformed star model
Cleaned and correctly typed. Three dimensions (region, site, device) and two facts.

**`dim_region`** — geographic regions (surrogate key from raw_site regions)
| Column | Type |
|--------|------|
| region_id (PK) | string |
| region_code | string |
| region_name | string |

**`dim_site`** — monitored facilities
| Column | Type |
|--------|------|
| site_id (PK) | string |
| site_name | string |
| country | string |
| region | string |
| region_id (FK → dim_region) | string |
| latitude | double |
| longitude | double |
| site_type | string |
| contracted_power_kw | double |
| commissioned_date | date |

**`dim_device`** — meters & sensors (e.g. PowerTag, PM8000)
| Column | Type |
|--------|------|
| device_id (PK) | string |
| site_id (FK → dim_site) | string |
| device_type | string |
| model | string |
| measurement_unit | string |
| install_location | string |
| is_active | boolean |

**`fact_energy_consumption`** — time-series meter readings *(partitioned by `reading_date`)*
| Column | Type |
|--------|------|
| reading_id (PK) | string |
| device_id (FK → dim_device) | string |
| site_id (FK → dim_site) | string |
| timestamp | timestamp |
| active_power_kw | double |
| energy_kwh | double |
| voltage_v | double |
| current_a | double |
| power_factor | double |
| reading_date | date |

**`fact_energy_cost`** — billing / tariff facts
| Column | Type |
|--------|------|
| cost_id (PK) | string |
| site_id (FK → dim_site) | string |
| billing_period | string |
| energy_consumed_kwh | double |
| peak_demand_kw | double |
| tariff_rate | double |
| energy_cost | double |
| co2_emissions_kg | double |
| currency | string |

### 🥇 Gold — aggregates & KPIs

**`agg_daily_consumption_by_site`** — daily consumption rollup
| Column | Type | Source |
|--------|------|--------|
| site_id | string | group key |
| reading_date | date | group key |
| total_energy_kwh | double | sum(energy_kwh) |
| avg_active_power_kw | double | avg(active_power_kw) |
| avg_power_factor | double | avg(power_factor) |
| reading_count | long | count(*) |

**`kpi_co2_by_region`** — sustainability & cost KPI by region
| Column | Type | Source |
|--------|------|--------|
| region_id | string | dim_site.region_id (FK → dim_region) |
| region | string | dim_site.region |
| billing_period | string | group key |
| total_co2_kg | double | sum(co2_emissions_kg) |
| total_energy_kwh | double | sum(energy_consumed_kwh) |
| total_cost | double | sum(energy_cost) |

## Transformation lineage

| From (Silver) | To (Gold) | Logic |
|---------------|-----------|-------|
| `fact_energy_consumption` | `agg_daily_consumption_by_site` | group by `site_id`, `reading_date`; sum/avg/count |
| `fact_energy_cost` + `dim_site` | `kpi_co2_by_region` | join on `site_id`; group by `region_id`, `region`, `billing_period`; sum CO₂/energy/cost |

Bronze → Silver applies type casting, `reading_date` derivation, null filtering on
keys, and deduplication on business keys.

## Ontology (Fabric IQ)

A **Fabric IQ Ontology** item is deployed on top of the medallion. It semantically
models the business entities and relationships and **binds** them to the silver
lakehouse tables (via the lakehouse SQL endpoint) — **no data is copied**. The
deploy source is [`ontology/energy_ontology.json`](ontology/energy_ontology.json),
turned into a Fabric ontology definition by
[`src/deploy_ontology.py`](src/deploy_ontology.py) and created via the
`/workspaces/{id}/ontologies` REST API.

```mermaid
classDiagram
    class Region
    class Site
    class Device
    class EnergyReading
    class BillingRecord
    class Supplier
    class MaintenanceEvent
    Site "*" --> "1" Region : locatedIn
    Site "1" --> "*" Device : hasDevice
    Device "1" --> "*" EnergyReading : produces
    Site "1" --> "*" BillingRecord : billedFor
    Device "1" --> "*" MaintenanceEvent : hasMaintenance
    MaintenanceEvent "*" --> "1" Supplier : performedBy
```

> **Cross-source unification.** `Supplier` and `MaintenanceEvent` come from a
> **second source system** (asset maintenance) conformed into silver as
> `dim_supplier` / `maintenance_event`. The ontology stitches them to the energy
> star schema via `Device → MaintenanceEvent → Supplier`, so you can traverse from
> energy/emissions data into maintenance and supplier data in a single semantic
> graph — something a single semantic model over one star schema cannot do.

**Entity types → bound silver table:**

| Entity type | Bound table (dbo) | Key |
|-------------|-------------------|-----|
| Region | `dim_region` | region_id |
| Site | `dim_site` | site_id |
| Device | `dim_device` | device_id |
| EnergyReading | `fact_energy_consumption` | reading_id |
| BillingRecord | `fact_energy_cost` | cost_id |
| Supplier | `dim_supplier` | supplier_id |
| MaintenanceEvent | `maintenance_event` | event_id |

**Relationship types (contextualized on silver tables):**

| Relationship | Source → Target | Binding table (keys) |
|--------------|-----------------|----------------------|
| `locatedIn` | Site → Region | `dim_site` (site_id → region_id) |
| `hasDevice` | Site → Device | `dim_device` (site_id → device_id) |
| `produces` | Device → EnergyReading | `fact_energy_consumption` (device_id → reading_id) |
| `billedFor` | Site → BillingRecord | `fact_energy_cost` (site_id → cost_id) |
| `hasMaintenance` | Device → MaintenanceEvent | `maintenance_event` (device_id → event_id) |
| `performedBy` | MaintenanceEvent → Supplier | `maintenance_event` (event_id → supplier_id) |

The ontology is deployed automatically with the rest of the structure. It requires
the **Ontology** preview to be enabled in the tenant, a Fabric capacity, and the
service principal's Contributor role. Once deployed you can browse/query it in
Fabric IQ and layer analytics or agents on top.
## Data Agent

A **Fabric Data Agent** (`energy_data_agent`) is deployed **on top of the
`energy_ontology`** item so business users can ask natural-language questions
("total energy cost by region?", "which site emits the most CO2?"). The ontology
is its data source, so answers reason over the entities and relationships (not raw
tables). It ships **AI instructions** describing the entities, relationships, units
and the per-site totals.

Built by [`src/deploy_data_agent.py`](src/deploy_data_agent.py) via the
`/workspaces/{id}/dataAgents` REST API. Requires the Data Agent preview + Fabric
Copilot/AI enabled in the tenant. Ask it questions from the Data Agent experience
in Fabric once the silver pipeline has loaded data.
## Repository layout

| Path | Purpose |
|------|---------|
| `src/auth.py` | Service-principal token for the Fabric REST API |
| `src/fabric_client.py` | Minimal REST client (auth verification) |
| `src/spark_utils.py` | Spark session authenticated to OneLake |
| `src/config.py` | Workspace / lakehouse config + OneLake paths |
| `src/schemas.py` | Per-layer Delta table schemas (`LAYER_TABLES`) |
| `src/provision_lakehouses.py` | Creates the 3 lakehouses via REST |
| `src/create_delta_tables.py` | Creates the per-layer Delta tables (empty) |
| `src/cleanup.py` | Resets lakehouse tables for a clean redeploy |
| `src/deploy_items.py` | Deploys Fabric notebooks + data pipelines via REST |
| `src/deploy_ontology.py` | Deploys the Fabric IQ Ontology item (binds to silver) |
| `src/deploy_semantic_model.py` | Deploys a Direct Lake semantic model (ontology design) |
| `src/deploy_data_agent.py` | Deploys the Fabric Data Agent (NL questions over silver) |
| `src/deploy_medallion.py` | End-to-end deployment orchestrator |
| `notebooks/nb_seed_dimensions.py` | (manual) seeds 20 sites + 100 devices into bronze |
| `notebooks/nb_seed_facts.py` | (manual, repeatable) appends >1000 readings + >1000 billing rows |
| `notebooks/nb_seed_maintenance.py` | (manual) seeds `dim_supplier` + `maintenance_event` (2nd source) into silver |
| `notebooks/nb_truncate_all.py` | (manual) deletes all rows from every table (schemas kept) |
| `notebooks/nb_bronze_to_silver.py` | bronze → silver transform (run by pipeline) |
| `notebooks/nb_silver_to_gold.py` | silver → gold transform (run by pipeline) |
| `ontology/energy_ontology.json` | Fabric IQ ontology definition (entities, relationships, bindings) |
| `.github/workflows/deploy.yml` | CI/CD deployment pipeline |

## Deployment

The GitHub Actions workflow (**Deploy Fabric medallion**) deploys the **structure
only**: it provisions the 3 lakehouses, creates the Delta tables **empty**, and
deploys the Fabric notebooks + data pipelines, and creates the **Fabric IQ
Ontology** item bound to the silver tables. It does **not** load or transform
data (that is done manually inside Fabric). Configuration:

- **Secret**: `AZURE_CLIENT_SECRET`
- **Variables**: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `FABRIC_WORKSPACE_ID`
- **Job env**: `RESET_TABLES` — default **`false`** (non-destructive: empty tables
  are created with `mode("ignore")`, so existing data is preserved). Set to
  `true` only for a full wipe-and-recreate.

The service principal requires the *"Service principals can use Fabric APIs"*
tenant setting, a **Member/Contributor** role on the workspace, and the workspace
must be on a **Fabric capacity**. Notebooks access OneLake with the workspace
identity and resolve lakehouse paths at runtime via
`notebookutils.lakehouse.get(...)`.

## Deployed Fabric artifacts

| Item | Type | Run by | Purpose |
|------|------|--------|---------|
| `nb_seed_dimensions` | Notebook | manual (once) | Create 20 sites + 100 devices in bronze |
| `nb_seed_facts` | Notebook | manual (repeatable) | Append >1000 readings + >1000 billing rows |
| `nb_seed_maintenance` | Notebook | manual (once) | Seed `dim_supplier` + `maintenance_event` (2nd source) into silver |
| `nb_bronze_to_silver` | Notebook | `pl_bronze_to_silver` | Clean / typecast / dedup bronze → silver |
| `nb_silver_to_gold` | Notebook | `pl_silver_to_gold` | Aggregate silver → gold KPIs |
| `nb_truncate_all` | Notebook | manual | Delete all rows from every table (schemas kept) |
| `pl_bronze_to_silver` | Data pipeline | manual | Runs `nb_bronze_to_silver` |
| `pl_silver_to_gold` | Data pipeline | manual | Runs `nb_silver_to_gold` |
| `energy_ontology` | Ontology (Fabric IQ) | — | Entity types + relationships bound to silver tables (incl. supplier/maintenance) |
| `energy_semantic_model` | Semantic model (Direct Lake) | — | Energy star schema over silver, with measures |
| `energy_data_agent` | Data Agent | — | Natural-language Q&A grounded on the ontology |
| `energy_semantic_data_agent` | Data Agent | — | Natural-language Q&A grounded on the semantic model |

## Running the demo (manual, inside Fabric)

After deployment the tables are empty. Load and transform data by running the
artifacts **in this order**:

1. **`nb_seed_dimensions`** — run once. Expect `raw_site written: 20 rows` and
   `raw_device written: 100 rows`.
2. **`nb_seed_facts`** — run one or more times. Each run appends >1000 random
   meter readings and >1000 billing rows, coherent with the sites/devices from
   step 1.
3. **`pl_bronze_to_silver`** pipeline — cleans/conforms bronze into silver.
4. **`pl_silver_to_gold`** pipeline — aggregates silver into gold KPIs.
5. **`nb_seed_maintenance`** — run once, **after** step 3 (needs `dim_device`).
   Seeds the 2nd source (`dim_supplier`, `maintenance_event`) into silver so the
   ontology's cross-source relationships resolve.

The **`energy_ontology`** Fabric IQ item is deployed by CI and binds directly to
the silver tables, so it reflects data as soon as the silver pipeline has run.

Order matters: each step overwrites its target from its source, so bronze must
contain data before running the pipelines.

To start over, run **`nb_truncate_all`** (empties every table, keeps schemas),
then repeat from step 1.
