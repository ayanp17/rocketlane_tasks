#ALL PROJECTS
# ---------------- CONFIG ----------------
API_KEY = "rl-8ecc0504-98fd-4a3d-870e-813bcece8ab6"
BASE_URL = "https://api.rocketlane.com/api/1.0/projects"
PAGE_SIZE = 100
BQ_TABLE = "sd-product-analytics.rocketlane.projects"

os.environ['GOOGLE_CLOUD_PROJECT'] = 'sd-product-analytics'

# ---------------- PIPELINE ----------------
try:
    all_projects = []
    next_page_token = None

    # -------- API FETCH --------
    while True:
        params = {
            "pageSize": PAGE_SIZE,
            "includeAllFields": True
        }

        if next_page_token:
            params["pageToken"] = next_page_token

        headers = {
            "Accept": "application/json",
            "api-key": API_KEY
        }

        try:
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as api_error:
            raise Exception(f"API Error: {api_error}")

        projects = data.get("data", [])
        all_projects.extend(projects)

        pagination = data.get("pagination", {})
        if pagination.get("hasMore"):
            next_page_token = pagination.get("nextPageToken")
        else:
            break

    if len(all_projects) == 0:
        raise Exception("No projects fetched from API")

    df_projects = pd.DataFrame(all_projects)

    # -------- COLUMN PROCESSING --------
    df_projects['companyName'] = df_projects['customer'].apply(
        lambda x: x['companyName'] if isinstance(x, dict) else None
    )

    columns_to_convert = [
        'customer', 'partnerCompanies', 'createdBy', 'updatedBy',
        'currency', 'financials', 'owner', 'teamMembers',
        'fields', 'status', 'sources', 'currentPhases'
    ]
    df_projects[columns_to_convert] = df_projects[columns_to_convert].astype(str)

    df_projects['fields'] = df_projects['fields'].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )

    def extract_field(fields, target_label, as_int=False):
        for item in fields:
            if item.get('fieldLabel') == target_label:
                val = item.get('fieldValue')
                if val is not None:
                    if as_int:
                        return str(int(float(val)))
                    else:
                        return str(val)
                else:
                    return None
        return None

    df_projects['HS_Deal_ID'] = df_projects['fields'].apply(
        lambda f: extract_field(f, 'HS Deal ID', as_int=True)
    )
    df_projects['Workspace_ID'] = df_projects['fields'].apply(
        lambda f: extract_field(f, 'Workspace ID', as_int=True)
    )
    df_projects['Cluster_ID'] = df_projects['fields'].apply(
        lambda f: extract_field(f, 'Cluster ID', as_int=False)
    )
    df_projects['Region'] = df_projects['fields'].apply(
        lambda f: extract_field(f, 'Region', as_int=False)
    )

    df_projects['fields'] = df_projects['fields'].apply(
        lambda x: json.dumps(x) if x is not None else ''
    )

    # -------- BIGQUERY LOAD --------
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("projectId", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("projectName", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("startDate", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("dueDate", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("createdAt", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("updatedAt", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("owner", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("teamMembers", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("status", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("fields", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("customer", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("partnerCompanies", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("archived", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("visibility", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("createdBy", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("updatedBy", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("currency", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("financials", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("startDateActual", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("annualizedRecurringRevenue", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("percentageBudgetedHoursConsumed", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("percentageBudgetConsumed", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("trackedHours", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("trackedMinutes", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("allocatedHours", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("allocatedMinutes", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("billableHours", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("billableMinutes", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("nonBillableHours", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("nonBillableMinutes", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("remainingHours", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("remainingMinutes", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("progressPercentage", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("currentPhases", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("autoAllocation", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("plannedDurationInDays", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("projectAgeInDays", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("customersInvited", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("customersJoined", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("sources", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("projectFee", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("budgetedHours", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("companyName", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("HS_Deal_ID", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df_projects, BQ_TABLE, job_config=job_config)
    job.result()

    print(f"Pipeline successful: {len(df_projects)} projects fetched and loaded into BigQuery.")

except Exception as e:
    print(f"Pipeline failed with error: {str(e)}", file=stderr)


# INCREMENTAL TASKS
# ---------------- CONFIG ----------------
API_KEY = "rl-8ecc0504-98fd-4a3d-870e-813bcece8ab6"
BASE_URL = "https://api.rocketlane.com/api/1.0/tasks"
BQ_TABLE = "sd-product-analytics.rocketlane.tasks"
PAGE_SIZE = 100

client = bigquery.Client()

HEADERS = {
    "accept": "application/json",
    "api-key": API_KEY
}

# ---------------- HELPERS ----------------

def get_existing_tasks_from_bq():
    try:
        query = f"SELECT * FROM `{BQ_TABLE}`"
        return client.query(query).to_dataframe()
    except Exception as e:
        raise Exception(f"Failed to fetch existing BigQuery data: {e}")


def get_last_updated_at_epoch_ms(df_existing):
    try:
        if df_existing.empty:
            return 0
        return int(pd.to_datetime(df_existing["updatedAt"].max()).timestamp() * 1000)
    except Exception as e:
        raise Exception(f"Failed to compute last updatedAt: {e}")


def fetch_tasks_updated_after(last_updated_epoch_ms):
    try:
        next_page_token = None
        all_tasks = []
        seen_tokens = set()

        while True:
            params = {
                "pageSize": PAGE_SIZE,
                "task.updatedAt.gt": last_updated_epoch_ms,
                "match": "all"
            }

            if next_page_token:
                params["pageToken"] = next_page_token

            response = requests.get(BASE_URL, headers=HEADERS, params=params)
            response.raise_for_status()

            data = response.json()
            tasks = data.get("data", [])

            if not tasks:
                break

            all_tasks.extend(tasks)

            pagination = data.get("pagination", {})
            next_token = pagination.get("nextPageToken")
            has_more = pagination.get("hasMore")

            if not has_more or not next_token:
                break

            if next_token in seen_tokens:
                break

            seen_tokens.add(next_token)
            next_page_token = next_token

        return pd.DataFrame(all_tasks)

    except Exception as e:
        raise Exception(f"Failed while fetching API data: {e}")


def convert_epoch_columns(df):
    try:
        for col in ["createdAt", "updatedAt"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], unit="ms", utc=True, errors="coerce")
        return df
    except Exception as e:
        raise Exception(f"Failed converting epoch columns: {e}")


def fix_date_columns(df):
    try:
        date_cols = ["startDate", "startDateActual", "dueDate"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        return df
    except Exception as e:
        raise Exception(f"Failed fixing date columns: {e}")


# ---------------- FLATTENING PIPELINE ----------------

def apply_full_flattening_pipeline(df):
    try:
        json_cols = ["assignees", "followers", "dependencies", "parent", "fields"]

        def normalize_json(val):
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except Exception:
                    return None
            return val

        for col in json_cols:
            if col in df.columns:
                df[col] = df[col].apply(normalize_json)

        def parse_assignees(val):
            if not isinstance(val, dict):
                return [], [], []

            members = val.get("members", [])
            placeholders = val.get("placeholders", [])

            member_emails = [m.get("emailId") for m in members if m.get("emailId")]
            member_names = [
                f"{m.get('firstName','')} {m.get('lastName','')}".strip()
                for m in members
                if m.get("firstName") or m.get("lastName")
            ]
            placeholder_names = [p.get("placeholderName") for p in placeholders if p.get("placeholderName")]

            return member_emails, member_names, placeholder_names

        def parse_followers(val):
            if not isinstance(val, dict):
                return []
            members = val.get("members", [])
            return [m.get("emailId") for m in members if m.get("emailId")]

        def safe_join(x):
            if not isinstance(x, list):
                return None
            cleaned = [i for i in x if i is not None and pd.notna(i)]
            return ", ".join(cleaned) if cleaned else None

        if "project" in df.columns:
            df["project_id"] = df["project"].apply(lambda x: x.get("projectId") if isinstance(x, dict) else None)
            df["project_name"] = df["project"].apply(lambda x: x.get("projectName") if isinstance(x, dict) else None)

        if "status" in df.columns:
            df["task_status"] = df["status"].apply(lambda x: x.get("label") if isinstance(x, dict) else None)

        if "phase" in df.columns:
            df["task_phase"] = df["phase"].apply(lambda x: x.get("phaseName") if isinstance(x, dict) else None)
        else:
            df["task_phase"] = None

        if "createdBy" in df.columns:
            df["createdByEmail"] = df["createdBy"].apply(lambda x: x.get("emailId") if isinstance(x, dict) else None)
            df["createdByName"] = df["createdBy"].apply(
                lambda x: f"{x.get('firstName','')} {x.get('lastName','')}".strip() if isinstance(x, dict) else None
            )

        if "updatedBy" in df.columns:
            df["updatedByEmail"] = df["updatedBy"].apply(lambda x: x.get("emailId") if isinstance(x, dict) else None)
            df["updatedByName"] = df["updatedBy"].apply(
                lambda x: f"{x.get('firstName','')} {x.get('lastName','')}".strip() if isinstance(x, dict) else None
            )

        if "assignees" in df.columns:
            df[["assigneeEmails", "assigneeNames", "assigneePlaceholders"]] = df["assignees"].apply(
                lambda x: pd.Series(parse_assignees(x))
            )
            df["assigneeEmails"] = df["assigneeEmails"].apply(safe_join)
            df["assigneeNames"] = df["assigneeNames"].apply(safe_join)
            df["assigneePlaceholders"] = df["assigneePlaceholders"].apply(safe_join)

        if "followers" in df.columns:
            df["followerEmails"] = df["followers"].apply(parse_followers)
            df["followerEmails"] = df["followerEmails"].apply(safe_join)

        if "parent" in df.columns:
            df["parentTaskName"] = df["parent"].apply(lambda x: x.get("taskName") if isinstance(x, dict) else None)

        if "dependencies" in df.columns:
            df["dependencyTaskNames"] = df["dependencies"].apply(
                lambda x: [d.get("taskName") for d in x] if isinstance(x, list) else []
            )
            df["dependencyTaskNames"] = df["dependencyTaskNames"].apply(safe_join)

        if "fields" in df.columns:
            df["fields"] = df["fields"].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None
            )

        df_flat = df.drop(columns=[
            "project","status","phase",
            "createdBy","updatedBy",
            "assignees","followers",
            "dependencies","parent"
        ], errors="ignore")

        return df_flat

    except Exception as e:
        raise Exception(f"Flattening pipeline failed: {e}")


# ---------------- DEDUP ----------------

def deduplicate_latest(df_combined):
    try:
        df_combined["taskId"] = df_combined["taskId"].astype(str)
        df_combined["updatedAt"] = pd.to_datetime(df_combined["updatedAt"])

        df_combined = df_combined.sort_values("updatedAt", ascending=False)
        df_latest = df_combined.drop_duplicates(subset=["taskId"], keep="first")

        return df_latest
    except Exception as e:
        raise Exception(f"Deduplication failed: {e}")


# ---------------- LOAD ----------------

def truncate_and_load(df):
    try:
        job = client.load_table_from_dataframe(
            df,
            BQ_TABLE,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"
            )
        )
        job.result()
    except Exception as e:
        raise Exception(f"BigQuery load failed: {e}")


# ---------------- RUN ----------------

def run_incremental_sync():
    try:
        df_existing = get_existing_tasks_from_bq()

        last_updated_epoch_ms = get_last_updated_at_epoch_ms(df_existing)

        df_new = fetch_tasks_updated_after(last_updated_epoch_ms)

        if not df_new.empty:
            df_new = convert_epoch_columns(df_new)
            df_new = fix_date_columns(df_new)
            df_new = apply_full_flattening_pipeline(df_new)

        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

        df_final = deduplicate_latest(df_combined)

        truncate_and_load(df_final)

        print(f"Data fetched successfully. Added {len(df_new)} new rows in the table.")

    except Exception as e:
        raise Exception(f"Pipeline failed: {e}")


if __name__ == "__main__":
    run_incremental_sync()
