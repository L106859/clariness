
import boto3
import json
import re
from datetime import datetime, timezone, timedelta
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from requests.auth import HTTPBasicAuth
import urllib3
 
# Suppress InsecureRequestWarning since we're ignoring SSL verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
 
#CONFIG
AWS_REGION = "us-east-2"
SECRET_NAME = "mulesoft"
DB_SECRET_NAME = "stamp_DB"
ABC_DB_SECRET_NAME = "jira/xray/credentials"
GLUE_JOB_NAME = "csv_data_comparison_trilok"
S3_BUCKET = "lly-edp-codeconfig-dummy"
OUTPUT_S3_KEY = "mdids-infohub/clariness_negative_output/"
FINAL_JSON_OUTPUT = "clariness_negative_output.json"
 
SCHEMA_NAME = "clariness_raw"
TABLE_CONFIG = {
    "study": "study_id",
    "site": "site_id",
    "funnels": "study_id",
    "funnel": "study_id",
    "referral":"referral_id"
}
 
POLL_INTERVAL_SECONDS = 10
MAX_WAIT_SECONDS = 7200
 
#AIRFLOW CONFIG
AIRFLOW_BASE_URL = "https://edb-airflow2-dev.aws.lilly.com:8080/api/v1"
AIRFLOW_USERNAME = "******"
AIRFLOW_PASSWORD = "******"
 
# DAG ID to trigger
DAG_ID = "execute_edb_mdids_clariness"
 
#AWS CLIENTS
secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)
glue_client = boto3.client("glue", region_name=AWS_REGION)
logs_client = boto3.client("logs", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)
 
#SECRET FUNCTIONS
def get_secret(secret_name: str):
    """
    Fetch a secret from AWS Secrets Manager and normalize keys case-insensitively.

    Args:
        secret_name: Name of the secret in AWS Secrets Manager.

    Returns:
        A dict containing host, port, dbname, user, and password values for DB use.
    """
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    secret = json.loads(resp["SecretString"])
    print("Retrieved secret successfully")
   
    # Create a case-insensitive dictionary
    secret_lower = {k.lower(): v for k, v in secret.items()}
   
    # Extract values with case-insensitive keys and convert to psycopg2 format
    return {
        "host": secret_lower.get("host"),
        "port": secret_lower.get("port", 5432),  # Default to 5432 if not provided
        "dbname": secret_lower.get("dbname"),
        "user": secret_lower.get("username") or secret_lower.get("user"),  # Handle both 'username' and 'user'
        "password": secret_lower.get("password")
    }
 
 
def update_secret(secret_name: str, secret_dict: dict):
    """
    Update a secret value in AWS Secrets Manager.

    Args:
        secret_name: Name or ARN of the Secrets Manager secret.
        secret_dict: Secret payload to store as JSON.
    """
    secrets_client.update_secret(
        SecretId=secret_name,
        SecretString=json.dumps(secret_dict)
    )
    print("Updated secret successfully")
 
 
def get_db_config(db_secret_name: str):
    """
    Fetch generic database configuration from AWS Secrets Manager.

    Args:
        db_secret_name: Name of the secret containing DB connection details.

    Returns:
        A dict with keys host, port, dbname, user, password.
    """
    try:
        secret_data = get_secret(db_secret_name)
        print(f"Retrieved database config from Secrets Manager")
        return secret_data
    except Exception as e:
        print(f"Error fetching database config: {e}")
        raise Exception(f"Failed to retrieve database configuration: {e}")
 
 
def get_abc_db_config(abc_secret_name: str):
    """
    Fetch ABC framework database configuration from AWS Secrets Manager.

    Args:
        abc_secret_name: Name of the secret containing ABC DB connection details.

    Returns:
        A dict with keys host, port, dbname, user, password.
    """
    try:
        secret_data = get_secret(abc_secret_name)
        print(f"Retrieved ABC database config from Secrets Manager")
        return secret_data
    except Exception as e:
        print(f"Error fetching ABC database config: {e}")
        raise Exception(f"Failed to retrieve ABC database configuration: {e}")
 
 
def invalidate_secret_password(secret_name: str):
    """
    Invalidate the password stored in a Secrets Manager secret.

    Args:
        secret_name: Name of the secret to update.

    Returns:
        The original password value before invalidation.
    """
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    secret_data = json.loads(resp["SecretString"])
   
    # Find password key case-insensitively
    password_key = None
    original_password = None
    for key in secret_data.keys():
        if key.lower() == "password":
            password_key = key
            original_password = secret_data[key]
            break
   
    if not original_password or not password_key:
        raise Exception("Password key not found in secret!")
   
    secret_data[password_key] = "zzz@invalid"
    update_secret(secret_name, secret_data)
    print("Secret password invalidated successfully")
    return original_password
 
 
def restore_secret_password(secret_name: str, original_password: str):
    """
    Restore the original password into a Secrets Manager secret.

    Args:
        secret_name: Name of the secret to restore.
        original_password: The original password value to write back.

    Returns:
        Success message string.
    """
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    secret_data = json.loads(resp["SecretString"])
   
    # Find password key case-insensitively
    password_key = None
    for key in secret_data.keys():
        if key.lower() == "password":
            password_key = key
            break
   
    if not password_key:
        raise Exception("Password key not found in secret!")
   
    secret_data[password_key] = original_password
    update_secret(secret_name, secret_data)
    print("Secret password restored successfully")
    return "Password restored successfully"
 
#DAG TRIGGER
def trigger_airflow_dag():
    """
    Trigger the configured Airflow DAG using the REST API.

    Returns:
        A tuple (success, result) with the DAG run response details.
    """
    url = f"{AIRFLOW_BASE_URL}/dags/{DAG_ID}/dagRuns"
    payload = {}
   
    try:
        response = requests.post(
            url,
            json=payload,
            auth=HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            verify=False,
            timeout=30
        )
       
        if response.status_code in [200, 201]:
            dag_result = {
                "status": "TRIGGERED",
                "message": f"Successfully triggered {DAG_ID}",
                "dag_id": DAG_ID
            }
            print(f"Successfully triggered {DAG_ID}")
            return True, dag_result
        else:
            dag_result = {
                "status": "FAILED",
                "message": f"Failed to trigger {DAG_ID}",
                "response": response.text
            }
            print(f"Failed to trigger {DAG_ID}")
            print(response.text)
            return False, dag_result
   
    except Exception as e:
        dag_result = {
            "status": "ERROR",
            "message": str(e),
            "dag_id": DAG_ID
        }
        print(f"Error triggering {DAG_ID}: {e}")
        return False, dag_result
 
#GLUE JOB VALIDATION
def wait_for_glue_job_completion(job_name):
    """
    Wait until Glue job completes successfully and return completion time
    """
    waited_time = 0
 
    while True:
        response = glue_client.get_job_runs(
            JobName=job_name,
            MaxResults=1
        )
 
        job_runs = response.get("JobRuns", [])
 
        if not job_runs:
            raise Exception("No Glue job runs found")
 
        latest_run = job_runs[0]
        job_state = latest_run.get("JobRunState")
 
        print(f"Glue Job State: {job_state}")
 
        # Success
        if job_state == "SUCCEEDED":
            completed_on = latest_run.get("CompletedOn")
 
            if not completed_on:
                raise Exception("CompletedOn missing")
 
            print(f"Glue Job Completed At: {completed_on}")
            return completed_on
 
        # Failure states
        elif job_state in ["FAILED", "TIMEOUT", "STOPPED"]:
            raise Exception(
                f"Glue job failed with state: {job_state}"
            )
 
        # Running states
        elif job_state in [
            "RUNNING",
            "STARTING",
            "WAITING",
            "STOPPING"
        ]:
            if waited_time >= MAX_WAIT_SECONDS:
                raise Exception(
                    "Timeout waiting for Glue job completion"
                )
 
            print(
                f"Glue job still running. "
                f"Sleeping for {POLL_INTERVAL_SECONDS} seconds..."
            )
 
            time.sleep(POLL_INTERVAL_SECONDS)
            waited_time += POLL_INTERVAL_SECONDS
 
        else:
            raise Exception(
                f"Unknown Glue job state: {job_state}"
            )
 
def extract_incident_number_from_glue_logs():
    """
    Search Glue CloudWatch error logs for an incident number.

    Returns:
        A tuple (found, incident_number_or_error) where found is True when an incident
        number was located.
    """
    log_group = "/aws-glue/jobs/error"
    incident_pattern = r"(INC\d{7,8})"
    try:
        streams_resp = logs_client.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            limit=10
        )
        for stream in streams_resp.get("logStreams", []):
            stream_name = stream["logStreamName"]
            events_resp = logs_client.get_log_events(
                logGroupName=log_group,
                logStreamName=stream_name,
                startFromHead=False,
                limit=500
            )
            for ev in events_resp.get("events", []):
                msg = ev.get("message", "")
                match = re.search(incident_pattern, msg)
                if match:
                    print(f"Found incident number in logs: {match.group(1)}")
                    return True, match.group(1)
        return False, None
    except Exception as e:
        return False, str(e)
 
 
def process_table(
    cursor,
    schema_name,
    table_name,
    primary_key_column,
    glue_end_time
):
    """
    Process a database table and find rows created or updated after Glue completion.

    Args:
        cursor: psycopg2 cursor for executing queries.
        schema_name: Database schema name.
        table_name: Database table name.
        primary_key_column: Primary key column name to report.
        glue_end_time: Datetime representing the Glue job completion time.

    Returns:
        A tuple (old_records, recent_updates) where recent_updates contains rows with
        created_utc >= glue_end_time.
    """
    print(
        f"\nProcessing Table: "
        f"{schema_name}.{table_name}"
    )
 
    query = f"""
        SELECT {primary_key_column}, created_utc
        FROM {schema_name}.{table_name}
    """
 
    cursor.execute(query)
    rows = cursor.fetchall()
    # We only consider records that were created on or after the Glue job end time.
    # Do not report older records.
    old_records = []
    recent_updates = []
 
    # Ensure glue_end_time is timezone-aware (assume UTC if not)
    if glue_end_time is not None and glue_end_time.tzinfo is None:
        glue_end_time = glue_end_time.replace(tzinfo=timezone.utc)
 
    for row in rows:
        primary_key_value = row[0]
        created_utc = row[1]
 
        if created_utc is None:
            continue
 
        if created_utc.tzinfo is None:
            created_utc = created_utc.replace(tzinfo=timezone.utc)
 
        # If the record was created on or after the Glue job end time, treat it as a recent update
        try:
            if glue_end_time is not None and created_utc >= glue_end_time:
                record_info = {
                    "table": table_name,
                    "primary_key_column": primary_key_column,
                    "primary_key_value": str(primary_key_value),
                    "created_utc": str(created_utc)
                }
                print(f"DB record updated at or after glue end time: {primary_key_value}, created_utc={created_utc}")
                recent_updates.append(record_info)
            else:
                # Older records are ignored and not reported
                continue
        except Exception:
            # In case of unexpected comparison issues, skip the row
            continue
 
    return old_records, recent_updates
 
 
def execute_step(step_name, func, *args):
    """
    Run a workflow step and normalize exceptions into a standard result.

    Args:
        step_name: Human-readable name of the step.
        func: Callable to execute.
        *args: Positional arguments passed to func.

    Returns:
        A tuple (success, result) where result contains either the return value or
        an error dict.
    """
    try:
        result = func(*args)
        return True, result
    except Exception as e:
        print(f"Error in {step_name}: {e}")
        return False, {"status": "ERROR", "error": str(e)}


def fetch_key_set_from_db(db_config, schema_name, table_name, key_column):
    """
    Fetch a set of primary key values from a database table.

    Args:
        db_config: Dict with psycopg2 connection parameters.
        schema_name: Database schema name.
        table_name: Name of the table to query.
        key_column: Column name used as the key.

    Returns:
        A set of string values representing the selected keys.
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        query = f"SELECT {key_column} FROM {schema_name}.{table_name}"
        cur.execute(query)
        keys = set()
        for row in cur:
            val = row[0]
            if val is not None:
                keys.add(str(val))
        return keys
    finally:
        if conn:
            conn.close()


def compare_key_sets(set1, set2):
    """
    Compare two sets of keys and return counts of matches and differences.

    Args:
        set1: First key set (before snapshot).
        set2: Second key set (after snapshot).

    Returns:
        Dictionary with counts for only_in_file1, only_in_file2, matched, and samples.
    """
    only_in_1 = set1 - set2
    only_in_2 = set2 - set1
    matched = set1 & set2
    return {
        "only_in_file1_count": len(only_in_1),
        "only_in_file2_count": len(only_in_2),
        "matched_count": len(matched),
        "only_in_file1_sample": list(list(only_in_1)[:10]),
        "only_in_file2_sample": list(list(only_in_2)[:10])
    }

RECENT_THRESHOLD_MINUTES = 1
 
#queries
query1 = """
select * from edb_dev_abc.t_batch_def tbd
where batch_name like 'edb-data-harmony-clariness-batch';
"""
 
query2 = """select * from edb_dev_abc.t_process_def tpd
where  batch_id = %s;
"""
 
query3 = """select * from edb_dev_abc.t_process_execution_log tpel
where process_id = %s
order by execution_start_time desc
LIMIT 1;"""
 
def verify_abc_framework(negative_test=False):
    """
    Validate the ABC framework execution status for a negative test.

    Args:
        negative_test: If True, expects the latest run to have FAILED status.

    Returns:
        A result dict containing status, message, and execution metadata.
    """
    conn = None
    results = {
        "status": "failed",
        "message": "",
        "batch_id": None,
        "process_id": None,
        "execution_start_time": None,
        "execution_status_id": None
    }
   
    try:
        # Fetch ABC database config from Secrets Manager
        abc_db_config = get_abc_db_config(ABC_DB_SECRET_NAME)
        if not abc_db_config:
            raise Exception("ABC database config is None")
       
        conn = psycopg2.connect(**abc_db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
 
        # Step 1: Fetch batch id
        cursor.execute(query1)
        batch_def = cursor.fetchone()
 
        if not batch_def or not batch_def.get('batch_id'):
            raise Exception("Batch Id not found.")
 
        batch_id = batch_def['batch_id']
        results["batch_id"] = batch_id
        #fetch process definitions for the batch
        cursor.execute(query2, (batch_id,))
        process_defs = cursor.fetchone()
 
        if not process_defs or not process_defs.get('process_id'):
            raise Exception("Process Id not found.")
 
        process_id = process_defs['process_id']
        results["process_id"] = process_id
 
        #fetch latest execution log for the process
        cursor.execute(query3, (process_id,))
        execution_log = cursor.fetchone()
 
        if not execution_log:
            raise Exception("No execution log found for the process.")
 
        execution_start_time = execution_log['execution_start_time']
        execution_status_id = execution_log['execution_status_id']
 
        results["execution_start_time"] = execution_start_time.isoformat() if execution_start_time else None
        results["execution_status_id"] = execution_status_id
 
        #validation
        if negative_test:
            if execution_status_id != 3:
                raise Exception("Latest execution status is not 'FAILED' (status_id=3) as expected for negative test.")
        else:
            if execution_status_id != 2:
                raise Exception("Latest execution status is not 'COMPLETED' (status_id=2) as expected for positive test.")
       
        if not execution_start_time:
            raise Exception("Execution start time is null.")
       
        current_time = datetime.now(timezone.utc)
 
        if execution_start_time.tzinfo is None:
            execution_start_time = execution_start_time.replace(tzinfo=timezone.utc)
 
        time_diff = current_time - timedelta(minutes=RECENT_THRESHOLD_MINUTES)
 
        if execution_start_time < time_diff:
            raise Exception(f"Latest execution start time is {execution_start_time} older than {RECENT_THRESHOLD_MINUTES} minutes.")
       
        results["status"] = "success"
        results["message"] = "ABC framework verification successful. Latest execution is recent and completed."
   
    except Exception as e:
        results["status"] = "failed"
        results["message"] = f"ABC framework verification failed: {str(e)}"
    finally:
        if conn:
            conn.close()
       
        # Skip file writing in Lambda (read-only filesystem)
        # Results are returned in lambda_handler output
        print(f"ABC verification results: {results}")
   
    return results
 
 
#MAIN FLOW
def lambda_handler(event, lambda_context):
    """
    Main Lambda handler orchestrating the negative scenario validation flow.

    Args:
        event: Lambda event payload.
        lambda_context: Lambda runtime context.

    Returns:
        A structured result dictionary describing each step outcome.
    """
    output = {
        "NegativeScenarioSuccessStatus": "FAIL",
        "IncidentNumber": None,
        "SecretInvalidation": {},
        "DAGTrigger": {},
        "GlueJobValidation": {},
        "SecretRestore": {},
        "TableProcessing": {},
        "ABCFrameworkVerification": {},
        "DbBeforeDagTrigger": 0,
        "DbAfterDagTrigger": 0,
        "DBComparison": {},
        "Discrepancies": []
    }
    original_password = None
    glue_end_time = None
    conn = None
    snapshot_before_keys = set()
   
    try:
        # Extract DB snapshot before invalidation (file1)
        try:
            db_config_tmp = get_db_config(DB_SECRET_NAME)
            snapshot_before_keys = fetch_key_set_from_db(db_config_tmp, "clariness_ref", "clariness_site_patient", "clariness_patient_id")
            output["DbBeforeDagTrigger"] = len(snapshot_before_keys)
        except Exception as e:
            snapshot_before_keys = set()
            output["DbBeforeDagTrigger"] = 0
            output["Discrepancies"].append({"type": "DBSnapshotBefore", "issue": "Failed to extract before snapshot", "details": str(e)})

        # 1. Invalidate secret password
        success, result = execute_step("SecretInvalidation", invalidate_secret_password, SECRET_NAME)
        output["SecretInvalidation"] = {"success": success, "details": "Password invalidated successfully" if success else result.get("error")}
        if success:
            original_password = result
        else:
            output["Discrepancies"].append({"type": "SecretInvalidation", "issue": "Failed to invalidate password", "details": result})
 
        # 2. Trigger Airflow DAG
        success, result = execute_step("DAGTrigger", trigger_airflow_dag)
        output["DAGTrigger"] = result
        if not success:
            output["Discrepancies"].append({"type": "DAGTrigger", "issue": "Failed to trigger DAG", "details": result})
 
        # 3. Wait for Glue job completion
        success, result = execute_step("GlueJob", wait_for_glue_job_completion, GLUE_JOB_NAME)
        output["GlueJobValidation"] = {"status": "SUCCEEDED", "end_time": str(result)} if success else {"status": "FAILED", "error": result.get("error")}
        if success:
            glue_end_time = result
        else:
            output["Discrepancies"].append({"type": "GlueJob", "issue": "Glue job failed", "details": result})
 
        # 4. Extract incident number from logs
        inc_ok, inc_data = extract_incident_number_from_glue_logs()
        output["IncidentNumber"] = inc_data if inc_ok else None
        if not inc_ok:
            output["Discrepancies"].append({"type": "IncidentNumber", "issue": "Incident number not found", "details": inc_data})
 
        # 5. Process database tables if Glue succeeded
        if glue_end_time is not None:
            try:
                db_config = get_db_config(DB_SECRET_NAME)
                conn = psycopg2.connect(**db_config)
                cursor = conn.cursor()
                all_old_records = []
                all_recent_updates = []
 
                for table_name, primary_key_column in TABLE_CONFIG.items():
                    old_recs, recent_upds = process_table(cursor, SCHEMA_NAME, table_name, primary_key_column, glue_end_time)
                    all_old_records.extend(old_recs)
                    all_recent_updates.extend(recent_upds)
 
                output["TableProcessing"] = {
                    "tables_processed": list(TABLE_CONFIG.keys()),
                    "recent_updates_count": len(all_recent_updates),
                    "recent_updates": all_recent_updates
                }
 
                # Mark as failure if recent updates found (on or after Glue job end time)
                if all_recent_updates:
                    output["Discrepancies"].append({
                        "type": "TableProcessing",
                        "issue": "DB records updated on or after Glue job end time",
                        "details": f"Found {len(all_recent_updates)} updated records on/after Glue end time"
                    })
                # Extract DB snapshot after glue run (file2) and compare
                try:
                    db_config_tmp = get_db_config(DB_SECRET_NAME)
                    snapshot_after_keys = fetch_key_set_from_db(db_config_tmp, "clariness_ref", "clariness_site_patient", "clariness_patient_id")
                    output["DbAfterDagTrigger"] = len(snapshot_after_keys)
                    comparison = compare_key_sets(snapshot_before_keys or set(), snapshot_after_keys or set())
                    output["DBComparison"] = comparison
                    if comparison.get("only_in_file1_count", 0) > 0 or comparison.get("only_in_file2_count", 0) > 0:
                        output["Discrepancies"].append({"type": "DBComparison", "issue": "Differences found between before/after snapshots", "details": comparison})
                except Exception as e:
                    output["DBComparison"] = {"status": "FAILED", "error": str(e)}
                    output["Discrepancies"].append({"type": "DBComparison", "issue": "Failed to extract after snapshot", "details": str(e)})
            except Exception as e:
                output["TableProcessing"] = {"status": "FAILED", "error": str(e)}
                output["Discrepancies"].append({"type": "TableProcessing", "issue": "Failed to process tables", "details": str(e)})
            finally:
                if conn:
                    conn.close()
 
        # 6. Verify ABC Framework
        try:
            abc_result = verify_abc_framework(negative_test=True)
            output["ABCFrameworkVerification"] = abc_result
            if abc_result.get("status") != "success":
                output["Discrepancies"].append({"type": "ABCFrameworkVerification", "issue": "ABC Framework verification failed", "details": abc_result.get("message")})
        except Exception as e:
            output["ABCFrameworkVerification"] = {"status": "failed", "error": str(e)}
            output["Discrepancies"].append({"type": "ABCFrameworkVerification", "issue": "ABC Framework verification error", "details": str(e)})
 
        # 7. Determine final status - PASS only if NO discrepancies and all steps succeed
        all_scenarios_pass = (
            output["SecretInvalidation"].get("success") and
            output["GlueJobValidation"].get("status") == "SUCCEEDED" and
            output["IncidentNumber"] is not None and
            output["ABCFrameworkVerification"].get("status") == "success" and
            len(output["Discrepancies"]) == 0
        )
        output["NegativeScenarioSuccessStatus"] = "PASS" if all_scenarios_pass else "FAIL"
 
    except Exception as e:
        print(f"Critical error in lambda_handler: {e}")
        output["Discrepancies"].append({"type": "LambdaHandler", "issue": "Critical error occurred", "details": str(e)})
        output["NegativeScenarioSuccessStatus"] = "FAIL"
   
    finally:
        # Restore secret password
        if original_password is not None:
            try:
                success, result = execute_step("SecretRestore", restore_secret_password, SECRET_NAME, original_password)
                output["SecretRestore"] = {"success": success, "details": result if success else result.get("error")}
                if not success:
                    output["Discrepancies"].append({"type": "SecretRestore", "issue": "Failed to restore password", "details": result})
            except Exception as e:
                print(f"Error restoring password: {e}")
                output["SecretRestore"] = {"success": False, "error": str(e)}
                output["NegativeScenarioSuccessStatus"] = "FAIL"
 
        # Upload output JSON directly to S3 (avoid local temp file)
        try:
            json_body = json.dumps(output, indent=4, default=str).encode("utf-8")
            if S3_BUCKET:
                s3_key = f"{OUTPUT_S3_KEY.rstrip('/')}/{FINAL_JSON_OUTPUT}"
                s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json_body)
                s3_path = f"s3://{S3_BUCKET}/{s3_key}"
                output["S3Upload"] = {"success": True, "details": {"s3_path": s3_path}}
            else:
                output["S3Upload"] = {"success": False, "details": "No S3_BUCKET configured"}
                output["Discrepancies"].append({"type": "S3Upload", "issue": "No S3 bucket configured", "details": ""})
                output["NegativeScenarioSuccessStatus"] = "FAIL"
        except Exception as e:
            output["S3Upload"] = {"success": False, "error": str(e)}
            output["Discrepancies"].append({"type": "S3Upload", "issue": "Failed to upload to S3", "details": str(e)})
            output["NegativeScenarioSuccessStatus"] = "FAIL"
 
        print(json.dumps(output, indent=4, default=str))
        return output