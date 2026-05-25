import json
import pandas as pd
import re
from datetime import datetime
 
SOURCE_FILE = "trlbee_site_patient.json"
TARGET_FILE = "trlbee_site_patient_$.json"
MAPPING_FILE = "trialbee_mapping.xlsx"

OUTPUT_FILE = "comparison_output.json"

 
def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
 
 
def get_first_record(wrapper_json):
 
    first_key = list(wrapper_json.keys())[0]
    return wrapper_json[first_key][0]

 
def normalize(val):
 
    if val in [None, "", [], {}, "null", "None"]:
        return None
 
    if isinstance(val, bool):
        return str(val).lower()
 
    if isinstance(val, (int, float)):
        return str(val)
 
    val = str(val).strip()

    try:
        if "T" in val:
            return datetime.fromisoformat(
                val.replace("Z", "+00:00")
            ).isoformat()
    except:
        pass
 
    return val
 
 
def parse_answer_value(val):
    """
    Converts:
    {"yes": true} -> yes
    {"white": true} -> white
    """
 
    if val is None:
        return None
 
    if not isinstance(val, str):
        return val
 
    val = val.strip()
 
    try:
 
        parsed = json.loads(val)
 
        if isinstance(parsed, dict):
 
            for k, v in parsed.items():
                if v is True:
                    return k
 
            return parsed
 
        return parsed
 
    except:
        return val
 
 
def build_question_lookup(data):
 
    lookup = {}
 
    sections = [
        "manuscript",
        "selfAssessment"
    ]
 
    for section in sections:
 
        questions = (
            data.get(section, {})
            .get("questions", [])
        )
 
        for q in questions:
 
            q_key = (
                q.get("question", {})
                .get("key")
            )
 
            answer_obj = q.get("answer")
 
            answer = None
 
            if answer_obj:
                answer = answer_obj.get("answer")
 
            lookup[q_key] = parse_answer_value(answer)
 
    return lookup
 
 
def deep_get(data, tokens):
 
    current = data
 
    try:
 
        for token in tokens:
 
            if current is None:
                return None
 
            if isinstance(token, int):
 
                current = current[token]
 
            else:
 
                if isinstance(current, dict):
                    current = current.get(token)
                else:
                    return None
 
        return current
 
    except:
        return None
 
def load_recruitment_mapping(file_path):
 
    df = pd.read_excel(file_path, sheet_name="recruitment_status")
 
    df.columns = [
        "current_status",
        "current_sub_status",
        "recruitment_status"
    ]
 
    mapping = {}
 
    for _, row in df.iterrows():
 
        key = (
            str(row["current_status"]).strip(),
            str(row["current_sub_status"]).strip()
        )
 
        mapping[key] = row["recruitment_status"]
 
    return mapping

recruitment_mapping = load_recruitment_mapping("trialbee_mapping.xlsx")

def get_recruitment_status(source_data, mapping_dict):
 
    current_status = (
        source_data.get("currentStatus", {})
        .get("type")
    )
 
    current_sub_status = (
        source_data.get("currentStatus", {})
        .get("label")
    )
 
    if current_sub_status is None:
        current_sub_status = "NULL"
 
    return mapping_dict.get(
        (current_status, current_sub_status)
    )
 
def evaluate_mapping_expression(expr, source_data, question_lookup):
 
    if expr is None:
        return None
 
    expr = str(expr).strip()
 
    if expr.lower() == "nan":
        return None
 
 
    if expr.lower() == "default value - trialbee":
        return "Trialbee"
 
    if expr.lower() == "autofill as trialbee":
        return "Trialbee"
 
    if expr.lower() == "as per maping provided":
        return get_recruitment_status(source_data, recruitment_mapping)
 
 
    if " else " in expr and "==" in expr:
 
        try:
 
            parts = expr.split(" else ")
 
            left_part = parts[0].strip()
 
            compare_parts = left_part.split(",")
 
            condition_part = compare_parts[0].strip()
 
            true_value = (
                compare_parts[1]
                .strip()
                .strip("'")
                .strip('"')
            )
 
            left_expr, expected = condition_part.split("==")
 
            left_expr = left_expr.strip()
 
            expected = (
                expected
                .strip()
                .strip("'")
                .strip('"')
            )
 
            actual = evaluate_mapping_expression(
                left_expr,
                source_data,
                question_lookup
            )
 
            if str(actual) == expected:
                return true_value
 
            return None
 
        except:
            return None
 

 
    q_match = re.search(
        r"\[key\]\s*==\s*'([^']+)'",
        expr
    )
 
    if q_match:
 
        q_key = q_match.group(1)
 
        return question_lookup.get(q_key)

 
    expr = re.sub(
        r"\.get\(['\"]([^'\"]+)['\"]\)",
        r"['\1']",
        expr
    )

 
    raw_tokens = re.findall(r"\[([^\]]+)\]", expr)
 
    tokens = []
 
    for token in raw_tokens:
 
        token = token.strip()
 
        token = token.strip("'").strip('"')
 
        # ignore helper keywords
        if token.lower() in [
            "question",
            "questions",
            "answer",
            "key"
        ]:
            continue
 
        # numeric indexes
        if token.isdigit():
            tokens.append(int(token))
        else:
            tokens.append(token)
 
    value = deep_get(source_data, tokens)
 
    return parse_answer_value(value)
 

 
source_wrapper = load_json(SOURCE_FILE)
target_wrapper = load_json(TARGET_FILE)
 
source_record = get_first_record(source_wrapper)
target_record = get_first_record(target_wrapper)
 
# nested source json
source_json = json.loads(source_record["json_data"])
 
# merged object
source_data = {
    **source_record,
    **source_json
}
 
# build question answer lookup
question_lookup = build_question_lookup(source_json)
 
 
 
if MAPPING_FILE.endswith(".csv"):
    mapping_df = pd.read_csv(MAPPING_FILE)
else:
    mapping_df = pd.read_excel(MAPPING_FILE)
 
# assumes first column = target column
# second column = mapping expression
 
target_col_name = mapping_df.columns[0]
mapping_col_name = mapping_df.columns[1]
 
 
 
results = []
 
for _, row in mapping_df.iterrows():
 
    target_column = str(row[target_col_name]).strip()
 
    mapping_expr = str(row[mapping_col_name]).strip()
 
    source_value = evaluate_mapping_expression(
        mapping_expr,
        source_data,
        question_lookup
    )
 
    target_value = target_record.get(target_column)
 
    match = (
        normalize(source_value)
        ==
        normalize(target_value)
    )
 
    results.append({
        "personid": source_record.get("personid"),
        "target_column": target_column,
        "mapping_expression": mapping_expr,
        "source_value": source_value,
        "target_value": target_value,
        "match": match
    })
 
 
matched = sum(r["match"] for r in results)
 
summary = {
    "total_fields": len(results),
    "matched": matched,
    "not_matched": len(results) - matched
}
 

 
final_output = {
    "personid": source_record.get("personid"),
    "summary": summary,
    "results": results
}

 
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(final_output, f, indent=4, ensure_ascii=False)

print("Comparison Completed")
print(f"Output File : {OUTPUT_FILE}")
print(f"Matched     : {matched}")
print(f"Not Matched : {len(results) - matched}")