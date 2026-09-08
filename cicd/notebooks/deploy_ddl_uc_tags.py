# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Unity Catalog tags
# MAGIC
# MAGIC **Purpose**: This notebook deploys Unity Catalog tags based on the `uc_tags.json` configuration file.
# MAGIC
# MAGIC ## Dependencies
# MAGIC 1. Tagging will be performed only if uc_tags.json has a correct JSON structure.
# MAGIC 2. A table tag will only be deployed if target table exists and tag has non-empty value.
# MAGIC
# MAGIC ## Process
# MAGIC 1. _find_uc_tags_file -> search for uc_tags.json file in the pillar repo (path variable) 
# MAGIC 2. _parse_json_content -> parse uc_tags.json input file contents
# MAGIC 3. _apply_tags -> apply tagging (catalog is set based on the current environment)

# COMMAND ----------

dbutils.widgets.dropdown("pillar", "TEST", ["NDA", "PDA", "CEDAR", "SODA", "TEST"])

# COMMAND ----------

dbutils.widgets.dropdown("mode", "info", ["info", "execute"])

# COMMAND ----------

dbutils.widgets.dropdown("repo_name", "ESG_CDL_TEST", ["ESG_CDL_TEST", "ESG_CDL","ESG_Launchpad"])

# COMMAND ----------

import os
import json

# COMMAND ----------

pillar = dbutils.widgets.get("pillar").lower()
pillar_u = dbutils.widgets.get("pillar").upper()
repo_name = dbutils.widgets.get("repo_name")
mode = dbutils.widgets.get("mode").lower()
workspace_prefix = '/Workspace'
repo_path = f'/Repos/CodeRepo-{pillar_u}/{repo_name}/'
notebook_jobs_folder = 'notebook_jobs'
path = f'{workspace_prefix}{repo_path}{notebook_jobs_folder}{os.sep}{pillar}'

# COMMAND ----------

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
params = json.load(open("/Workspace/Repos/CodeRepo-CORE/ESG_Core/config/params.json"))

if workspace_url in params:
    notebook_params = {key: value for key, value in params[workspace_url].items() if isinstance(value, str)}
else:
    notebook_params = {}

# COMMAND ----------

MANDATORY_KEYS = {"catalog_name", "schema_name", "table_name"}


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _escape_identifier(value: str) -> str:
    return value.replace("`", "``")


def _find_uc_tags_file(search_root: str, file_name: str = "uc_tags.json") -> str:
    if not search_root:
        return ""

    if os.path.isfile(search_root):
        return search_root if os.path.basename(search_root).lower() == file_name else ""

    if not os.path.isdir(search_root):
        return ""

    for root, _, files in os.walk(search_root):
        for file in files:
            if file.lower() == file_name:
                return os.path.join(root, file)

    return ""


def _parse_json_content(raw_content: str):
	raw_text = (raw_content or "").strip()
	if not raw_text:
		raise ValueError("File uc_tags.json is empty.")

	try:
		parsed_payload = json.loads(raw_text)
		if isinstance(parsed_payload, str):
			try:
				return json.loads(parsed_payload)
			except json.JSONDecodeError:
				normalized = parsed_payload.replace('\\"', '"')
				return json.loads(normalized)
		return parsed_payload
	except json.JSONDecodeError:
		normalized = raw_text

		if (normalized.startswith('"') and normalized.endswith('"')) or (
			normalized.startswith("'") and normalized.endswith("'")
		):
			try:
				normalized = json.loads(normalized)
			except json.JSONDecodeError:
				normalized = normalized[1:-1]

		normalized = normalized.replace('\\"', '"')

		try:
			return json.loads(normalized)
		except json.JSONDecodeError as error:
			raise ValueError(f"Invalid JSON structure in uc_tags.json: {error}") from error


def _normalize_and_validate_payload(payload):
	if isinstance(payload, dict):
		payload = [payload]

	if not isinstance(payload, list):
		raise ValueError("Invalid uc_tags.json structure. Expected a JSON array of objects.")

	normalized_entries = []
	for index, entry in enumerate(payload, start=1):
		if not isinstance(entry, dict):
			raise ValueError(f"Invalid entry at position {index}. Expected key-value JSON object.")

		normalized_entry = {}
		for key in MANDATORY_KEYS:
			value = entry.get(key)
			if value is None or str(value).strip() == "":
				raise ValueError(f"Mandatory key '{key}' is missing or empty in entry {index}.")
			normalized_entry[key] = str(value).strip()

		tag_values = {}
		for key, value in entry.items():
			if key in MANDATORY_KEYS:
				continue

			if value is None:
				continue

			clean_value = str(value).strip()
			if clean_value != "":
				tag_values[key] = clean_value

		normalized_entry["tags"] = tag_values
		normalized_entries.append(normalized_entry)

	return normalized_entries


def _table_exists(catalog_name: str, schema_name: str, table_name: str) -> bool:
	full_table_name = (
		f"`{_escape_identifier(catalog_name)}`."
		f"`{_escape_identifier(schema_name)}`."
		f"`{_escape_identifier(table_name)}`"
	)

	try:
		spark.sql(f"DESCRIBE TABLE {full_table_name}")
		return True
	except Exception:
		return False


def _apply_tags(catalog_name: str, schema_name: str, table_name: str, tags: dict):
	full_table_name = (
		f"`{_escape_identifier(catalog_name)}`."
		f"`{_escape_identifier(schema_name)}`."
		f"`{_escape_identifier(table_name)}`"
	)

	tag_expressions = [
		f"'{_escape_sql_literal(tag_key)}' = '{_escape_sql_literal(tag_value)}'"
		for tag_key, tag_value in tags.items()
	]
	sql = f"ALTER TABLE {full_table_name} SET TAGS ({', '.join(tag_expressions)})"
	print(sql)
	spark.sql(sql)


def _exit_success(message: str):
	print(message)
	try:
		dbutils.notebook.exit(message)
	except Exception:
		return


def main():
	uc_tags_path = _find_uc_tags_file(path, "uc_tags.json")

	if not uc_tags_path:
		_exit_success(f"No uc_tags.json found under path: {path}. Skipping tagging process.")
		return

	print(f"Using uc_tags.json file: {uc_tags_path}")

	with open(uc_tags_path, "r", encoding="utf-8") as file:
		raw_payload = file.read()

	parsed_payload = _parse_json_content(raw_payload)
	validated_entries = _normalize_and_validate_payload(parsed_payload)

	if mode not in {"info", "execute"}:
		raise ValueError(f"Invalid mode '{mode}'. Allowed values: info, execute")

	target_catalog = str(notebook_params.get("catalog", "")).strip()
	if target_catalog == "":
		raise ValueError("Missing required notebook parameter: notebook_params['catalog']")

	tagged_tables = 0
	planned_tables = 0
	skipped_missing_table = 0
	skipped_empty_tags = 0

	for entry in validated_entries:
		catalog_name = target_catalog
		schema_name = entry["schema_name"]
		table_name = entry["table_name"]
		tags = entry["tags"]

		fully_qualified_name = f"{catalog_name}.{schema_name}.{table_name}"

		if not _table_exists(catalog_name, schema_name, table_name):
			skipped_missing_table += 1
			print(f"Table does not exist, skipping: {fully_qualified_name}")
			continue

		if not tags:
			skipped_empty_tags += 1
			print(f"No non-empty tags for table: {fully_qualified_name}. Nothing to apply.")
			continue

		if mode == "execute":
			_apply_tags(catalog_name, schema_name, table_name, tags)
			tagged_tables += 1
			print(f"Applied tags for table: {fully_qualified_name}")
		else:
			planned_tables += 1
			print(f"INFO mode: would apply tags for table: {fully_qualified_name}")

		for tag_key, tag_value in tags.items():
			print(f"  - {tag_key}: {tag_value}")

	print("Tagging process finished.")
	print(f"Mode: {mode}")
	print(f"Entries processed: {len(validated_entries)}")
	print(f"Tables tagged: {tagged_tables}")
	print(f"Tables planned (info mode): {planned_tables}")
	print(f"Skipped (missing table): {skipped_missing_table}")
	print(f"Skipped (no non-empty tags): {skipped_empty_tags}")


main()

