job_files="$JOB_FILE_PATH/*.json"

#select ACL: either pillar specific if delivered or default one
if [ -f "$JOBS_ACL_FILE_PILLAR" ]; then
    job_acl_file="$JOBS_ACL_FILE_PILLAR"
    echo "[Info] Use pillar ACL: $job_acl_file"
else 
    job_acl_file="$JOBS_ACL_FILE_DEFAULT"
    echo "[Info] Use default ACL: $job_acl_file"
fi

#select RUN AS: either pillar specific if delivered or default one
default_run_as=$(cat $RUN_AS_FILE | jq .default )
pillar_run_as=$(cat $RUN_AS_FILE | jq -r .${PILLAR} )

echo "Test for pillar: $PILLAR"
echo "Run as file: $RUN_AS_FILE"

if [[ $pillar_run_as == 'null' || $pillar_run_as == '' ]]; then
    run_as="$default_run_as"
    echo "[Info] Use default run as: $run_as"
else
    run_as="$pillar_run_as"
    echo "[Info] Use pillar run as: $run_as"
fi

# Check for environment-specific schedule configurations in the job repository
SCHEDULE_CONFIG_PATH="$JOB_FILE_PATH/schedules/${TARGET_ENV}"
if [ -d "$SCHEDULE_CONFIG_PATH" ]; then
    echo "[Info] Found environment-specific schedule configurations at: $SCHEDULE_CONFIG_PATH"
fi

#sorting jobs, so firstly simple jobs will be created, then complex jobs
jobs=()
complex_jobs=()
result_code=0

for f in $job_files
do 
    find_job_calls=$(cat "$f" |  jq -r '.tasks[] 
    | select(.run_job_task != null) 
    | .run_job_task
    | .job_id')
    if [[ $find_job_calls == '' ]]
    then
        jobs+=("$f")
    else
        complex_jobs+=("$f")
    fi
done

jobs+=("${complex_jobs[@]}")

if [ -d "$JOB_FILE_PATH" ] 
then
  for f in "${jobs[@]}"
  do 
    file_name=$(basename "$f")

    echo "Deploying: $file_name"
    JOB_FILE="$JOB_FILE_PATH/$file_name"
    JOB_NAME=$(cat "$JOB_FILE" | jq -r .name)
    
    # Check for job-specific schedule configuration
    job_base_name="${file_name%.*}"
    SCHEDULE_FILE="$SCHEDULE_CONFIG_PATH/${job_base_name}.json"
    
    # Apply schedule configuration if it exists for this job and environment
    if [ -f "$SCHEDULE_FILE" ]; then
      echo "[Info] Applying environment-specific schedule for job: $job_base_name"
      # Merge the schedule configuration with the job definition
      # The jq command merges the schedule into the job definition
      jq -s '.[0] * .[1]' "$JOB_FILE" "$SCHEDULE_FILE" > "${JOB_FILE}.tmp" && mv "${JOB_FILE}.tmp" "$JOB_FILE"
    else
      echo "[Info] No environment-specific schedule found for job: $job_base_name"
    fi
    
    JOB_ID=$(databricks jobs list -o json |jq -r --arg job_name "$JOB_NAME" '.[] | select(.settings.name == $job_name) | .job_id')
    JOB_CNT=$(printf "%s" "$JOB_ID" | grep -c "^")

    if [ $JOB_CNT -gt 1 ]
    then
      echo "##vso[task.logissue type=error]Job name: $JOB_NAME is not unique"
      result_code=1
      exit $result_code
    fi
    ##Step 0:
    # Define the tag to add
    TAG_KEY="ETL"
    TAG_VALUE="${PILLAR^^}"  # Convert PILLAR to uppercase

    # Check if the job_clusters array exists
    JOB_CLUSTERS_EXISTS=$(jq '. | has("job_clusters")' "$JOB_FILE")   

    if [ "$JOB_CLUSTERS_EXISTS" = "true" ]; then
      # Check if custom_tags exists
      CUSTOM_TAGS_EXISTS=$(jq '.job_clusters[].new_cluster | has("custom_tags")' "$JOB_FILE" | grep -c true)    

      if [ "$CUSTOM_TAGS_EXISTS" -eq 0 ]; then
        # Add custom_tags if it does not exist
        jq --arg key "$TAG_KEY" --arg value "$TAG_VALUE" '.job_clusters[].new_cluster += {custom_tags: {($key): $value}}' "$JOB_FILE" > tmp.$$.json && mv tmp.$$.json "$JOB_FILE"
      else
        # Add the tag to existing custom_tags
        jq --arg key "$TAG_KEY" --arg value "$TAG_VALUE" '.job_clusters[].new_cluster.custom_tags += {($key): $value}' "$JOB_FILE" > tmp.$$.json && mv tmp.$$.json "$JOB_FILE"
      fi

      # Check if policy_id exists and remove it
      POLICY_ID_EXISTS=$(jq '.job_clusters[].new_cluster | has("policy_id")' "$JOB_FILE" | grep -q true && echo "true" || echo "false")
      
      if [ "$POLICY_ID_EXISTS" = "true" ]; then
        echo "[Info] Removing policy_id from job_clusters"
        jq '.job_clusters[].new_cluster |= del(.policy_id)' "$JOB_FILE" > tmp.$$.json && mv tmp.$$.json "$JOB_FILE"
      else
        echo "[Info] No policy_id found in job_clusters"
      fi
    else
      echo "[Info] The path .job_clusters does not exist in the JSON file. Skipping custom_tags section."
    fi

    # Handle job-level tags
    TAGS_EXISTS=$(jq '. | has("tags")' "$JOB_FILE")
    
    if [ "$TAGS_EXISTS" = "true" ]; then
      # Tags node exists, add or update the tag
      echo "[Info] Updating job-level tag: $TAG_KEY = $TAG_VALUE"
      jq --arg key "$TAG_KEY" --arg value "$TAG_VALUE" '.tags += {($key): $value}' "$JOB_FILE" > tmp.$$.json && mv tmp.$$.json "$JOB_FILE"
    else
      # Tags node does not exist, create it with the tag
      echo "[Info] Adding job-level tags node with tag: $TAG_KEY = $TAG_VALUE"
      jq --arg key "$TAG_KEY" --arg value "$TAG_VALUE" '. += {tags: {($key): $value}}' "$JOB_FILE" > tmp.$$.json && mv tmp.$$.json "$JOB_FILE"
    fi

    ##Step 1: Replace Notebooks Paths
    #Find all paths to notebook jobs
    NOTEBOOK_PATHS=$(cat "$JOB_FILE" | jq -r '.tasks[] | select(.notebook_task.notebook_path != null) | select(.notebook_task.notebook_path | startswith("/Repos/CodeRepo-CORE/ESG_Core/notebook_jobs") | not ) | .notebook_task.notebook_path' | awk -F'/notebook_jobs' '{print $1 "/notebook_jobs"}' | sort | uniq)
    #Replace path to point into pillar specific location
    if [ "$NOTEBOOK_PATHS" != "$REPO_NOTEBOOK_PATH" ]
    then
      echo "PATHS to replace:"
      echo "$NOTEBOOK_PATHS"
      echo "REPLACEMENT:"   
      echo "$REPO_NOTEBOOK_PATH"
      echo "$NOTEBOOK_PATHS" | while IFS= read -r line ; do sed -i "s|$line|$REPO_NOTEBOOK_PATH|g" "$JOB_FILE"; done
    fi

    ##Step 2: Replace Jobs Placeholders
    jobs_to_replace=$(cat "$JOB_FILE" |  jq -r '.tasks[] 
    | select(.run_job_task != null) 
    | .run_job_task
    | .job_id'  | sort | uniq)

    if ! [[ $jobs_to_replace == '' ]]
    then
        echo "Replace jobs placeholders: $file_name"
        echo "$jobs_to_replace" | while IFS= read -r line ; 
        do 
          INNER_JOB_ID=$(databricks jobs list -o json | jq -r --arg job_name "$line" '.[] | select(.settings.name == $job_name) | .job_id')
          INNER_JOB_CNT=$(printf "%s" "$INNER_JOB_ID" | grep -c "^")
          if [ "${#INNER_JOB_ID}" == "0" ]
          then
            echo "##vso[task.logissue type=error]Job $line does not exist"
            exit 1
          elif [ "$INNER_JOB_CNT" -gt 1 ]
          then
            echo "##vso[task.logissue type=error]Job name: $line is not unique"
            exit 1
          else
            inner_job_name=$(echo "$line" | sed -e 's/[]\/$*.^|[]/\\&/g')
            replace_old="\"job_id\": \"$inner_job_name\""
            replace_new="\"job_id\": $INNER_JOB_ID"
            echo "Replace $replace_old with $replace_new"
            sed -i "s@$replace_old@$replace_new@g" "$JOB_FILE";       
          fi
        done
    fi

    #replace run_as values
    JSON_JOB=$(cat "$JOB_FILE")
    echo "$JSON_JOB" | jq --argjson run_as "$run_as" '.run_as=$run_as' > "$JOB_FILE"

    # Replace the library catalog in the JSON file (environment agnostic for any wheel file)
    if [ "$TARGET_ENV" == "QA" ]; then
        # Replace any wheel file path from dev to QA environment
        sed -i 's|/Volumes/cdl_esg_dev/tech/lib/|/Volumes/cdl_esg_qa/tech/lib/|g' $JOB_FILE
    elif [ "$TARGET_ENV" == "PROD" ]; then
        # Replace any wheel file path from dev to PROD environment
        sed -i 's|/Volumes/cdl_esg_dev/tech/lib/|/Volumes/cdl_esg_prod/tech/lib/|g' $JOB_FILE
    fi

    # Replace Core with core for logging job completion scripts
    sed -i 's|/Core|/core|g' $JOB_FILE

    if [ "${#JOB_ID}" == "0" ]
    then
        JOB_ID=$(databricks jobs create --json "@$JOB_FILE" | jq '.job_id')
        result_code=$?

        echo "$JOB_NAME created: $JOB_ID" 
    else
        JSON_JOB=$(cat "$JOB_FILE")
        JSON_JOB="{\"job_id\": $JOB_ID, \"new_settings\": $JSON_JOB }"
        echo "$JSON_JOB" > "$JOB_FILE"
        databricks jobs reset --json "@$JOB_FILE"
        result_code=$?
        echo "$JOB_NAME reset: $JOB_ID" 
    fi
    if [[ "${#JOB_ID}" == "0" ]] || [[ "$result_code" != 0 ]]
    then
        echo "##vso[task.logissue type=error]Error: $JOB_NAME could not be created due to an error"
        exit $result_code
    fi
    acl=$(databricks jobs set-permissions $JOB_ID --json "@$job_acl_file")
  done;
fi

exit $result_code