JOB_NAME=$(cat $JOB_FILE | jq -r .name)

echo "JOB NAME: $JOB_NAME"
JOB_ID=$(databricks jobs list | grep --fixed-string "$JOB_NAME" | awk '{print $1}')

echo "JOB ID: $JOB_ID"

# Replace the environment in the JSON file
if [ "$TARGET_ENV" == "QA" ]; then
    sed -i 's|/Volumes/cdl_esg_dev/tech/lib/esg_utils-1.0.0-py3-none-any.whl|/Volumes/cdl_esg_qa/tech/lib/esg_utils-1.0.0-py3-none-any.whl|g' $JOB_FILE
elif [ "$TARGET_ENV" == "PROD" ]; then
    sed -i 's|/Volumes/cdl_esg_dev/tech/lib/esg_utils-1.0.0-py3-none-any.whl|/Volumes/cdl_esg_prod/tech/lib/esg_utils-1.0.0-py3-none-any.whl|g' $JOB_FILE
fi

if [ "${#JOB_ID}" == "0" ]
then
    JOB_ID=$(databricks jobs create --json "@$JOB_FILE" | jq '.job_id') 
    echo "$JOB_NAME created: $JOB_ID" 
else
    JSON_JOB=$(cat $JOB_FILE)
    JSON_JOB="{\"job_id\": $JOB_ID, \"new_settings\": $JSON_JOB }"
    echo $JSON_JOB > $JOB_FILE
    databricks jobs reset --json @$JOB_FILE
    echo "$JOB_NAME reset: $JOB_ID" 
fi
if [ "${#JOB_ID}" == "0" ]
then
    echo "$JOB_NAME creation error" 
fi

echo "Deploy with Ion: $DEPL_WITH_ION"

if [ "$DEPL_WITH_ION" == "True" ]; then
    acl=$(databricks jobs set-permissions $JOB_ID --json "@$JOBS_ACL_ION_FILE")
else
    acl=$(databricks jobs set-permissions $JOB_ID --json "@$JOBS_ACL_FILE")
fi

job_params="{\"job_id\":$JOB_ID, \"notebook_params\": {\"diff-file\": \"$DIFF_FILE\",\"mode\": \"$MODE\",\"pillar\": \"$PILLAR\",\"repo_name\": \"$REPO_NAME\"}}"

echo "JOB PARAMS: $job_params"
run_id=$(databricks jobs run-now --timeout "120m0s" --json "$job_params" | jq '.tasks[0].run_id')

result=$(databricks jobs get-run-output $run_id)
result_code=$?
notebook_out=$(echo $result | jq '.notebook_output.result')
result_state=$(echo $result | jq -r '.metadata.tasks[0].state.result_state')
result_msg=$(echo $result | jq -r '.metadata.tasks[0].state.state_message')

if [[ $result_state == "FAILED" ]] || [[ "$result_code" != 0 ]]
then
  echo $result_msg
  echo "##vso[task.logissue type=error;]$result_msg"
  echo "##vso[task.complete result=Failed;done=true;]$result_msg"
  exit 1
fi

echo $notebook_out | sed 's/\\n/\n/g' | tr -d '"'
exit 0