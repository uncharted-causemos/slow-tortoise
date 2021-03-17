## GraphQL queries with cURL

Prefect documentation [here](https://docs.prefect.io/orchestration/concepts/api.html)  
To write your own query with curl just replace the GraphQL query in the `query` variable and leave the rest of the bash dark magic alone.  
**NOTE**: You will need to double escape `\\"` any quotes that appear inside a JSON string (like the parameters in the first query.)  
To craft GraphQL queries you can use the [Interactive API](http://10.65.18.52:8080/default/api) tab in the Prefect UI where you have full access to the schemas and type information

### Sample queries

**Start a new flow run**
```
query='mutation {
  create_flow_run(input: {
    flow_id: "2c362d3d-f087-4004-9d2c-7a7b8deec9b0",
    flow_run_name: "<optional name of run>",
    parameters: "{\\"model_id\\": \\"e0a14dbf-e8e6-42bd-b908-e72a956fadd5\\", \\"run_id\\": \\"749916f0-be24-4e4b-9a6c-798808a5be3c\\" }"
  }) {
    id
  }
}'
query="${query//[$'\n']}"
curl 'http://10.65.18.52:4200/' -H 'Content-Type: application/json' -d '{"query":"'"${query//[\"]/\\\"}"'"}'
```
**NOTE:** The `flow_id` of a flow changes with each version. To create a "stable" query that continues to work between flow updates use the flow's `version_group_id` instead.

**Check the state of a flow run**
```
query='query {
  flow(where: { id: {_eq: "<Set flow_id here>"} }) {
    name
    flow_runs(where: { id: {_eq: "<flow_run_id returned from start mutation>"}}) {
      state
      start_time
      end_time
    }
  }
}'
query="${query//[$'\n']}"
curl 'http://10.65.18.52:4200/' -H 'Content-Type: application/json' -d '{"query":"'"${query//[\"]/\\\"}"'"}'
```

**Get the logs of a flow run**
```
query='query {
  flow(where: { id: {_eq: "<Set flow_id here>"} }) {
    name
    flow_runs(where: { id: {_eq: "<flow_run_id returned from start mutation>"}}) {
      state
      start_time
      end_time
      logs(order_by: {timestamp: asc}) {
        timestamp
        level
        message
      }
    }
  }
}'
query="${query//[$'\n']}"
curl 'http://10.65.18.52:4200/' -H 'Content-Type: application/json' -d '{"query":"'"${query//[\"]/\\\"}"'"}'
```