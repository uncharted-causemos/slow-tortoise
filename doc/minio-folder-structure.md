## Minio folder structure and file format
```
{model_id}
  \- {run_id}
    \- {time_res}
      \- {feature}
        \- stats
          \- grid
            \- {timestamp}.csv  | zoom | min_agg_func1 | max_agg_func1 | ...
        \- tiles
          \- {timestamp}-{zoom}-{x}-{y}.tile
        \- timeseries
          \- {agg-func}.json: [{“timestamp”: 1200, “value”: 5}, ...]
        \- regional
          \- {country | admin1 | admin2 | admin3}
            \- timeseries
              \- {region id}.csv  | timestamp | agg-func-1 | agg-func-2 | ...
                                     18000000       2             3       
            \- aggs
              \- {timestamp}
                \- {agg-func}.json: [{“id”: {region-id}, “value”: 3}, ...]
```
### Region ID format
  - admin3 id: `{country}__{admin1}__{admin2}__{admin3}`
  - admin2 id: `{country}__{admin1}__{admin2}`
  - admin1 id: `{country}__{admin1}`
  - country id: `{country}`
