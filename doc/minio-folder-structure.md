## Minio folder structure and file format
This structure applies to both model output and indicator data.  
Note: our convention is that indicators always have a single `run_id` with the value `indicator`.
```
{data_id}
  \- {run_id}
    \- raw
      \- {feature}
        \- info
          \- qualifiers
            -- {qualifier}.json
          -- region_lists.json
          -- qualifier_counts.json
        \- raw
          -- raw.csv
    \- results
      -- results.json
    \- {time_res}
      \- {feature}
        \- stats
          \- grid
            -- {timestamp}.csv    | zoom | min_agg_func1 | max_agg_func1 | ...
        \- tiles
          -- {timestamp}-{zoom}-{x}-{y}.tile
        \- timeseries
          \- global
            -- global.csv    | timestamp | agg-func-1 | agg-func-2 | ...
          \- qualifiers
            \- {qualifier}
              -- {agg-func.csv}    | timestamp | qualifier-value-1 | ...
        \- regional
          \- {country | admin1 | admin2 | admin3}
            \- stats
              \- default
                extrema.json
            \- timeseries
              \- default
                -- {region-id}.csv    | timestamp | agg-func-1 | ...
              \- qualifiers
                \- {qualifier}
                  \- {qualifier-value}
                    -- {region-id}.csv    | timestamp | agg-func-1 | ...
            \- aggs
              \- {timestamp}
                \- default
                  -- default.csv    | id | agg-func-1 | agg-func-2 | ...
                \- qualifiers
                  -- {qualifier}.csv    | id | qualifier-value | agg-func-1 | ...
```

### Examples
- Qualifier list: [{qualifier}.json](./examples/camp.json)
- Region lists: [region_lists.json](./examples/region_lists.json)
- Qualifier counts: [qualifier_counts.json](./examples/qualifier_counts.json)
- Raw data: [raw.csv](./examples/raw.csv)
- Pipeline results: [results.json](./examples/results.json)
- Grid stats: [{timestamp.csv}](./examples/1614556800000.csv)
- Tile data (protobuf binary): [{timestamp}-{zoom}-{x}-{y}.tile](./examples/1606780800000-7-78-59.tile)
- Global timeseries: [global.csv](./examples/global.csv)
- Qualifier timeseries: [{agg-func}.csv](./examples/s_mean_t_mean.csv)
- Regional stats: [extrema.json](./examples/extrema.json)
- Regional timesereis: [{region-id}.csv](./examples/Eritrea__Maekel.csv)
- Regional qualifier timeseries: [{region-id}.csv](./examples/Djibouti__Dikhil.csv)
- Regional data: [default.csv](./examples/default.csv)
- Qualifier regional data: [{qualifier}.csv](./examples/camp.csv)

### Region ID format
  - admin3 id: `{country}__{admin1}__{admin2}__{admin3}`
  - admin2 id: `{country}__{admin1}__{admin2}`
  - admin1 id: `{country}__{admin1}`
  - country id: `{country}`
