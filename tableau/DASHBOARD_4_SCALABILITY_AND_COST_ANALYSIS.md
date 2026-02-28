# Dashboard 4: Scalability and Cost Analysis

## Input file
- `data/outputs/dashboard4_scalability_cost_analysis.csv`

## Goal
Show strong/weak scaling behavior and cost-performance tradeoffs.

## Step-by-step
1. Create Sheet `D4_Strong_Scaling_Speedup`:
   - Filter: `scaling_type = strong`
   - Columns: `resource_level`
   - Rows: `throughput_or_speedup`
   - Marks: Line + Circle.
2. Create Sheet `D4_Weak_Scaling_Throughput`:
   - Filter: `scaling_type = weak`
   - Columns: `resource_level`
   - Rows: `throughput_or_speedup`
   - Marks: Line + Circle.
3. Create Sheet `D4_Cost_Per_10k`:
   - Columns: `resource_level`
   - Rows: `cost_per_10k_rows_usd`
   - Color: `scaling_type`
   - Marks: Bar.
4. Create Sheet `D4_Elapsed_Time`:
   - Columns: `resource_level`
   - Rows: `elapsed_seconds`
   - Color: `scaling_type`
   - Marks: Bar.
5. Build Dashboard `Dashboard 4 - Scalability and Cost`:
   - Top row: `D4_Strong_Scaling_Speedup` + `D4_Weak_Scaling_Throughput`
   - Bottom row: `D4_Cost_Per_10k` + `D4_Elapsed_Time`
6. Add filters:
   - `scaling_type`
   - `resource_level`
7. Add parameter `Cost Sensitivity`:
   - Numeric parameter default `1.0`
   - Calculated field: `Adjusted Cost per 10k = [cost_per_10k_rows_usd] * [Cost Sensitivity]`
8. Add actions:
   - Selecting a `resource_level` in one view highlights same level in all views.

## Bottleneck callouts
Use text boxes to mark likely bottlenecks:
- If elapsed time is flat but cost rises: overhead/network/coordination bottleneck.
- If throughput does not scale with resource level: shuffle or I/O bottleneck.

## Final checks
1. Strong and weak scaling are both shown.
2. Cost/performance tradeoff is obvious at a glance.
3. Export image and PDF.
