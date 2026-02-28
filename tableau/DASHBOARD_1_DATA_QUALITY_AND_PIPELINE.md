# Dashboard 1: Data Quality and Pipeline Monitoring (Loan/Credit)

## Input file
- `data/outputs/dashboard1_data_quality_pipeline_monitoring.csv`

## Goal
Show data quality and segment mix for the UCI loan/credit dataset.

## Step-by-step
1. Create Sheet `D1_Records_By_Segment`:
   - Columns: `A13`
   - Rows: `records`
   - Color: `target`
   - Marks: Bar
2. Create Sheet `D1_Missing_By_Segment`:
   - Columns: `A13`
   - Rows: `avg_missing_fields`
   - Color: `target`
   - Marks: Line
3. Create Sheet `D1_Income_vs_Debt`:
   - Columns: `avg_income_proxy`
   - Rows: `avg_debt_proxy`
   - Detail: `A13`
   - Color: `target`
   - Marks: Circle
4. Create KPI sheet `D1_Total_Records`:
   - Text: `SUM(records)`
5. Build Dashboard `Dashboard 1 - Data Quality`:
   - Top: `D1_Total_Records`
   - Left: `D1_Records_By_Segment`
   - Right: `D1_Missing_By_Segment`
   - Bottom: `D1_Income_vs_Debt`
6. Add filters:
   - `target`
   - `A13`

## Final checks
1. Tooltips show segment, class, records, missing, income proxy, debt proxy.
2. Filters affect all sheets.
3. Export image and PDF.
