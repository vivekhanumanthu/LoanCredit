# Tableau Dashboard Build Guide (Step-by-Step)

This folder contains instructions to build 4 dashboards from the generated CSV outputs.

## Data sources
Use these files from `data/outputs/`:
- `dashboard1_data_quality_pipeline_monitoring.csv`
- `dashboard2_model_performance_feature_importance.csv`
- `dashboard3_business_insights_recommendations.csv`
- `dashboard4_scalability_cost_analysis.csv`
- `test_predictions_for_storytelling.csv` (used in Dashboard 3)

## Build order
1. Dashboard 1: [DASHBOARD_1_DATA_QUALITY_AND_PIPELINE.md](DASHBOARD_1_DATA_QUALITY_AND_PIPELINE.md)
2. Dashboard 2: [DASHBOARD_2_MODEL_PERFORMANCE_AND_FEATURE_IMPORTANCE.md](DASHBOARD_2_MODEL_PERFORMANCE_AND_FEATURE_IMPORTANCE.md)
3. Dashboard 3: [DASHBOARD_3_BUSINESS_INSIGHTS_AND_RECOMMENDATIONS.md](DASHBOARD_3_BUSINESS_INSIGHTS_AND_RECOMMENDATIONS.md)
4. Dashboard 4: [DASHBOARD_4_SCALABILITY_AND_COST_ANALYSIS.md](DASHBOARD_4_SCALABILITY_AND_COST_ANALYSIS.md)

## Common setup (do once)
1. Open Tableau Desktop.
2. Connect to each CSV listed above.
3. Verify numeric vs categorical types.
4. Create extracts for all data sources.
5. Save workbook as `tableau/uci_loan_credit_dashboards.twbx`.
