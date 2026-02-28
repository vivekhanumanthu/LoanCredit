# Dashboard 3: Business Insights and Recommendations

## Input files
- `data/outputs/dashboard3_business_insights_recommendations.csv`
- `data/outputs/test_predictions_for_storytelling.csv`

## Goal
Translate loan/credit model outputs into business-facing KPI story.

## Step-by-step
1. Data model:
   - Use `dashboard3_business_insights_recommendations.csv` as primary.
   - Add `test_predictions_for_storytelling.csv` as secondary source.
2. Create Sheet `D3_Business_KPIs`:
   - Rows: `metric`
   - Text: `value`
   - Filter metrics to: `expected_profit`, `precision`, `recall`, `f1`.
3. Create Sheet `D3_Confusion_Components`:
   - Rows: `metric`
   - Columns: `value`
   - Filter metrics to: `tp`, `fp`, `tn`, `fn`.
   - Marks: Bar.
4. Create Sheet `D3_Prediction_Probability`:
   - Columns: `probability` (bin)
   - Rows: `COUNT(*)`
   - Color: `target`
   - Marks: Histogram.
5. Create KPI sheet `D3_Expected_Profit`:
   - Show only metric `expected_profit`.
6. Build Dashboard `Dashboard 3 - Business Insights`:
   - Top: `D3_Expected_Profit`
   - Left: `D3_Business_KPIs`
   - Right: `D3_Confusion_Components`
   - Bottom: `D3_Prediction_Probability`

## Storytelling text (recommended)
- Insight: model separates approval-risk classes with measurable precision/recall.
- Impact: expected profit translates model quality to business value.
- Action: tune threshold based on FP cost vs FN opportunity loss.

## Final checks
1. All business metrics are visible.
2. Probability distribution and confusion components are consistent.
3. Export image and PDF.
