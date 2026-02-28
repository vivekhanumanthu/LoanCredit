# Dashboard 2: Model Performance and Runtime

## Input file
- `data/outputs/dashboard2_model_performance_feature_importance.csv`

## Goal
Compare Spark MLlib and scikit-learn model quality and runtime on loan/credit classification.

## Step-by-step
1. Create calculated field `Model Family`:
```tableau
IF STARTSWITH([model], "sk_") THEN "scikit-learn" ELSE "Spark MLlib" END
```
2. Create Sheet `D2_Test_AUC`:
   - Rows: `model`
   - Columns: `test_metric`
   - Color: `Model Family`
   - Marks: Bar
3. Create Sheet `D2_Test_Accuracy`:
   - Rows: `model`
   - Columns: `test_accuracy`
   - Color: `Model Family`
   - Marks: Bar
4. Create Sheet `D2_Training_Time`:
   - Rows: `model`
   - Columns: `training_seconds`
   - Color: `Model Family`
   - Marks: Bar
5. Create Sheet `D2_Validation_vs_Test_AUC`:
   - Columns: `validation_metric`
   - Rows: `test_metric`
   - Detail: `model`
   - Color: `Model Family`
   - Marks: Circle
6. Build Dashboard `Dashboard 2 - Model Performance`:
   - Top: `D2_Test_AUC`
   - Middle left: `D2_Test_Accuracy`
   - Middle right: `D2_Training_Time`
   - Bottom: `D2_Validation_vs_Test_AUC`

## Final checks
1. Spark and sklearn models are both visible.
2. Null validation AUC values are labeled clearly for models without validation scoring.
3. Export image and PDF.
