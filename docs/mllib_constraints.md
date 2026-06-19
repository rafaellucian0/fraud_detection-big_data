# Restrições MLlib

O treinamento principal usa apenas Spark MLlib.

Não use:

- XGBoost
- LightGBM
- CatBoost
- SHAP
- SMOTE externo
- TensorFlow
- PyTorch
- scikit-learn como treinamento principal

A interpretabilidade usa recursos simples e compatíveis, como `featureImportances`, métricas por threshold e matriz de confusão.

