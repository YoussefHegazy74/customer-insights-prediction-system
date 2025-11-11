
# Customer Churn Prediction – ML & Streamlit Module

This repository module contains the Machine Learning model and web interface for predicting customer churn using telecom data.

## 🎯 Goal

Predict which customers are likely to churn (leave the service), so action can be taken to retain them.

## 🧠 Machine Learning Overview

- **Model**: XGBoost Classifier
- **Data Balancing**: SMOTE oversampling
- **Evaluation Focus**: Maximized recall for churn class
- **Threshold Tuning**: Custom threshold set at 0.25 to boost recall
- **Feature Encoding**: One-hot encoding for categorical variables

## 🧪 Model Files

- `churn_model.pkl` – Trained XGBoost model
- `feature_names.pkl` – List of features used during model training

## 🌐 Streamlit App

A lightweight app built with Streamlit allowing users to:
- Input customer data manually
- Get immediate churn prediction (with probability)
- Built-in model threshold control

## 🛠️ How to Run the App

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Launch the Streamlit app:
```bash
streamlit run app.py
```

## 📂 Files Included

- `app.py` – Streamlit app code
- `churn_model.pkl` – Trained model
- `feature_names.pkl` – Feature columns
- `README.md` – This file

## 👤 Author

[Youssef Hegazy](https://github.com/youssefhegazy74)

> Part of the graduation project (ML module only)
