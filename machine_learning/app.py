import streamlit as st
import pandas as pd
import joblib

# Load model and feature names
model = joblib.load("churn_model.pkl")
feature_names = joblib.load("feature_names.pkl")
THRESHOLD = 0.25  # From your threshold tuning

st.title("🔮 Customer Churn Prediction App")

st.markdown("Fill in the customer information below:")

# Input fields
SeniorCitizen = st.selectbox("Senior Citizen", [0, 1])
tenure = st.slider("Tenure (Months)", 0, 72, 12)
MonthlyCharges = st.number_input("Monthly Charges", 0.0, 200.0, 70.0)
TotalCharges = st.number_input("Total Charges", 0.0, 10000.0, 2000.0)

gender = st.selectbox("Gender", ["Male", "Female"])
Partner = st.selectbox("Partner", ["Yes", "No"])
Dependents = st.selectbox("Dependents", ["Yes", "No"])
PhoneService = st.selectbox("Phone Service", ["Yes", "No"])
MultipleLines = st.selectbox("Multiple Lines", ["Yes", "No", "No phone service"])
InternetService = st.selectbox("Internet Service", ["DSL", "Fiber optic", "No"])
OnlineSecurity = st.selectbox("Online Security", ["Yes", "No", "No internet service"])
OnlineBackup = st.selectbox("Online Backup", ["Yes", "No", "No internet service"])
DeviceProtection = st.selectbox("Device Protection", ["Yes", "No", "No internet service"])
TechSupport = st.selectbox("Tech Support", ["Yes", "No", "No internet service"])
StreamingTV = st.selectbox("Streaming TV", ["Yes", "No", "No internet service"])
StreamingMovies = st.selectbox("Streaming Movies", ["Yes", "No", "No internet service"])
Contract = st.selectbox("Contract", ["Month-to-month", "One year", "Two year"])
PaperlessBilling = st.selectbox("Paperless Billing", ["Yes", "No"])
PaymentMethod = st.selectbox("Payment Method", [
    "Electronic check", "Mailed check", "Bank transfer (automatic)", "Credit card (automatic)"
])

# Prepare input dictionary
input_data = {
    'SeniorCitizen': SeniorCitizen,
    'tenure': tenure,
    'MonthlyCharges': MonthlyCharges,
    'TotalCharges': TotalCharges,
    'gender_Male': 1 if gender == "Male" else 0,
    'Partner_Yes': 1 if Partner == "Yes" else 0,
    'Dependents_Yes': 1 if Dependents == "Yes" else 0,
    'PhoneService_Yes': 1 if PhoneService == "Yes" else 0,
    'MultipleLines_No phone service': 1 if MultipleLines == "No phone service" else 0,
    'MultipleLines_Yes': 1 if MultipleLines == "Yes" else 0,
    'InternetService_Fiber optic': 1 if InternetService == "Fiber optic" else 0,
    'InternetService_No': 1 if InternetService == "No" else 0,
    'OnlineSecurity_No internet service': 1 if OnlineSecurity == "No internet service" else 0,
    'OnlineSecurity_Yes': 1 if OnlineSecurity == "Yes" else 0,
    'OnlineBackup_No internet service': 1 if OnlineBackup == "No internet service" else 0,
    'OnlineBackup_Yes': 1 if OnlineBackup == "Yes" else 0,
    'DeviceProtection_No internet service': 1 if DeviceProtection == "No internet service" else 0,
    'DeviceProtection_Yes': 1 if DeviceProtection == "Yes" else 0,
    'TechSupport_No internet service': 1 if TechSupport == "No internet service" else 0,
    'TechSupport_Yes': 1 if TechSupport == "Yes" else 0,
    'StreamingTV_No internet service': 1 if StreamingTV == "No internet service" else 0,
    'StreamingTV_Yes': 1 if StreamingTV == "Yes" else 0,
    'StreamingMovies_No internet service': 1 if StreamingMovies == "No internet service" else 0,
    'StreamingMovies_Yes': 1 if StreamingMovies == "Yes" else 0,
    'Contract_One year': 1 if Contract == "One year" else 0,
    'Contract_Two year': 1 if Contract == "Two year" else 0,
    'PaperlessBilling_Yes': 1 if PaperlessBilling == "Yes" else 0,
    'PaymentMethod_Credit card (automatic)': 1 if PaymentMethod == "Credit card (automatic)" else 0,
    'PaymentMethod_Electronic check': 1 if PaymentMethod == "Electronic check" else 0,
    'PaymentMethod_Mailed check': 1 if PaymentMethod == "Mailed check" else 0
}

# Create DataFrame and align columns
X_input = pd.DataFrame([input_data])

# Add any missing columns (just in case)
for col in feature_names:
    if col not in X_input.columns:
        X_input[col] = 0

X_input = X_input[feature_names]

# Prediction
if st.button("Predict"):
    proba = model.predict_proba(X_input)[0][1]
    churn = proba >= THRESHOLD

    st.subheader("Prediction Result")
    st.write(f"Churn Probability: **{proba:.2f}**")

    if churn:
        st.error("⚠️ The customer is likely to churn.")
    else:
        st.success("✅ The customer is likely to stay.")
