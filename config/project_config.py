PROJECT_NAME = "Hospital Analytics POC"

RAW_DATA_PATH = "dbfs:/FileStore/hospital_analytics/raw/"

BRONZE_DATABASE = "bronze"
SILVER_DATABASE = "silver"
GOLD_DATABASE = "gold"

FILE_NAMES = {
    "patients": "patients.csv",
    "doctors": "doctors.csv",
    "appointments": "appointments.csv",
    "treatments": "treatments.csv",
    "billing": "billing.csv",
}
