param([string]$Username = "masonmirtech")
git init; git branch -M main; git add .
git commit -m "feat: Real-Time Data Streaming and Lakehouse Platform - Kafka, PySpark Structured Streaming, Delta Lake, Airflow"
git remote remove origin 2>$null
git remote add origin "https://github.com/$Username/realtime-lakehouse-streaming-platform.git"
git push -u origin main
