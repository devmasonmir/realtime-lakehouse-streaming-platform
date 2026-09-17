@echo off
setlocal
echo ===================================================
echo  Real-Time Lakehouse Streaming Platform
echo ===================================================
git init
git branch -M main
git add .
git commit -m "feat: Real-Time Data Streaming and Lakehouse Platform - Kafka, PySpark Structured Streaming, Delta Lake, Airflow"
set /p GH_USER="Enter GitHub username (or press enter for soniablaketech/masonmirtech): "
git remote remove origin 2>nul
git remote add origin https://github.com/%GH_USER%/realtime-lakehouse-streaming-platform.git
git push -u origin main
pause
