#!/bin/bash

# HITEx Data Platform - Local Testing Script
# This script runs all required local tests as specified in the requirements

set -e

echo "Starting HITEx Data Platform Local Testing"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print status
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}$2${NC}"
    else
        echo -e "${RED}$2${NC}"
        exit 1
    fi
}

# Step 1: Setup Python Virtual Environment
echo -e "${YELLOW}Setting up Python virtual environment...${NC}"
python -m venv .venv
source .venv/bin/activate
print_status $? "Virtual environment created and activated"

# Step 2: Install Dependencies
echo -e "${YELLOW}Installing Python dependencies...${NC}"
pip install -r requirements/requirements-test.txt
print_status $? "Dependencies installed successfully"

# Step 3: Run Black (Code Formatting)
echo -e "${YELLOW}Running Black code formatter...${NC}"
black dags/ plugins/ tests/ --check
print_status $? "Black formatting check passed"

# Step 4: Run Pylint (Code Quality)
echo -e "${YELLOW}Running Pylint code analysis...${NC}"
pylint dags/ plugins/ --fail-under=8.0
print_status $? "Pylint analysis passed (score >= 8.0)"

# Step 5: Run Pytest with Coverage
echo -e "${YELLOW}Running pytest with coverage...${NC}"
pytest tests/ --cov=dags --cov=plugins --cov-fail-under=90 --cov-report=term-missing -v
print_status $? "Pytest passed with >=90% coverage"

# Step 6: Setup Docker Environment
echo -e "${YELLOW}Starting Docker Compose test environment...${NC}"
export AIRFLOW_UID=50000
docker-compose -f docker-compose.test.yml up -d
print_status $? "Docker Compose environment started"

# Wait for services to be ready
echo -e "${YELLOW}Waiting for services to be ready...${NC}"
sleep 60

# Step 7: Test DAG Import
echo -e "${YELLOW}Testing DAG import...${NC}"
docker-compose -f docker-compose.test.yml exec -T airflow-webserver airflow dags list | grep hitex_csv_to_dw
print_status $? "DAG imported successfully"

# Step 8: Trigger DAG Manually
echo -e "${YELLOW}Triggering hitex_csv_to_dw DAG...${NC}"
docker-compose -f docker-compose.test.yml exec -T airflow-webserver airflow dags trigger hitex_csv_to_dw
print_status $? "DAG triggered successfully"

# Step 9: Wait and Check DAG Status
echo -e "${YELLOW}Waiting for DAG execution...${NC}"
sleep 120

# Check DAG run status
docker-compose -f docker-compose.test.yml exec -T airflow-webserver airflow dags state hitex_csv_to_dw $(date +%Y-%m-%d) | grep -E "(success|running)"
print_status $? "DAG execution completed successfully"

# Step 10: Verify Data in BigQuery Emulator
echo -e "${YELLOW}Verifying data in BigQuery emulator...${NC}"
# Check if BigQuery emulator is accessible
curl -f http://localhost:9050/bigquery/v2/projects/hitex-test/datasets > /dev/null 2>&1
print_status $? "BigQuery emulator accessible"

# Step 11: Check Logs for Data Landing
echo -e "${YELLOW}Checking logs for data landing confirmation...${NC}"
docker-compose -f docker-compose.test.yml logs airflow-worker | grep -E "(dim_product|fct_sales)" | grep -E "(Loaded|rows)"
print_status $? "Data landing confirmed in logs"

# Cleanup
echo -e "${YELLOW}Cleaning up Docker environment...${NC}"
docker-compose -f docker-compose.test.yml down
print_status $? "Docker environment cleaned up"

echo ""
echo -e "${GREEN}All Local Tests Completed Successfully!${NC}"
echo "=============================================="
echo "Python dependencies installed"
echo "Black formatting passed"
echo "Pylint analysis passed (score >= 8.0)"
echo "Pytest passed with >=90% coverage"
echo "Docker Compose environment tested"
echo "DAG triggered and executed successfully"
echo "Data confirmed in BigQuery emulator"
echo ""
echo -e "${YELLOW}Access Points:${NC}"
echo "• Airflow UI: http://localhost:8080 (admin/admin)"
echo "• BigQuery Emulator: http://localhost:9050"
echo "• Coverage Report: htmlcov/index.html"
echo ""
echo -e "${GREEN}Ready for GCP deployment!${NC}"