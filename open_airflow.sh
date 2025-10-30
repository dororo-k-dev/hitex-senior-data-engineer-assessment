#!/bin/bash

echo "=========================================="
echo "HITEx Data Platform - Local Demo"
echo "=========================================="
echo ""
echo "Docker containers are running:"
docker-compose -f docker-compose.simple.yml ps --format "table {{.Service}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "Airflow DAGs loaded:"
docker-compose -f docker-compose.simple.yml exec -T airflow-webserver airflow dags list 2>/dev/null | grep -E "(hitex_|dag_id)"
echo ""
echo "Access Points:"
echo "• Airflow Web UI: http://localhost:8080"
echo "• Username: admin"
echo "• Password: admin"
echo ""
echo "Available DAGs:"
echo "• hitex_csv_to_dw (Main assessment pipeline)"
echo "• hitex_amazon_fba_pipeline (Amazon FBA integration)"
echo "• hitex_erp_pipeline (ERP system integration)"
echo ""
echo "Opening Airflow in your browser..."
echo "   (If it doesn't open automatically, visit: http://localhost:8080)"
echo ""

# Try to open in browser (works on macOS)
if command -v open >/dev/null 2>&1; then
    open http://localhost:8080
elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open http://localhost:8080
else
    echo "Please manually open: http://localhost:8080"
fi

echo "Your HITEx Data Platform is ready for demonstration!"