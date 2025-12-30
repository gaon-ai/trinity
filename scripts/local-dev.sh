#!/bin/bash
# Local Airflow development helper
# Usage: ./scripts/local-dev.sh [start|stop|logs|test]

set -e
cd "$(dirname "$0")/../airflow"

case "${1:-start}" in
  start)
    echo "Starting local Airflow..."

    # Kill any existing SSH tunnel to VM (they share port 8080)
    pkill -f "ssh.*8080:localhost:8080" 2>/dev/null || true

    # Setup .env if not exists
    if [ ! -f .env ]; then
      if [ -f .env.local ]; then
        cp .env.local .env
        echo "Created .env from .env.local"
      else
        echo "ERROR: No .env file. Create one from .env.example or .env.local"
        exit 1
      fi
    fi

    docker compose up -d
    echo ""
    echo "Airflow starting at http://localhost:8080"
    echo "Login: admin / admin"
    echo ""
    echo "Waiting for healthy status..."
    sleep 10
    docker compose ps
    ;;

  stop)
    echo "Stopping local Airflow..."
    docker compose down
    echo "Stopped."
    ;;

  logs)
    docker compose logs -f "${2:-airflow-scheduler}"
    ;;

  test)
    echo "Running DAG validation..."
    python3 ../scripts/validate_dags.py
    ;;

  shell)
    echo "Opening shell in scheduler container..."
    docker compose exec airflow-scheduler bash
    ;;

  trigger)
    if [ -z "$2" ]; then
      echo "Usage: ./local-dev.sh trigger <dag_id>"
      exit 1
    fi
    docker compose exec airflow-scheduler airflow dags trigger "$2"
    ;;

  *)
    echo "Usage: ./scripts/local-dev.sh [start|stop|logs|test|shell|trigger <dag>]"
    echo ""
    echo "Commands:"
    echo "  start        - Start local Airflow (default)"
    echo "  stop         - Stop local Airflow"
    echo "  logs [svc]   - Tail logs (default: airflow-scheduler)"
    echo "  test         - Validate DAG syntax"
    echo "  shell        - Open bash in scheduler container"
    echo "  trigger <d>  - Trigger a DAG by ID"
    ;;
esac
