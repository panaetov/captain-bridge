#!/usr/bin/env bash

case "$1" in
    "bash")
        echo "Starting bash ..."
        exec bash -c "$2"
        ;;

    "server")
        echo "Starting server ..."
        exec uvicorn service.app:app \
            --port ${SERVICE_SERVER_PORT:-9000} \
            --host 0.0.0.0
        ;;

	"index_jira")
		exec python service/periodics/index_jira.py
		;;

	"migrate")
		exec python service/migrate.py
		;;

	"actualize_plannings")
		exec python service/periodics/actualize_plannings.py
		;;

    "front_server")
        echo "Starting front-server ..."
        exec uvicorn front-service.app:app \
            --port ${SERVICE_SERVER_PORT:-8000} \
            --host 0.0.0.0
        ;;

esac
