param(
    [string]$Action = 'help'
)

function Show-Help {
    Write-Host "dev.ps1 help:`n  init - run airflow db upgrade and create admin user`n  up - start stack (background)`n  down - stop and remove stack`n  logs - tail compose logs`n  ps - show compose ps"
}

switch ($Action) {
    'init' {
        docker compose up airflow-init
    }
    'up' {
        docker compose up -d postgres redis airflow-webserver airflow-scheduler airflow-worker
    }
    'down' {
        docker compose down -v
    }
    'logs' {
        docker compose logs -f --tail 200
    }
    'ps' {
        docker compose ps
    }
    default {
        Show-Help
    }
}
