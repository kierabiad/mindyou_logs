# Mind You Logs

A Django logging and analytics project with Docker, PostgreSQL, Redis, Celery, and Mailpit.

## What Is Tracked In Git

- Source code
- Django app files
- Exported CSV log files
- README and project scripts

## What Stays Local

- `.envs/.local/.gmail`
- Other machine-specific secrets
- Virtual environments and build caches

## Step-by-Step Setup

### 1. Clone the repository

```bash
git clone https://github.com/kierabiad/mindyou_logs.git
cd mindyou_logs
```

### 2. Create the local Gmail secret file

Copy the example file and fill in real values on your machine only.

```bash
copy .envs\.local\.gmail.example .envs\.local\.gmail
```

On macOS or Linux, use:

```bash
cp .envs/.local/.gmail.example .envs/.local/.gmail
```

Edit `.envs/.local/.gmail` and set:

```bash
GMAIL_SENDER_EMAIL=your_real_gmail_address@gmail.com
GMAIL_APP_PASSWORD=your_16_character_google_app_password
```

### 3. Start Docker services

```bash
docker compose -f docker-compose.local.yml up --build
```

This starts Django, PostgreSQL, Redis, Celery, and Mailpit.

### 4. Run database migrations

In a second terminal:

```bash
docker compose -f docker-compose.local.yml exec django python manage.py migrate
```

### 5. Create a Django superuser

```bash
docker compose -f docker-compose.local.yml exec django python manage.py createsuperuser
```

### 6. Check service status

```bash
docker compose -f docker-compose.local.yml ps
```

## Open the App

- App: http://localhost:8000
- Admin: http://localhost:8000/admin
- Mailpit: http://127.0.0.1:8025

## Gmail SMTP Setup

Use Gmail App Passwords only.

1. Turn on 2-Step Verification on the Google account that will send mail.
2. Create a Google App Password.
3. Put the sender email and app password in `.envs/.local/.gmail`.
4. Do not commit `.envs/.local/.gmail`.

## Export Logs and Email Them

To export the current database rows and send them through Gmail:

```bash
docker compose -f docker-compose.local.yml exec django python export_consolidated_logs_and_send_gmail.py --send-email --rows-per-csv 32000 --db-chunk-size 2000
```

Notes:
- Acuity and Zoho logs are exported separately.
- CSV chunks default to 32,000 rows.
- The email body contains the summary.
- The script will use your local `.envs/.local/.gmail` file automatically.

If you only want to export files and not send email:

```bash
docker compose -f docker-compose.local.yml exec django python export_consolidated_logs_and_send_gmail.py --rows-per-csv 32000 --db-chunk-size 2000
```

## Service Notes

- Django runs on port 8000
- Mailpit runs on port 8025
- PostgreSQL and Redis are started by Docker Compose
- Celery workers are included in the local compose file

## Project Scripts

- `scripts/export_consolidated_logs_and_send_gmail.py` exports logs, zips them, and sends email
- `export_consolidated_logs_and_send_gmail.py` is a root-level launcher for the same script

## Common Commands

```bash
docker compose -f docker-compose.local.yml ps
docker compose -f docker-compose.local.yml logs -f django
docker compose -f docker-compose.local.yml exec django python manage.py shell
```
