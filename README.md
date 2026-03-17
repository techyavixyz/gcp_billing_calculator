# GCP Billing Tool — with PostgreSQL Auth

## Quick Start

```bash
docker compose up --build
```

Then open **http://localhost:5000**

---

## Default Credentials

| Username | Password   | Role   |
|----------|------------|--------|
| admin    | admin123   | admin  |
| viewer   | viewer123  | viewer |

> ⚠️ Change these passwords immediately after first login (see below).

---

## Project Structure

```
gcp-price-calculator-enhanced/
├── docker-compose.yml          ← Orchestrates app + PostgreSQL
├── init.sql                    ← DB schema + seed users (runs once on first start)
├── README.md
└── gcp-price-calculator/
    ├── Dockerfile
    ├── app.py                  ← Flask app with login_required on all routes
    ├── requirements.txt
    ├── GCP_Discount_applied.xlsx
    ├── static/
    └── templates/
        ├── login.html          ← Login page
        ├── logout.html         ← Logout confirmation
        ├── base.html           ← Topbar shows username + logout button
        └── ...
```

---

## Managing Users via psql

```bash
# Connect to the database
docker exec -it gcp_billing_db psql -U gcpuser -d gcpauth

# List all users
SELECT id, username, role, is_active FROM users;

# Add a new user (generate the hash first — see below)
INSERT INTO users (username, password_hash, role) VALUES ('alice', '<hash>', 'editor');

# Disable a user
UPDATE users SET is_active = FALSE WHERE username = 'olduser';

# Change a password
UPDATE users SET password_hash = '<new_hash>' WHERE username = 'admin';
```

### Generating a password hash (Python)

```python
from werkzeug.security import generate_password_hash
print(generate_password_hash("your_new_password"))
```

---

## Environment Variables

All configurable via `docker-compose.yml`:

| Variable            | Default       | Description              |
|---------------------|---------------|--------------------------|
| POSTGRES_HOST       | db            | DB hostname              |
| POSTGRES_PORT       | 5432          | DB port                  |
| POSTGRES_DB         | gcpauth       | Database name            |
| POSTGRES_USER       | gcpuser       | DB username              |
| POSTGRES_PASSWORD   | gcppassword   | DB password ← **Change** |
| SECRET_KEY          | (weak default)| Flask session secret ← **Change** |
