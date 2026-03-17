import os, psycopg2, psycopg2.extras, json, io, smtplib, logging, threading, uuid as _uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from functools  import wraps
from datetime   import datetime, date, timedelta, timezone

import pandas as pd
from flask import (Flask, request, jsonify, render_template,
                   send_file, redirect, url_for, session)
from werkzeug.security import check_password_hash
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron         import CronTrigger

# ── App ───────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key  = os.environ.get("SECRET_KEY", "dev-secret-change-me")
log = logging.getLogger(__name__)

# ── DB ────────────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        host     = os.environ.get("POSTGRES_HOST", "localhost"),
        port     = int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname   = os.environ.get("POSTGRES_DB",   "gcpauth"),
        user     = os.environ.get("POSTGRES_USER",  "gcpuser"),
        password = os.environ.get("POSTGRES_PASSWORD", "gcppassword"),
    )

def _safe(v, decimals=4):
    try:
        v = float(v or 0)
        return round(max(-9.9e15, min(9.9e15, v)), decimals)
    except Exception:
        return 0.0

# ── User helpers ──────────────────────────────────────────────────────────────
def get_user(username):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT id,username,password_hash,role,is_active FROM users WHERE username=%s", (username,))
        row = cur.fetchone(); cur.close(); conn.close()
        if row:
            return {"id":row[0],"username":row[1],"password_hash":row[2],"role":row[3],"is_active":row[4]}
    except Exception as e: log.error("get_user: %s", e)
    return None

def update_last_login(uid):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("UPDATE users SET last_login=NOW() WHERE id=%s", (uid,))
        conn.commit(); cur.close(); conn.close()
    except Exception as e: log.error("last_login: %s", e)

# ── Billing run helpers ───────────────────────────────────────────────────────
def save_billing_run(run_name, uploaded_by, billing_date,
                     total_rows, matched_rows,
                     total_list_cost, total_after_discount, total_savings,
                     service_breakdown, row_records):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO billing_runs
              (run_name,uploaded_by,billing_date,total_rows,matched_rows,
               total_list_cost,total_after_discount,total_savings)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
        """, (run_name, uploaded_by, billing_date, total_rows, matched_rows,
              _safe(total_list_cost), _safe(total_after_discount), _safe(total_savings)))
        run_id = cur.fetchone()[0]

        psycopg2.extras.execute_batch(cur, """
            INSERT INTO billing_run_services
              (run_id,service_name,list_cost,after_discount,savings,row_count)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, [(run_id, s.get("Service description",""),
               _safe(s.get("list_cost",0)), _safe(s.get("after_discount",0)),
               _safe(s.get("savings",0)),   int(s.get("rows",0)))
              for s in service_breakdown])

        psycopg2.extras.execute_batch(cur, """
            INSERT INTO billing_run_rows
              (run_id,service_desc,sku_desc,usage_amount,usage_unit,
               list_cost,discount_pct,discounted_value,after_discount,cost_per_unit)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [(run_id,
               r.get("Service description",""), r.get("SKU description",""),
               _safe(r.get("Usage amount",0),6), str(r.get("Usage unit",""))[:80],
               _safe(r.get("List cost",0)), _safe(r.get("(₹)discount %",0),4),
               _safe(r.get("Discounted value",0)), _safe(r.get("after Discount",0)),
               _safe(r.get("Cost per Unit",0),8))
              for r in row_records], page_size=500)
        conn.commit()
        return run_id
    except Exception as e:
        conn.rollback(); log.error("save_billing_run: %s", e); raise
    finally:
        cur.close(); conn.close()

def get_billing_history(limit=100):
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id,run_name,uploaded_by,billing_date,processed_at,
                   total_rows,matched_rows,total_list_cost,total_after_discount,total_savings
            FROM billing_runs ORDER BY processed_at DESC LIMIT %s
        """, (limit,))
        rows = cur.fetchall(); cur.close(); conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log.error("get_billing_history: %s", e); return []

def get_run_detail(run_id):
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM billing_runs WHERE id=%s", (run_id,))
        run = dict(cur.fetchone() or {})
        cur.execute("""SELECT service_name,list_cost,after_discount,savings,row_count
                       FROM billing_run_services WHERE run_id=%s ORDER BY list_cost DESC""", (run_id,))
        run["services"] = [dict(r) for r in cur.fetchall()]
        cur.execute("""SELECT service_desc,sku_desc,usage_amount,usage_unit,
                              list_cost,discount_pct,discounted_value,after_discount,cost_per_unit
                       FROM billing_run_rows WHERE run_id=%s ORDER BY id LIMIT 500""", (run_id,))
        run["rows"] = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close(); return run
    except Exception as e:
        log.error("get_run_detail: %s", e); return {}

# ── Email config helpers ──────────────────────────────────────────────────────
def get_email_config():
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM email_alert_config WHERE id=1")
        row = cur.fetchone(); cur.close(); conn.close()
        return dict(row) if row else {}
    except Exception as e:
        log.error("get_email_config: %s", e); return {}

def save_email_config(data, updated_by):
    try:
        conn = get_db(); cur = conn.cursor()
        # migrate: add new columns if missing (idempotent for existing deployments)
        for col_def in [
            "send_mode     VARCHAR(20) NOT NULL DEFAULT 'manual'",
            "schedule_days VARCHAR(64) NOT NULL DEFAULT ''",
        ]:
            try:
                cur.execute(f"ALTER TABLE email_alert_config ADD COLUMN IF NOT EXISTS {col_def}")
                conn.commit()
            except Exception:
                conn.rollback()
        cur.execute("""
            UPDATE email_alert_config SET
              enabled=%s, smtp_host=%s, smtp_port=%s, smtp_user=%s,
              smtp_password=%s, smtp_use_tls=%s, from_address=%s,
              recipients=%s, schedule_time=%s,
              send_mode=%s, schedule_days=%s,
              updated_at=NOW(), updated_by=%s
            WHERE id=1
        """, (data.get("enabled", False),
              data.get("smtp_host",""),    int(data.get("smtp_port",587)),
              data.get("smtp_user",""),    data.get("smtp_password",""),
              data.get("smtp_use_tls", True),
              data.get("from_address",""), data.get("recipients",""),
              data.get("schedule_time","08:00"),
              data.get("send_mode","manual"),
              data.get("schedule_days",""),
              updated_by))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        log.error("save_email_config: %s", e); raise

def log_email(status, recipients, subject, run_id=None, prev_run_id=None,
              total=None, prev_total=None, delta=None, error_msg=None):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO email_alert_log
              (status,recipients,subject,error_msg,run_id,prev_run_id,
               total_after_discount,prev_after_discount,delta)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (status, recipients, subject, error_msg, run_id, prev_run_id,
              _safe(total) if total else None,
              _safe(prev_total) if prev_total else None,
              _safe(delta) if delta else None))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        log.error("log_email: %s", e)

def get_email_logs(limit=30):
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id,sent_at,status,recipients,subject,error_msg,
                   run_id,prev_run_id,total_after_discount,prev_after_discount,delta
            FROM email_alert_log ORDER BY sent_at DESC LIMIT %s
        """, (limit,))
        rows = cur.fetchall(); cur.close(); conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log.error("get_email_logs: %s", e); return []

# ── Compare last two runs for daily delta ─────────────────────────────────────
def get_last_two_runs():
    """Return (current_run, previous_run) dicts, each with services list."""
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id,run_name,billing_date,processed_at,
                   total_list_cost,total_after_discount,total_savings,total_rows
            FROM billing_runs ORDER BY processed_at DESC LIMIT 2
        """)
        rows = [dict(r) for r in cur.fetchall()]
        # fetch services for each
        for run in rows:
            cur.execute("""
                SELECT service_name,list_cost,after_discount,savings,row_count
                FROM billing_run_services WHERE run_id=%s ORDER BY after_discount DESC
            """, (run["id"],))
            run["services"] = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        current = rows[0] if len(rows) >= 1 else None
        previous = rows[1] if len(rows) >= 2 else None
        return current, previous
    except Exception as e:
        log.error("get_last_two_runs: %s", e); return None, None

# ── HTML Email builder ────────────────────────────────────────────────────────
def build_email_html(current, previous):
    """Build a professional HTML email comparing current vs previous run."""
    cur_total  = float(current.get("total_after_discount") or 0)
    prev_total = float(previous.get("total_after_discount") or 0) if previous else None
    delta      = (cur_total - prev_total) if prev_total is not None else None

    def fmt_inr(v):
        try: return "₹{:,.2f}".format(float(v or 0))
        except: return "₹0.00"

    def delta_badge(d):
        if d is None: return ""
        if d > 0:
            return f'<span style="background:#fff3cd;color:#856404;padding:3px 10px;border-radius:12px;font-weight:700;font-size:13px;">▲ {fmt_inr(abs(d))} increased</span>'
        elif d < 0:
            return f'<span style="background:#d1fae5;color:#065f46;padding:3px 10px;border-radius:12px;font-weight:700;font-size:13px;">▼ {fmt_inr(abs(d))} decreased</span>'
        else:
            return f'<span style="background:#e5e7eb;color:#374151;padding:3px 10px;border-radius:12px;font-weight:700;font-size:13px;">→ No change</span>'

    # build prev service lookup (after_discount from previous run for delta)
    prev_svc_map = {}
    if previous:
        for s in previous.get("services", []):
            prev_svc_map[s["service_name"]] = float(s.get("after_discount") or 0)

    # service rows
    svc_rows = ""
    for s in current.get("services", []):
        name      = s["service_name"] or "Unknown"
        list_cost = float(s.get("list_cost") or 0)
        cur_val   = float(s.get("after_discount") or 0)   # After Discount column
        pre_val   = prev_svc_map.get(name)                # previous run's after_discount for delta
        sav       = float(s.get("savings") or 0)
        sav_pct   = (sav / list_cost * 100) if list_cost else 0

        if pre_val is not None:
            d = cur_val - pre_val
            if d > 0:
                diff_cell = f'<td style="padding:12px 16px;text-align:right;color:#b45309;font-weight:600;">▲ {fmt_inr(d)}</td>'
            elif d < 0:
                diff_cell = f'<td style="padding:12px 16px;text-align:right;color:#059669;font-weight:600;">▼ {fmt_inr(abs(d))}</td>'
            else:
                diff_cell = f'<td style="padding:12px 16px;text-align:right;color:#6b7280;">—</td>'
        else:
            diff_cell = '<td style="padding:12px 16px;text-align:right;color:#6b7280;font-style:italic;">New</td>'

        svc_rows += f"""
        <tr style="border-bottom:1px solid #f3f4f6;">
          <td style="padding:12px 16px;font-weight:500;color:#111827;">{name}</td>
          <td style="padding:12px 16px;text-align:right;color:#374151;font-weight:600;">{fmt_inr(list_cost)}</td>
          <td style="padding:12px 16px;text-align:right;font-weight:700;color:#1d4ed8;">{fmt_inr(cur_val)}</td>
          {diff_cell}
          <td style="padding:12px 16px;text-align:right;color:#059669;font-weight:600;">{fmt_inr(sav)} <span style="font-size:11px;color:#9ca3af;font-weight:400;">({sav_pct:.1f}%)</span></td>
        </tr>"""

    cur_date   = str(current.get("billing_date") or current.get("processed_at",""))[0:10]
    prev_date  = str(previous.get("billing_date") or previous.get("processed_at",""))[0:10] if previous else "—"
    proc_time  = str(current.get("processed_at",""))[:19]
    run_name   = current.get("run_name","—")

    subject_delta = ""
    if delta is not None:
        if delta > 0:   subject_delta = f"↑ {fmt_inr(delta)} vs previous"
        elif delta < 0: subject_delta = f"↓ {fmt_inr(abs(delta))} vs previous"
        else:           subject_delta = "No change vs previous"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>GCP Billing Alert</title></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:32px 0;">
<tr><td align="center">
<table width="640" cellpadding="0" cellspacing="0" style="max-width:640px;width:100%;">

  <!-- HEADER -->
  <tr><td style="background:linear-gradient(135deg,#1e3a5f 0%,#1a56db 100%);border-radius:16px 16px 0 0;padding:36px 40px;">
    <table width="100%"><tr>
      <td>
        <div style="font-size:22px;font-weight:700;color:#ffffff;margin-bottom:4px;">☁ GCP Billing Alert</div>
        <div style="font-size:13px;color:#93c5fd;letter-spacing:1px;text-transform:uppercase;">Daily Cost Summary</div>
      </td>
      <td align="right">
        <div style="background:rgba(255,255,255,0.15);border-radius:10px;padding:10px 16px;display:inline-block;">
          <div style="font-size:11px;color:#bfdbfe;text-transform:uppercase;letter-spacing:1px;margin-bottom:2px;">Billing Date</div>
          <div style="font-size:16px;font-weight:700;color:#ffffff;">{cur_date}</div>
        </div>
      </td>
    </tr></table>
  </td></tr>

  <!-- BODY -->
  <tr><td style="background:#ffffff;padding:36px 40px;">

    <!-- Top summary cards -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:32px;">
    <tr>
      <td width="32%" style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:12px;padding:20px;text-align:center;">
        <div style="font-size:11px;color:#0369a1;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;">List Cost</div>
        <div style="font-size:22px;font-weight:700;color:#0c4a6e;">{fmt_inr(current.get("total_list_cost",0))}</div>
      </td>
      <td width="4%"></td>
      <td width="32%" style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:20px;text-align:center;">
        <div style="font-size:11px;color:#15803d;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;">After Discount</div>
        <div style="font-size:22px;font-weight:700;color:#14532d;">{fmt_inr(cur_total)}</div>
      </td>
      <td width="4%"></td>
      <td width="28%" style="background:#fffbeb;border:1px solid #fde68a;border-radius:12px;padding:20px;text-align:center;">
        <div style="font-size:11px;color:#b45309;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;">Savings</div>
        <div style="font-size:22px;font-weight:700;color:#78350f;">{fmt_inr(current.get("total_savings",0))}</div>
      </td>
    </tr></table>

    <!-- Day-over-day comparison -->
    {"" if delta is None else f'''
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#f8fafc;border:2px solid {"#fde68a" if delta > 0 else "#bbf7d0" if delta < 0 else "#e5e7eb"};border-radius:12px;padding:20px;margin-bottom:32px;">
    <tr>
      <td>
        <div style="font-size:13px;color:#6b7280;margin-bottom:6px;">Day-over-Day Change</div>
        <div style="display:flex;align-items:center;gap:12px;">
          {delta_badge(delta)}
        </div>
        <div style="font-size:12px;color:#9ca3af;margin-top:8px;">
          Previous run ({prev_date}): <strong style="color:#374151;">{fmt_inr(prev_total)}</strong>
          &nbsp;→&nbsp;
          Current ({cur_date}): <strong style="color:#374151;">{fmt_inr(cur_total)}</strong>
        </div>
      </td>
    </tr>
    </table>'''}

    <!-- Service breakdown table -->
    <div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:16px;">
      💰 Total &amp; After-Discount Amount — Service Wise
    </div>
    <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;margin-bottom:32px;">
      <thead>
        <tr style="background:#f9fafb;">
          <th style="padding:12px 16px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Service</th>
          <th style="padding:12px 16px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;font-weight:600;">List Cost</th>
          <th style="padding:12px 16px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;font-weight:600;">After Discount</th>
          <th style="padding:12px 16px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Change vs Prev</th>
          <th style="padding:12px 16px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Savings</th>
        </tr>
      </thead>
      <tbody>{svc_rows}</tbody>
      <tfoot>
        <tr style="background:#f9fafb;border-top:2px solid #e5e7eb;">
          <td style="padding:14px 16px;font-weight:700;color:#111827;">TOTAL</td>
          <td style="padding:14px 16px;text-align:right;font-weight:700;color:#374151;">{fmt_inr(current.get("total_list_cost",0))}</td>
          <td style="padding:14px 16px;text-align:right;font-weight:700;color:#1d4ed8;">{fmt_inr(cur_total)}</td>
          <td style="padding:14px 16px;text-align:right;font-weight:700;">{delta_badge(delta)}</td>
          <td style="padding:14px 16px;text-align:right;font-weight:700;color:#059669;">{fmt_inr(current.get("total_savings",0))}</td>
        </tr>
      </tfoot>
    </table>

    <!-- Meta info -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#f8fafc;border-radius:10px;padding:16px 20px;margin-bottom:16px;">
    <tr>
      <td style="font-size:12px;color:#6b7280;">
        <strong style="color:#374151;">Run ID:</strong> #{current.get("id","—")} &nbsp;|&nbsp;
        <strong style="color:#374151;">File:</strong> {run_name} &nbsp;|&nbsp;
        <strong style="color:#374151;">Processed:</strong> {proc_time} UTC &nbsp;|&nbsp;
        <strong style="color:#374151;">Rows:</strong> {current.get("total_rows",0):,}
      </td>
    </tr>
    </table>

  </td></tr>

  <!-- FOOTER -->
  <tr><td style="background:#1e3a5f;border-radius:0 0 16px 16px;padding:24px 40px;text-align:center;">
    <div style="font-size:12px;color:#93c5fd;margin-bottom:4px;">GCP Billing Discount Tool — Automated Alert</div>
    <div style="font-size:11px;color:#60a5fa;">This email was generated automatically. Do not reply.</div>
  </td></tr>

</table>
</td></tr>
</table>
</body></html>"""

    subject = f"GCP Billing Alert | {cur_date} | After Discount: {fmt_inr(cur_total)}"
    if subject_delta:
        subject += f" | {subject_delta}"

    return subject, html

# ── Send email ────────────────────────────────────────────────────────────────
def send_alert_email(cfg=None, run_id_override=None):
    """Core send function. Used by scheduler and by manual test trigger."""
    if cfg is None:
        cfg = get_email_config()
    if not cfg or not cfg.get("enabled"):
        return False, "Email alerts are disabled"

    current, previous = get_last_two_runs()
    if not current:
        return False, "No billing runs found in database"

    subject, html_body = build_email_html(current, previous)

    recipients_raw = cfg.get("recipients","")
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    if not recipients:
        return False, "No recipients configured"

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = cfg.get("from_address","")
        msg["To"]      = ", ".join(recipients)
        msg.attach(MIMEText(html_body, "html"))

        smtp_cls = smtplib.SMTP
        server = smtp_cls(cfg["smtp_host"], int(cfg["smtp_port"]), timeout=15)
        if cfg.get("smtp_use_tls", True):
            server.starttls()
        if cfg.get("smtp_user") and cfg.get("smtp_password"):
            server.login(cfg["smtp_user"], cfg["smtp_password"])
        server.sendmail(cfg.get("from_address",""), recipients, msg.as_string())
        server.quit()

        prev_total = float(previous.get("total_after_discount") or 0) if previous else None
        cur_total  = float(current.get("total_after_discount") or 0)
        delta      = (cur_total - prev_total) if prev_total is not None else None

        log_email("sent", recipients_raw, subject,
                  run_id=current["id"],
                  prev_run_id=previous["id"] if previous else None,
                  total=cur_total, prev_total=prev_total, delta=delta)
        log.info("Email sent: %s", subject)
        return True, subject

    except Exception as e:
        log_email("failed", recipients_raw, subject, error_msg=str(e))
        log.error("Email failed: %s", e)
        return False, str(e)

# ── Scheduler ─────────────────────────────────────────────────────────────────
scheduler = BackgroundScheduler(daemon=True)

def scheduled_email_job():
    log.info("Scheduled email job running…")
    ok, msg = send_alert_email()
    log.info("Scheduled email result: ok=%s msg=%s", ok, msg)

def reschedule_email(schedule_time="08:00", send_mode="daily", schedule_days=""):
    """Remove existing job and add a new one based on send_mode."""
    try:
        scheduler.remove_job("daily_email")
    except Exception:
        pass
    if send_mode == "manual":
        log.info("Email mode=manual — no scheduled job")
        return
    try:
        h, m = schedule_time.split(":")
        h, m = int(h), int(m)
        if send_mode == "daily":
            trigger = CronTrigger(hour=h, minute=m)
        elif send_mode == "weekly":
            days = schedule_days.strip(",") or "0"
            trigger = CronTrigger(day_of_week=days, hour=h, minute=m)
        elif send_mode == "monthly":
            trigger = CronTrigger(day=1, hour=h, minute=m)
        else:
            trigger = CronTrigger(hour=h, minute=m)
        scheduler.add_job(scheduled_email_job, trigger,
                          id="daily_email", replace_existing=True)
        log.info("Email scheduled: mode=%s time=%s:%s days=%s", send_mode, h, m, schedule_days)
    except Exception as e:
        log.error("reschedule_email: %s", e)

def init_scheduler():
    cfg = get_email_config()
    if cfg and cfg.get("enabled") and cfg.get("send_mode","manual") != "manual":
        reschedule_email(
            cfg.get("schedule_time","08:00"),
            cfg.get("send_mode","daily"),
            cfg.get("schedule_days","")
        )
    if not scheduler.running:
        scheduler.start()

# ── Auth ──────────────────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login", next=request.path))
        if session["user"]["role"] != "admin":
            return jsonify({"error": "Admin only"}), 403
        return f(*args, **kwargs)
    return decorated

# ── Discount helpers (DB-backed, replaces xlsx file) ─────────────────────────
def load_discounts():
    """Load all discount SKUs from DB. Returns (df, discount_map, pricing_map).
    discount_map: {sku -> discount_pct (0-100)}
    pricing_map:  {sku -> {"pricing_model": "discount"|"unit", "discount": pct, "unit_price": float|None}}
    """
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Add new columns if they don't exist yet (idempotent migration)
        for ddl in [
            "ALTER TABLE discount_skus ADD COLUMN IF NOT EXISTS pricing_model VARCHAR(20) NOT NULL DEFAULT 'discount'",
            "ALTER TABLE discount_skus ADD COLUMN IF NOT EXISTS unit_price NUMERIC(20,8) DEFAULT NULL",
        ]:
            try:
                cur2 = conn.cursor(); cur2.execute(ddl); conn.commit(); cur2.close()
            except Exception: conn.rollback()
        cur.execute("""
            SELECT service_description AS "Service description",
                   sku_description     AS "SKU description",
                   COALESCE(pricing_model, 'discount') AS "pricing_model",
                   discount            AS "Discount",
                   unit_price          AS "unit_price"
            FROM discount_skus
            ORDER BY service_description, sku_description
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
    except Exception as e:
        log.error("load_discounts: %s", e)
        rows = []

    if rows:
        df = pd.DataFrame([dict(r) for r in rows])
    else:
        df = pd.DataFrame(columns=["Service description", "SKU description", "pricing_model", "Discount", "unit_price"])

    df["SKU description"]     = df["SKU description"].astype(str).str.strip()
    df["Service description"] = df["Service description"].astype(str).str.strip()
    df["Discount"]            = pd.to_numeric(df["Discount"], errors="coerce").fillna(0)
    df["pricing_model"]       = df["pricing_model"].fillna("discount").astype(str)
    df["unit_price"]          = pd.to_numeric(df["unit_price"], errors="coerce")

    discount_map = df.set_index("SKU description")["Discount"].mul(100).to_dict()
    pricing_map  = {
        row["SKU description"]: {
            "pricing_model": row["pricing_model"],
            "discount":      float(row["Discount"]) * 100,
            "unit_price":    float(row["unit_price"]) if pd.notna(row["unit_price"]) else None,
        }
        for _, row in df.iterrows()
    }
    return df, discount_map, pricing_map


def get_discount_sku_count():
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM discount_skus")
        n = cur.fetchone()[0]; cur.close(); conn.close()
        return n
    except Exception:
        return 0


def _apply_discount_changes_to_db(changes, updated_by):
    """Apply add/edit/delete changes to discount_skus table."""
    conn = get_db()
    try:
        cur = conn.cursor()
        for ch in changes:
            action        = ch.get("action")
            sku           = str(ch.get("sku", "")).strip()
            svc           = str(ch.get("service", "")).strip()
            disc          = float(ch.get("discount_pct", 0)) / 100.0
            pricing_model = str(ch.get("pricing_model", "discount")).strip() or "discount"
            unit_price_raw = ch.get("unit_price")
            unit_price    = float(unit_price_raw) if unit_price_raw not in (None, "", "null") else None

            if action == "delete":
                cur.execute("DELETE FROM discount_skus WHERE sku_description=%s", (sku,))
            elif action in ("edit", "add"):
                cur.execute("""
                    INSERT INTO discount_skus
                        (service_description, sku_description, pricing_model, discount, unit_price, updated_at, updated_by)
                    VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                    ON CONFLICT (sku_description) DO UPDATE SET
                        service_description = EXCLUDED.service_description,
                        pricing_model       = EXCLUDED.pricing_model,
                        discount            = EXCLUDED.discount,
                        unit_price          = EXCLUDED.unit_price,
                        updated_at          = NOW(),
                        updated_by          = EXCLUDED.updated_by
                """, (svc, sku, pricing_model, disc, unit_price, updated_by))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback(); log.error("_apply_discount_changes_to_db: %s", e); raise
    finally:
        conn.close()

# ── Routes: Auth ──────────────────────────────────────────────────────────────
@app.route("/login", methods=["GET","POST"])
def login():
    if "user" in session: return redirect(url_for("index"))
    error = None
    if request.method == "POST":
        username = request.form.get("username","").strip()
        password = request.form.get("password","")
        next_url = request.form.get("next","") or url_for("index")
        user = get_user(username)
        if not user:                                          error = "Invalid username or password."
        elif not user["is_active"]:                          error = "Account disabled."
        elif not check_password_hash(user["password_hash"], password): error = "Invalid username or password."
        else:
            session["user"] = {"id":user["id"],"username":user["username"],"role":user["role"]}
            update_last_login(user["id"])
            return redirect(next_url)
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    username = session.get("user",{}).get("username","")
    session.clear()
    return render_template("logout.html", username=username)

# ── Routes: Main pages ────────────────────────────────────────────────────────
@app.route("/")
@login_required
def index():
    sku_count = get_discount_sku_count()
    return render_template("index.html", sku_count=sku_count, current_user=session["user"])

@app.route("/matrix")
@login_required
def matrix():
    df, _, _ = load_discounts()
    skus     = [{"service":r["Service description"],"sku":r["SKU description"],"discount_pct":round(r["Discount"]*100,2),"pricing_model":r.get("pricing_model","discount"),"unit_price":r.get("unit_price")} for _,r in df.iterrows()]
    services = sorted(df["Service description"].unique().tolist())
    return render_template("matrix.html", skus=skus, services=services, sku_count=len(df), current_user=session["user"])

@app.route("/analytics")
@login_required
def analytics():
    df, _, _ = load_discounts()
    stats = (df.groupby("Service description")["Discount"]
               .agg(count="count", avg=lambda x:round(x.mean()*100,2),
                    min=lambda x:round(x.min()*100,2), max=lambda x:round(x.max()*100,2))
               .reset_index())
    return render_template("analytics.html", service_stats=stats.to_dict("records"), sku_count=len(df), current_user=session["user"])


@app.route("/discount-editor")
@login_required
def discount_editor():
    df, _, _ = load_discounts()
    skus     = [{"service": r["Service description"], "sku": r["SKU description"],
                 "discount_pct": round(r["Discount"] * 100, 2),
                 "pricing_model": r.get("pricing_model", "discount"),
                 "unit_price": float(r["unit_price"]) if pd.notna(r.get("unit_price")) else None}
                for _, r in df.iterrows()]
    services = sorted(df["Service description"].unique().tolist())
    return render_template("discount_editor.html", skus=skus, services=services,
                           sku_count=len(df), current_user=session["user"])

@app.route("/discount-editor/save", methods=["POST"])
@login_required
def discount_editor_save():
    try:
        data    = request.get_json(force=True)
        changes = data.get("changes", [])
        _apply_discount_changes_to_db(changes, session["user"]["username"])
        df, _, _ = load_discounts()
        return jsonify({
            "ok": True,
            "sku_count":     len(df),
            "service_count": df["Service description"].nunique()
        })
    except Exception as e:
        log.error("discount_editor_save: %s", e)
        return jsonify({"error": str(e)}), 500


# ── Routes: Discount Import (xlsx → DB) ──────────────────────────────────────
@app.route("/discount-editor/check-import", methods=["POST"])
@login_required
def discount_editor_check_import():
    """
    Pre-flight: parse uploaded xlsx, return list of rows with duplicate info.
    Supports two file formats:
      OLD: Service description, SKU description, Discount
      NEW: service_description, sku_description, pricing_model, discount, unit_price
    Client decides skip/override/update-discount per duplicate before committing.
    """
    try:
        file = request.files.get("file")
        if not file:
            return jsonify({"error": "No file uploaded"}), 400
        if not file.filename.lower().endswith((".xlsx", ".xls")):
            return jsonify({"error": "Only .xlsx / .xls files are accepted"}), 400

        df = pd.read_excel(file)
        df.columns = df.columns.str.strip()

        # ── Detect file format ────────────────────────────────────────────────
        NEW_COLS = ["service_description", "sku_description", "pricing_model", "discount", "unit_price"]
        OLD_COLS = ["Service description", "SKU description", "Discount"]

        is_new_format = all(c in df.columns for c in NEW_COLS)
        is_old_format = all(c in df.columns for c in OLD_COLS)

        if not is_new_format and not is_old_format:
            # Try to give a helpful error
            missing_new = [c for c in NEW_COLS if c not in df.columns]
            missing_old = [c for c in OLD_COLS if c not in df.columns]
            return jsonify({"error":
                f"Unrecognised file format. "
                f"For NEW format, missing: {', '.join(missing_new)}. "
                f"For OLD format, missing: {', '.join(missing_old)}."}), 400

        file_format = "new" if is_new_format else "old"

        if is_new_format:
            df = df.rename(columns={
                "service_description": "Service description",
                "sku_description":     "SKU description",
            })
            df["pricing_model"] = df["pricing_model"].fillna("discount").astype(str).str.strip().str.lower()
            df["unit_price"]    = pd.to_numeric(df["unit_price"], errors="coerce")
        else:
            df["pricing_model"] = "discount"
            df["unit_price"]    = None

        df["SKU description"]     = df["SKU description"].astype(str).str.strip()
        df["Service description"] = df["Service description"].astype(str).str.strip()
        df["Discount"]            = pd.to_numeric(df["discount"] if is_new_format else df["Discount"],
                                                  errors="coerce").fillna(0)
        df = df.dropna(subset=["SKU description"])
        df = df[df["SKU description"] != ""]

        # Fetch existing SKUs from DB
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        incoming_skus = df["SKU description"].tolist()
        cur.execute("""
            SELECT sku_description, service_description,
                   COALESCE(pricing_model,'discount') AS pricing_model,
                   discount,
                   unit_price
            FROM discount_skus
            WHERE sku_description = ANY(%s)
        """, (incoming_skus,))
        existing = {r["sku_description"]: dict(r) for r in cur.fetchall()}
        cur.close(); conn.close()

        rows = []
        for _, r in df.iterrows():
            sku        = r["SKU description"]
            svc        = r["Service description"]
            disc       = float(r["Discount"])
            pm         = str(r.get("pricing_model", "discount"))
            up_raw     = r.get("unit_price")
            unit_price = float(up_raw) if up_raw is not None and not (isinstance(up_raw, float) and pd.isna(up_raw)) else None
            ex         = existing.get(sku)
            rows.append({
                "sku":           sku,
                "service":       svc,
                "pricing_model": pm,
                "discount_pct":  round(disc * 100, 4),
                "unit_price":    unit_price,
                "is_duplicate":  ex is not None,
                "existing_discount_pct": round(float(ex["discount"]) * 100, 4) if ex else None,
                "existing_service":      ex["service_description"] if ex else None,
                "existing_pricing_model": ex.get("pricing_model","discount") if ex else None,
                "existing_unit_price":   float(ex["unit_price"]) if ex and ex.get("unit_price") else None,
            })

        duplicates = [r for r in rows if r["is_duplicate"]]
        return jsonify({
            "ok":             True,
            "file_format":    file_format,
            "total_rows":     len(rows),
            "duplicate_count": len(duplicates),
            "new_count":      len(rows) - len(duplicates),
            "rows":           rows,
            "headers":        list(df.columns),
            "filename":       file.filename,
        })
    except Exception as e:
        log.error("discount_editor_check_import: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/discount-editor/commit-import", methods=["POST"])
@login_required
def discount_editor_commit_import():
    """
    Commit import with per-row decisions for duplicates.
    Body: { rows: [{sku, service, pricing_model, discount_pct, unit_price, action}], filename }
      action = 'insert' | 'skip' | 'override' | 'update_discount'
    """
    try:
        data     = request.get_json(force=True)
        rows     = data.get("rows", [])
        filename = data.get("filename", "")
        if not rows:
            return jsonify({"error": "No rows provided"}), 400

        inserted = updated = skipped = 0
        conn = get_db()
        cur  = conn.cursor()
        for r in rows:
            action        = r.get("action", "insert")
            sku           = str(r.get("sku", "")).strip()
            svc           = str(r.get("service", "")).strip()
            disc          = float(r.get("discount_pct", 0)) / 100.0
            pricing_model = str(r.get("pricing_model", "discount")).strip() or "discount"
            up_raw        = r.get("unit_price")
            unit_price    = float(up_raw) if up_raw not in (None, "", "null") else None

            if action == "skip":
                skipped += 1
                continue

            if action == "insert":
                cur.execute("""
                    INSERT INTO discount_skus
                        (service_description, sku_description, pricing_model, discount, unit_price, updated_at, updated_by)
                    VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                    ON CONFLICT (sku_description) DO NOTHING
                """, (svc, sku, pricing_model, disc, unit_price, session["user"]["username"]))
                inserted += cur.rowcount

            elif action == "override":
                cur.execute("""
                    INSERT INTO discount_skus
                        (service_description, sku_description, pricing_model, discount, unit_price, updated_at, updated_by)
                    VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                    ON CONFLICT (sku_description) DO UPDATE SET
                        service_description = EXCLUDED.service_description,
                        pricing_model       = EXCLUDED.pricing_model,
                        discount            = EXCLUDED.discount,
                        unit_price          = EXCLUDED.unit_price,
                        updated_at          = NOW(),
                        updated_by          = EXCLUDED.updated_by
                """, (svc, sku, pricing_model, disc, unit_price, session["user"]["username"]))
                updated += 1

            elif action == "update_discount":
                cur.execute("""
                    UPDATE discount_skus
                    SET discount      = %s,
                        pricing_model = %s,
                        unit_price    = %s,
                        updated_at    = NOW(),
                        updated_by    = %s
                    WHERE sku_description = %s
                """, (disc, pricing_model, unit_price, session["user"]["username"], sku))
                updated += 1

        # Log the import
        cur.execute("""
            INSERT INTO discount_import_log
                (imported_by, filename, total_rows, inserted, updated, skipped)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (session["user"]["username"], filename, len(rows), inserted, updated, skipped))
        conn.commit(); cur.close(); conn.close()

        df, _, _ = load_discounts()
        return jsonify({
            "ok":            True,
            "inserted":      inserted,
            "updated":       updated,
            "skipped":       skipped,
            "sku_count":     len(df),
            "service_count": df["Service description"].nunique(),
        })
    except Exception as e:
        log.error("discount_editor_commit_import: %s", e)
        return jsonify({"error": str(e)}), 500

@login_required
def export():
    return render_template("export.html", current_user=session["user"])

@app.route("/history")
@login_required
def history():
    runs = get_billing_history(100)
    return render_template("history.html", runs=runs, current_user=session["user"])

@app.route("/history/<int:run_id>")
@login_required
def history_detail(run_id):
    run = get_run_detail(run_id)
    if not run: return redirect(url_for("history"))
    return render_template("history_detail.html", run=run, current_user=session["user"])

@app.route("/history/<int:run_id>/delete", methods=["POST"])
@admin_required
def history_delete(run_id):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("DELETE FROM billing_runs WHERE id=%s", (run_id,))
        conn.commit(); cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Routes: Email alerts ──────────────────────────────────────────────────────
@app.route("/alerts")
@admin_required
def alerts():
    cfg  = get_email_config()
    logs = get_email_logs(30)
    return render_template("alerts.html", cfg=cfg, logs=logs, current_user=session["user"])

@app.route("/alerts/save", methods=["POST"])
@admin_required
def alerts_save():
    try:
        data = request.json
        # keep existing password if blank sent
        if not data.get("smtp_password","").strip():
            existing = get_email_config()
            data["smtp_password"] = existing.get("smtp_password","")
        save_email_config(data, session["user"]["username"])
        # reschedule based on mode
        if data.get("enabled") and data.get("send_mode","manual") != "manual":
            reschedule_email(
                data.get("schedule_time","08:00"),
                data.get("send_mode","daily"),
                data.get("schedule_days","")
            )
        else:
            try: scheduler.remove_job("daily_email")
            except: pass
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── In-memory job store for async test sends ──────────────────────────────────
_email_jobs: dict = {}   # job_id → {"status": "pending"|"done"|"error", "message": str}

def _send_in_background(job_id: str, cfg=None):
    """Run send_alert_email in a daemon thread; update _email_jobs when done."""
    try:
        ok, msg = send_alert_email(cfg=cfg)
        _email_jobs[job_id] = {"status": "done" if ok else "error", "message": msg}
    except Exception as e:
        _email_jobs[job_id] = {"status": "error", "message": str(e)}

@app.route("/alerts/validate", methods=["POST"])
@admin_required
def alerts_validate():
    """Async SMTP connection validator — returns job_id immediately."""
    data   = request.json or {}
    job_id = str(_uuid.uuid4())
    _email_jobs[job_id] = {"status": "pending", "message": "Connecting…"}

    def _validate(jid, cfg_data):
        errors = []
        steps  = []
        try:
            host = cfg_data.get("smtp_host","").strip()
            port = int(cfg_data.get("smtp_port") or 587)
            user = cfg_data.get("smtp_user","").strip()
            pwd  = cfg_data.get("smtp_password","").strip()
            from_addr = cfg_data.get("from_address","").strip()
            use_tls   = cfg_data.get("smtp_use_tls", True)
            recipients_raw = cfg_data.get("recipients","").strip()

            # ── Field checks ───────────────────────────────────────────
            if not host:        errors.append("SMTP Host is required")
            if not (1 <= port <= 65535): errors.append(f"Invalid port: {port}")
            if not user:        errors.append("SMTP Username is required")
            if not from_addr:   errors.append("From Address is required")
            if not recipients_raw: errors.append("At least one recipient is required")

            # email format check
            import re
            email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
            if from_addr and not email_re.match(from_addr):
                errors.append(f"Invalid From Address: {from_addr}")
            bad_recips = [r.strip() for r in recipients_raw.split(",")
                          if r.strip() and not email_re.match(r.strip())]
            if bad_recips:
                errors.append(f"Invalid recipient email(s): {', '.join(bad_recips)}")

            if errors:
                _email_jobs[jid] = {"status": "error", "steps": steps,
                                    "errors": errors, "message": "; ".join(errors)}
                return

            # ── Network / SMTP checks ──────────────────────────────────
            import socket
            steps.append({"label": "Field validation", "ok": True, "detail": "All fields valid"})

            # 1. DNS / TCP connect
            try:
                sock = socket.create_connection((host, port), timeout=8)
                sock.close()
                steps.append({"label": f"TCP connect {host}:{port}", "ok": True, "detail": "Port reachable"})
            except Exception as e:
                steps.append({"label": f"TCP connect {host}:{port}", "ok": False, "detail": str(e)})
                errors.append(f"Cannot connect to {host}:{port} — {e}")
                _email_jobs[jid] = {"status": "error", "steps": steps,
                                    "errors": errors, "message": errors[-1]}
                return

            # 2. SMTP handshake + optional STARTTLS + login
            try:
                server = smtplib.SMTP(host, port, timeout=10)
                ehlo_resp = server.ehlo()
                steps.append({"label": "SMTP EHLO", "ok": True,
                               "detail": f"Server: {ehlo_resp[1][:80].decode(errors='replace') if isinstance(ehlo_resp[1], bytes) else str(ehlo_resp[1])[:80]}"})

                if use_tls:
                    server.starttls()
                    server.ehlo()
                    steps.append({"label": "STARTTLS", "ok": True, "detail": "TLS negotiated successfully"})

                if user and pwd:
                    server.login(user, pwd)
                    steps.append({"label": "SMTP Login", "ok": True, "detail": f"Authenticated as {user}"})
                elif user and not pwd:
                    # check if existing password in DB
                    existing = get_email_config()
                    stored_pwd = existing.get("smtp_password","")
                    if stored_pwd:
                        server.login(user, stored_pwd)
                        steps.append({"label": "SMTP Login", "ok": True,
                                      "detail": f"Authenticated as {user} (using stored password)"})
                    else:
                        steps.append({"label": "SMTP Login", "ok": False,
                                      "detail": "No password provided or stored"})
                        errors.append("SMTP password is required for authentication")

                server.quit()
                steps.append({"label": "SMTP Quit", "ok": True, "detail": "Connection closed cleanly"})

            except smtplib.SMTPAuthenticationError as e:
                steps.append({"label": "SMTP Login", "ok": False, "detail": str(e)})
                errors.append(f"Authentication failed — check username/password. {e}")
            except smtplib.SMTPException as e:
                steps.append({"label": "SMTP handshake", "ok": False, "detail": str(e)})
                errors.append(f"SMTP error: {e}")

            if errors:
                _email_jobs[jid] = {"status": "error",  "steps": steps,
                                    "errors": errors, "message": errors[-1]}
            else:
                _email_jobs[jid] = {"status": "done", "steps": steps,
                                    "errors": [], "message": "All checks passed ✓"}

        except Exception as e:
            _email_jobs[jid] = {"status": "error", "steps": steps,
                                "errors": [str(e)], "message": str(e)}

    t = threading.Thread(target=_validate, args=(job_id, data), daemon=True)
    t.start()
    return jsonify({"job_id": job_id}), 202


@app.route("/alerts/test", methods=["POST"])
@admin_required
def alerts_test():
    job_id = str(_uuid.uuid4())
    _email_jobs[job_id] = {"status": "pending", "message": "Sending…"}
    t = threading.Thread(target=_send_in_background, args=(job_id,), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": job_id}), 202

@app.route("/alerts/send-for-run/<int:run_id>", methods=["POST"])
@login_required
def alerts_send_for_run(run_id):
    """Trigger an email report for a specific billing run.
    Uses that run as 'current' and the preceding run as 'previous' for delta."""
    def _send(jid, rid):
        try:
            cfg = get_email_config()
            if not cfg or not cfg.get("enabled"):
                _email_jobs[jid] = {"status": "error", "message": "Email alerts are disabled — enable and configure in Email Alerts settings."}
                return

            # Fetch the target run + the run just before it
            conn = get_db(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT id,run_name,billing_date,processed_at,
                       total_list_cost,total_after_discount,total_savings,total_rows
                FROM billing_runs WHERE id=%s
            """, (rid,))
            current = cur.fetchone()
            if not current:
                _email_jobs[jid] = {"status": "error", "message": f"Run #{rid} not found"}
                cur.close(); conn.close(); return
            current = dict(current)

            cur.execute("""
                SELECT id,run_name,billing_date,processed_at,
                       total_list_cost,total_after_discount,total_savings,total_rows
                FROM billing_runs WHERE processed_at < %s
                ORDER BY processed_at DESC LIMIT 1
            """, (current["processed_at"],))
            prev_row = cur.fetchone()
            previous = dict(prev_row) if prev_row else None

            # fetch services for both
            for run in [r for r in [current, previous] if r]:
                cur.execute("""
                    SELECT service_name,list_cost,after_discount,savings,row_count
                    FROM billing_run_services WHERE run_id=%s ORDER BY after_discount DESC
                """, (run["id"],))
                run["services"] = [dict(r) for r in cur.fetchall()]
            cur.close(); conn.close()

            subject, html_body = build_email_html(current, previous)
            recipients_raw = cfg.get("recipients","")
            recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
            if not recipients:
                _email_jobs[jid] = {"status": "error", "message": "No recipients configured"}
                return

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"]    = cfg.get("from_address","")
            msg["To"]      = ", ".join(recipients)
            msg.attach(MIMEText(html_body, "html"))

            server = smtplib.SMTP(cfg["smtp_host"], int(cfg["smtp_port"]), timeout=15)
            if cfg.get("smtp_use_tls", True):
                server.starttls()
            if cfg.get("smtp_user") and cfg.get("smtp_password"):
                server.login(cfg["smtp_user"], cfg["smtp_password"])
            server.sendmail(cfg.get("from_address",""), recipients, msg.as_string())
            server.quit()

            prev_total = float(previous.get("total_after_discount") or 0) if previous else None
            cur_total  = float(current.get("total_after_discount") or 0)
            delta      = (cur_total - prev_total) if prev_total is not None else None
            log_email("sent", recipients_raw, subject,
                      run_id=current["id"],
                      prev_run_id=previous["id"] if previous else None,
                      total=cur_total, prev_total=prev_total, delta=delta)
            _email_jobs[jid] = {"status": "done", "message": subject}
        except Exception as e:
            log.error("alerts_send_for_run: %s", e)
            _email_jobs[jid] = {"status": "error", "message": str(e)}

    job_id = str(_uuid.uuid4())
    _email_jobs[job_id] = {"status": "pending", "message": "Sending…"}
    t = threading.Thread(target=_send, args=(job_id, run_id), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": job_id}), 202

@app.route("/alerts/test/status/<job_id>")
@admin_required
def alerts_test_status(job_id):
    job = _email_jobs.get(job_id)
    if not job:
        return jsonify({"status": "unknown", "message": "Job not found"}), 404
    return jsonify(job)

@app.route("/alerts/preview")
@admin_required
def alerts_preview():
    """Return raw HTML of the email that would be sent."""
    current, previous = get_last_two_runs()
    if not current:
        return "<p style='font-family:sans-serif;color:red;padding:20px'>No billing runs found.</p>", 200
    _, html = build_email_html(current, previous)
    return html, 200, {"Content-Type": "text/html"}

# ── Process ───────────────────────────────────────────────────────────────────
@app.route("/process", methods=["POST"])
@login_required
def process():
    try:
        file = request.files.get("file")
        if not file: return jsonify({"error":"No file uploaded"}), 400

        billing_date   = request.form.get("billing_date","") or None

        billing_df = pd.read_csv(file)
        billing_df.columns = billing_df.columns.str.strip()
        required = ["Service description","SKU description","Usage amount","Usage unit","List cost"]
        missing  = [c for c in required if c not in billing_df.columns]
        if missing: return jsonify({"error":f"Missing columns: {', '.join(missing)}"}), 400

        billing_df["SKU description"] = billing_df["SKU description"].astype(str).str.strip()
        billing_df["List cost"]       = pd.to_numeric(billing_df["List cost"], errors="coerce").fillna(0)
        billing_df["Usage amount"]    = pd.to_numeric(billing_df["Usage amount"], errors="coerce").fillna(0)

        _, discount_map, pricing_map = load_discounts()

        # ── Per-row calculation depending on pricing_model ──────────────────
        def calc_row(row):
            sku  = row["SKU description"]
            pm   = pricing_map.get(sku, {})
            model = pm.get("pricing_model", "discount") if pm else "discount"

            if model == "unit":
                unit_price = pm.get("unit_price")
                if unit_price is not None:
                    after  = float(row["Usage amount"]) * float(unit_price)
                    disc_v = max(float(row["List cost"]) - after, 0)
                    disc_p = (disc_v / float(row["List cost"]) * 100) if float(row["List cost"]) else 0
                else:
                    # unit model but no unit_price set → fall through to 0% discount
                    disc_p = 0
                    disc_v = 0
                    after  = float(row["List cost"])
            else:
                # discount model (default / old format)
                disc_p = float(discount_map.get(sku, 0))
                disc_v = float(row["List cost"]) * disc_p / 100
                after  = float(row["List cost"]) - disc_v

            return pd.Series({
                "(₹)discount %":   round(disc_p, 4),
                "Discounted value": round(disc_v, 4),
                "after Discount":   round(after,  4),
            })

        computed = billing_df.apply(calc_row, axis=1)
        billing_df["(₹)discount %"]   = computed["(₹)discount %"]
        billing_df["Discounted value"] = computed["Discounted value"]
        billing_df["after Discount"]   = computed["after Discount"]

        # Cost per Unit = after_discount / usage_amount (0 when usage_amount is 0)
        billing_df["Cost per Unit"] = billing_df.apply(
            lambda r: round(r["after Discount"] / r["Usage amount"], 8)
                      if r["Usage amount"] and float(r["Usage amount"]) != 0 else 0.0,
            axis=1
        )

        billing_df = billing_df.round(4)

        matched = int((billing_df["(₹)discount %"] > 0).sum())
        svc = (billing_df.groupby("Service description")
                         .agg(list_cost=("List cost","sum"), after_discount=("after Discount","sum"), rows=("List cost","count"))
                         .reset_index())
        svc["savings"] = svc["list_cost"] - svc["after_discount"]

        output_cols = ["Service description","SKU description","Usage amount","Usage unit",
                       "List cost","(₹)discount %","Discounted value","after Discount","Cost per Unit"]
        result_df   = billing_df[output_cols]

        total_list    = float(billing_df["List cost"].sum())
        total_after   = float(billing_df["after Discount"].sum())
        total_savings = float(billing_df["Discounted value"].sum())

        run_id = save_billing_run(
            run_name=file.filename, uploaded_by=session["user"]["username"],
            billing_date=billing_date, total_rows=len(result_df), matched_rows=matched,
            total_list_cost=total_list, total_after_discount=total_after, total_savings=total_savings,
            service_breakdown=svc.round(4).to_dict("records"), row_records=result_df.to_dict("records"),
        )

        # Build unmatched SKU list: rows with no discount match (discount % == 0)
        unmatched_mask = billing_df["(₹)discount %"] == 0
        unmatched_df = billing_df[unmatched_mask][["Service description", "SKU description", "List cost"]].copy()
        unmatched_df = (unmatched_df.groupby(["Service description", "SKU description"])
                                    .agg(total_list_cost=("List cost", "sum"), occurrences=("List cost", "count"))
                                    .reset_index()
                                    .sort_values("total_list_cost", ascending=False))
        unmatched_skus = unmatched_df.round(4).to_dict("records")

        return jsonify({
            "run_id":run_id, "total_rows":len(result_df), "matched_rows":matched,
            "total_list_cost":total_list, "total_after_discount":total_after, "total_savings":total_savings,
            "preview":result_df.head(50).to_dict("records"),
            "csv_data":result_df.to_dict("records"),
            "service_breakdown":svc.round(2).to_dict("records"),
            "unmatched_skus": unmatched_skus,
            "unmatched_count": len(unmatched_df),
        })
    except Exception as e:
        log.error("process: %s", e)
        return jsonify({"error":str(e)}), 500

# ── Downloads ─────────────────────────────────────────────────────────────────
@app.route("/download/xlsx", methods=["POST"])
@login_required
def download_xlsx():
    try:
        data=request.json.get("data",[]); summary=request.json.get("summary",{})
        df=pd.DataFrame(data); buf=io.BytesIO()
        with pd.ExcelWriter(buf,engine="xlsxwriter") as writer:
            df.to_excel(writer,index=False,sheet_name="Discounted Billing")
            pd.DataFrame([summary]).to_excel(writer,index=False,sheet_name="Summary")
            wb=writer.book; ws=writer.sheets["Discounted Billing"]
            fmt=wb.add_format({"bold":True,"bg_color":"#1a56db","font_color":"white"})
            for i,col in enumerate(df.columns):
                ws.write(0,i,col,fmt); ws.set_column(i,i,max(15,len(col)+4))
        buf.seek(0)
        return send_file(buf,mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True,download_name="gcp_discounted_billing.xlsx")
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/download/csv", methods=["POST"])
@login_required
def download_csv():
    try:
        df=pd.DataFrame(request.json.get("data",[])); buf=io.BytesIO(df.to_csv(index=False).encode())
        return send_file(buf,mimetype="text/csv",as_attachment=True,download_name="gcp_discounted_billing.csv")
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/history/<int:run_id>/download/xlsx")
@login_required
def history_download_xlsx(run_id):
    run=get_run_detail(run_id)
    if not run: return "Not found",404
    df=pd.DataFrame(run["rows"]); buf=io.BytesIO()
    with pd.ExcelWriter(buf,engine="xlsxwriter") as writer:
        df.to_excel(writer,index=False,sheet_name="Discounted Billing")
        pd.DataFrame([{"Run ID":run_id,"File":run.get("run_name",""),
                       "Billing Date":str(run.get("billing_date","")),
                       "Processed At":str(run.get("processed_at","")),
                       "Total List Cost":float(run.get("total_list_cost") or 0),
                       "Total After Discount":float(run.get("total_after_discount") or 0),
                       "Total Savings":float(run.get("total_savings") or 0)}]).to_excel(
            writer,index=False,sheet_name="Summary")
        wb=writer.book; ws=writer.sheets["Discounted Billing"]
        fmt=wb.add_format({"bold":True,"bg_color":"#1a56db","font_color":"white"})
        for i,col in enumerate(df.columns):
            ws.write(0,i,col,fmt); ws.set_column(i,i,max(15,len(col)+4))
    buf.seek(0)
    fname=f"run_{run_id}_{run.get('run_name','billing')}.xlsx".replace(" ","_")
    return send_file(buf,mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True,download_name=fname)

@app.route("/api/matrix")
@login_required
def api_matrix():
    df, _, _ = load_discounts()
    return jsonify({"skus":[{"service":r["Service description"],"sku":r["SKU description"],
                              "discount_pct":round(r["Discount"]*100,2),
                              "pricing_model":r.get("pricing_model","discount"),
                              "unit_price":float(r["unit_price"]) if pd.notna(r.get("unit_price")) else None}
                             for _,r in df.iterrows()],
                    "total":len(df)})

# ── CDN Calculator ────────────────────────────────────────────────────────────

# ── CDN Formula DB helpers ────────────────────────────────────────────────────
def _cdn_migrate(conn):
    """Idempotent: ensure cdn_formula_buckets table + defaults exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cdn_formula_buckets (
            id             SERIAL PRIMARY KEY,
            bucket_name    VARCHAR(100) NOT NULL,
            bucket_role    VARCHAR(20)  NOT NULL,
            sku_patterns   TEXT         NOT NULL DEFAULT '',
            require_list_cost_gt_zero BOOLEAN NOT NULL DEFAULT TRUE,
            lookup_divisor NUMERIC(12,2) DEFAULT 10000,
            sort_order     INTEGER      NOT NULL DEFAULT 0,
            is_active      BOOLEAN      NOT NULL DEFAULT TRUE,
            created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_by     VARCHAR(80)
        )
    """)
    for ddl in [
        "ALTER TABLE cdn_formula_buckets ADD COLUMN IF NOT EXISTS require_list_cost_gt_zero BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE cdn_formula_buckets ADD COLUMN IF NOT EXISTS lookup_divisor NUMERIC(12,2) DEFAULT 10000",
        "ALTER TABLE cdn_formula_buckets ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0",
    ]:
        try: cur.execute(ddl)
        except Exception: pass
    # seed defaults if empty
    cur.execute("SELECT COUNT(*) FROM cdn_formula_buckets")
    if cur.fetchone()[0] == 0:
        cur.executemany("""
            INSERT INTO cdn_formula_buckets
                (bucket_name, bucket_role, sku_patterns, require_list_cost_gt_zero, lookup_divisor, sort_order, updated_by)
            VALUES (%s,%s,%s,%s,%s,%s,'system')
        """, [
            ('Total Hit GB',       'hit',
             'networking cloud cdn traffic cache data transfer\nnetwork internet data transfer out\nnetwork inter zone data transfer out',
             True, None, 1),
            ('Total Miss GB',      'miss',
             'cache fill\ncdn cache fill\ndownload',
             True, None, 2),
            ('Total Cache Lookup', 'lookup',
             'cloud cdn cache lookup\ncdn cache lookup',
             False, 10000, 3),
        ])
    conn.commit(); cur.close()


def load_cdn_buckets():
    """Return list of active bucket dicts from DB."""
    try:
        conn = get_db()
        _cdn_migrate(conn)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, bucket_name, bucket_role, sku_patterns,
                   require_list_cost_gt_zero, lookup_divisor, sort_order, is_active
            FROM cdn_formula_buckets ORDER BY sort_order, id
        """)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        log.error("load_cdn_buckets: %s", e); return []


@app.route("/cdn")
@login_required
def cdn():
    buckets = load_cdn_buckets()
    return render_template("cdn.html", current_user=session["user"], buckets=buckets)


@app.route("/cdn-setup")
@admin_required
def cdn_setup():
    buckets = load_cdn_buckets()
    return render_template("cdn_setup.html", current_user=session["user"], buckets=buckets)


@app.route("/api/cdn/buckets", methods=["GET"])
@login_required
def api_cdn_buckets_get():
    return jsonify({"buckets": load_cdn_buckets()})


@app.route("/api/cdn/buckets/save", methods=["POST"])
@admin_required
def api_cdn_buckets_save():
    """Upsert a bucket. Body: {id?, bucket_name, bucket_role, sku_patterns,
                                require_list_cost_gt_zero, lookup_divisor, sort_order, is_active}"""
    try:
        data = request.get_json(force=True)
        conn = get_db(); _cdn_migrate(conn); cur = conn.cursor()
        bid = data.get("id")
        if bid:
            cur.execute("""
                UPDATE cdn_formula_buckets SET
                    bucket_name=%(n)s, bucket_role=%(r)s, sku_patterns=%(p)s,
                    require_list_cost_gt_zero=%(lc)s, lookup_divisor=%(ld)s,
                    sort_order=%(so)s, is_active=%(ia)s,
                    updated_at=NOW(), updated_by=%(by)s
                WHERE id=%(id)s
            """, {"n": data["bucket_name"], "r": data["bucket_role"],
                  "p": data["sku_patterns"], "lc": bool(data.get("require_list_cost_gt_zero", True)),
                  "ld": data.get("lookup_divisor") or None,
                  "so": int(data.get("sort_order", 0)), "ia": bool(data.get("is_active", True)),
                  "by": session["user"]["username"], "id": bid})
        else:
            cur.execute("""
                INSERT INTO cdn_formula_buckets
                    (bucket_name, bucket_role, sku_patterns, require_list_cost_gt_zero,
                     lookup_divisor, sort_order, is_active, updated_by)
                VALUES (%(n)s,%(r)s,%(p)s,%(lc)s,%(ld)s,%(so)s,%(ia)s,%(by)s)
                RETURNING id
            """, {"n": data["bucket_name"], "r": data["bucket_role"],
                  "p": data["sku_patterns"], "lc": bool(data.get("require_list_cost_gt_zero", True)),
                  "ld": data.get("lookup_divisor") or None,
                  "so": int(data.get("sort_order", 0)), "ia": bool(data.get("is_active", True)),
                  "by": session["user"]["username"]})
        conn.commit(); cur.close(); conn.close()
        return jsonify({"ok": True, "buckets": load_cdn_buckets()})
    except Exception as e:
        log.error("api_cdn_buckets_save: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/cdn/buckets/<int:bid>/delete", methods=["POST"])
@admin_required
def api_cdn_buckets_delete(bid):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("DELETE FROM cdn_formula_buckets WHERE id=%s", (bid,))
        conn.commit(); cur.close(); conn.close()
        return jsonify({"ok": True, "buckets": load_cdn_buckets()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cdn/dates")
@login_required
def api_cdn_dates():
    """Return all distinct billing dates and months available in billing_runs."""
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT DISTINCT billing_date
            FROM billing_runs
            WHERE billing_date IS NOT NULL
            ORDER BY billing_date DESC
        """)
        dates  = [str(r["billing_date"]) for r in cur.fetchall()]
        months = sorted(set(d[:7] for d in dates), reverse=True)
        cur.close(); conn.close()
        return jsonify({"dates": dates, "months": months})
    except Exception as e:
        log.error("api_cdn_dates: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/cdn")
@login_required
def api_cdn():
    """
    CDN cost aggregation using admin-configured formula buckets from cdn_formula_buckets table.
    Each bucket defines a set of SKU keyword patterns and a role (hit/miss/lookup).
    Rows where after_discount < 1 are always excluded.
    """
    from collections import Counter, defaultdict

    try:
        start_str = request.args.get("start")
        end_str   = request.args.get("end")
        if not start_str or not end_str:
            return jsonify({"error": "start and end required"}), 400

        start_date = date.fromisoformat(start_str)
        end_date   = date.fromisoformat(end_str)

        conn = get_db()
        _cdn_migrate(conn)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # migration guard for cost_per_unit
        try:
            c2 = conn.cursor()
            c2.execute("ALTER TABLE billing_run_rows ADD COLUMN IF NOT EXISTS cost_per_unit NUMERIC(20,8)")
            conn.commit(); c2.close()
        except Exception: conn.rollback()

        cur.execute("""
            SELECT brr.sku_desc, brr.usage_amount, brr.usage_unit,
                   brr.list_cost, brr.discount_pct, brr.after_discount, brr.cost_per_unit,
                   brr.service_desc, br.billing_date
            FROM billing_run_rows brr
            JOIN billing_runs br ON br.id = brr.run_id
            WHERE br.billing_date BETWEEN %s AND %s
              AND brr.sku_desc IS NOT NULL
              AND brr.after_discount >= 1
        """, (start_date, end_date))
        raw = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT billing_date FROM billing_runs
            WHERE billing_date BETWEEN %s AND %s ORDER BY billing_date
        """, (start_date, end_date))
        dates = [str(r["billing_date"]) for r in cur.fetchall()]

        # Load active buckets from DB
        cur.execute("""
            SELECT id, bucket_name, bucket_role, sku_patterns,
                   require_list_cost_gt_zero, lookup_divisor, sort_order
            FROM cdn_formula_buckets
            WHERE is_active = TRUE
            ORDER BY sort_order, id
        """)
        buckets = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()

        def sl(r): return (r["sku_desc"] or "").lower()

        def matches_bucket(row, bucket):
            s = sl(row)
            patterns = [p.strip().lower() for p in (bucket["sku_patterns"] or "").splitlines() if p.strip()]
            if not patterns: return False
            if bucket["require_list_cost_gt_zero"] and float(row.get("list_cost") or 0) <= 0:
                return False
            return any(p in s for p in patterns)

        # Classify rows into buckets
        hit_rows    = []
        miss_rows   = []
        lookup_rows = []
        # use first active hit/miss/lookup bucket respectively (or all matching)
        for bucket in buckets:
            role = bucket["bucket_role"]
            matched = [r for r in raw if matches_bucket(r, bucket)]
            if role == "hit":
                hit_rows.extend(matched)
            elif role == "miss":
                miss_rows.extend(matched)
            elif role == "lookup":
                lookup_rows.extend(matched)

        # De-duplicate by object identity within each list
        def dedup(rows):
            seen = set(); out = []
            for r in rows:
                k = id(r)
                if k not in seen: seen.add(k); out.append(r)
            return out
        hit_rows    = dedup(hit_rows)
        miss_rows   = dedup(miss_rows)
        lookup_rows = dedup(lookup_rows)

        # Lookup divisor from first lookup bucket
        lookup_divisor = 10000
        for b in buckets:
            if b["bucket_role"] == "lookup" and b.get("lookup_divisor"):
                lookup_divisor = float(b["lookup_divisor"])
                break

        # Aggregator
        def agg(rows, divisor=None):
            if not rows: return None
            usage   = sum(float(r["usage_amount"]   or 0) for r in rows)
            list_c  = sum(float(r["list_cost"]       or 0) for r in rows)
            after_d = sum(float(r["after_discount"]  or 0) for r in rows)
            unit_counts = Counter(r["usage_unit"] or "" for r in rows)
            unit = unit_counts.most_common(1)[0][0] if unit_counts else ""
            if divisor and usage:
                units    = usage / divisor
                disc_cpu = round(after_d / units, 8) if units else 0
                list_cpu = round(list_c  / units, 8) if units else 0
            else:
                disc_cpu = round(after_d / usage, 8) if usage else 0
                list_cpu = round(list_c  / usage, 8) if usage else 0
            return {
                "usage_amount":       round(usage,   4),
                "usage_unit":         unit,
                "list_cost":          round(list_c,  4),
                "after_discount":     round(after_d, 4),
                "disc_cost_per_unit": disc_cpu,
                "list_cost_per_unit": list_cpu,
                "row_count":          len(rows),
            }

        # Per-SKU miss/fill table
        fill_by_sku = defaultdict(lambda: {"usage_amount": 0.0, "list_cost": 0.0,
                                           "after_discount": 0.0, "usage_unit": ""})
        for r in miss_rows:
            k = r["sku_desc"]
            fill_by_sku[k]["usage_amount"]  += float(r["usage_amount"]  or 0)
            fill_by_sku[k]["list_cost"]      += float(r["list_cost"]     or 0)
            fill_by_sku[k]["after_discount"] += float(r["after_discount"]or 0)
            fill_by_sku[k]["usage_unit"]      = r["usage_unit"] or ""

        fill_skus = [
            {"sku": sku, "usage_amount": round(v["usage_amount"], 4),
             "usage_unit": v["usage_unit"], "list_cost": round(v["list_cost"], 4),
             "after_discount": round(v["after_discount"], 4)}
            for sku, v in sorted(fill_by_sku.items(), key=lambda x: -x[1]["list_cost"])
        ]

        # Summary
        hit_usage    = sum(float(r["usage_amount"] or 0) for r in hit_rows)
        miss_usage   = sum(float(r["usage_amount"] or 0) for r in miss_rows)
        lookup_usage = sum(float(r["usage_amount"] or 0) for r in lookup_rows)
        all_rows     = hit_rows + miss_rows + lookup_rows
        total_disc   = sum(float(r["after_discount"] or 0) for r in all_rows)
        total_list   = sum(float(r["list_cost"]      or 0) for r in all_rows)

        # Daily timeline
        daily_hit  = defaultdict(float)
        daily_miss = defaultdict(float)
        for r in hit_rows:  daily_hit [str(r["billing_date"])] += float(r["after_discount"] or 0)
        for r in miss_rows: daily_miss[str(r["billing_date"])] += float(r["after_discount"] or 0)
        timeline = [
            {"date": d, "hit_cost": round(daily_hit[d], 4), "miss_cost": round(daily_miss[d], 4)}
            for d in dates
        ]

        return jsonify({
            "summary_scalars": {
                "total_gb_used":      round(hit_usage + miss_usage, 4),
                "total_lookup_count": round(lookup_usage, 0),
                "total_discounted":   round(total_disc, 4),
                "total_list_cost":    round(total_list, 4),
                "billing_dates":      len(dates),
                "date_range":         f"{start_str} to {end_str}",
            },
            "items": {
                "miss_gb":  agg(miss_rows),
                "hit_gb":   agg(hit_rows),
                "lookup":   agg(lookup_rows, divisor=lookup_divisor),
            },
            "fill_skus": fill_skus,
            "timeline":  timeline,
        })
    except Exception as e:
        log.error("api_cdn: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html", current_user=session["user"])

@app.route("/api/dashboard/latest-date")
@login_required
def api_dashboard_latest_date():
    """Return the most recent billing_date that has data."""
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT MAX(billing_date) FROM billing_runs")
        row = cur.fetchone(); cur.close(); conn.close()
        latest = str(row[0]) if row and row[0] else None
        return jsonify({"latest_date": latest})
    except Exception as e:
        log.error("api_dashboard_latest_date: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/api/dashboard")
@login_required
def api_dashboard():
    """
    Query params:
      start  – YYYY-MM-DD  (required)
      end    – YYYY-MM-DD  (required)
    Returns aggregated billing data including SKU-level cost trend.
    """
    try:
        start_str = request.args.get("start")
        end_str   = request.args.get("end")
        if not start_str or not end_str:
            return jsonify({"error": "start and end required"}), 400

        start_date = date.fromisoformat(start_str)
        end_date   = date.fromisoformat(end_str)

        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # ── Daily totals ──────────────────────────────────────────────────────
        cur.execute("""
            SELECT billing_date,
                   SUM(total_list_cost)      AS list_total,
                   SUM(total_after_discount) AS after_discount,
                   SUM(total_savings)        AS savings_total
            FROM billing_runs
            WHERE billing_date BETWEEN %s AND %s
            GROUP BY billing_date
            ORDER BY billing_date
        """, (start_date, end_date))
        daily_rows = [dict(r) for r in cur.fetchall()]

        # ── Service breakdown for period ──────────────────────────────────────
        cur.execute("""
            SELECT brs.service_name,
                   SUM(brs.list_cost)      AS list_total,
                   SUM(brs.after_discount) AS after_discount,
                   SUM(brs.savings)        AS savings
            FROM billing_run_services brs
            JOIN billing_runs br ON br.id = brs.run_id
            WHERE br.billing_date BETWEEN %s AND %s
            GROUP BY brs.service_name
            ORDER BY after_discount DESC
        """, (start_date, end_date))
        services = [dict(r) for r in cur.fetchall()]

        # ── Service-per-day for stacked area chart ────────────────────────────
        cur.execute("""
            SELECT br.billing_date,
                   brs.service_name,
                   SUM(brs.list_cost)      AS list_total,
                   SUM(brs.after_discount) AS after_discount
            FROM billing_run_services brs
            JOIN billing_runs br ON br.id = brs.run_id
            WHERE br.billing_date BETWEEN %s AND %s
            GROUP BY br.billing_date, brs.service_name
            ORDER BY br.billing_date
        """, (start_date, end_date))
        svc_daily_rows = [dict(r) for r in cur.fetchall()]

        # ── Top SKUs by cost for period (for SKU trend chart) ────────────────
        # First find top 8 SKUs by total after_discount cost
        cur.execute("""
            SELECT brr.sku_desc,
                   brr.service_desc,
                   SUM(brr.after_discount) AS after_discount,
                   SUM(brr.list_cost)      AS list_total
            FROM billing_run_rows brr
            JOIN billing_runs br ON br.id = brr.run_id
            WHERE br.billing_date BETWEEN %s AND %s
              AND brr.sku_desc IS NOT NULL AND brr.sku_desc <> ''
            GROUP BY brr.sku_desc, brr.service_desc
            ORDER BY after_discount DESC
            LIMIT 8
        """, (start_date, end_date))
        top_skus = [dict(r) for r in cur.fetchall()]
        top_sku_names = [r["sku_desc"] for r in top_skus]

        # Per-day cost for each top SKU
        sku_daily_rows = []
        if top_sku_names:
            placeholders = ",".join(["%s"] * len(top_sku_names))
            cur.execute(f"""
                SELECT br.billing_date,
                       brr.sku_desc,
                       brr.service_desc,
                       SUM(brr.after_discount) AS after_discount,
                       SUM(brr.list_cost)      AS list_total
                FROM billing_run_rows brr
                JOIN billing_runs br ON br.id = brr.run_id
                WHERE br.billing_date BETWEEN %s AND %s
                  AND brr.sku_desc IN ({placeholders})
                GROUP BY br.billing_date, brr.sku_desc, brr.service_desc
                ORDER BY br.billing_date
            """, (start_date, end_date, *top_sku_names))
            sku_daily_rows = [dict(r) for r in cur.fetchall()]

        # ── Previous period for KPI deltas ────────────────────────────────────
        period_days = (end_date - start_date).days + 1
        prev_end    = start_date - timedelta(days=1)
        prev_start  = prev_end   - timedelta(days=period_days - 1)
        cur.execute("""
            SELECT COALESCE(SUM(total_list_cost),0)      AS prev_list,
                   COALESCE(SUM(total_after_discount),0) AS prev_after,
                   COALESCE(SUM(total_savings),0)        AS prev_savings
            FROM billing_runs
            WHERE billing_date BETWEEN %s AND %s
        """, (prev_start, prev_end))
        prev_row = dict(cur.fetchone() or {})

        cur.close(); conn.close()

        # ── Serialise ─────────────────────────────────────────────────────────
        for r in daily_rows:
            r["billing_date"] = str(r["billing_date"])
            for k in ("list_total","after_discount","savings_total"):
                r[k] = float(r[k] or 0)
        for r in svc_daily_rows:
            r["billing_date"] = str(r["billing_date"])
            for k in ("list_total","after_discount"):
                r[k] = float(r[k] or 0)
        for r in services:
            for k in ("list_total","after_discount","savings"):
                r[k] = float(r[k] or 0)
        for r in top_skus:
            for k in ("after_discount","list_total"):
                r[k] = float(r[k] or 0)
        for r in sku_daily_rows:
            r["billing_date"] = str(r["billing_date"])
            for k in ("after_discount","list_total"):
                r[k] = float(r[k] or 0)

        grand_list    = sum(r["list_total"]    for r in daily_rows)
        grand_after   = sum(r["after_discount"] for r in daily_rows)
        grand_savings = sum(r["savings_total"]  for r in daily_rows)

        return jsonify({
            # KPI totals
            "grand_list":    grand_list,
            "grand_after":   grand_after,
            "grand_savings": grand_savings,
            # Previous period (for deltas)
            "prev_list":    float(prev_row.get("prev_list",0)),
            "prev_after":   float(prev_row.get("prev_after",0)),
            "prev_savings": float(prev_row.get("prev_savings",0)),
            # Chart data
            "daily":        daily_rows,
            "services":     services,
            "svc_daily":    svc_daily_rows,
            "top_skus":     top_skus,
            "sku_daily":    sku_daily_rows,
            "period_days":  period_days,
        })

    except Exception as e:
        log.error("api_dashboard: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/download/sample-discount-old")
@login_required
def download_sample_old():
    """Download sample OLD format discount XLSX: Service description, SKU description, Discount"""
    sample = [
        {"Service description": "Compute Engine", "SKU description": "N1 Predefined Instance Core running in Americas", "Discount": 0.15},
        {"Service description": "Compute Engine", "SKU description": "N1 Predefined Instance Ram running in Americas", "Discount": 0.15},
        {"Service description": "Cloud Storage",  "SKU description": "Regional Storage Iowa",                          "Discount": 0.10},
        {"Service description": "Cloud Storage",  "SKU description": "Standard Storage US-EAST1",                     "Discount": 0.10},
        {"Service description": "BigQuery",        "SKU description": "Analysis",                                       "Discount": 0.20},
        {"Service description": "BigQuery",        "SKU description": "Storage",                                        "Discount": 0.12},
        {"Service description": "Cloud SQL",       "SKU description": "Cloud SQL for MySQL: Zonal - 2 vCPU + 3.75GB RAM in Americas", "Discount": 0.18},
        {"Service description": "Kubernetes Engine","SKU description": "Kubernetes Clusters",                           "Discount": 0.05},
    ]
    df = pd.DataFrame(sample)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Discounts")
        wb = writer.book; ws = writer.sheets["Discounts"]
        hdr_fmt = wb.add_format({"bold": True, "bg_color": "#1a56db", "font_color": "white", "border": 1})
        note_fmt = wb.add_format({"italic": True, "font_color": "#6b7280", "font_size": 10})
        for i, col in enumerate(df.columns):
            ws.write(0, i, col, hdr_fmt)
            ws.set_column(i, i, max(20, len(col) + 4))
        ws.write(len(sample) + 2, 0, "Notes:", wb.add_format({"bold": True}))
        ws.write(len(sample) + 3, 0, "Discount column: use fraction (0.15 = 15%). Pricing model = discount (% off List cost).", note_fmt)
    buf.seek(0)
    return send_file(buf, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True, download_name="sample_discount_old_format.xlsx")


@app.route("/download/sample-discount-new")
@login_required
def download_sample_new():
    """Download sample NEW format discount XLSX: service_description, sku_description, pricing_model, discount, unit_price"""
    sample = [
        {"service_description": "Compute Engine", "sku_description": "N1 Predefined Instance Core running in Americas", "pricing_model": "discount", "discount": 0.15, "unit_price": None},
        {"service_description": "Compute Engine", "sku_description": "N1 Predefined Instance Ram running in Americas",  "pricing_model": "discount", "discount": 0.15, "unit_price": None},
        {"service_description": "Cloud Storage",  "sku_description": "Regional Storage Iowa",                           "pricing_model": "unit",     "discount": 0,    "unit_price": 0.020},
        {"service_description": "Cloud Storage",  "sku_description": "Standard Storage US-EAST1",                      "pricing_model": "unit",     "discount": 0,    "unit_price": 0.023},
        {"service_description": "BigQuery",        "sku_description": "Analysis",                                        "pricing_model": "discount", "discount": 0.20, "unit_price": None},
        {"service_description": "BigQuery",        "sku_description": "Storage",                                         "pricing_model": "discount", "discount": 0.12, "unit_price": None},
        {"service_description": "Cloud SQL",       "sku_description": "Cloud SQL for MySQL: Zonal - 2 vCPU + 3.75GB RAM in Americas", "pricing_model": "unit", "discount": 0, "unit_price": 0.1900},
        {"service_description": "Kubernetes Engine","sku_description": "Kubernetes Clusters",                            "pricing_model": "discount", "discount": 0.05, "unit_price": None},
    ]
    df = pd.DataFrame(sample)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Discounts")
        wb = writer.book; ws = writer.sheets["Discounts"]
        hdr_fmt  = wb.add_format({"bold": True, "bg_color": "#059669", "font_color": "white", "border": 1})
        note_fmt = wb.add_format({"italic": True, "font_color": "#6b7280", "font_size": 10})
        col_widths = [22, 55, 16, 12, 14]
        for i, col in enumerate(df.columns):
            ws.write(0, i, col, hdr_fmt)
            ws.set_column(i, i, col_widths[i] if i < len(col_widths) else 18)
        ws.write(len(sample) + 2, 0, "Notes:", wb.add_format({"bold": True}))
        ws.write(len(sample) + 3, 0, "pricing_model: 'discount' → after_discount = list_cost * (1 - discount)", note_fmt)
        ws.write(len(sample) + 4, 0, "pricing_model: 'unit'     → after_discount = usage_amount * unit_price", note_fmt)
        ws.write(len(sample) + 5, 0, "discount column: fraction (0.15 = 15%). Leave blank/0 when pricing_model = 'unit'.", note_fmt)
        ws.write(len(sample) + 6, 0, "unit_price column: price per usage unit. Leave blank when pricing_model = 'discount'.", note_fmt)
    buf.seek(0)
    return send_file(buf, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True, download_name="sample_discount_new_format.xlsx")


# ── Boot ──────────────────────────────────────────────────────────────────────
with app.app_context():
    try: init_scheduler()
    except Exception as e: log.warning("Scheduler init warning: %s", e)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
