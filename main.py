"""
SEC Membership Renewal Agent — data backend (Postgres).

Reads + writes over Railway Postgres. `data/members.json` in GitHub stays the
canonical seed; Postgres is the live mirror (seeded on first boot if empty).
No Supabase / no PostgREST / no Lovable — direct asyncpg.
"""
import json
import os
from datetime import date
from pathlib import Path

import asyncpg
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="SEC Renewal Agent — Data API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

DATA_PATH = Path(__file__).parent / "data" / "members.json"
DB_URL = os.environ.get("DATABASE_PRIVATE_URL") or os.environ.get("DATABASE_URL")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")            # guards /admin/reset
DEFAULT_THRESHOLDS = [60, 30, 14, 7]

STATUS_AR = {"Active": "عضوية سارية", "Expired": "منتهية",
             "Frozen": "مجمدة", "Nearing Expiry": "شارفت على الانتهاء"}

COLS = ["member_no", "member_type", "first_name_en", "first_name_ar", "phone",
        "nationality_ar", "nationality_en", "status_ar", "status_en",
        "grade_ar", "grade_en", "classification_ar", "specialization_ar",
        "membership_start", "membership_end"]

DDL = """
CREATE TABLE IF NOT EXISTS members (
    member_no              text PRIMARY KEY,
    member_type            text,
    first_name_en          text,
    first_name_ar          text,
    phone                  text,
    nationality_ar         text,
    nationality_en         text,
    status_ar              text,
    status_en              text,
    grade_ar               text,
    grade_en               text,
    classification_ar      text,
    specialization_ar      text,
    membership_start       date,
    membership_end         date,
    last_notified_at       date,
    last_notified_threshold text
);
CREATE INDEX IF NOT EXISTS idx_members_end ON members (membership_end);
"""

pool: asyncpg.Pool | None = None


@app.on_event("startup")
async def _startup():
    global pool
    if not DB_URL:
        raise RuntimeError("DATABASE_URL / DATABASE_PRIVATE_URL not set")
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as con:
        await con.execute(DDL)
        await _seed_if_empty(con)


@app.on_event("shutdown")
async def _shutdown():
    if pool:
        await pool.close()


async def _seed_if_empty(con) -> int:
    n = await con.fetchval("SELECT count(*) FROM members")
    if n:
        return 0
    rows = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    records = [
        tuple(
            date.fromisoformat(r[c]) if c in ("membership_start", "membership_end") and r.get(c)
            else r.get(c)
            for c in COLS
        )
        for r in rows
    ]
    await con.copy_records_to_table("members", records=records, columns=COLS)
    return len(records)


def _as_of(v: str | None) -> date:
    if not v:
        return date.today()
    try:
        return date.fromisoformat(v)
    except ValueError:
        raise HTTPException(400, f"as_of must be YYYY-MM-DD, got {v!r}")


def _renewal_block(m: dict, ref: date) -> dict:
    d = (m["membership_end"] - ref).days
    state = ("expired" if d < 0 else "nearing_expiry" if d <= 30
             else "renewal_window" if d <= 60 else "active")
    return {
        "days_to_expiry": d,
        "state": state,
        "reminder_bucket": next((f"T-{t}" for t in DEFAULT_THRESHOLDS if d == t), None),
        "membership_end": m["membership_end"].isoformat(),
        "renew_required": d <= 60,
        "last_notified_at": m["last_notified_at"].isoformat() if m["last_notified_at"] else None,
        "last_notified_threshold": m["last_notified_threshold"],
    }


def _member_public(m: dict) -> dict:
    out = {k: m[k] for k in COLS}
    out["membership_start"] = m["membership_start"].isoformat() if m["membership_start"] else None
    out["membership_end"] = m["membership_end"].isoformat() if m["membership_end"] else None
    return out


@app.get("/")
async def root():
    n = await pool.fetchval("SELECT count(*) FROM members") if pool else 0
    return {"service": "SEC Renewal Agent — Data API", "members": n,
            "endpoints": ["/health", "/member", "/renewals/due", "/renewals/summary",
                          "/notifications (POST)", "/membership (POST)", "/admin/reset (POST)"]}


@app.get("/health")
async def health():
    n = await pool.fetchval("SELECT count(*) FROM members")
    return {"status": "ok", "members": n}


@app.get("/member")
async def get_member(member_no: str = Query(..., description="e.g. M42"),
                     as_of: str | None = Query(None)):
    ref = _as_of(as_of)
    row = await pool.fetchrow("SELECT * FROM members WHERE member_no = $1", member_no.strip().upper())
    if not row:
        raise HTTPException(404, f"No member with member_no {member_no!r}")
    m = dict(row)
    return {"member": _member_public(m), "renewal": _renewal_block(m, ref)}


@app.get("/renewals/due")
async def renewals_due(
    as_of: str | None = Query(None),
    mode: str = Query("exact", pattern="^(exact|window)$"),
    thresholds: str = Query("60,30,14,7"),
    window: int = Query(30, ge=1, le=365),
    exclude_notified: bool = Query(False, description="drop members already notified on as_of date"),
    limit: int = Query(500, ge=1, le=2000),
):
    ref = _as_of(as_of)
    where = ["(membership_end - $1::date) >= 0"] if mode == "window" else []
    args: list = [ref]
    th = None
    if mode == "exact":
        try:
            th = sorted({int(x) for x in thresholds.split(",") if x.strip()})
        except ValueError:
            raise HTTPException(400, f"thresholds must be comma-separated ints, got {thresholds!r}")
        args.append(th)
        where.append(f"(membership_end - $1::date) = ANY(${len(args)}::int[])")
    else:
        args.append(window)
        where.append(f"(membership_end - $1::date) <= ${len(args)}")
    if exclude_notified:
        where.append("last_notified_at IS DISTINCT FROM $1::date")
    args.append(limit)

    sql = f"""
        SELECT member_no, first_name_en, first_name_ar, phone, grade_en, grade_ar,
               specialization_ar, membership_end,
               (membership_end - $1::date) AS days_to_expiry, status_en
        FROM members
        WHERE {' AND '.join(where)}
        ORDER BY days_to_expiry
        LIMIT ${len(args)}
    """
    rows = await pool.fetch(sql, *args)
    members = []
    for r in rows:
        d = r["days_to_expiry"]
        members.append({
            "member_no": r["member_no"], "first_name_en": r["first_name_en"],
            "first_name_ar": r["first_name_ar"], "phone": r["phone"],
            "grade_en": r["grade_en"], "grade_ar": r["grade_ar"],
            "specialization_ar": r["specialization_ar"],
            "membership_end": r["membership_end"].isoformat(), "days_to_expiry": d,
            "status_en": r["status_en"],
            "reminder_bucket": f"T-{d}" if mode == "exact" else None,
        })
    return {"as_of": ref.isoformat(), "mode": mode,
            "thresholds": th if mode == "exact" else None,
            "window": window if mode == "window" else None,
            "exclude_notified": exclude_notified, "count": len(members), "members": members}


@app.get("/renewals/summary")
async def renewals_summary(as_of: str | None = Query(None)):
    ref = _as_of(as_of)
    by_status = {(r["status_en"] or r["status_ar"] or "Unknown"): r["c"] for r in
                 await pool.fetch("SELECT status_en, status_ar, count(*) c FROM members GROUP BY 1,2")}
    windows = {}
    for w in (7, 14, 30, 60, 90):
        windows[str(w)] = await pool.fetchval(
            "SELECT count(*) FROM members WHERE (membership_end - $1::date) BETWEEN 0 AND $2", ref, w)
    total = await pool.fetchval("SELECT count(*) FROM members")
    return {"as_of": ref.isoformat(), "total": total,
            "by_status": by_status, "expiring_within_days": windows}


@app.post("/notifications")
async def record_notification(member_no: str, threshold: str, as_of: str | None = None):
    """System bookkeeping after a template send. No user confirmation — auto."""
    ref = _as_of(as_of)
    row = await pool.fetchrow(
        """UPDATE members SET last_notified_at = $2, last_notified_threshold = $3
           WHERE member_no = $1
           RETURNING member_no, last_notified_at, last_notified_threshold""",
        member_no.strip().upper(), ref, threshold)
    if not row:
        raise HTTPException(404, f"No member with member_no {member_no!r}")
    return {"ok": True, "member_no": row["member_no"],
            "last_notified_at": row["last_notified_at"].isoformat(),
            "last_notified_threshold": row["last_notified_threshold"]}


@app.post("/membership")
async def update_membership(member_no: str,
                            membership_end: str | None = None,
                            status: str | None = Query(None, description="Active|Expired|Frozen|Nearing Expiry")):
    """Live renewal update (member renews → record changes). Agent confirms before calling."""
    sets, args = [], [member_no.strip().upper()]
    if membership_end:
        try:
            args.append(date.fromisoformat(membership_end))
        except ValueError:
            raise HTTPException(400, "membership_end must be YYYY-MM-DD")
        sets.append(f"membership_end = ${len(args)}")
    if status:
        if status not in STATUS_AR:
            raise HTTPException(400, f"status must be one of {list(STATUS_AR)}")
        args.append(status); sets.append(f"status_en = ${len(args)}")
        args.append(STATUS_AR[status]); sets.append(f"status_ar = ${len(args)}")
    if not sets:
        raise HTTPException(400, "provide membership_end and/or status")
    row = await pool.fetchrow(
        f"UPDATE members SET {', '.join(sets)} WHERE member_no = $1 "
        f"RETURNING member_no, membership_end, status_en, status_ar", *args)
    if not row:
        raise HTTPException(404, f"No member with member_no {member_no!r}")
    return {"ok": True, "member_no": row["member_no"],
            "membership_end": row["membership_end"].isoformat(),
            "status_en": row["status_en"], "status_ar": row["status_ar"]}


@app.post("/admin/reset")
async def admin_reset(x_admin_token: str | None = Header(None)):
    """Truncate + re-seed from the canonical JSON. Baseline restore between demos."""
    if ADMIN_TOKEN and x_admin_token != ADMIN_TOKEN:
        raise HTTPException(401, "bad admin token")
    async with pool.acquire() as con:
        async with con.transaction():
            await con.execute("TRUNCATE members")
            n = await _seed_if_empty(con)
    return {"ok": True, "reseeded": n}
