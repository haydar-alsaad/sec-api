"""
SEC Membership Renewal Agent — data backend.
GET-only Custom API Endpoint surface for the Nebelus agent.
No Supabase / no Lovable: members.json is loaded in-process and served read-only.
"""
import json
from datetime import date
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="SEC Renewal Agent — Data API", version="1.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

DATA_PATH = Path(__file__).parent / "data" / "members.json"
DEFAULT_THRESHOLDS = [60, 30, 14, 7]

# --- in-process store (loaded once at startup) ---
MEMBERS: list[dict] = []
BY_NO: dict[str, dict] = {}


@app.on_event("startup")
def _load():
    global MEMBERS, BY_NO
    MEMBERS = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    BY_NO = {m["member_no"]: m for m in MEMBERS}


# --- helpers ---
def _as_of(as_of: str | None) -> date:
    if not as_of:
        return date.today()
    try:
        return date.fromisoformat(as_of)
    except ValueError:
        raise HTTPException(400, f"as_of must be YYYY-MM-DD, got {as_of!r}")


def _days_to_expiry(member: dict, ref: date) -> int:
    return (date.fromisoformat(member["membership_end"]) - ref).days


def _renewal_block(member: dict, ref: date) -> dict:
    d = _days_to_expiry(member, ref)
    if d < 0:
        state = "expired"
    elif d <= 30:
        state = "nearing_expiry"
    elif d <= 60:
        state = "renewal_window"
    else:
        state = "active"
    bucket = next((f"T-{t}" for t in DEFAULT_THRESHOLDS if d == t), None)
    return {
        "days_to_expiry": d,
        "state": state,
        "reminder_bucket": bucket,             # set only on an exact threshold day
        "membership_end": member["membership_end"],
        "renew_required": d <= 60,
    }


def _due_record(member: dict, ref: date) -> dict:
    return {
        "member_no": member["member_no"],
        "first_name_en": member["first_name_en"],
        "first_name_ar": member["first_name_ar"],
        "phone": member["phone"],
        "grade_en": member["grade_en"],
        "grade_ar": member["grade_ar"],
        "specialization_ar": member["specialization_ar"],
        "membership_end": member["membership_end"],
        "days_to_expiry": _days_to_expiry(member, ref),
        "status_en": member["status_en"],
    }


# --- endpoints ---
@app.get("/")
def root():
    return {
        "service": "SEC Renewal Agent — Data API",
        "members": len(MEMBERS),
        "endpoints": ["/health", "/member", "/renewals/due", "/renewals/summary"],
    }


@app.get("/health")
def health():
    return {"status": "ok", "members": len(MEMBERS)}


@app.get("/member")
def member(member_no: str = Query(..., description="e.g. M42"),
           as_of: str | None = Query(None)):
    """Workhorse lookup: full member package + computed renewal state, one call."""
    ref = _as_of(as_of)
    m = BY_NO.get(member_no.strip().upper())
    if not m:
        raise HTTPException(404, f"No member with member_no {member_no!r}")
    return {"member": m, "renewal": _renewal_block(m, ref)}


@app.get("/renewals/due")
def renewals_due(
    as_of: str | None = Query(None, description="defaults to today"),
    mode: str = Query("exact", pattern="^(exact|window)$"),
    thresholds: str = Query("60,30,14,7", description="exact-day buckets"),
    window: int = Query(30, ge=1, le=365, description="used when mode=window"),
    limit: int = Query(500, ge=1, le=2000),
):
    """
    Deterministic due-filter — the scan target.
    exact  : days_to_expiry lands exactly on one of `thresholds` (daily-scan semantics).
    window : 0 <= days_to_expiry <= `window` (ad-hoc browse).
    The agent receives a short, provably-complete list and does the send.
    """
    ref = _as_of(as_of)
    try:
        th = sorted({int(x) for x in thresholds.split(",") if x.strip()}, reverse=True)
    except ValueError:
        raise HTTPException(400, f"thresholds must be comma-separated ints, got {thresholds!r}")

    out = []
    for m in MEMBERS:
        d = _days_to_expiry(m, ref)
        hit = (d in th) if mode == "exact" else (0 <= d <= window)
        if hit:
            rec = _due_record(m, ref)
            rec["reminder_bucket"] = f"T-{d}" if (mode == "exact" and d in th) else None
            out.append(rec)
    out.sort(key=lambda r: r["days_to_expiry"])
    return {
        "as_of": ref.isoformat(),
        "mode": mode,
        "thresholds": th if mode == "exact" else None,
        "window": window if mode == "window" else None,
        "count": len(out),
        "members": out[:limit],
    }


@app.get("/renewals/summary")
def renewals_summary(as_of: str | None = Query(None)):
    """Ad-hoc analytics: counts by status and by forward window."""
    ref = _as_of(as_of)
    by_status: dict[str, int] = {}
    windows = {"7": 0, "14": 0, "30": 0, "60": 0, "90": 0}
    for m in MEMBERS:
        k = m["status_en"] or m["status_ar"] or "Unknown"
        by_status[k] = by_status.get(k, 0) + 1
        d = _days_to_expiry(m, ref)
        for w in windows:
            if 0 <= d <= int(w):
                windows[w] += 1
    return {"as_of": ref.isoformat(), "total": len(MEMBERS),
            "by_status": by_status, "expiring_within_days": windows}
