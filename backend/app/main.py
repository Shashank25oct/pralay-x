from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import os
from datetime import datetime, timezone
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, Response
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

DATABASE_URL = "sqlite:///./pralay_x.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class Base(DeclarativeBase):
    pass

class Incident(Base):
    __tablename__ = "incidents"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    disaster_type: Mapped[str] = mapped_column(String(40))
    severity: Mapped[str] = mapped_column(String(20))
    latitude: Mapped[float] = mapped_column(Float)
    longitude: Mapped[float] = mapped_column(Float)
    state: Mapped[str] = mapped_column(String(80))
    district: Mapped[str] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(30), default="ACTIVE")
    affected_population: Mapped[int] = mapped_column(Integer, default=0)
    casualties: Mapped[int] = mapped_column(Integer, default=0)
    description: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class Alert(Base):
    __tablename__ = "alerts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(160))
    severity: Mapped[str] = mapped_column(String(20))
    state: Mapped[str] = mapped_column(String(80))
    district: Mapped[str] = mapped_column(String(80))
    message: Mapped[str] = mapped_column(Text)
    official: Mapped[bool] = mapped_column(Boolean, default=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class Complaint(Base):
    __tablename__ = "complaints"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    complaint_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    category: Mapped[str] = mapped_column(String(60))
    message: Mapped[str] = mapped_column(Text)
    location: Mapped[str] = mapped_column(String(160), default="")
    contact: Mapped[str] = mapped_column(String(120), default="")
    status: Mapped[str] = mapped_column(String(30), default="RECEIVED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class RiskPrediction(Base):
    __tablename__ = "risk_predictions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    disaster_type: Mapped[str] = mapped_column(String(40))
    risk_score: Mapped[int] = mapped_column(Integer)
    risk_level: Mapped[str] = mapped_column(String(20))
    probability: Mapped[int] = mapped_column(Integer)
    prediction_window: Mapped[str] = mapped_column(String(80))
    state: Mapped[str] = mapped_column(String(80))
    district: Mapped[str] = mapped_column(String(80))
    factors: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class ResourceDispatch(Base):
    __tablename__ = "resource_dispatches"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dispatch_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    resource_type: Mapped[str] = mapped_column(String(80))
    quantity: Mapped[int] = mapped_column(Integer, default=1)
    state: Mapped[str] = mapped_column(String(80))
    district: Mapped[str] = mapped_column(String(80))
    note: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(30), default="DISPATCHED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class EvacuationOrder(Base):
    __tablename__ = "evacuation_orders"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    evacuation_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    incident_id: Mapped[str] = mapped_column(String(32), index=True)
    state: Mapped[str] = mapped_column(String(80))
    district: Mapped[str] = mapped_column(String(80))
    affected_population: Mapped[int] = mapped_column(Integer, default=0)
    destination_shelter_id: Mapped[str] = mapped_column(String(32), default="")
    status: Mapped[str] = mapped_column(String(30), default="ADVISORY")
    official: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class EmergencyRoute(Base):
    __tablename__ = "emergency_routes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    route_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    incident_id: Mapped[str] = mapped_column(String(32), index=True)
    route_type: Mapped[str] = mapped_column(String(40))
    state: Mapped[str] = mapped_column(String(80))
    district: Mapped[str] = mapped_column(String(80))
    origin_lat: Mapped[float] = mapped_column(Float)
    origin_lon: Mapped[float] = mapped_column(Float)
    destination_lat: Mapped[float] = mapped_column(Float)
    destination_lon: Mapped[float] = mapped_column(Float)
    distance_km: Mapped[float] = mapped_column(Float, default=0)
    eta_minutes: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(30), default="PLANNED")
    official: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class ChatHistory(Base):
    __tablename__ = "chat_history"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_id: Mapped[str] = mapped_column(String(120), index=True)
    audience: Mapped[str] = mapped_column(String(20), default="AUTHORITY")
    role: Mapped[str] = mapped_column(String(20))
    message: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String(80), default="ALL INDIA")
    confidence: Mapped[int] = mapped_column(Integer, default=0)
    sources: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

Base.metadata.create_all(engine)

app = FastAPI(title="Pralay X API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[x.strip() for x in os.getenv("PRALAY_FRONTEND_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",") if x.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Cache-Control"] = "no-store" if request.url.path.startswith("/api/v1/auth") else "no-cache"
    return response

# Prototype-only in-memory SSE subscribers.
subscribers: set[asyncio.Queue] = set()
sessions: dict[str, str] = {}

STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", "Goa",
    "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka", "Kerala",
    "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland",
    "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal", "Andaman and Nicobar Islands",
    "Chandigarh", "Dadra and Nagar Haveli and Daman and Diu", "Delhi", "Jammu and Kashmir",
    "Ladakh", "Lakshadweep", "Puducherry",
]

SHELTERS = [
    {"id": "SH-001", "name": "Raipur Community Relief Centre", "lat": 21.2514, "lon": 81.6296, "capacity": 2000, "occupancy": 1240, "medical": True, "food": True, "water": True},
    {"id": "SH-002", "name": "Central Sports Complex Shelter", "lat": 21.2387, "lon": 81.6338, "capacity": 1500, "occupancy": 620, "medical": True, "food": True, "water": True},
    {"id": "SH-003", "name": "District Relief School", "lat": 21.2668, "lon": 81.6112, "capacity": 900, "occupancy": 310, "medical": False, "food": True, "water": True},
]

HOSPITALS = [
    {"id": "HO-001", "name": "District Government Hospital", "lat": 21.2462, "lon": 81.6298, "open": True},
    {"id": "HO-002", "name": "Emergency Medical Centre", "lat": 21.2601, "lon": 81.6415, "open": True},
]

# Seeded emergency-response bases for simulation. These are clearly labelled prototype assets.
RESPONSE_BASES = {
    "POLICE": {"name": "Raipur Police Control Station", "lat": 21.2560, "lon": 81.6200, "emoji": "🚓"},
    "FIRE": {"name": "Raipur Fire Station", "lat": 21.2650, "lon": 81.6480, "emoji": "🚒"},
    "NDRF": {"name": "NDRF Rescue Base", "lat": 21.2300, "lon": 81.6000, "emoji": "🛟"},
    "JCB": {"name": "Municipal Heavy Equipment Yard", "lat": 21.2750, "lon": 81.6100, "emoji": "🚜"},
    "CRANE": {"name": "Heavy Rescue Crane Depot", "lat": 21.2200, "lon": 81.6500, "emoji": "🏗️"},
    "AMBULANCE": {"name": "District Ambulance Base", "lat": 21.2420, "lon": 81.6180, "emoji": "🚑"},
}

# Nationwide emergency service. State-specific entries are intentionally configurable rather than invented.
EMERGENCY_CONTACTS = [
    {"state": state, "emergency": "112", "state_specific": "Not configured — verify with the relevant state authority", "notes": "112 is the nationwide emergency number; operational integrations should be verified before deployment."}
    for state in STATES
]

STATE_CENTERS = {
    "Andhra Pradesh": (17.6868, 83.2185), "Assam": (26.1445, 91.7362), "Bihar": (25.5941, 85.1376),
    "Chhattisgarh": (21.2514, 81.6296), "Delhi": (28.6139, 77.2090), "Gujarat": (23.0225, 72.5714),
    "Haryana": (28.4595, 77.0266), "Himachal Pradesh": (31.1048, 77.1734), "Jharkhand": (23.3441, 85.3096),
    "Karnataka": (12.9716, 77.5946), "Kerala": (9.9312, 76.2673), "Madhya Pradesh": (23.2599, 77.4126),
    "Maharashtra": (19.0760, 72.8777), "Odisha": (20.2961, 85.8245), "Punjab": (30.9010, 75.8573),
    "Rajasthan": (26.9124, 75.7873), "Tamil Nadu": (13.0827, 80.2707), "Telangana": (17.3850, 78.4867),
    "Uttar Pradesh": (26.8467, 80.9462), "Uttarakhand": (30.3165, 78.0322), "West Bengal": (22.5726, 88.3639),
}

def state_layers(state: str | None = None):
    states = [state] if state and state != "ALL INDIA" else list(STATE_CENTERS)
    shelters, hospitals, bases = [], [], {}
    for idx, st in enumerate(states):
        lat, lon = STATE_CENTERS[st]
        shelters.extend([
            {"id": f"SH-{idx+1:03d}A", "name": f"{st} Community Relief Centre", "lat": lat+0.012, "lon": lon+0.010, "capacity": 2000, "occupancy": 620, "medical": True, "food": True, "water": True, "state": st},
            {"id": f"SH-{idx+1:03d}B", "name": f"{st} District Relief Shelter", "lat": lat-0.010, "lon": lon-0.008, "capacity": 1200, "occupancy": 310, "medical": True, "food": True, "water": True, "state": st},
        ])
        hospitals.extend([
            {"id": f"HO-{idx+1:03d}A", "name": f"{st} District Government Hospital", "lat": lat-0.005, "lon": lon+0.006, "open": True, "state": st},
            {"id": f"HO-{idx+1:03d}B", "name": f"{st} Emergency Medical Centre", "lat": lat+0.006, "lon": lon-0.004, "open": True, "state": st},
        ])
        bases[st] = {
            "POLICE": {"name": f"{st} Police Control Station", "lat": lat+0.005, "lon": lon-0.010, "emoji": "🚓"},
            "FIRE": {"name": f"{st} Fire Station", "lat": lat+0.010, "lon": lon+0.015, "emoji": "🚒"},
            "NDRF": {"name": f"{st} Rescue Base", "lat": lat-0.018, "lon": lon-0.020, "emoji": "🛟"},
            "JCB": {"name": f"{st} Municipal Heavy Equipment Yard", "lat": lat+0.020, "lon": lon-0.015, "emoji": "🚜"},
            "CRANE": {"name": f"{st} Heavy Rescue Crane Depot", "lat": lat-0.025, "lon": lon+0.020, "emoji": "🏗️"},
            "AMBULANCE": {"name": f"{st} District Ambulance Base", "lat": lat-0.009, "lon": lon-0.012, "emoji": "🚑"},
        }
    return shelters, hospitals, bases

def response_bases_for_state(state: str):
    return state_layers(state)[2].get(state, RESPONSE_BASES)


class LoginIn(BaseModel):
    user_id: str
    password: str

class ComplaintIn(BaseModel):
    category: str = Field(min_length=2, max_length=60)
    message: str = Field(min_length=5, max_length=4000)
    location: str = Field(default="", max_length=160)
    contact: str = Field(default="", max_length=120)

class ComplaintStatusIn(BaseModel):
    status: str

class SimulationIn(BaseModel):
    disaster_type: str = "Flood"
    severity: str = "HIGH"
    state: str = "Chhattisgarh"
    district: str = "Raipur"
    latitude: float = 21.2514
    longitude: float = 81.6296
    rainfall: float = 220
    river_level: float = 5.4
    population: int = 184000

class IncidentStatusIn(BaseModel):
    status: str

class AuthorityIncidentIn(BaseModel):
    disaster_type: str = Field(min_length=2, max_length=40)
    severity: str = "HIGH"
    state: str = Field(min_length=2, max_length=80)
    district: str = Field(min_length=2, max_length=80)
    latitude: float
    longitude: float
    description: str = Field(default="", max_length=4000)
    affected_population: int = Field(default=0, ge=0)

class AlertIn(BaseModel):
    title: str = Field(min_length=2, max_length=160)
    severity: str = "HIGH"
    state: str = Field(min_length=2, max_length=80)
    district: str = Field(default="", max_length=80)
    message: str = Field(min_length=5, max_length=4000)

class ResourceDispatchIn(BaseModel):
    resource_type: str = Field(min_length=2, max_length=80)
    quantity: int = Field(default=1, ge=1, le=10000)
    state: str = Field(min_length=2, max_length=80)
    district: str = Field(min_length=2, max_length=80)
    note: str = Field(default="", max_length=2000)

class BroadcastIn(AlertIn):
    pass

class EvacuationIn(BaseModel):
    incident_id: str = Field(min_length=3, max_length=32)
    state: str = Field(min_length=2, max_length=80)
    district: str = Field(min_length=2, max_length=80)
    affected_population: int = Field(default=0, ge=0)
    destination_shelter_id: str = Field(min_length=2, max_length=32)
    status: str = "ADVISORY"

class RouteIn(BaseModel):
    incident_id: str = Field(min_length=3, max_length=32)
    route_type: str = Field(min_length=2, max_length=40)
    state: str = Field(min_length=2, max_length=80)
    district: str = Field(min_length=2, max_length=80)
    origin_lat: float
    origin_lon: float
    destination_lat: float
    destination_lon: float



def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def password_hash(password: str) -> str:
    return hashlib.sha256(("pralay-demo-salt:" + password).encode()).hexdigest()

DEMO_PASSWORD_HASH = password_hash("PralayX@123")


def db():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def auth_user(request: Request):
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token or sessions.get(token) != "authority":
        raise HTTPException(status_code=401, detail="Unauthorized")
    return "authority"

async def broadcast(event: str, payload: dict):
    data = f"event: {event}\ndata: {json.dumps(payload)}\n\n"
    dead = []
    for q in list(subscribers):
        try:
            q.put_nowait(data)
        except Exception:
            dead.append(q)
    for q in dead:
        subscribers.discard(q)



class ChatIn(BaseModel):
    message: str = Field(min_length=1, max_length=2000)
    state: str = "ALL INDIA"
    session_id: str = Field(default="", max_length=120)

class SearchIn(BaseModel):
    query: str = ""
    state: str = "ALL INDIA"
    category: str = "ALL"


def _scope_state(value: str, state: str):
    return state == "ALL INDIA" or value == state

@app.get("/api/v1/workspace/analytics")
def workspace_analytics(state: str = "ALL INDIA", _: str = Depends(auth_user), db: Session = Depends(db)):
    incidents = db.scalars(select(Incident).order_by(Incident.created_at.desc())).all()
    scoped = [i for i in incidents if _scope_state(i.state, state)]
    active = [i for i in scoped if i.status in ["ACTIVE", "VERIFIED"]]
    by_disaster = {}
    by_severity = {}
    for i in active:
        by_disaster[i.disaster_type] = by_disaster.get(i.disaster_type, 0) + 1
        by_severity[i.severity] = by_severity.get(i.severity, 0) + 1
    return {"state": state, "active_incidents": len(active), "affected_population": sum(i.affected_population for i in active), "by_disaster": by_disaster, "by_severity": by_severity, "states_covered": len(set(i.state for i in scoped)), "incidents_total": len(scoped)}

@app.get("/api/v1/workspace/search")
def workspace_search(q: str = "", state: str = "ALL INDIA", category: str = "ALL", _: str = Depends(auth_user), db: Session = Depends(db)):
    ql = q.strip().lower()
    def match(text): return not ql or ql in str(text).lower()
    out=[]
    if category in ["ALL","INCIDENTS"]:
        for i in db.scalars(select(Incident).order_by(Incident.created_at.desc())).all():
            if _scope_state(i.state,state) and match(f"{i.incident_id} {i.disaster_type} {i.state} {i.district} {i.severity} {i.description}"):
                out.append({"type":"INCIDENT","id":i.incident_id,"title":f"{i.disaster_type} · {i.district}","detail":f"{i.state} · {i.severity} · {i.status}","state":i.state,"status":i.status})
    if category in ["ALL","ALERTS"]:
        for a in db.scalars(select(Alert).order_by(Alert.created_at.desc())).all():
            if _scope_state(a.state,state) and match(f"{a.id} {a.title} {a.state} {a.district} {a.severity} {a.message}"):
                out.append({"type":"ALERT","id":a.id,"title":a.title,"detail":f"{a.state} · {a.severity} · {'ACTIVE' if a.active else 'ENDED'}","state":a.state,"status":"ACTIVE" if a.active else "ENDED"})
    if category in ["ALL","RESOURCES"]:
        for r in db.scalars(select(ResourceDispatch).order_by(ResourceDispatch.created_at.desc())).all():
            if _scope_state(r.state,state) and match(f"{r.dispatch_id} {r.resource_type} {r.state} {r.district} {r.status}"):
                out.append({"type":"RESOURCE","id":r.dispatch_id,"title":f"{r.quantity} × {r.resource_type}","detail":f"{r.state} · {r.district} · {r.status}","state":r.state,"status":r.status})
    if category in ["ALL","SHELTERS"]:
        for x in state_layers(state)[0]:
            if match(f"{x['id']} {x['name']} {x['state']}"):
                out.append({"type":"SHELTER","id":x['id'],"title":x['name'],"detail":f"{x['state']} · {x['capacity']-x['occupancy']} spaces available","state":x['state'],"status":"OPEN"})
    return out[:60]

@app.post("/api/v1/workspace/ask")
def workspace_ask(body: ChatIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    incidents = [i for i in db.scalars(select(Incident).order_by(Incident.created_at.desc())).all() if _scope_state(i.state, body.state)]
    active = [i for i in incidents if i.status in ["ACTIVE","VERIFIED"]]
    alerts = [a for a in db.scalars(select(Alert).where(Alert.active == True)).all() if _scope_state(a.state, body.state)]
    risks = [r for r in db.scalars(select(RiskPrediction).order_by(RiskPrediction.created_at.desc())).all() if _scope_state(r.state, body.state)]
    risk = risks[0] if risks else None
    text=body.message.lower()
    if body.session_id:
        db.add(ChatHistory(session_id=body.session_id,audience="AUTHORITY",role="user",message=body.message,state=body.state)); db.commit()
    if any(k in text for k in ["resource","team","rescue","dispatch"]):
        answer=f"{len(db.scalars(select(ResourceDispatch)).all())} resource dispatch records are available. Current scoped active incidents: {len(active)}. Review Response Resources before assigning an operational action."
    elif any(k in text for k in ["evac","shelter","safe route"]):
        sh=state_layers(body.state)[0]
        answer=f"{len(sh)} prototype shelters are mapped for {body.state}. Evacuation orders and routes remain authority-controlled; use Evacuation & Shelters to inspect capacity and status."
    elif any(k in text for k in ["alert","warning"]):
        answer=f"There are {len(alerts)} active alerts in the selected scope. Verify source, timestamp and operational status before broadcasting or escalating."
    elif any(k in text for k in ["risk","forecast","prediction"]):
        answer=f"Current risk for {body.state}: {risk.risk_level} ({risk.risk_score}/100), {risk.probability}% probability over {risk.prediction_window}." if risk else f"No risk prediction is currently recorded for {body.state}. The prediction engine is waiting for validated data or simulation."
    else:
        answer=f"For {body.state}, PRALAY X currently shows {len(active)} active/verified incidents, {len(alerts)} active alerts and {sum(i.affected_population for i in active):,} people affected. This assistant is advisory; operational actions require authorized human review."
    sources=["PRALAY X operational data","simulation/seeded data where marked"]
    confidence=88 if risk else 72
    if body.session_id:
        db.add(ChatHistory(session_id=body.session_id,audience="AUTHORITY",role="assistant",message=answer,state=body.state,confidence=confidence,sources=json.dumps(sources))); db.commit()
    return {"answer":answer,"scope":body.state,"sources":sources,"confidence":confidence,"timestamp":now_iso()}

@app.get("/api/v1/workspace/chat-history")
def workspace_chat_history(session_id: str, state: str = "ALL INDIA", _: str = Depends(auth_user), db: Session = Depends(db)):
    rows=db.scalars(select(ChatHistory).where(ChatHistory.session_id==session_id, ChatHistory.audience=="AUTHORITY").order_by(ChatHistory.created_at.asc())).all()
    return [{"role":r.role,"message":r.message,"state":r.state,"confidence":r.confidence,"sources":json.loads(r.sources) if r.sources else [],"created_at":r.created_at.isoformat()} for r in rows[-100:]]

def _public_ai_answer(message: str, state: str, db: Session):
    incidents=[i for i in db.scalars(select(Incident).order_by(Incident.created_at.desc())).all() if _scope_state(i.state,state) and i.status in ["ACTIVE","VERIFIED"]]
    alerts=[a for a in db.scalars(select(Alert).where(Alert.active==True)).all() if _scope_state(a.state,state)]
    risks=[r for r in db.scalars(select(RiskPrediction).order_by(RiskPrediction.created_at.desc())).all() if _scope_state(r.state,state)]
    risk=risks[0] if risks else None
    text=message.lower()
    if any(k in text for k in ["shelter","relief centre","relief center","where can i go"]):
        shelters=state_layers(state)[0]
        answer=f"{len(shelters)} mapped prototype relief centres are available for {state}. Check the HELP NEAR YOU section for capacity and services. Follow official evacuation instructions if an emergency is active."
    elif any(k in text for k in ["alert","warning","notification"]):
        answer=f"There are {len(alerts)} active safety alerts in the selected scope. {alerts[0].title if alerts else 'No active alert is currently recorded.'}"
    elif any(k in text for k in ["safe","safety","danger","risk"]):
        answer=f"Current public safety status for {state}: {risk.risk_level} risk ({risk.risk_score}/100)" if risk else f"No critical emergency is currently recorded for {state}. PRALAY X continues monitoring; follow official local instructions."
        if risk: answer += f", with {risk.probability}% probability over {risk.prediction_window}."
    elif any(k in text for k in ["flood","cyclone","earthquake","fire","landslide","heat"]):
        answer="Follow official alerts, move away from immediate hazards, avoid floodwater or damaged structures, keep emergency contacts available, and use an official evacuation route when instructed. PRALAY X provides informational guidance and does not replace emergency services."
    else:
        answer=f"For {state}, PRALAY X currently shows {len(incidents)} active/verified incidents and {len(alerts)} active alerts. Use the map and official alerts for the latest verified situation."
    return answer, ["PRALAY X public safety data","simulation/seeded data where marked"], 86 if risk else 72

@app.post("/api/v1/public/ai/ask")
def public_ai_ask(body: ChatIn, db: Session = Depends(db)):
    answer,sources,confidence=_public_ai_answer(body.message,body.state,db)
    if body.session_id:
        db.add(ChatHistory(session_id=body.session_id,audience="PUBLIC",role="user",message=body.message,state=body.state))
        db.add(ChatHistory(session_id=body.session_id,audience="PUBLIC",role="assistant",message=answer,state=body.state,confidence=confidence,sources=json.dumps(sources)))
        db.commit()
    return {"answer":answer,"scope":body.state,"sources":sources,"confidence":confidence,"timestamp":now_iso()}

@app.get("/api/v1/public/ai/history")
def public_ai_history(session_id: str, db: Session = Depends(db)):
    rows=db.scalars(select(ChatHistory).where(ChatHistory.session_id==session_id, ChatHistory.audience=="PUBLIC").order_by(ChatHistory.created_at.asc())).all()
    return [{"role":r.role,"message":r.message,"state":r.state,"confidence":r.confidence,"sources":json.loads(r.sources) if r.sources else [],"created_at":r.created_at.isoformat()} for r in rows[-100:]]


@app.get("/api/v1/workspace/operational-plan")
def operational_plan(state: str = "ALL INDIA", incident_id: str = "", _: str = Depends(auth_user), db: Session = Depends(db)):
    """Return a transparent, deterministic 10/20/30-minute response plan for the latest simulated incident.
    This is an advisory planning view: it does not itself dispatch assets or publish an alert.
    """
    incidents = db.scalars(select(Incident).order_by(Incident.created_at.desc())).all()
    incidents = [i for i in incidents if _scope_state(i.state, state)]
    incident = next((i for i in incidents if i.incident_id == incident_id), incidents[0] if incidents else None)
    if not incident:
        return {"incident": None, "timeline": [], "summary": "No incident is available for this scope."}
    routes = db.scalars(select(EmergencyRoute).where(EmergencyRoute.incident_id == incident.incident_id).order_by(EmergencyRoute.id.asc())).all()
    route_map = {r.route_type: r for r in routes}
    labels = {
        "POLICE": ("POLICE", "Police unit", "🚓", "secure approach roads and establish traffic control"),
        "FIRE": ("FIRE", "Fire response", "🚒", "stage fire-response equipment and confirm access"),
        "RESCUE": ("NDRF", "NDRF rescue", "🛟", "mobilize rescue team and assess trapped/isolated persons"),
        "AMBULANCE": ("MEDICAL", "Ambulance", "🚑", "move medical transport toward the receiving hospital"),
        "JCB": ("MUNICIPAL", "JCB / debris clearance", "🚜", "prepare heavy equipment and clear the assigned corridor"),
        "CRANE": ("MUNICIPAL", "Heavy rescue crane", "🏗️", "stage crane support where access or lifting is required"),
        "EVACUATION": ("SHELTER", "Evacuation route", "🏠", "prepare the safe route and confirm shelter capacity"),
    }
    stages = {10: [], 20: [], 30: []}
    for route_type, r in route_map.items():
        dept, label, emoji, work = labels.get(route_type, (route_type, route_type.title(), "•", "complete the assigned response task"))
        eta = int(r.eta_minutes or 0)
        if route_type == "POLICE":
            t = 10 if eta <= 20 else 20
            action = f"{emoji} {label} assigned · road/traffic control corridor marked · route {r.distance_km:.1f} km · ETA {eta} min"
        elif route_type == "AMBULANCE":
            t = 10 if eta <= 20 else 20
            action = f"{emoji} {label} dispatched · receiving hospital linked · route {r.distance_km:.1f} km · ETA {eta} min"
        elif route_type == "RESCUE":
            t = 10 if eta <= 20 else 20
            action = f"{emoji} {label} team assigned · access corridor confirmed · route {r.distance_km:.1f} km · ETA {eta} min"
        elif route_type in ("FIRE", "JCB", "CRANE"):
            t = 20 if eta <= 30 else 30
            action = f"{emoji} {label} assigned · staging/access point set · route {r.distance_km:.1f} km · ETA {eta} min"
        else:
            t = 10
            action = f"{emoji} Shelter route prepared · destination capacity check required · {r.distance_km:.1f} km · ETA {eta} min"
        stages[t].append({"department": dept, "asset": label, "route_type": route_type, "action": action, "status": "PLANNED / ADVISORY"})
    # Fill each time horizon with coordination actions so the card remains useful even with few routes.
    stages[10].append({"department": "COMMAND", "asset": "Incident command", "route_type": "COMMAND", "action": f"Validate {incident.incident_id} · confirm {incident.district}, {incident.state} operating picture · issue only authorized actions", "status": "HUMAN REVIEW"})
    stages[20].append({"department": "COMMAND", "asset": "Situation update", "route_type": "COMMAND", "action": "Recheck route status, affected population and new field reports before expanding the response", "status": "REASSESS"})
    stages[30].append({"department": "COMMAND", "asset": "30-minute checkpoint", "route_type": "COMMAND", "action": "Confirm arrivals, road access, shelter readiness and outstanding resource gaps; update the operational picture", "status": "REASSESS"})
    for minute in stages:
        stages[minute].sort(key=lambda x: (x["department"] == "COMMAND", x["route_type"]))
    return {"incident": serialize_incident(incident), "timeline": [{"minute": minute, "title": f"T+{minute} MIN", "actions": stages[minute]} for minute in (10,20,30)], "summary": f"Advisory response plan for {incident.incident_id}. Route ETAs and current response assets are used to place work into the 10/20/30-minute checkpoints."}

@app.get("/api/v1/public/operational-plan")
def public_operational_plan(state: str = "ALL INDIA", incident_id: str = "", db: Session = Depends(db)):
    """Public, informational mirror of the response timeline. It never dispatches or publishes actions."""
    incidents = db.scalars(select(Incident).order_by(Incident.created_at.desc())).all()
    incidents = [i for i in incidents if _scope_state(i.state, state) and i.status.upper() != "RESOLVED"]
    incident = next((i for i in incidents if i.incident_id == incident_id), incidents[0] if incidents else None)
    if not incident:
        return {"incident": None, "timeline": [], "summary": "No active simulated response is available for this area."}
    routes = db.scalars(select(EmergencyRoute).where(EmergencyRoute.incident_id == incident.incident_id).order_by(EmergencyRoute.id.asc())).all()
    labels = {
        "POLICE": ("Police unit", "🚓", "Police response: approach corridor and traffic-control point are planned"),
        "FIRE": ("Fire response", "🚒", "Fire response: staging/access point is planned"),
        "RESCUE": ("NDRF rescue", "🛟", "Rescue response: access corridor and field assessment are planned"),
        "AMBULANCE": ("Ambulance", "🚑", "Medical response: receiving hospital and route are linked"),
        "JCB": ("JCB / debris clearance", "🚜", "Road access: heavy equipment is planned for the assigned corridor"),
        "CRANE": ("Heavy rescue crane", "🏗️", "Heavy rescue: lifting/access support is planned where required"),
        "EVACUATION": ("Safe evacuation route", "🏠", "Evacuation: destination and route information are available"),
    }
    stages = {10: [], 20: [], 30: []}
    for r in routes:
        label, emoji, work = labels.get(r.route_type, (r.route_type.title(), "•", "Response route is planned"))
        eta = int(r.eta_minutes or 0)
        t = 10 if eta <= 20 else (20 if eta <= 30 else 30)
        if r.route_type in ("FIRE", "JCB", "CRANE") and t < 20: t = 20
        stages[t].append({"asset": label, "emoji": emoji, "action": f"{work} · {r.distance_km:.1f} km · ETA {eta} min", "status": "INFORMATIONAL / SIMULATION"})
    stages[10].append({"asset": "Public safety information", "emoji": "📢", "action": "Follow official alerts, avoid the hazard area and keep an evacuation route available if instructed", "status": "SAFETY GUIDANCE"})
    stages[20].append({"asset": "Situation reassessment", "emoji": "🧭", "action": "Check updated alerts, route access and shelter status before moving", "status": "REASSESS"})
    stages[30].append({"asset": "Safety checkpoint", "emoji": "🛡️", "action": "Review the latest official situation and follow any new authority instruction", "status": "REASSESS"})
    timeline=[{"minute":m,"title":f"T+{m} MIN","actions":stages[m]} for m in (10,20,30)]
    return {"incident": serialize_incident(incident), "timeline": timeline, "summary": "Public informational timeline based on the current incident, mapped response routes and their ETA. It does not dispatch assets or replace official instructions."}

@app.get("/api/v1/workspace/forecast")
def workspace_forecast(state: str = "ALL INDIA", _: str = Depends(auth_user), db: Session = Depends(db)):
    risks=[r for r in db.scalars(select(RiskPrediction).order_by(RiskPrediction.created_at.desc())).all() if _scope_state(r.state,state)]
    r=risks[0] if risks else None
    return {"state":state,"risk":serialize_risk(r) if r else {"risk_score":12,"risk_level":"LOW","probability":14,"prediction_window":"Next 24 hrs","factors":["No validated active prediction in this scope"]},"forecast_windows":[{"window":"0–6 hrs","status":"MONITOR","note":"Review realtime indicators and new validated incidents."},{"window":"6–24 hrs","status":"ASSESS","note":"Compare risk trend with rainfall, river and incident observations."},{"window":"24–72 hrs","status":"PLAN","note":"Prepare resources only after authorized assessment."}]}

@app.get("/api/v1/workspace/impact")
def workspace_impact(state: str = "ALL INDIA", _: str = Depends(auth_user), db: Session = Depends(db)):
    inc=[i for i in db.scalars(select(Incident)).all() if _scope_state(i.state,state) and i.status in ["ACTIVE","VERIFIED"]]
    return {"state":state,"population":sum(i.affected_population for i in inc),"incidents":len(inc),"critical":sum(i.severity=="CRITICAL" for i in inc),"districts":len(set(i.district for i in inc)),"disaster_mix":sorted([{ "disaster":k,"count":v} for k,v in {d:sum(x.disaster_type==d for x in inc) for d in set(x.disaster_type for x in inc)}.items()], key=lambda x:-x['count'])}

@app.get("/api/v1/workspace/historical")
def workspace_historical(state: str = "ALL INDIA", _: str = Depends(auth_user), db: Session = Depends(db)):
    inc=[i for i in db.scalars(select(Incident).order_by(Incident.created_at.desc())).all() if _scope_state(i.state,state)]
    return {"state":state,"records":[{"date":i.created_at.strftime('%Y-%m-%d'),"incident":i.incident_id,"disaster":i.disaster_type,"severity":i.severity,"status":i.status,"population":i.affected_population} for i in inc[:30]]}

@app.get("/api/v1/workspace/compare")
def workspace_compare(state_a: str="Chhattisgarh", state_b: str="Maharashtra", _: str = Depends(auth_user), db: Session = Depends(db)):
    all_inc=db.scalars(select(Incident)).all()
    def pack(st):
        x=[i for i in all_inc if i.state==st and i.status in ["ACTIVE","VERIFIED"]]
        return {"state":st,"incidents":len(x),"population":sum(i.affected_population for i in x),"critical":sum(i.severity=="CRITICAL" for i in x)}
    return {"left":pack(state_a),"right":pack(state_b)}

@app.get("/api/v1/workspace/communication")
def workspace_communication(state: str="ALL INDIA", _: str=Depends(auth_user), db: Session=Depends(db)):
    rows=[a for a in db.scalars(select(Alert).order_by(Alert.created_at.desc())).all() if _scope_state(a.state,state)]
    return [{"id":a.id,"title":a.title,"state":a.state,"district":a.district,"severity":a.severity,"status":"DELIVERED" if a.official else "SIMULATION","created_at":a.created_at.isoformat(),"message":a.message} for a in rows[:40]]

@app.get("/api/v1/workspace/admin")
def workspace_admin(_: str=Depends(auth_user), db: Session=Depends(db)):
    incidents=db.scalars(select(Incident)).all(); alerts=db.scalars(select(Alert)).all(); complaints=db.scalars(select(Complaint)).all()
    audit=[]
    for i in incidents[:10]: audit.append({"time":i.created_at.isoformat(),"action":"INCIDENT","actor":"Authority / Simulation","reference":i.incident_id,"status":i.status})
    for a in alerts[:10]: audit.append({"time":a.created_at.isoformat(),"action":"ALERT","actor":"Authority / Simulation","reference":str(a.id),"status":"ACTIVE" if a.active else "ENDED"})
    audit=sorted(audit,key=lambda x:x['time'],reverse=True)[:20]
    return {"users":[{"id":"USR-001","name":"National Authority","role":"NATIONAL_AUTHORITY","status":"ACTIVE","mfa":"READY"}],"roles":[{"name":"NATIONAL_AUTHORITY","permissions":"Full command-centre operations"},{"name":"STATE_AUTHORITY","permissions":"State-scoped investigation and response"},{"name":"FIELD_OPERATOR","permissions":"Field updates and assigned operations"}],"permissions":["VIEW_GIS","VERIFY_INCIDENT","PUBLISH_ALERT","DISPATCH_RESOURCE","ORDER_EVACUATION","GENERATE_REPORT","MANAGE_DATA"],"audit_logs":audit,"system_health":{"api":"ONLINE","database":"HEALTHY","gis":"ONLINE","ai_engine":"ONLINE","realtime":"ONLINE"},"settings":{"simulation_mode":"ENABLED","ai_actions":"HUMAN APPROVAL REQUIRED","data_policy":"SOURCE + TIMESTAMP + VERIFICATION"},"complaints_total":len(complaints)}

@app.get("/api/v1/health")
def health():
    return {"status": "ONLINE", "api": "ONLINE", "database": "HEALTHY", "gis": "ONLINE", "ai_engine": "ONLINE", "data_ingestion": "ONLINE", "alert_service": "ONLINE"}

@app.post("/api/v1/auth/login")
def login(body: LoginIn):
    if body.user_id.lower() != "admin@pralayx.gov.in" or password_hash(body.password) != DEMO_PASSWORD_HASH:
        raise HTTPException(status_code=401, detail="Email or password is incorrect.")
    token = secrets.token_urlsafe(32)
    sessions[token] = "authority"
    return {"access_token": token, "role": "NATIONAL_AUTHORITY", "display_name": "Pralay X Authority"}

@app.post("/api/v1/auth/logout")
def logout(request: Request):
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    sessions.pop(token, None)
    return {"ok": True}

@app.get("/api/v1/sse")
async def sse(request: Request):
    q: asyncio.Queue = asyncio.Queue()
    subscribers.add(q)
    async def generator() -> AsyncGenerator[str, None]:
        try:
            yield f"event: connected\ndata: {json.dumps({'time': now_iso()})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    item = await asyncio.wait_for(q.get(), timeout=20)
                    yield item
                except asyncio.TimeoutError:
                    yield f": heartbeat {now_iso()}\n\n"
        finally:
            subscribers.discard(q)
    return StreamingResponse(generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/api/v1/public/dashboard")
def public_dashboard(db: Session = Depends(db)):
    incidents = db.scalars(select(Incident).where(Incident.status.in_(["ACTIVE", "VERIFIED"]))).all()
    alerts = db.scalars(select(Alert).where(Alert.active == True).order_by(Alert.created_at.desc())).all()
    risks = db.scalars(select(RiskPrediction).order_by(RiskPrediction.created_at.desc())).all()
    risk = risks[0] if risks else None
    evacuation_rows = db.scalars(select(EvacuationOrder).order_by(EvacuationOrder.created_at.desc())).all()
    route_rows = db.scalars(select(EmergencyRoute).order_by(EmergencyRoute.created_at.desc())).all()
    current_evac = next((e for e in evacuation_rows if e.status not in {"COMPLETED", "CANCELLED"}), None)
    evacuation_view = None
    if current_evac:
        evac_shelters = state_layers(current_evac.state)[0]
        shelter = next((x for x in evac_shelters if x["id"] == current_evac.destination_shelter_id), evac_shelters[0])
        er = next((r for r in route_rows if r.incident_id == current_evac.incident_id and r.route_type == "EVACUATION"), None)
        evacuation_view = {**serialize_evacuation(current_evac), "shelter_name": shelter["name"], "available": shelter["capacity"]-shelter["occupancy"], "distance_km": er.distance_km if er else 0, "eta_minutes": er.eta_minutes if er else 0}
    return {
        "safety_status": "HIGH ALERT" if risk and risk.risk_score >= 70 else "NO ACTIVE CRITICAL ALERT",
        "safety_message": "A simulated hazard is active in the selected area." if risk else "Your selected area currently has no critical emergency.",
        "active_alerts": [serialize_alert(a) for a in alerts],
        "nearby_incidents": [serialize_incident(i) for i in incidents],
        "nearby_shelters": state_layers("ALL INDIA")[0],
        "nearby_hospitals": state_layers("ALL INDIA")[1],
        "risk_outlook": serialize_risk(risk) if risk else None,
        "official_updates": [serialize_alert(a) for a in alerts if a.official][:5],
        "emergency_contacts": EMERGENCY_CONTACTS,
        "evacuations": [serialize_evacuation(x) for x in db.scalars(select(EvacuationOrder).order_by(EvacuationOrder.created_at.desc())).all()],
        "routes": [serialize_route(x) for x in route_rows],
        "evacuation": evacuation_view,
    }

@app.get("/api/v1/dashboard/overview")
def authority_dashboard(_: str = Depends(auth_user), db: Session = Depends(db)):
    incidents = db.scalars(select(Incident).order_by(Incident.created_at.desc())).all()
    alerts = db.scalars(select(Alert).where(Alert.active == True).order_by(Alert.created_at.desc())).all()
    complaints = db.scalars(select(Complaint).order_by(Complaint.created_at.desc())).all()
    risks = db.scalars(select(RiskPrediction).order_by(RiskPrediction.created_at.desc())).all()
    active = [i for i in incidents if i.status in ["ACTIVE", "VERIFIED"]]
    critical = [i for i in active if i.severity == "CRITICAL"]
    affected = sum(i.affected_population for i in active)
    return {
        "active_incidents": len(active),
        "states_affected": len(set(i.state for i in active)),
        "critical_incidents": len(critical),
        "affected_population": affected,
        "rescue_operations": 126 + len(active) * 4,
        "active_resources": 1842 - len(active) * 8,
        "ai_risk": serialize_risk(risks[0]) if risks else {"risk_score": 12, "risk_level": "LOW", "probability": 14, "prediction_window": "Next 24 hrs", "factors": ["No active simulated hazard"]},
        "risk_predictions": [serialize_risk(r) for r in risks[:20]],
        "incidents": [serialize_incident(i) for i in incidents[:20]],
        "alerts": [serialize_alert(a) for a in alerts[:20]],
        "complaints": [serialize_complaint(c) for c in complaints[:20]],
        "state_summary": state_summary(active),
        "disaster_summary": disaster_summary(active),
        "shelters": state_layers("ALL INDIA")[0],
        "hospitals": state_layers("ALL INDIA")[1],
        "response_bases": state_layers("ALL INDIA")[2],
        "evacuations": [serialize_evacuation(x) for x in db.scalars(select(EvacuationOrder).order_by(EvacuationOrder.created_at.desc())).all()],
        "routes": [serialize_route(x) for x in db.scalars(select(EmergencyRoute).order_by(EmergencyRoute.created_at.desc())).all()],
        "system_health": {"api": "ONLINE", "database": "HEALTHY", "gis": "ONLINE", "ai_engine": "ONLINE", "data_ingestion": "ONLINE", "alert_service": "ONLINE"},
    }

@app.get("/api/v1/map/layers")
def map_layers(state: str = "ALL INDIA"):
    shelters, hospitals, bases = state_layers(state)
    return {"shelters": shelters, "hospitals": hospitals, "response_bases": bases}

@app.post("/api/v1/citizen-reports")
async def create_complaint(body: ComplaintIn, db: Session = Depends(db)):
    c = Complaint(complaint_id=f"PX-C-{secrets.token_hex(4).upper()}", category=body.category, message=body.message, location=body.location, contact=body.contact)
    db.add(c); db.commit(); db.refresh(c)
    payload = serialize_complaint(c)
    await broadcast("complaint.created", payload)
    return payload

@app.get("/api/v1/citizen-reports")
def list_complaints(_: str = Depends(auth_user), db: Session = Depends(db)):
    return [serialize_complaint(c) for c in db.scalars(select(Complaint).order_by(Complaint.created_at.desc())).all()]

@app.patch("/api/v1/citizen-reports/{complaint_id}")
async def update_complaint(complaint_id: str, body: ComplaintStatusIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    c = db.scalar(select(Complaint).where(Complaint.complaint_id == complaint_id))
    if not c: raise HTTPException(404, "Complaint not found")
    c.status = body.status; db.commit(); db.refresh(c)
    payload = serialize_complaint(c); await broadcast("complaint.updated", payload); return payload

@app.get("/api/v1/emergency-contacts")
def emergency_contacts():
    return EMERGENCY_CONTACTS

@app.post("/api/v1/incidents")
async def create_authority_incident(body: AuthorityIncidentIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    incident = Incident(incident_id=f"PX-{secrets.randbelow(90000)+10000}", disaster_type=body.disaster_type.upper(), severity=body.severity.upper(), latitude=body.latitude, longitude=body.longitude, state=body.state, district=body.district, status="UNVERIFIED", affected_population=body.affected_population, casualties=0, description=body.description)
    db.add(incident); db.commit(); db.refresh(incident)
    payload = serialize_incident(incident); await broadcast("incident.created", payload); return payload

@app.post("/api/v1/alerts")
async def create_authority_alert(body: AlertIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    alert = Alert(title=body.title, severity=body.severity.upper(), state=body.state, district=body.district, message=body.message, official=True, active=True)
    db.add(alert); db.commit(); db.refresh(alert)
    payload = serialize_alert(alert); await broadcast("alert.created", payload); return payload

@app.patch("/api/v1/alerts/{alert_id}")
async def update_alert(alert_id: int, body: dict, _: str = Depends(auth_user), db: Session = Depends(db)):
    alert = db.get(Alert, alert_id)
    if not alert:
        raise HTTPException(404, "Alert not found")
    if "active" in body:
        alert.active = bool(body["active"])
    db.commit(); db.refresh(alert)
    payload = serialize_alert(alert)
    await broadcast("alert.updated", payload)
    return payload

@app.post("/api/v1/resources/dispatch")
async def dispatch_resource(body: ResourceDispatchIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    dispatch = ResourceDispatch(dispatch_id=f"PX-D-{secrets.token_hex(4).upper()}", resource_type=body.resource_type, quantity=body.quantity, state=body.state, district=body.district, note=body.note)
    db.add(dispatch); db.commit(); db.refresh(dispatch)
    payload = {"dispatch_id": dispatch.dispatch_id, "resource_type": dispatch.resource_type, "quantity": dispatch.quantity, "state": dispatch.state, "district": dispatch.district, "note": dispatch.note, "status": dispatch.status, "created_at": dispatch.created_at.isoformat()}
    await broadcast("resource.dispatched", payload); return payload

@app.get("/api/v1/resources")
def list_resources(_: str = Depends(auth_user), db: Session = Depends(db)):
    rows = db.scalars(select(ResourceDispatch).order_by(ResourceDispatch.created_at.desc())).all()
    return [{"dispatch_id": r.dispatch_id, "resource_type": r.resource_type, "quantity": r.quantity, "state": r.state, "district": r.district, "note": r.note, "status": r.status, "created_at": r.created_at.isoformat() if r.created_at else None} for r in rows]

@app.patch("/api/v1/resources/dispatch/{dispatch_id}")
async def update_resource_dispatch(dispatch_id: str, body: dict, _: str = Depends(auth_user), db: Session = Depends(db)):
    r = db.scalar(select(ResourceDispatch).where(ResourceDispatch.dispatch_id == dispatch_id))
    if not r: raise HTTPException(404, "Dispatch not found")
    if "status" in body:
        allowed = {"AVAILABLE", "DEPLOYED", "IN TRANSIT", "MAINTENANCE"}
        status = str(body["status"]).upper()
        if status not in allowed: raise HTTPException(400, "Invalid resource status")
        r.status = status
    db.commit(); db.refresh(r)
    payload = {"dispatch_id": r.dispatch_id, "resource_type": r.resource_type, "quantity": r.quantity, "state": r.state, "district": r.district, "note": r.note, "status": r.status, "created_at": r.created_at.isoformat() if r.created_at else None}
    await broadcast("resource.updated", payload); return payload

@app.get("/api/v1/ai/recommendations")
def ai_recommendations(_: str = Depends(auth_user), db: Session = Depends(db)):
    incidents = [i for i in db.scalars(select(Incident).order_by(Incident.created_at.desc())).all() if str(i.status).upper() not in {"RESOLVED", "CANCELLED"}]
    rows = []
    for i in incidents[:8]:
        sev = str(i.severity).upper()
        priority = "CRITICAL" if sev == "CRITICAL" else "HIGH" if sev == "HIGH" else "MEDIUM"
        actions = [
            ("NDRF / RESCUE", "Pre-position rescue teams and establish an incident command point.", "Rescue Team"),
            ("MEDICAL", "Prepare emergency medical capacity and ambulance coverage.", "Ambulance"),
            ("POLICE", "Coordinate traffic control, access management and public safety perimeter.", "Vehicle"),
            ("MUNICIPAL", "Clear local access routes and coordinate civic emergency support.", "Vehicle"),
            ("SHELTER", "Review nearby shelter capacity and prepare overflow arrangements.", "Relief Materials"),
        ]
        for dept, text, resource in actions:
            rows.append({"id": f"REC-{i.incident_id}-{dept.replace(' ','-')}", "incident_id": i.incident_id, "department": dept, "priority": priority, "reason": text, "confidence": 92 if priority == "CRITICAL" else 86, "state": i.state, "district": i.district, "resource_type": resource, "status": "RECOMMENDED"})
    return rows[:18]

@app.post("/api/v1/data-entry")
async def add_data(body: AuthorityIncidentIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    incident = Incident(incident_id=f"PX-DATA-{secrets.token_hex(3).upper()}", disaster_type=body.disaster_type.upper(), severity=body.severity.upper(), latitude=body.latitude, longitude=body.longitude, state=body.state, district=body.district, status="UNVERIFIED", affected_population=body.affected_population, casualties=0, description=body.description)
    db.add(incident); db.commit(); db.refresh(incident)
    payload = serialize_incident(incident); await broadcast("incident.created", payload); return payload

@app.post("/api/v1/communication/broadcast")
async def create_broadcast(body: BroadcastIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    alert = Alert(title=body.title, severity=body.severity.upper(), state=body.state, district=body.district, message=body.message, official=True, active=True)
    db.add(alert); db.commit(); db.refresh(alert)
    payload = serialize_alert(alert); await broadcast("alert.created", payload); return payload

@app.post("/api/v1/simulation/reset")
async def reset_simulation(_: str = Depends(auth_user), db: Session = Depends(db)):
    """Remove only records generated by Simulation Mode and restore the clean demo baseline."""
    sim_incidents = db.scalars(select(Incident).where(Incident.description.like("Simulation event:%"))).all()
    sim_ids = {i.incident_id for i in sim_incidents}
    deleted = 0
    for model in (EmergencyRoute, EvacuationOrder):
        rows = db.scalars(select(model)).all()
        for row in rows:
            if getattr(row, "incident_id", None) in sim_ids:
                db.delete(row); deleted += 1
    for row in db.scalars(select(Alert)).all():
        if not row.official and row.message.lower().startswith("simulated "):
            db.delete(row); deleted += 1
    for row in db.scalars(select(RiskPrediction)).all():
        if row.state and any(i.state == row.state and i.district == row.district for i in sim_incidents):
            db.delete(row); deleted += 1
    for row in sim_incidents:
        db.delete(row); deleted += 1
    db.commit()
    await broadcast("simulation.reset", {"deleted": deleted, "time": now_iso()})
    return {"ok": True, "deleted": deleted, "message": "Simulation data reset. Operational and citizen-submitted records were retained."}

@app.post("/api/v1/simulation/run")
async def run_simulation(body: SimulationIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    # Prototype risk logic: intentionally transparent and deterministic.
    base = {"LOW": 35, "MODERATE": 55, "HIGH": 78, "CRITICAL": 92}.get(body.severity.upper(), 55)
    rainfall_factor = min(15, max(0, int((body.rainfall - 100) / 10)))
    river_factor = min(12, max(0, int((body.river_level - 3) * 4)))
    score = min(99, base + rainfall_factor + river_factor)
    level = "CRITICAL" if score >= 85 else "HIGH" if score >= 70 else "MODERATE" if score >= 45 else "LOW"
    probability = min(98, score + 4)
    factors = ["Rainfall forecast increased", "River level rising", "Soil saturation / terrain proxy", "Population and infrastructure exposure"]
    incident = Incident(incident_id=f"PX-{secrets.randbelow(90000)+10000}", disaster_type=body.disaster_type.upper(), severity=level, latitude=body.latitude, longitude=body.longitude, state=body.state, district=body.district, affected_population=body.population, casualties=0, description=f"Simulation event: {body.disaster_type} with rainfall {body.rainfall} mm and river level {body.river_level} m.")
    db.add(incident)
    risk = RiskPrediction(disaster_type=body.disaster_type.upper(), risk_score=score, risk_level=level, probability=probability, prediction_window="Next 12–24 hrs", state=body.state, district=body.district, factors=json.dumps(factors))
    db.add(risk)
    alert = Alert(title=f"{body.disaster_type.title()} Warning — {body.district}", severity=level, state=body.state, district=body.district, message=f"Simulated {body.disaster_type.lower()} risk has reached {level.lower()} level. Review safety instructions and response readiness.", official=False, active=True)
    db.add(alert); db.commit(); db.refresh(incident); db.refresh(risk); db.refresh(alert)
    if score >= 70:
        state_shelters, state_hospitals, state_bases = state_layers(body.state)
        shelter = next((s for s in state_shelters if s["capacity"] - s["occupancy"] >= max(1, min(body.population, 500))), state_shelters[0])
        evac = EvacuationOrder(evacuation_id=f"PX-E-{secrets.token_hex(4).upper()}", incident_id=incident.incident_id, state=body.state, district=body.district, affected_population=body.population, destination_shelter_id=shelter["id"], status="ORDERED" if score >= 85 else "ADVISORY", official=False)
        db.add(evac)
        hospital = next((h for h in state_hospitals if h["open"]), state_hospitals[0])
        bases = state_bases.get(body.state, RESPONSE_BASES)
        route_specs = [
            ("EVACUATION", body.latitude, body.longitude, shelter["lat"], shelter["lon"]),
            ("AMBULANCE", bases["AMBULANCE"]["lat"], bases["AMBULANCE"]["lon"], hospital["lat"], hospital["lon"]),
            ("RESCUE", bases["NDRF"]["lat"], bases["NDRF"]["lon"], body.latitude, body.longitude),
            ("POLICE", bases["POLICE"]["lat"], bases["POLICE"]["lon"], body.latitude, body.longitude),
            ("FIRE", bases["FIRE"]["lat"], bases["FIRE"]["lon"], body.latitude, body.longitude),
            ("JCB", bases["JCB"]["lat"], bases["JCB"]["lon"], body.latitude, body.longitude),
            ("CRANE", bases["CRANE"]["lat"], bases["CRANE"]["lon"], body.latitude, body.longitude),
        ]
        for route_type, olat, olon, dlat, dlon in route_specs:
            distance=((dlat-olat)**2+(dlon-olon)**2)**0.5*111
            db.add(EmergencyRoute(route_id=f"PX-R-{secrets.token_hex(4).upper()}", incident_id=incident.incident_id, route_type=route_type, state=body.state, district=body.district, origin_lat=olat, origin_lon=olon, destination_lat=dlat, destination_lon=dlon, distance_km=round(distance,1), eta_minutes=max(4,round(distance/0.65)), status="PLANNED", official=False))
        db.commit(); db.refresh(evac)
    await broadcast("incident.created", serialize_incident(incident))
    await broadcast("risk.updated", serialize_risk(risk))
    await broadcast("alert.created", serialize_alert(alert))
    for e in db.scalars(select(EvacuationOrder).where(EvacuationOrder.incident_id == incident.incident_id)).all(): await broadcast("evacuation.created", serialize_evacuation(e))
    for r in db.scalars(select(EmergencyRoute).where(EmergencyRoute.incident_id == incident.incident_id)).all(): await broadcast("route.created", serialize_route(r))
    return {"incident": serialize_incident(incident), "risk": serialize_risk(risk), "alert": serialize_alert(alert)}

@app.patch("/api/v1/incidents/{incident_id}")
async def update_incident(incident_id: str, body: IncidentStatusIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    i = db.scalar(select(Incident).where(Incident.incident_id == incident_id))
    if not i: raise HTTPException(404, "Incident not found")
    i.status = body.status; db.commit(); db.refresh(i)
    payload = serialize_incident(i); await broadcast("incident.updated", payload); return payload

@app.get("/api/v1/evacuations")
def list_evacuations(_: str = Depends(auth_user), db: Session = Depends(db)):
    return [serialize_evacuation(x) for x in db.scalars(select(EvacuationOrder).order_by(EvacuationOrder.created_at.desc())).all()]

@app.post("/api/v1/evacuations")
async def create_evacuation(body: EvacuationIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    if not db.scalar(select(Incident).where(Incident.incident_id == body.incident_id)):
        raise HTTPException(404, "Incident not found")
    # Shelter IDs shown by the dashboard are state-scoped prototype assets
    # produced by state_layers(). Validate against the selected incident state
    # rather than the legacy Chhattisgarh-only seed list.
    incident = db.scalar(select(Incident).where(Incident.incident_id == body.incident_id))
    if not incident:
        raise HTTPException(404, "Incident not found")
    if incident.state != body.state:
        raise HTTPException(400, "Evacuation state does not match incident state")
    state_shelters, _, _ = state_layers(body.state)
    shelter = next((s for s in state_shelters if s["id"] == body.destination_shelter_id), None)
    if not shelter:
        raise HTTPException(404, "Shelter not found for selected state")
    e = EvacuationOrder(evacuation_id=f"PX-E-{secrets.token_hex(4).upper()}", incident_id=body.incident_id, state=body.state, district=body.district, affected_population=body.affected_population, destination_shelter_id=body.destination_shelter_id, status=body.status.upper(), official=True)
    db.add(e); db.commit(); db.refresh(e)
    payload=serialize_evacuation(e); await broadcast("evacuation.created", payload); return payload

@app.patch("/api/v1/evacuations/{evacuation_id}")
async def update_evacuation(evacuation_id: str, body: dict, _: str = Depends(auth_user), db: Session = Depends(db)):
    e=db.scalar(select(EvacuationOrder).where(EvacuationOrder.evacuation_id == evacuation_id))
    if not e: raise HTTPException(404, "Evacuation order not found")
    status=str(body.get("status", e.status)).upper()
    if status not in {"ADVISORY","ORDERED","IN PROGRESS","COMPLETED","CANCELLED"}: raise HTTPException(400,"Invalid evacuation status")
    e.status=status; db.commit(); db.refresh(e); payload=serialize_evacuation(e); await broadcast("evacuation.updated",payload); return payload

@app.get("/api/v1/routes")
def list_routes(_: str = Depends(auth_user), db: Session = Depends(db)):
    return [serialize_route(x) for x in db.scalars(select(EmergencyRoute).order_by(EmergencyRoute.created_at.desc())).all()]

@app.post("/api/v1/routes")
async def create_route(body: RouteIn, _: str = Depends(auth_user), db: Session = Depends(db)):
    distance=((body.destination_lat-body.origin_lat)**2+(body.destination_lon-body.origin_lon)**2)**0.5*111
    eta=max(4, round(distance/0.65))
    r=EmergencyRoute(route_id=f"PX-R-{secrets.token_hex(4).upper()}", incident_id=body.incident_id, route_type=body.route_type.upper(), state=body.state, district=body.district, origin_lat=body.origin_lat, origin_lon=body.origin_lon, destination_lat=body.destination_lat, destination_lon=body.destination_lon, distance_km=round(distance,1), eta_minutes=eta, status="PLANNED", official=True)
    db.add(r); db.commit(); db.refresh(r); payload=serialize_route(r); await broadcast("route.created",payload); return payload

@app.get("/api/v1/reports/situation")
def situation_report(state: str = "ALL INDIA", _: str = Depends(auth_user), db: Session = Depends(db)):
    incidents = db.scalars(select(Incident).order_by(Incident.created_at.desc())).all()
    complaints = db.scalars(select(Complaint).order_by(Complaint.created_at.desc())).all()
    alerts = db.scalars(select(Alert).order_by(Alert.created_at.desc())).all()
    risks = db.scalars(select(RiskPrediction).order_by(RiskPrediction.created_at.desc())).all()
    scoped_risks = risks if state == "ALL INDIA" else [r for r in risks if r.state == state]
    scoped_evac = db.scalars(select(EvacuationOrder).order_by(EvacuationOrder.created_at.desc())).all()
    scoped_routes = db.scalars(select(EmergencyRoute).order_by(EmergencyRoute.created_at.desc())).all()
    if state != "ALL INDIA":
        scoped_evac = [e for e in scoped_evac if e.state == state]
        scoped_routes = [r for r in scoped_routes if r.state == state]
    scoped = [i for i in incidents if state == "ALL INDIA" or i.state == state]
    scoped_complaints = complaints if state == "ALL INDIA" else [c for c in complaints if state.lower() in (c.location or "").lower()]
    scoped_alerts = alerts if state == "ALL INDIA" else [a for a in alerts if a.state == state]
    current_incidents = [i for i in scoped if i.status.upper() != "RESOLVED"]
    active_incidents = [i for i in scoped if i.status.upper() in {"ACTIVE", "VERIFIED"}]
    risk = scoped_risks[0] if scoped_risks else None
    states = sorted({i.state for i in current_incidents})
    primary_state = state if state != "ALL INDIA" else (states[0] if len(states) == 1 else "ALL INDIA")
    primary_district = current_incidents[0].district if len(current_incidents) == 1 else ("MULTI-DISTRICT" if current_incidents else "NATIONAL MONITOR")
    report_id = f"PX-SR-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{secrets.token_hex(3).upper()}"
    return {
        "report_id": report_id, "generated_at": now_iso(),
        "title": "PRALAY X — SITUATION REPORT",
        "department": "PRALAY X NATIONAL / STATE DISASTER INTELLIGENCE & COMMAND CENTRE",
        "authority": "DISASTER MANAGEMENT & EMERGENCY RESPONSE",
        "state": primary_state, "district": primary_district,
        "active_incidents": len(active_incidents),
        "current_incidents": len(current_incidents),
        "affected_population": sum((i.affected_population or 0) for i in current_incidents),
        "critical_incidents": sum(1 for i in active_incidents if i.severity.upper() == "CRITICAL"),
        "complaints_received": len(scoped_complaints),
        "active_alerts": sum(1 for a in scoped_alerts if a.active),
        "risk": serialize_risk(risk) if risk else {"risk_score": 12, "risk_level": "LOW", "probability": 14, "prediction_window": "Next 24 hrs", "factors": ["No active simulated hazard"]},
        "incidents": [serialize_incident(i) for i in current_incidents[:12]],
        "alerts": [serialize_alert(a) for a in scoped_alerts if a.active][:8],
        "complaints": [serialize_complaint(c) for c in scoped_complaints[:8]],
        "evacuations": [serialize_evacuation(e) for e in scoped_evac[:8]],
        "routes": [serialize_route(r) for r in scoped_routes[:12]],
        "summary": "Operational situation report generated from the current PRALAY X data platform, including incident, risk, alert, response and evacuation information.",
        "verification_note": "Operational records retain status, source context and timestamps; critical actions remain under authorized human control.",
    }


@app.get("/api/v1/reports/situation/pdf")
def situation_report_pdf(state: str = "ALL INDIA", _: str = Depends(auth_user), db: Session = Depends(db)):
    """Return a real self-contained PDF for the report download action."""
    from io import BytesIO
    from pathlib import Path
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, KeepTogether, Flowable
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    data = situation_report(state=state, _="", db=db)
    base_dir = Path(__file__).resolve().parent
    asset_dir = base_dir / "assets"
    logo_path = asset_dir / "pralay-x-logo.png"
    font_path = asset_dir / "DejaVuSans.ttf"
    bold_path = asset_dir / "DejaVuSans-Bold.ttf"
    try:
        pdfmetrics.registerFont(TTFont("PXSans", str(font_path)))
        pdfmetrics.registerFont(TTFont("PXSans-Bold", str(bold_path)))
        regular, bold = "PXSans", "PXSans-Bold"
    except Exception:
        regular, bold = "Helvetica", "Helvetica-Bold"

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="PXBody", parent=styles["BodyText"], fontName=regular, fontSize=8.3, leading=11.5, textColor=colors.HexColor("#132235"), spaceAfter=4))
    styles.add(ParagraphStyle(name="PXSmall", parent=styles["BodyText"], fontName=regular, fontSize=6.8, leading=9, textColor=colors.HexColor("#66778a")))
    styles.add(ParagraphStyle(name="PXHead", parent=styles["Heading2"], fontName=bold, fontSize=10, leading=12, textColor=colors.HexColor("#0b2947"), spaceBefore=8, spaceAfter=4))
    styles.add(ParagraphStyle(name="PXTitle", parent=styles["Title"], fontName=bold, fontSize=16, leading=19, textColor=colors.HexColor("#071b2d"), alignment=TA_LEFT))

    def esc(v):
        return str(v if v is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    buf=BytesIO()
    doc=SimpleDocTemplate(buf,pagesize=A4,leftMargin=12*mm,rightMargin=12*mm,topMargin=12*mm,bottomMargin=14*mm,title=data["title"],author="PRALAY X")
    story=[]
    header_cells=[]
    if logo_path.exists():
        header_cells.append(Image(str(logo_path),width=18*mm,height=18*mm,kind="proportional"))
    else:
        header_cells.append(Paragraph("PRALAY X",styles["PXTitle"]))
    header_cells.append(Paragraph(f"<font size='16'><b>{esc(data['title'])}</b></font><br/><font size='7.5'>{esc(data['department'])}</font><br/><font size='7.5'>{esc(data['authority'])}</font>",styles["PXBody"]))
    ht=Table([header_cells],colWidths=[23*mm,155*mm]); ht.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"MIDDLE"),("LINEBELOW",(0,0),(-1,-1),1.3,colors.HexColor("#0b2947")),("BOTTOMPADDING",(0,0),(-1,-1),7)])); story += [ht,Spacer(1,5)]

    meta=[("REPORT ID",data["report_id"]),("GENERATED",data["generated_at"]),("STATE",data["state"]),("DISTRICT",data["district"]),("RISK",f"{data['risk'].get('risk_level')} · {data['risk'].get('risk_score')}/100"),("STATUS","OPERATIONAL SITUATION")]
    mt=Table([[Paragraph(f"<b>{esc(k)}</b><br/>{esc(v)}",styles["PXBody"]) for k,v in meta[i:i+3]] for i in range(0,6,3)],colWidths=[59*mm]*3)
    mt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#f4f7f9")),("BOX",(0,0),(-1,-1),.5,colors.HexColor("#dce3e8")),("INNERGRID",(0,0),(-1,-1),.4,colors.HexColor("#dce3e8")),("VALIGN",(0,0),(-1,-1),"TOP"),("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5)])); story += [mt,Spacer(1,4)]
    story += [Paragraph("1. EXECUTIVE SITUATION SUMMARY",styles["PXHead"]),Paragraph(esc(data["summary"]),styles["PXBody"])]

    kpi=[(data["active_incidents"],"ACTIVE INCIDENTS"),(f"{data['affected_population']:,}","PEOPLE AFFECTED"),(data["critical_incidents"],"CRITICAL INCIDENTS"),(data["active_alerts"],"ACTIVE ALERTS")]
    kt=Table([[Paragraph(f"<font size='15'><b>{a}</b></font><br/><font size='6'>{b}</font>",styles["PXBody"]) for a,b in kpi]],colWidths=[44.5*mm]*4); kt.setStyle(TableStyle([("BOX",(0,0),(-1,-1),.5,colors.HexColor("#dce3e8")),("INNERGRID",(0,0),(-1,-1),.4,colors.HexColor("#dce3e8")),("ALIGN",(0,0),(-1,-1),"CENTER"),("VALIGN",(0,0),(-1,-1),"MIDDLE"),("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6)])); story += [kt]

    story += [Paragraph("2. MAP SNAPSHOT",styles["PXHead"]),Paragraph("Compact GIS snapshot showing recorded response paths and incident locations at report generation time.",styles["PXSmall"]),Spacer(1,2)]
    class RouteMap(Flowable):
        def __init__(self,w=178*mm,h=40*mm): Flowable.__init__(self); self.width=w; self.height=h
        def draw(self):
            c=self.canv; W,H=self.width,self.height; c.saveState(); c.setFillColor(colors.HexColor('#eaf1f5')); c.roundRect(0,0,W,H,3*mm,fill=1,stroke=0)
            routes=data.get('routes',[])[:10]; incidents=data.get('incidents',[])[:8]; pts=[]
            def rp(r):
                olat,olon=float(r.get('origin_lat')),float(r.get('origin_lon')); dlat,dlon=float(r.get('destination_lat')),float(r.get('destination_lon')); dx,dy=dlon-olon,dlat-olat; ln=max((dx*dx+dy*dy)**0.5,.0001); px,py=-dy/ln,dx/ln; bend=min(.012,max(.004,ln*.18)); return [[olat,olon],[olat+dy*.25+py*bend,olon+dx*.25+px*bend],[olat+dy*.55-py*bend*.7,olon+dx*.55-px*bend*.7],[olat+dy*.78+py*bend*.45,olon+dx*.78+px*bend*.45],[dlat,dlon]]
            for r in routes: pts+=rp(r)
            pts += [[i.get('latitude'),i.get('longitude')] for i in incidents if i.get('latitude') is not None and i.get('longitude') is not None]
            if pts:
                lats=[float(x[0]) for x in pts]; lons=[float(x[1]) for x in pts]; minla,maxla=min(lats),max(lats); minlo,maxlo=min(lons),max(lons); dx=max(maxlo-minlo,.02); dy=max(maxla-minla,.02)
                def xy(p): return 7*mm+(float(p[1])-minlo)/dx*(W-14*mm), 7*mm+(float(p[0])-minla)/dy*(H-14*mm)
                c.setStrokeColor(colors.HexColor('#b9cbd5')); c.setLineWidth(2)
                for yy in [11*mm,20*mm,29*mm]:
                    q=c.beginPath(); q.moveTo(0,yy); q.curveTo(W*.3,yy+3*mm,W*.7,yy-3*mm,W,yy+2*mm); c.drawPath(q,stroke=1,fill=0)
                c.setStrokeColor(colors.HexColor('#173f61')); c.setLineWidth(1.4)
                for r in routes:
                    path=rp(r); q=c.beginPath(); a,b=xy(path[0]); q.moveTo(a,b)
                    for p in path[1:]: a,b=xy(p); q.lineTo(a,b)
                    c.drawPath(q,stroke=1,fill=0); a,b=xy(path[0]); c.setFillColor(colors.white); c.circle(a,b,1.2*mm,fill=1,stroke=1); a,b=xy(path[-1]); c.setFillColor(colors.HexColor('#173f61')); c.circle(a,b,1.4*mm,fill=1,stroke=0)
                    c.setFillColor(colors.HexColor('#173f61')); c.setFont(bold,5); c.drawCentredString((xy(path[0])[0]+xy(path[-1])[0])/2,(xy(path[0])[1]+xy(path[-1])[1])/2+1.5*mm,str(r.get('route_type','ROUTE')).replace('_',' ')[:10])
                c.setFillColor(colors.HexColor('#8e2525'))
                for i in incidents:
                    if i.get('latitude') is not None and i.get('longitude') is not None:
                        a,b=xy([i['latitude'],i['longitude']]); c.circle(a,b,1.7*mm,fill=1,stroke=0)
            c.setFillColor(colors.HexColor('#36516a')); c.setFont(regular,5.2); c.drawString(5*mm,2.2*mm,'PRALAY X · LIVE MAP SNAPSHOT · ROUTES / INCIDENTS · SIMULATION/SEEDED DATA WHERE MARKED'); c.restoreState()
    story += [RouteMap(),Paragraph("Map snapshot is derived from recorded PRALAY X route/incident data; it is not a live external map image.",styles["PXSmall"])]

    story += [Paragraph("3. INCIDENT SITUATION",styles["PXHead"])]
    rows=[["ID","DISASTER","STATE","DISTRICT","SEVERITY","AFFECTED","STATUS"]]
    for i in data.get("incidents",[]): rows.append([i.get("incident_id",""),i.get("disaster_type",""),i.get("state",""),i.get("district",""),i.get("severity",""),f"{int(i.get('affected_population') or 0):,}",i.get("status","")])
    it=Table([[Paragraph(esc(x),styles["PXSmall"]) for x in row] for row in rows],colWidths=[24*mm,25*mm,27*mm,29*mm,22*mm,25*mm,25*mm],repeatRows=1); it.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor('#0b2947')),("TEXTCOLOR",(0,0),(-1,0),colors.white),("GRID",(0,0),(-1,-1),.35,colors.HexColor('#d9e0e5')),("VALIGN",(0,0),(-1,-1),'TOP'),("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3)])); story += [it]
    story += [Paragraph("4. RISK & PREDICTION",styles["PXHead"]),Paragraph(f"<b>{esc(data['risk'].get('risk_level'))}</b> · {esc(data['risk'].get('risk_score'))}/100 · {esc(data['risk'].get('probability'))}% probability · {esc(data['risk'].get('prediction_window'))}<br/>"+"<br/>".join("• "+esc(x) for x in data['risk'].get('factors',[])),styles["PXBody"])]
    story += [Paragraph("5. ALERTS & COMMUNICATION",styles["PXHead"]),Paragraph("<br/>".join(f"• <b>{esc(a.get('severity'))} — {esc(a.get('title'))}</b>: {esc(a.get('message'))} ({esc(a.get('district') or 'State-wide')}, {esc(a.get('state'))})" for a in data.get('alerts',[])) or "No active alerts.",styles["PXBody"])]
    story += [Paragraph("6. EVACUATION & SHELTERS",styles["PXHead"]),Paragraph("<br/>".join(f"• <b>{esc(e.get('evacuation_id'))}</b> — {esc(e.get('district'))}, {esc(e.get('state'))} — {int(e.get('affected_population') or 0):,} people — shelter {esc(e.get('destination_shelter_id'))} — {esc(e.get('status'))}" for e in data.get('evacuations',[])) or "No evacuation orders recorded.",styles["PXBody"])]
    story += [Paragraph("7. RESPONSE ROUTES & RESOURCES",styles["PXHead"]),Paragraph("<br/>".join(f"• <b>{esc(r.get('route_type'))}</b> — {esc(r.get('district'))}, {esc(r.get('state'))} — {esc(r.get('distance_km'))} km / {esc(r.get('eta_minutes'))} min — {esc(r.get('status'))}" for r in data.get('routes',[])) or "No operational routes recorded.",styles["PXBody"])]
    story += [Paragraph("8. VERIFICATION & AUDIT NOTE",styles["PXHead"]),Paragraph(esc(data["verification_note"]),styles["PXBody"]),Spacer(1,6),Paragraph(f"Generated by PRALAY X · {esc(data['department'])} · Report ID {esc(data['report_id'])}",styles["PXSmall"])]

    def footer(c,doc):
        c.saveState(); c.setStrokeColor(colors.HexColor('#d9e0e5')); c.line(doc.leftMargin,8*mm,A4[0]-doc.rightMargin,8*mm); c.setFont(regular,6.3); c.setFillColor(colors.HexColor('#71818f')); c.drawString(doc.leftMargin,5*mm,'PRALAY X — Disaster Intelligence & Early Warning'); c.drawRightString(A4[0]-doc.rightMargin,5*mm,f'Page {doc.page}'); c.restoreState()
    doc.build(story,onFirstPage=footer,onLaterPages=footer)
    return Response(content=buf.getvalue(),media_type='application/pdf',headers={'Content-Disposition':f'attachment; filename="{data["report_id"]}.pdf"','Cache-Control':'no-store'})

def serialize_evacuation(e: EvacuationOrder):
    return {"evacuation_id": e.evacuation_id, "incident_id": e.incident_id, "state": e.state, "district": e.district, "affected_population": e.affected_population, "destination_shelter_id": e.destination_shelter_id, "status": e.status, "official": e.official, "created_at": e.created_at.isoformat() if e.created_at else None}

def route_points(r: EmergencyRoute):
    # Deterministic seeded bends make prototype routes visually follow a corridor
    # rather than drawing unexplained straight lines. Live road routing can replace this later.
    olat, olon, dlat, dlon = r.origin_lat, r.origin_lon, r.destination_lat, r.destination_lon
    dx, dy = dlon - olon, dlat - olat
    length = max((dx*dx + dy*dy) ** 0.5, 0.0001)
    px, py = -dy / length, dx / length
    bend = min(0.012, max(0.004, length * 0.18))
    return [[olat, olon], [olat + dy*0.25 + py*bend, olon + dx*0.25 + px*bend], [olat + dy*0.55 - py*bend*0.7, olon + dx*0.55 - px*bend*0.7], [olat + dy*0.78 + py*bend*0.45, olon + dx*0.78 + px*bend*0.45], [dlat, dlon]]

def route_asset(route_type: str):
    return {
        "EVACUATION": ("🚗", "CIVILIAN EVACUATION"), "AMBULANCE": ("🚑", "AMBULANCE"),
        "RESCUE": ("🛟", "NDRF / RESCUE"), "POLICE": ("🚓", "POLICE"),
        "FIRE": ("🚒", "FIRE BRIGADE"), "JCB": ("🚜", "JCB / HEAVY EQUIPMENT"),
        "CRANE": ("🏗️", "CRANE / HEAVY RESCUE"),
    }.get(route_type, ("🚗", route_type))

def serialize_route(r: EmergencyRoute):
    emoji, label = route_asset(r.route_type)
    return {"route_id": r.route_id, "incident_id": r.incident_id, "route_type": r.route_type, "label": label, "emoji": emoji, "state": r.state, "district": r.district, "origin_lat": r.origin_lat, "origin_lon": r.origin_lon, "destination_lat": r.destination_lat, "destination_lon": r.destination_lon, "path": route_points(r), "distance_km": r.distance_km, "eta_minutes": r.eta_minutes, "status": r.status, "official": r.official, "created_at": r.created_at.isoformat() if r.created_at else None}

def serialize_incident(i: Incident):
    return {"incident_id": i.incident_id, "disaster_type": i.disaster_type, "severity": i.severity, "latitude": i.latitude, "longitude": i.longitude, "state": i.state, "district": i.district, "status": i.status, "affected_population": i.affected_population, "casualties": i.casualties, "description": i.description, "created_at": i.created_at.isoformat() if i.created_at else None}

def serialize_alert(a: Alert):
    return {"id": a.id, "title": a.title, "severity": a.severity, "state": a.state, "district": a.district, "message": a.message, "official": a.official, "active": a.active, "created_at": a.created_at.isoformat() if a.created_at else None}

def serialize_complaint(c: Complaint):
    return {"complaint_id": c.complaint_id, "category": c.category, "message": c.message, "location": c.location, "contact": c.contact, "status": c.status, "created_at": c.created_at.isoformat() if c.created_at else None}

def serialize_risk(r: RiskPrediction | None):
    if not r: return None
    return {"disaster_type": r.disaster_type, "risk_score": r.risk_score, "risk_level": r.risk_level, "probability": r.probability, "prediction_window": r.prediction_window, "state": r.state, "district": r.district, "factors": json.loads(r.factors), "created_at": r.created_at.isoformat() if r.created_at else None}

def state_summary(incidents):
    out = []
    for state in sorted(set(i.state for i in incidents)):
        rows = [i for i in incidents if i.state == state]
        out.append({"state": state, "incidents": len(rows), "risk": max((i.severity for i in rows), key=lambda x: {"LOW":1,"MODERATE":2,"HIGH":3,"CRITICAL":4}.get(x,0)), "people": sum(i.affected_population for i in rows), "status": "ACTIVE"})
    return out

def disaster_summary(incidents):
    counts = {}
    for i in incidents: counts[i.disaster_type] = counts.get(i.disaster_type, 0) + 1
    total = max(1, sum(counts.values()))
    return [{"type": k, "count": v, "percent": round(v / total * 100)} for k, v in sorted(counts.items(), key=lambda x: -x[1])]
