import asyncio
import datetime
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.websocket_manager import ws_manager
from app.services.live_train_service import LiveTrainService
from app.services.weather_service import WeatherService
from app.services.live_conflict_monitor import LiveConflictMonitor
from app.models.block import MaintenanceBlock

router = APIRouter(prefix="/live", tags=["Real-Time Operations & Live Telemetry"])

@router.get("/telemetry")
async def get_live_telemetry(db: Session = Depends(get_db)):
    """
    Returns unified live operations snapshot:
    - Live running trains with GPS coordinates and dynamic delays
    - Weather conditions and rail-temperature hazard levels
    - Real-time conflict matrix and block burst alerts
    """
    trains = await LiveTrainService.get_corridor_train_feed()
    weather = await WeatherService.get_corridor_weather()
    conflicts = await LiveConflictMonitor.evaluate_conflicts(db)

    active_blocks_count = db.query(MaintenanceBlock).filter(
        MaintenanceBlock.status.in_(["APPROVED", "IN_PROGRESS"])
    ).count()

    return {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "corridor": "Ghaziabad - Kanpur Main Line (NCR)",
        "active_blocks_count": active_blocks_count,
        "weather": weather,
        "trains": trains,
        "conflicts": conflicts
    }

@router.get("/weather")
async def get_live_weather():
    """Live corridor weather and thermal track expansion risk."""
    return await WeatherService.get_corridor_weather()

@router.websocket("/ws")
async def live_websocket_endpoint(websocket: WebSocket, db: Session = Depends(get_db)):
    """
    WebSocket endpoint for Control Room Dashboard and Field Engineer Mobile Apps.
    Emits live train tracking, block countdowns, and safety alarms.
    """
    await ws_manager.connect(websocket)
    try:
        # Send immediate initial telemetry snapshot
        initial_payload = {
            "event": "INITIAL_STATE",
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "trains": await LiveTrainService.get_corridor_train_feed(),
            "weather": await WeatherService.get_corridor_weather(),
            "conflicts": await LiveConflictMonitor.evaluate_conflicts(db)
        }
        await websocket.send_json(initial_payload)

        # Keep connection open and listen for client commands / heartbeats
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"event": "pong", "time": datetime.datetime.utcnow().isoformat()})
            elif data == "poll_telemetry":
                snapshot = {
                    "event": "TELEMETRY_UPDATE",
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                    "trains": await LiveTrainService.get_corridor_train_feed(),
                    "conflicts": await LiveConflictMonitor.evaluate_conflicts(db)
                }
                await websocket.send_json(snapshot)

    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
