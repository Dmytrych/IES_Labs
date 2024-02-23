import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime, update,
)
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import select
from datetime import datetime
from pydantic import BaseModel, field_validator, Field
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()


Base = declarative_base()


# Define SQLAlchemy ORM mapped class
class ProcessedAgentDataDbModel(Base):
    __tablename__ = 'processed_agent_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    road_state = Column(String)
    user_id = Column(Integer)
    x = Column(Float)
    y = Column(Float)
    z = Column(Float)
    latitude = Column(Float)
    longitude = Column(Float)
    timestamp = Column(DateTime)


Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)


# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int = Field(default=None, allow_mutation=True)
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


def convert_agent_data(raw_data: ProcessedAgentData):
    return ProcessedAgentDataInDB(
        road_state=raw_data.road_state,
        user_id=raw_data.agent_data.user_id,
        x=raw_data.agent_data.accelerometer.x,
        y=raw_data.agent_data.accelerometer.y,
        z=raw_data.agent_data.accelerometer.z,
        latitude=raw_data.agent_data.gps.latitude,
        longitude=raw_data.agent_data.gps.longitude,
        timestamp=raw_data.agent_data.timestamp,
    )


# FastAPI CRUDL endpoints


@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    # Insert data to database
    # Send data to subscribers
    with SessionLocal() as session:
        converted_data = []

        for dataItem in data:
            converted_data.append(ProcessedAgentDataDbModel(**convert_agent_data(dataItem).dict()))

        print(converted_data[0].latitude)

        session.add_all(converted_data)
        session.commit()

        for processed_subscriber in subscriptions:
            await send_data_to_subscribers(processed_subscriber, converted_data)

@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int):
    # Get data by id
    with SessionLocal() as session:
        return session.query(ProcessedAgentDataDbModel).get(processed_agent_data_id)


@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    # Get list of data
    with SessionLocal() as session:
        return session.query(ProcessedAgentDataDbModel).all()

@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
async def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    # Update data
    # Insert data to database
    # Send data to subscribers
    with SessionLocal() as session:
        updated_entry = session.query(ProcessedAgentDataDbModel).get(processed_agent_data_id)
        if updated_entry is None:
            return None

        for key, value in convert_agent_data(data).model_dump():
            setattr(updated_entry, key, value)

        session.commit()

        for subscriber_id in subscriptions:
            await send_data_to_subscribers(subscriber_id, updated_entry)

        return updated_entry


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    # Delete by id
    with SessionLocal() as session:
        session.query(ProcessedAgentDataDbModel).delete(processed_agent_data_id)
        session.commit()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
