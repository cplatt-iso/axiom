import asyncio
from sqlalchemy import text
from app.db.session import SessionLocal
from app.db.models.dimse_listener_config import DimseListenerConfig

async def seed_dcm4che_listener():
    db = SessionLocal()
    try:
        print("Seeding dcm4che listener configuration...")
        listener = db.query(DimseListenerConfig).filter(DimseListenerConfig.instance_id == "dcm4che_1").first()
        if not listener:
            listener = DimseListenerConfig(
                instance_id="dcm4che_1",
                name="DCM4CHE_LISTENER",
                ae_title="DCM4CHE",
                port=11114,
                is_enabled=True,
                listener_type="dcm4che",
            )
            db.add(listener)
            db.commit()
            print("dcm4che listener configuration seeded.")
        else:
            print("dcm4che listener configuration already exists.")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(seed_dcm4che_listener())
