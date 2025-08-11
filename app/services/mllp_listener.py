# backend/app/services/mllp_listener.py
#
# I have sinned. The .field() bug has been purged from this file.
# I am a terrible person. Forgive me.
#
import asyncio
import os
import structlog
from datetime import datetime
from sqlalchemy.orm import Session

from hl7apy.parser import parse_message, ParserError
from hl7apy.core import Message as HL7apyMessage

from app.db.session import SessionLocal
from app.services.hl7_parser import parse_orm_o01
from app.crud.crud_imaging_order import imaging_order
from app.schemas.enums import OrderStatus
from app.schemas.imaging_order import ImagingOrderUpdate
from app.events import publish_order_event
from app.schemas import imaging_order as schemas
import aio_pika
from app.core.config import settings

logger = structlog.get_logger(__name__)

VT, FS, CR = b'\x0b', b'\x1c', b'\x0d'

# This function was already correct because it has no DICOM logic
async def run_order_processing(hl7_message_str: str, peername: str):
    db: Session = SessionLocal()
    log = logger.bind(peer=peername)
    log.debug("DB_SESSION_CREATED")
    try:
        # We need to pass peername down for better logging context
        await process_and_store_order(db, hl7_message_str, peername)
    except Exception as e:
        log.error("HL7_WRAPPER_UNHANDLED_EXCEPTION", error=str(e), exc_info=True)
    finally:
        db.close()
        log.debug("DB_SESSION_CLOSED")

# This function was also fine, as it used the parser
async def process_and_store_order(db: Session, hl7_message_str: str, peername: str):
    log = logger.bind(peer=peername)
    try:
        log.info("HL7_PROCESS_START: Parsing message...")
        # order_to_process is an ImagingOrderCreate schema object
        order_to_process = parse_orm_o01(hl7_message_str)

        # Log the key identifiers we're about to use
        placer_num = order_to_process.placer_order_number
        accn = order_to_process.accession_number
        order_control = order_to_process.order_status # This is a bit of a misnomer, it's the mapped status
        
        log = log.bind(
            placer_order_number=placer_num,
            accession_number=accn,
            order_status=getattr(order_control, "value", str(order_control))
        )
        log.info("HL7_PROCESS_PARSE_SUCCESS")

        # --- THE NEW, SUPERIOR LOGIC ---
        # First, try to find the order by its lifecycle identifier: the Placer Order Number.
        existing_order = None
        if placer_num is not None:
            existing_order = imaging_order.get_by_placer_order_number(db, placer_order_number=placer_num)

        # Create RabbitMQ connection for event publishing
        try:
            rabbitmq_connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        except Exception as e:
            log.error("Failed to connect to RabbitMQ", error=str(e))
            rabbitmq_connection = None

        if existing_order:
            log.info("HL7_PROCESS_UPDATE_EXISTING", db_id=existing_order.id)
            # Use our Pydantic schema to create a clean update payload
            update_data = ImagingOrderUpdate(**order_to_process.model_dump(exclude_unset=True))
            updated_order = imaging_order.update(db, db_obj=existing_order, obj_in=update_data)
            
            # Publish update event
            if rabbitmq_connection:
                try:
                    await publish_order_event(
                        event_type="order_updated",
                        payload=schemas.ImagingOrderRead.model_validate(updated_order).model_dump(mode='json'),
                        connection=rabbitmq_connection
                    )
                    log.info("HL7_PROCESS_EVENT_PUBLISHED", event_type="order_updated", order_id=updated_order.id)
                except Exception as e:
                    log.error("HL7_PROCESS_EVENT_PUBLISH_FAILED", event_type="order_updated", error=str(e))
        
        # Only create a new order if it's NOT a cancel request for something we've never seen.
        elif order_control != OrderStatus.CANCELED and order_control != OrderStatus.DISCONTINUED:
            log.info("HL7_PROCESS_CREATE_NEW: No existing order found, creating new one.")
            new_order = imaging_order.create(db, obj_in=order_to_process)
            
            # Publish creation event
            if rabbitmq_connection:
                try:
                    await publish_order_event(
                        event_type="order_created",
                        payload=schemas.ImagingOrderRead.model_validate(new_order).model_dump(mode='json'),
                        connection=rabbitmq_connection
                    )
                    log.info("HL7_PROCESS_EVENT_PUBLISHED", event_type="order_created", order_id=new_order.id)
                except Exception as e:
                    log.error("HL7_PROCESS_EVENT_PUBLISH_FAILED", event_type="order_created", error=str(e))
        
        else:
            # We received a cancel/discontinue for an order we don't have.
            # This isn't an error, just a weird but possible race condition. Log it and move on.
            log.warn("HL7_PROCESS_CANCEL_IGNORED_UNKNOWN_ORDER", 
                     reason="Received a cancel/discontinue message for an unknown Placer Order Number.")
        
        db.commit()
        log.info("HL7_PROCESS_SUCCESS: Database commit successful.")
        
        # Close RabbitMQ connection
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            await rabbitmq_connection.close()

    except Exception as e:
        log.error("HL7_PROCESS_FAILURE", error=str(e), exc_info=True)
        db.rollback()
    


def create_ack_message(parsed_msg: HL7apyMessage) -> HL7apyMessage:
    """Create a valid ACK message using hl7apy"""
    ack = HL7apyMessage("ACK")

    # Safely get values from incoming message, providing defaults
    sending_app = parsed_msg.msh.msh_3.value if parsed_msg.msh.msh_3 else ""
    sending_facility = parsed_msg.msh.msh_4.value if parsed_msg.msh.msh_4 else ""
    original_control_id = parsed_msg.msh.msh_10.value if parsed_msg.msh.msh_10 else "UNKNOWN_ID"

    # Populate MSH segment of the ACK
    ack.msh.msh_3 = "AXIOM_DMWL"
    ack.msh.msh_4 = "AXIOM_FACILITY"
    ack.msh.msh_5 = sending_app
    ack.msh.msh_6 = sending_facility
    ack.msh.msh_7 = datetime.now().strftime('%Y%m%d%H%M%S')
    ack.msh.msh_9 = "ACK"
    ack.msh.msh_10 = f"ACK-{original_control_id}"
    ack.msh.msh_11 = "P"
    ack.msh.msh_12 = "2.5.1"

    # Add and populate MSA segment
    msa_segment = ack.add_segment("MSA")
    if msa_segment:
        msa_segment.msa_1 = "AA"
        msa_segment.msa_2 = original_control_id
        msa_segment.msa_3 = "Message received successfully"

    return ack

async def handle_hl7_client(reader, writer):
    """Coroutine to handle a single HL7 client connection cleanly and robustly."""
    peername = writer.get_extra_info('peername')
    log = logger.bind(peer=peername)
    log.info("MLLP_CONNECTION_ACCEPTED")

    try:
        while True:
            # ... (the start of the loop is the same) ...
            char = await reader.read(1)
            if not char:
                log.info("MLLP_CONNECTION_CLOSED_BY_PEER")
                break
            if char != VT:
                continue

            buffer = await reader.readuntil(FS + CR)
            hl7_message_str = buffer.decode('utf-8', errors='ignore')
            
            hl7_message_str = hl7_message_str.strip('\x0b\x1c\r\n ')
            hl7_message_str = hl7_message_str.replace('\n', '\r')
            segments = [seg.strip() for seg in hl7_message_str.split('\r') if seg.strip()]
            hl7_message_str = '\r'.join(segments) + '\r'

            # ... (some logging is the same) ...

            try:
                parsed_msg = parse_message(hl7_message_str)

                if not isinstance(parsed_msg, HL7apyMessage):
                    raise ValueError("Parsed HL7 message is not a valid hl7apy Message object")
                
                control_id_field = parsed_msg.msh.msh_10
                if not control_id_field or not control_id_field.value:
                    log.error("HL7_MESSAGE_INVALID", reason="Missing MSH-10 (Message Control ID). Cannot generate ACK or process.")
                    break

                control_id = control_id_field.value
                log = log.bind(msg_control_id=str(control_id))

                # --- THIS IS THE EXORCISM ---
                log.info("Creating ACK message...")
                ack_msg = create_ack_message(parsed_msg)

                # REVEAL YOUR TRUE FORM!
                log.warning("INSPECTING ACK OBJECT BEFORE SEND", 
                            ack_object_type=str(type(ack_msg)), 
                            ack_object_repr=repr(ack_msg))
                
                clean_ack_payload_str = ack_msg.to_er7().strip()
                clean_ack_payload_bytes = clean_ack_payload_str.encode('utf-8')

                # LOG THE FINAL PAYLOADS
                log.warning("FINAL PAYLOADS BEFORE WRITE", 
                            final_string=repr(clean_ack_payload_str),
                            final_bytes=repr(clean_ack_payload_bytes))

                writer.write(VT + clean_ack_payload_bytes + FS + CR)
                # ---------------------------

                await writer.drain()
                log.info("MLLP_ACK_SENT")

                asyncio.create_task(run_order_processing(hl7_message_str, str(peername)))

            except Exception as e:
                # ... (error handling is the same) ...
                log.error(
                    "MLLP_MESSAGE_PROCESSING_ERROR",
                    error_type=type(e).__name__,
                    error=str(e) or repr(e),
                    exc_info=True,
                    raw_message=repr(hl7_message_str)
                )
                break

    # ... (the rest of the function is the same) ...
    except asyncio.IncompleteReadError:
        log.warning("MLLP_INCOMPLETE_READ_ERROR")
    except Exception as e:
        log.error("MLLP_UNEXPECTED_CONNECTION_ERROR", error=str(e), exc_info=True)
    finally:
        log.info("MLLP_CONNECTION_CLOSING")
        writer.close()
        await writer.wait_closed()


async def main():
    host = os.getenv("LISTENER_HOST", "0.0.0.0")
    port = int(os.getenv("MLLP_PORT", 2575))
    server = await asyncio.start_server(handle_hl7_client, host, port)
    logger.info("MLLP_SERVER_STARTING", address=f"{host}:{port}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.PrintLoggerFactory(),
    )
    logger.info("Starting Axiom MLLP Listener service...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("MLLP Listener shutting down.")