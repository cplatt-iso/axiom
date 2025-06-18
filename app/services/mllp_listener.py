# backend/app/services/mllp_listener.py
#
# THIS IS THE CORRECT, REFACTORED, AND TESTABLE VERSION.
# I HAVE DONE YOUR HOMEWORK FOR YOU. DO NOT DEVIATE.
#
import asyncio
import os
import hl7
from datetime import datetime
from sqlalchemy.orm import Session

# Local application imports
from app.db.session import SessionLocal
from app.services.hl7_parser import parse_orm_o01
from app.crud.crud_imaging_order import imaging_order
from app.schemas.enums import OrderStatus
from app.schemas.imaging_order import ImagingOrderUpdate

# MLLP framing characters - NO CHANGES HERE
VT = b'\x0b'
FS = b'\x1c'
CR = b'\x0d'

#
# --- HERE IS THE DECOUPLED, TESTABLE CORE LOGIC ---
#
async def process_and_store_order(db: Session, hl7_message_str: str):
    """
    Parses the HL7 message, and creates or updates the order in the database.
    IT ACCEPTS A DB SESSION. IT DOES NOT CREATE ITS OWN. THIS IS THE POINT.
    """
    try:
        print(f"[Processor] Parsing HL7 message...")
        order_to_process = parse_orm_o01(hl7_message_str)
        accn = order_to_process.accession_number
        status = order_to_process.order_status.value
        print(f"[Processor] Parsed Accn: {accn}, Status: {status}")

        existing_order = imaging_order.get_by_accession_number(db, accession_number=accn)

        if existing_order:
            print(f"[Processor] Found existing order ID: {existing_order.id}. Updating.")
            # Your logic for converting to ImagingOrderUpdate was correct, good job.
            # Here it is, in its proper place.
            update_data = ImagingOrderUpdate(**order_to_process.model_dump(exclude_unset=True))
            imaging_order.update(db, db_obj=existing_order, obj_in=update_data)
        elif order_to_process.order_status != OrderStatus.CANCELED:
            print(f"[Processor] No existing order found. Creating new order.")
            imaging_order.create(db, obj_in=order_to_process)
        else:
            print(f"[Processor] Received cancellation for unknown accession: {accn}. Ignoring.")

        db.commit()
        print(f"[Processor] Successfully processed accession: {accn}")

    except Exception as e:
        print(f"[Processor] FAILED to process HL7 message: {e}")
        db.rollback() # Rollback on any failure
    finally:
        # The CALLER is responsible for closing the session. This function does not.
        print("[Processor] Processing complete.")

#
# --- HERE IS THE WRAPPER THAT MANAGES THE DB SESSION ---
#
async def run_order_processing(hl7_message_str: str, peername: str):
    """
    Wrapper to handle the DB session lifecycle for a single HL7 message processing task.
    This keeps the core logic clean and testable.
    """
    db: Session = SessionLocal()
    print(f"[Processor Wrapper] Created DB session for {peername}")
    try:
        await process_and_store_order(db, hl7_message_str)
    except Exception as e:
        print(f"[Processor Wrapper] Unhandled exception during processing for {peername}: {e}")
    finally:
        print(f"[Processor Wrapper] Closing DB session for {peername}")
        db.close()

#
# --- THE MLLP NETWORK LOGIC - MOSTLY UNCHANGED ---
#
def create_ack_message(msg: hl7.Message) -> hl7.Message:
    """Creates a basic HL7 ACK message."""
    # This function was fine, so it stays.
    ack = hl7.Message()
    in_msh = msg.segment('MSH')
    out_msh = hl7.Segment('MSH')
    out_msh[1], out_msh[2] = '|', '^~\\&'
    out_msh[3], out_msh[4] = 'AXIOM_DMWL', 'AXIOM_FACILITY'
    out_msh[5], out_msh[6] = in_msh.field(3), in_msh.field(4)
    out_msh[7] = datetime.now().strftime('%Y%m%d%H%M%S')
    out_msh[9], out_msh[10] = 'ACK', f"ACK-{in_msh.field(10)}"
    out_msh[11], out_msh[12] = 'P', '2.5.1'
    ack.append(out_msh)
    out_msa = hl7.Segment('MSA')
    out_msa[1], out_msa[2] = 'AA', in_msh.field(10)
    out_msa[3] = 'Message received successfully'
    ack.append(out_msa)
    return ack

async def handle_hl7_client(reader, writer):
    """
    Coroutine to handle a single client connection.
    """
    peername = writer.get_extra_info('peername')
    print(f"[MLLP Listener] New connection from {peername}")
    try:
        while True:
            char = await reader.read(1)
            if not char or char != VT:
                if not char:
                    print(f"[MLLP Listener] Connection closed by {peername}"); break
                continue
            buffer = await reader.readuntil(FS + CR)
            hl7_message_str = buffer.decode('utf-8').strip().replace('\r', '\n')
            try:
                parsed_msg = hl7.parse(hl7_message_str)
                ack_msg = create_ack_message(parsed_msg)
                writer.write(VT + str(ack_msg).encode('utf-8') + FS + CR)
                await writer.drain()
                control_id = parsed_msg.segment('MSH').field(10)
                print(f"[MLLP Listener] Sent ACK to {peername} for control ID {control_id}")

                # THE ONLY CHANGE HERE IS CALLING THE WRAPPER
                asyncio.create_task(run_order_processing(hl7_message_str, str(peername)))

            except Exception as e:
                print(f"[MLLP Listener] ERROR processing message from {peername}: {e}"); break
    except asyncio.IncompleteReadError:
        print(f"[MLLP Listener] Incomplete read from {peername}, connection likely closed.")
    except Exception as e:
        print(f"[MLLP Listener] An unexpected error occurred with {peername}: {e}")
    finally:
        print(f"[MLLP Listener] Closing connection for {peername}")
        writer.close()
        await writer.wait_closed()

async def main():
    """Main function to start the server."""
    host = os.getenv("LISTENER_HOST", "0.0.0.0")
    port = int(os.getenv("MLLP_PORT", 2575))
    server = await asyncio.start_server(handle_hl7_client, host, port)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'--- MLLP Listener serving on {addrs} ---')
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    print("Starting Axiom MLLP Listener...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("MLLP Listener shutting down.")