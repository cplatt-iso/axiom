#!/usr/bin/env python3
"""
Complete DICOM Pipeline Performance Monitor

Tracks the complete pipeline:
DCM4CHE_1 LISTENER → WORKER → DCM4CHE-SENDER
"""

import subprocess
import threading
import time
from datetime import datetime, timezone
from collections import defaultdict
import re
import json
import sys

class CompletePipelineMonitor:
    def __init__(self):
        self.studies = defaultdict(dict)  # study_uid -> phase data
        self.associations = {}  # association data
        self.batches = {}  # batch_id -> batch data
        self.batch_files = {}  # track files per batch
        self.start_time = datetime.now()
        
    def start_monitoring(self):
        """Start comprehensive monitoring."""
        print("🔍 COMPLETE DICOM Pipeline Performance Monitor")
        print(f"⏰ Start Time: {self.start_time.strftime('%H:%M:%S.%f')[:-3]}")
        print("=" * 80)
        print("📥 TRACKING: DCM4CHE_1 Listener → Worker → DCM4CHE-Sender")
        print("=" * 80)
        
        # Start log monitoring threads
        listener_thread = threading.Thread(target=self.monitor_listener_logs, daemon=True)
        worker_thread = threading.Thread(target=self.monitor_worker_logs, daemon=True)
        sender_thread = threading.Thread(target=self.monitor_sender_logs, daemon=True)
        api_thread = threading.Thread(target=self.monitor_api_logs, daemon=True)
        
        listener_thread.start()
        worker_thread.start() 
        sender_thread.start()
        api_thread.start()
        
        # Main monitoring loop
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Monitoring stopped")
            self.print_complete_summary()
            
    def monitor_listener_logs(self):
        """Monitor dcm4che_1 listener logs for incoming DICOM."""
        try:
            process = subprocess.Popen(
                ["./axiomctl", "logs", "-f", "dcm4che_1"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd="/home/icculus/axiom/backend"
            )
            
            for line in process.stdout:
                self.process_listener_line(line.strip())
        except Exception as e:
            print(f"Error monitoring listener logs: {e}")
            
    def monitor_worker_logs(self):
        """Monitor worker logs for processing events.""" 
        try:
            process = subprocess.Popen(
                ["./axiomctl", "logs", "-f", "worker"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd="/home/icculus/axiom/backend"
            )
            
            for line in process.stdout:
                self.process_worker_line(line.strip())
        except Exception as e:
            print(f"Error monitoring worker logs: {e}")
            
    def monitor_sender_logs(self):
        """Monitor dcm4che-sender logs for transmission events."""
        try:
            process = subprocess.Popen(
                ["./axiomctl", "logs", "-f", "dcm4che-sender"],
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                text=True,
                cwd="/home/icculus/axiom/backend"
            )
            
            for line in process.stdout:
                self.process_sender_line(line.strip())
        except Exception as e:
            print(f"Error monitoring sender logs: {e}")
            
    def monitor_api_logs(self):
        """Monitor API logs for batch events."""
        try:
            process = subprocess.Popen(
                ["./axiomctl", "logs", "-f", "api"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd="/home/icculus/axiom/backend"
            )
            
            for line in process.stdout:
                self.process_api_line(line.strip())
        except Exception as e:
            print(f"Error monitoring API logs: {e}")
    
    def process_listener_line(self, line):
        """Process dcm4che_1 listener log lines."""
        # Look for batch processing completion
        if "Successfully processed batch of" in line:
            try:
                time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                files_match = re.search(r'batch of (\d+) files', line)
                
                if time_match and files_match:
                    timestamp = time_match.group(1)
                    file_count = files_match.group(1)
                    
                    now = datetime.now()
                    batch_key = f"listener_{now.strftime('%H%M%S')}"
                    
                    self.batch_files[batch_key] = {
                        'file_count': int(file_count),
                        'listener_complete': now,
                        'timestamp': timestamp
                    }
                    
                    print(f"📥 LISTENER: Processed batch of {file_count} files at {timestamp}")
                    
            except Exception as e:
                print(f"Error processing listener batch line: {e}")
                
        # Look for association accept/release
        elif "Association accepted" in line or "Accepting Association" in line:
            try:
                time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                if time_match:
                    timestamp = time_match.group(1)
                    print(f"🤝 ASSOCIATION: Started at {timestamp}")
            except Exception as e:
                print(f"Error processing association line: {e}")
                
        elif "Association released" in line or "Association Released" in line:
            try:
                time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                if time_match:
                    timestamp = time_match.group(1)
                    print(f"🔚 ASSOCIATION: Released at {timestamp}")
            except Exception as e:
                print(f"Error processing association release line: {e}")
    
    def process_worker_line(self, line):
        """Process worker log lines for task processing."""
        # Look for association tasks
        if "process_dicom_association_task" in line:
            try:
                time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                if time_match:
                    timestamp = time_match.group(1)
                    
                    if "started" in line.lower():
                        print(f"⚙️ WORKER: Association processing started at {timestamp}")
                    elif "succeeded" in line.lower():
                        print(f"✅ WORKER: Association processing completed at {timestamp}")
                    elif "failed" in line.lower():
                        print(f"❌ WORKER: Association processing failed at {timestamp}")
                        
            except Exception as e:
                print(f"Error processing worker association line: {e}")
                
        # Look for batch creation  
        elif "Added instance to exam batch" in line or "Creating exam batch" in line:
            try:
                time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                batch_match = re.search(r'batch (\d+)', line)
                study_match = re.search(r'Study=([^\s,]+)', line) or re.search(r'study_uid[=:]([^\s,]+)', line)
                
                if time_match and batch_match:
                    timestamp = time_match.group(1)
                    batch_id = batch_match.group(1)
                    study_uid = study_match.group(1) if study_match else "Unknown"
                    
                    if batch_id not in self.batches:
                        self.batches[batch_id] = {
                            'created': datetime.now(),
                            'timestamp': timestamp,
                            'study_uid': study_uid
                        }
                        print(f"📦 BATCH: Created batch {batch_id} for study {study_uid[-12:]}... at {timestamp}")
                        
            except Exception as e:
                print(f"Error processing batch creation line: {e}")
                
        # Look for general task success/failure
        elif "Task succeeded" in line or "Task failed" in line:
            try:
                time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                task_match = re.search(r'task_name=([^\s]+)', line)
                
                if time_match and task_match:
                    timestamp = time_match.group(1)
                    task_name = task_match.group(1)
                    
                    # Skip periodic tasks to reduce noise
                    if task_name not in ['poll_all_dimse_qr_sources', 'poll_all_dicomweb_sources', 'retry_pending_exceptions_task']:
                        status = "✅ SUCCESS" if "succeeded" in line else "❌ FAILED"
                        print(f"🎯 TASK: {task_name} {status} at {timestamp}")
                        
            except Exception as e:
                print(f"Error processing task completion line: {e}")
    
    def process_api_line(self, line):
        """Process API log lines for batch events."""
        if "Marking exam batch as READY" in line:
            try:
                time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                batch_match = re.search(r'"batch_id":\s*(\d+)', line) or re.search(r'batch (\d+)', line)
                study_match = re.search(r'"study_uid":\s*"([^"]+)"', line)
                count_match = re.search(r'"instance_count":\s*(\d+)', line)
                
                if time_match and batch_match:
                    timestamp = time_match.group(1)
                    batch_id = batch_match.group(1)
                    study_uid = study_match.group(1) if study_match else "Unknown"
                    instance_count = count_match.group(1) if count_match else "?"
                    
                    if batch_id in self.batches:
                        self.batches[batch_id]['ready'] = datetime.now()
                        self.batches[batch_id]['ready_timestamp'] = timestamp
                        self.batches[batch_id]['instance_count'] = instance_count
                        
                        # Calculate batch wait time
                        if 'created' in self.batches[batch_id]:
                            wait_time = (self.batches[batch_id]['ready'] - self.batches[batch_id]['created']).total_seconds()
                            print(f"📦 BATCH READY: Batch {batch_id} ({instance_count} files) ready after {wait_time:.3f}s wait")
                        else:
                            print(f"📦 BATCH READY: Batch {batch_id} ({instance_count} files) at {timestamp}")
                    
            except Exception as e:
                print(f"Error processing batch ready line: {e}")
                
        elif "Successfully queued batch to cstore_dcm4che_jobs" in line:
            try:
                time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                batch_match = re.search(r'"batch_id":\s*(\d+)', line) or re.search(r'batch (\d+)', line)
                
                if time_match and batch_match:
                    timestamp = time_match.group(1)
                    batch_id = batch_match.group(1)
                    
                    if batch_id in self.batches:
                        self.batches[batch_id]['queued'] = datetime.now()
                        self.batches[batch_id]['queued_timestamp'] = timestamp
                        
                        # Calculate queue delay
                        if 'ready' in self.batches[batch_id]:
                            queue_delay = (self.batches[batch_id]['queued'] - self.batches[batch_id]['ready']).total_seconds()
                            print(f"🚀 QUEUED: Batch {batch_id} queued after {queue_delay:.3f}s delay")
                        else:
                            print(f"🚀 QUEUED: Batch {batch_id} at {timestamp}")
                    
            except Exception as e:
                print(f"Error processing queue line: {e}")
    
    def process_sender_line(self, line):
        """Process dcm4che-sender logs for transmission events."""
        if "Sent " in line and " objects " in line and "MB/s)" in line:
            try:
                sent_match = re.search(r'Sent (\d+) objects \(=([^)]+)\) in ([^s]+)s \(=([^)]+)\)', line)
                if sent_match:
                    object_count = sent_match.group(1)
                    data_size = sent_match.group(2)
                    duration = sent_match.group(3)
                    throughput = sent_match.group(4)
                    
                    now = datetime.now()
                    print(f"🏁 TRANSMITTED: {object_count} objects ({data_size}) in {duration}s @ {throughput}")
                    
                    # Try to match to recent batches
                    for batch_id, batch_data in self.batches.items():
                        if 'transmitted' not in batch_data and 'instance_count' in batch_data:
                            # Check if this transmission matches the batch size
                            if batch_data['instance_count'] == object_count:
                                batch_data['transmitted'] = now
                                batch_data['transmission_details'] = {
                                    'objects': object_count,
                                    'size': data_size,
                                    'duration': duration,
                                    'throughput': throughput
                                }
                                
                                self.print_complete_batch_timing(batch_id)
                                break
                    
            except Exception as e:
                print(f"Error processing sender transmission line: {e}")
                
        elif "Processing message for batch" in line:
            try:
                time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                batch_match = re.search(r'batch (\d+)', line)
                
                if time_match and batch_match:
                    timestamp = time_match.group(1)
                    batch_id = batch_match.group(1)
                    
                    print(f"📡 SENDER: Processing batch {batch_id} at {timestamp}")
                    
            except Exception as e:
                print(f"Error processing sender processing line: {e}")
    
    def print_complete_batch_timing(self, batch_id):
        """Print complete timing analysis for a batch."""
        batch_data = self.batches[batch_id]
        
        print("\n" + "🔥" * 80)
        print(f"📊 COMPLETE BATCH ANALYSIS: Batch {batch_id}")
        print("🔥" * 80)
        
        times = {}
        times['created'] = batch_data.get('created')
        times['ready'] = batch_data.get('ready')
        times['queued'] = batch_data.get('queued')
        times['transmitted'] = batch_data.get('transmitted')
        
        instance_count = batch_data.get('instance_count', '?')
        print(f"📋 BATCH DETAILS: {instance_count} instances")
        
        # Calculate durations
        total_time = None
        if times['created'] and times['transmitted']:
            total_time = (times['transmitted'] - times['created']).total_seconds()
            print(f"🎯 TOTAL END-TO-END: {total_time:.3f}s")
        
        if times['created'] and times['ready']:
            batch_wait = (times['ready'] - times['created']).total_seconds()
            print(f"⏳ BATCH TIMEOUT WAIT: {batch_wait:.3f}s")
            
            if batch_wait > 2.0:
                print("🚨 BOTTLENECK: Batch timeout is the main delay!")
        
        if times['ready'] and times['queued']:
            queue_delay = (times['queued'] - times['ready']).total_seconds()
            print(f"🚀 QUEUE DELAY: {queue_delay:.3f}s")
            
        if times['queued'] and times['transmitted']:
            transmission_time = (times['transmitted'] - times['queued']).total_seconds()
            print(f"📡 TRANSMISSION TIME: {transmission_time:.3f}s")
            
        # Transmission details
        tx_details = batch_data.get('transmission_details', {})
        if tx_details:
            print(f"📊 TRANSMISSION: {tx_details.get('throughput')} - {tx_details.get('size')}")
        
        # Performance rating
        if total_time:
            if total_time < 2.0:
                print("🔥 PERFORMANCE: EXCELLENT (Sub-2-second)")
            elif total_time < 5.0:
                print("👍 PERFORMANCE: GOOD")
            else:
                print("⚠️ PERFORMANCE: NEEDS OPTIMIZATION")
        
        print("🔥" * 80 + "\n")
    
    def print_complete_summary(self):
        """Print comprehensive performance summary."""
        print("\n" + "=" * 80)
        print("📊 COMPLETE PIPELINE PERFORMANCE SUMMARY")
        print("=" * 80)
        
        completed_batches = sum(1 for b in self.batches.values() if 'transmitted' in b)
        total_batches = len(self.batches)
        
        print(f"Total Batches Tracked: {total_batches}")
        print(f"Completed Batches: {completed_batches}")
        
        if completed_batches > 0:
            batch_waits = []
            total_times = []
            
            for batch_id, batch_data in self.batches.items():
                if 'transmitted' not in batch_data:
                    continue
                    
                if batch_data.get('created') and batch_data.get('ready'):
                    batch_wait = (batch_data['ready'] - batch_data['created']).total_seconds()
                    batch_waits.append(batch_wait)
                
                if batch_data.get('created') and batch_data.get('transmitted'):
                    total_time = (batch_data['transmitted'] - batch_data['created']).total_seconds()
                    total_times.append(total_time)
            
            if batch_waits:
                avg_wait = sum(batch_waits) / len(batch_waits)
                print(f"\nBatch Timeout Analysis:")
                print(f"  Average Wait: {avg_wait:.3f}s")
                print(f"  Min/Max Wait: {min(batch_waits):.3f}s / {max(batch_waits):.3f}s")
                
                if avg_wait > 2.0:
                    print(f"  🚨 MAJOR BOTTLENECK: {avg_wait:.1f}s average batch timeout!")
                
            if total_times:
                avg_total = sum(total_times) / len(total_times)
                print(f"\nEnd-to-End Performance:")
                print(f"  Average Total: {avg_total:.3f}s")
                print(f"  Best/Worst: {min(total_times):.3f}s / {max(total_times):.3f}s")

if __name__ == "__main__":
    monitor = CompletePipelineMonitor()
    monitor.start_monitoring()
    
    def process_api_line(self, line):
        """Process API log lines for batch events."""
        if "Marking exam batch as READY" in line:
            try:
                study_match = re.search(r'"study_uid":\\s*"([^"]+)"', line)
                batch_match = re.search(r'"batch_id":\\s*(\\d+)', line)
                time_match = re.search(r'"timestamp":\\s*"([^"]+)"', line)
                count_match = re.search(r'"instance_count":\\s*(\\d+)', line)
                
                if study_match and batch_match and time_match:
                    study_uid = study_match.group(1)
                    batch_id = batch_match.group(1)
                    timestamp = time_match.group(1)
                    instance_count = count_match.group(1) if count_match else "?"
                    
                    self.batches[batch_id] = {
                        'study_uid': study_uid,
                        'ready_time': datetime.now(),
                        'ready_timestamp': timestamp,
                        'instance_count': instance_count
                    }
                    
                    if study_uid not in self.studies:
                        self.studies[study_uid] = {}
                    
                    self.studies[study_uid]['batch_ready'] = {
                        'batch_id': batch_id,
                        'timestamp': timestamp,
                        'time': datetime.now(),
                        'instance_count': instance_count
                    }
                    
                    print(f"📦 BATCH READY: Batch {batch_id} ({instance_count} files) - Study {study_uid[-12:]}... at {timestamp[-12:-4]}")
                    
            except Exception as e:
                print(f"Error processing API batch ready line: {e}")
                
        elif "Successfully queued batch to cstore_dcm4che_jobs" in line:
            try:
                study_match = re.search(r'"study_uid":\\s*"([^"]+)"', line)
                batch_match = re.search(r'"batch_id":\\s*(\\d+)', line)
                time_match = re.search(r'"timestamp":\\s*"([^"]+)"', line)
                
                if study_match and batch_match and time_match:
                    study_uid = study_match.group(1)
                    batch_id = batch_match.group(1)
                    timestamp = time_match.group(1)
                    
                    self.studies[study_uid]['queued'] = {
                        'batch_id': batch_id,
                        'timestamp': timestamp,
                        'time': datetime.now()
                    }
                    
                    print(f"🚀 QUEUED: Batch {batch_id} queued to dcm4che at {timestamp[-12:-4]}")
                    
            except Exception as e:
                print(f"Error processing API queue line: {e}")
    
    def process_worker_line(self, line):
        """Process worker log lines for processing events."""
        # Track association start
        if "process_dicom_association_task" in line and "Task started" in line:
            try:
                time_match = re.search(r'(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})', line)
                task_match = re.search(r'task_id=([^\\s]+)', line)
                file_match = re.search(r'file_count=(\\d+)', line)
                
                if time_match and task_match:
                    timestamp = time_match.group(1)
                    task_id = task_match.group(1)
                    file_count = file_match.group(1) if file_match else "?"
                    
                    self.associations[task_id] = {
                        'start_time': datetime.now(),
                        'start_timestamp': timestamp,
                        'file_count': file_count,
                        'instances': []
                    }
                    
                    print(f"📥 INGRESS: Association started - {file_count} files at {timestamp}")
                    
            except Exception as e:
                print(f"Error processing worker start line: {e}")
                
        # Track individual file processing with instance UIDs
        elif "Processing file in association" in line:
            try:
                task_match = re.search(r'task_id=([^\\s]+)', line)
                filepath_match = re.search(r'filepath=([^\\s]+)', line)
                index_match = re.search(r'file_index=(\\d+)', line)
                
                if task_match and filepath_match:
                    task_id = task_match.group(1)
                    filepath = filepath_match.group(1)
                    file_index = index_match.group(1) if index_match else "?"
                    
                    # Extract instance UID from filepath
                    instance_uid = filepath.split('/')[-1] if filepath else "Unknown"
                    
                    if task_id in self.associations:
                        self.associations[task_id]['instances'].append(instance_uid)
                    
                    # Only print for first and last files to avoid spam
                    if file_index == "1":
                        print(f"⚙️ PROCESSING: File 1/{self.associations.get(task_id, {}).get('file_count', '?')} - {instance_uid[-12:]}...")
                    elif file_index.isdigit() and (int(file_index) % 10 == 0 or file_index == self.associations.get(task_id, {}).get('file_count', '0')):
                        print(f"⚙️ PROCESSING: File {file_index}/{self.associations.get(task_id, {}).get('file_count', '?')} - {instance_uid[-12:]}...")
                        
            except Exception as e:
                print(f"Error processing file line: {e}")
                
        # Track association completion
        elif "Association processing completed" in line:
            try:
                time_match = re.search(r'(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})', line)
                task_match = re.search(r'task_id=([^\\s]+)', line)
                success_match = re.search(r'successful_files=(\\d+)', line)
                failed_match = re.search(r'failed_files=(\\d+)', line)
                
                if time_match and task_match:
                    timestamp = time_match.group(1)
                    task_id = task_match.group(1)
                    successful = success_match.group(1) if success_match else "?"
                    failed = failed_match.group(1) if failed_match else "?"
                    
                    if task_id in self.associations:
                        association = self.associations[task_id]
                        association['end_time'] = datetime.now()
                        association['end_timestamp'] = timestamp
                        association['successful_files'] = successful
                        association['failed_files'] = failed
                        
                        duration = (association['end_time'] - association['start_time']).total_seconds()
                        print(f"✅ PROCESSING COMPLETE: {successful} files processed in {duration:.3f}s (Failed: {failed})")
                        
                        # Mark processing complete for instances
                        for instance_uid in association['instances']:
                            # We need to wait for batch creation to link instances to studies
                            pass
                    
            except Exception as e:
                print(f"Error processing completion line: {e}")
                
        # Track batch creation
        elif "Added instance to exam batch" in line:
            try:
                instance_match = re.search(r'instance_uid=([^\\s,]+)', line) or re.search(r'Instance=([^\\s,]+)', line)
                study_match = re.search(r'Study=([^\\s,]+)', line)
                batch_match = re.search(r'exam batch (\\d+)', line)
                
                if instance_match and study_match and batch_match:
                    instance_uid = instance_match.group(1)
                    study_uid = study_match.group(1)
                    batch_id = batch_match.group(1)
                    
                    # Link instance to study
                    self.instance_to_study[instance_uid] = study_uid
                    
                    # Initialize study data if needed
                    if study_uid not in self.studies:
                        self.studies[study_uid] = {}
                        
                    if 'instances' not in self.studies[study_uid]:
                        self.studies[study_uid]['instances'] = []
                        
                    if instance_uid not in self.studies[study_uid]['instances']:
                        self.studies[study_uid]['instances'].append(instance_uid)
                        self.studies[study_uid]['batched'] = datetime.now()
                        
            except Exception as e:
                print(f"Error processing batch creation line: {e}")
    
    def process_dcm4che_line(self, line):
        """Process dcm4che sender logs for transmission events."""
        if "Sent " in line and " objects " in line and "MB/s)" in line:
            try:
                sent_match = re.search(r'Sent (\\d+) objects \\(=([^)]+)\\) in ([^s]+)s \\(=([^)]+)\\)', line)
                if sent_match:
                    object_count = sent_match.group(1)
                    data_size = sent_match.group(2)
                    duration = sent_match.group(3)
                    throughput = sent_match.group(4)
                    
                    now = datetime.now()
                    print(f"🏁 TRANSMITTED: {object_count} objects ({data_size}) in {duration}s @ {throughput}")
                    
                    # Try to match to recent batches
                    for batch_id, batch_data in self.batches.items():
                        if 'transmitted' not in batch_data:
                            # Check if this transmission matches the batch size
                            if batch_data.get('instance_count') == object_count:
                                batch_data['transmitted'] = {
                                    'time': now,
                                    'objects': object_count,
                                    'size': data_size,
                                    'duration': duration,
                                    'throughput': throughput
                                }
                                
                                study_uid = batch_data['study_uid']
                                if study_uid in self.studies:
                                    self.studies[study_uid]['transmitted'] = batch_data['transmitted']
                                    self.print_complete_study_timing(study_uid, batch_id)
                                break
                    
            except Exception as e:
                print(f"Error processing dcm4che transmission line: {e}")
    
    def print_complete_study_timing(self, study_uid, batch_id):
        """Print complete timing analysis for a study."""
        study_data = self.studies[study_uid]
        batch_data = self.batches[batch_id]
        
        print("\n" + "🔥" * 80)
        print(f"📊 COMPLETE STUDY ANALYSIS: {study_uid[-20:]}... (Batch {batch_id})")
        print("🔥" * 80)
        
        times = {}
        
        # Find associated association timing
        for task_id, assoc_data in self.associations.items():
            if any(instance in study_data.get('instances', []) for instance in assoc_data.get('instances', [])):
                times['ingress_start'] = assoc_data.get('start_time')
                times['processing_complete'] = assoc_data.get('end_time')
                break
        
        times['batched'] = study_data.get('batched')
        times['batch_ready'] = study_data.get('batch_ready', {}).get('time')
        times['queued'] = study_data.get('queued', {}).get('time') 
        times['transmitted'] = study_data.get('transmitted', {}).get('time')
        
        # Calculate durations
        if times.get('ingress_start') and times.get('processing_complete'):
            processing_duration = (times['processing_complete'] - times['ingress_start']).total_seconds()
            print(f"⚙️ PROCESSING: {processing_duration:.3f}s (Ingress → Processing Complete)")
        
        if times.get('processing_complete') and times.get('batch_ready'):
            batch_wait = (times['batch_ready'] - times['processing_complete']).total_seconds()
            print(f"⏳ BATCH TIMEOUT WAIT: {batch_wait:.3f}s (Processing → Batch Ready)")
            
        if times.get('batch_ready') and times.get('queued'):
            queue_delay = (times['queued'] - times['batch_ready']).total_seconds()
            print(f"🚀 QUEUE DELAY: {queue_delay:.3f}s (Batch Ready → Queued)")
            
        if times.get('queued') and times.get('transmitted'):
            transmission_delay = (times['transmitted'] - times['queued']).total_seconds()
            print(f"📡 TRANSMISSION: {transmission_delay:.3f}s (Queued → Transmitted)")
            
        if times.get('ingress_start') and times.get('transmitted'):
            total_time = (times['transmitted'] - times['ingress_start']).total_seconds()
            print(f"🎯 TOTAL END-TO-END: {total_time:.3f}s")
            
            # Performance rating
            if batch_wait > 2.0:
                print("🚨 BOTTLENECK IDENTIFIED: Batch timeout wait is the main delay!")
                print(f"   💡 Optimization: Reduce EXAM_BATCH_COMPLETION_TIMEOUT from current value")
            elif total_time < 2.0:
                print("🔥 EXCELLENT: Sub-2-second end-to-end performance!")
            elif total_time < 5.0:
                print("👍 GOOD: Fast end-to-end performance")
            else:
                print("⚠️  NEEDS OPTIMIZATION: End-to-end time is slow")
        
        # Transmission details
        tx_data = study_data.get('transmitted', {})
        if tx_data:
            print(f"📊 TRANSMISSION DETAILS: {tx_data.get('objects')} objects, {tx_data.get('size')}, {tx_data.get('throughput')}")
        
        print("🔥" * 80 + "\\n")
    
    def print_complete_summary(self):
        """Print comprehensive performance summary."""
        print("\\n" + "=" * 80)
        print("📊 COMPLETE PIPELINE PERFORMANCE ANALYSIS")
        print("=" * 80)
        
        total_studies = len(self.studies)
        completed_studies = sum(1 for s in self.studies.values() if 'transmitted' in s)
        
        print(f"Total Studies Tracked: {total_studies}")
        print(f"Completed Studies: {completed_studies}")
        print(f"Total Associations: {len(self.associations)}")
        
        if completed_studies > 0:
            processing_times = []
            batch_waits = []
            total_times = []
            
            for study_uid, study_data in self.studies.items():
                if 'transmitted' not in study_data:
                    continue
                    
                # Find associated association
                for task_id, assoc_data in self.associations.items():
                    if any(instance in study_data.get('instances', []) for instance in assoc_data.get('instances', [])):
                        if assoc_data.get('start_time') and assoc_data.get('end_time'):
                            proc_time = (assoc_data['end_time'] - assoc_data['start_time']).total_seconds()
                            processing_times.append(proc_time)
                        break
                
                # Batch wait time
                if study_data.get('batched') and study_data.get('batch_ready', {}).get('time'):
                    batch_wait = (study_data['batch_ready']['time'] - study_data['batched']).total_seconds()
                    batch_waits.append(batch_wait)
                
                # Total time (if we have ingress start)
                for task_id, assoc_data in self.associations.items():
                    if any(instance in study_data.get('instances', []) for instance in assoc_data.get('instances', [])):
                        if assoc_data.get('start_time') and study_data.get('transmitted', {}).get('time'):
                            total_time = (study_data['transmitted']['time'] - assoc_data['start_time']).total_seconds()
                            total_times.append(total_time)
                        break
            
            if processing_times:
                print(f"\\nProcessing Performance:")
                print(f"  Average Processing Time: {sum(processing_times)/len(processing_times):.3f}s")
                print(f"  Fastest Processing: {min(processing_times):.3f}s")
                print(f"  Slowest Processing: {max(processing_times):.3f}s")
                
            if batch_waits:
                print(f"\\nBatch Wait Analysis:")
                print(f"  Average Batch Wait: {sum(batch_waits)/len(batch_waits):.3f}s")
                print(f"  Shortest Wait: {min(batch_waits):.3f}s") 
                print(f"  Longest Wait: {max(batch_waits):.3f}s")
                
                avg_wait = sum(batch_waits) / len(batch_waits)
                if avg_wait > 2.0:
                    print(f"  🚨 MAJOR BOTTLENECK: Batch timeout is causing {avg_wait:.1f}s delays!")
                
            if total_times:
                print(f"\\nEnd-to-End Performance:")
                print(f"  Average Total Time: {sum(total_times)/len(total_times):.3f}s")
                print(f"  Fastest Complete: {min(total_times):.3f}s")
                print(f"  Slowest Complete: {max(total_times):.3f}s")
                
                avg_total = sum(total_times) / len(total_times)
                if avg_total < 2.0:
                    print("  🔥 PERFORMANCE RATING: EXCELLENT")
                elif avg_total < 5.0:
                    print("  👍 PERFORMANCE RATING: GOOD")
                else:
                    print("  ⚠️  PERFORMANCE RATING: NEEDS OPTIMIZATION")

if __name__ == "__main__":
    monitor = CompletePipelineMonitor()
    monitor.start_monitoring()
