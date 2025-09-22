#!/usr/bin/env python3

"""
MultiCam Controller - Gradio Web Interface

A user-friendly web application for controlling multiple iPhone cameras (via multiCam iOS app)
and OAK cameras (via integrated OAK-Controller-Rpi server).

Features:
- Start/stop OAK server
- Discover all multiCam devices on network
- Synchronized recording across all devices
- Download recorded files
- Real-time status updates

Usage:
    python multicam_app.py

Then open browser to: http://localhost:7860
"""

import gradio as gr
import time
import logging
import sys
import os
import signal
from multicam_controller import MultiCamController
from s3_controller import S3Controller

class MultiCamApp:
    def __init__(self):
        # Initialize logging with debug level for OAK submodule
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("MultiCam Controller initializing...")

        self.controller = MultiCamController()
        self.last_file_ids = {}
        self.recording_in_progress = False

        # S3 Configuration - update these values for your bucket
        self.s3_bucket_name = "87c3e07f-3661-4489-829a-ddfa26943cb3"
        self.s3_region = "us-east-1"
        self.s3_controller = S3Controller(self.s3_bucket_name, self.s3_region)

        # Automatically start OAK server on app initialization
        print("🎥 Starting OAK server automatically...")
        oak_success = self.controller.start_oak_server()
        if oak_success:
            print("✅ OAK server started successfully")
        else:
            print("⚠️  OAK server failed to start (OAK camera will not be available)")

        # Automatically discover devices on startup
        print("🔍 Discovering devices automatically...")
        self.controller.discover_devices(timeout=5)
        device_count = len(self.controller.discovered_devices)
        if device_count > 0:
            print(f"✅ Found {device_count} device(s) on startup")
        else:
            print("⚠️  No devices found on startup (you can discover again later)")


    def discover_devices(self):
        """Discover all multiCam devices and return device list"""
        try:
            self.controller.discover_devices(timeout=5)
            devices = self.controller.discovered_devices

            if not devices:
                return "No multiCam devices found", self.format_device_list(devices)

            device_count = len(devices)
            message = f"✅ Found {device_count} device(s)"

            return message, self.format_device_list(devices)

        except Exception as e:
            return f"❌ Error during discovery: {str(e)}", ""

    def format_device_list(self, devices):
        """Format the device list for display"""
        if not devices:
            return "No devices discovered yet.\n\nMake sure:\n• iPhone multiCam apps are running\n• All devices on same WiFi network\n• OAK server is started (if using OAK camera)"

        device_list = []
        for name, device in devices.items():
            # Determine device type based on multiple criteria
            is_oak_device = (
                'oak' in name.lower() or
                device['ip'] in ['localhost', '127.0.0.1'] or
                device['port'] == 8081 or  # OAK server typically runs on 8081
                'oak' in str(device.get('info', '')).lower()
            )

            if is_oak_device:
                icon = "🎥"
                device_type = "OAK Camera"
            else:
                icon = "📱"
                device_type = "Phone"

            device_list.append(f"{icon} {device_type}: {device['ip']}:{device['port']}")

        return "\n".join(device_list)

    def start_recording(self):
        """Start synchronized recording on all devices with 3-second delay"""
        try:
            if not self.controller.discovered_devices:
                return "❌ No devices available. Discover devices first.", "🔴 Ready"

            if self.recording_in_progress:
                return "❌ Recording already in progress", "🔴 Recording..."

            self.recording_in_progress = True
            device_count = len(self.controller.discovered_devices)
            sync_delay = 3.0  # Fixed 3-second delay

            # Start recording with 3-second sync delay
            result = self.controller.send_command_to_all("START_RECORDING", sync_delay=sync_delay)

            if result:
                return f"✅ Started recording on {device_count} device(s) with 3s sync delay", "🔴 Recording..."
            else:
                self.recording_in_progress = False
                return "❌ Failed to start recording", "🔴 Ready"

        except Exception as e:
            self.recording_in_progress = False
            return f"❌ Error starting recording: {str(e)}", "🔴 Ready"

    def stop_recording(self):
        """Stop recording on all devices, download files, upload to S3, and cleanup"""
        try:
            if not self.recording_in_progress:
                return "❌ No recording in progress", "🔴 Ready"

            device_count = len(self.controller.discovered_devices)

            # Stop recording and get file IDs
            file_ids = self.controller.send_command_to_all("STOP_RECORDING")
            self.recording_in_progress = False

            if isinstance(file_ids, dict) and file_ids:
                file_count = len(file_ids)

                # Step 1: Download all files locally
                downloaded_files = self.controller.download_all_files(file_ids)

                if downloaded_files:
                    # Step 2: Upload to S3 and cleanup local files
                    upload_result = self.s3_controller.upload_and_cleanup(downloaded_files)

                    if upload_result['upload_success']:
                        if upload_result['cleanup_success']:
                            # Complete success: uploaded and cleaned up
                            session_folder = upload_result['session_folder']
                            uploaded_count = upload_result['uploaded_count']
                            return f"✅ Recording complete! {uploaded_count} files uploaded to S3 folder: {session_folder}. Local files cleaned up.", "🔴 Ready"
                        else:
                            # Uploaded but cleanup failed
                            session_folder = upload_result['session_folder']
                            uploaded_count = upload_result['uploaded_count']
                            return f"✅ Recording complete! {uploaded_count} files uploaded to S3 folder: {session_folder}. Warning: some local files remain.", "🔴 Ready"
                    else:
                        # Upload failed, keep local files
                        failed_count = upload_result.get('failed_count', 0)
                        downloaded_count = len(downloaded_files)
                        download_dir = "~/Downloads/multiCam"
                        return f"✅ Recording complete. {downloaded_count} files downloaded to {download_dir}. S3 upload failed for {failed_count} files - local copies preserved.", "🔴 Ready"
                else:
                    return f"✅ Stopped recording on {device_count} device(s). {file_count} files available but download failed.", "🔴 Ready"
            else:
                return f"✅ Stopped recording on {device_count} device(s), but no files returned.", "🔴 Ready"

        except Exception as e:
            self.recording_in_progress = False
            return f"❌ Error stopping recording: {str(e)}", "🔴 Ready"


    def get_device_status(self):
        """Get status from all devices"""
        try:
            if not self.controller.discovered_devices:
                return "❌ No devices available"

            status_results = self.controller.send_command_to_all("DEVICE_STATUS")
            device_count = len(status_results) if status_results else 0

            return f"✅ Got status from {device_count} device(s)"

        except Exception as e:
            return f"❌ Error getting device status: {str(e)}"

    def quit_application(self):
        """Quit the entire application gracefully"""
        try:
            print("\n🛑 Quit button pressed - shutting down...")

            # Stop recording if in progress
            if self.recording_in_progress:
                print("⏹️  Stopping recording before shutdown...")
                self.controller.send_command_to_all("STOP_RECORDING")
                self.recording_in_progress = False

            # Cleanup resources
            self.cleanup()
            cleanup_pid_file()
            print("✅ Cleanup complete")

            # Force exit the application
            os._exit(0)

        except Exception as e:
            print(f"❌ Error during quit: {str(e)}")
            # Cleanup PID file even if other cleanup fails
            cleanup_pid_file()
            # Force exit even if cleanup fails
            os._exit(1)

    def create_interface(self):
        """Create the Gradio interface"""
        with gr.Blocks(title="🎥 MultiCam Controller", theme=gr.themes.Soft()) as app:
            # Header with title and quit button
            with gr.Row():
                with gr.Column(scale=4):
                    gr.Markdown("# 🎥 MultiCam Controller")
                with gr.Column(scale=1, min_width=120):
                    quit_btn = gr.Button("🛑 Quit Controller", variant="stop", size="sm")

            gr.Markdown("Control multiple iPhone and OAK cameras for synchronized recording")

            # Status display at the top
            status_display = gr.Textbox(
                label="Status",
                value="🔴 Ready",
                interactive=False,
                max_lines=1
            )

            # Device Discovery
            gr.Markdown("## 🔍 Device Discovery")

            discover_btn = gr.Button("🔍 Discover All Devices", variant="primary", size="lg")

            device_list = gr.Textbox(
                label="Discovered Devices",
                value=self.format_device_list(self.controller.discovered_devices),
                interactive=False,
                lines=6
            )

            # Recording Controls
            gr.Markdown("## 🎬 Recording Controls")
            gr.Markdown("*Synchronized recording with 3-second delay*")

            with gr.Row():
                start_recording_btn = gr.Button("🎬 Start Recording All Cameras", variant="primary", size="lg")
                stop_recording_btn = gr.Button("⏹️ Stop Recording All Cameras", variant="stop", size="lg")


            # Messages area
            startup_message = "Ready to start recording!" if self.controller.discovered_devices else "Ready to start. Click 'Discover All Devices' if no devices shown above."
            messages = gr.Textbox(
                label="Messages",
                value=startup_message,
                interactive=False,
                lines=3
            )

            # Wire up the event handlers

            # Device discovery
            discover_btn.click(
                fn=self.discover_devices,
                outputs=[messages, device_list]
            )

            # Recording controls
            start_recording_btn.click(
                fn=self.start_recording,
                outputs=[messages, status_display]
            )

            stop_recording_btn.click(
                fn=self.stop_recording,
                outputs=[messages, status_display]
            )

            # Quit button
            quit_btn.click(
                fn=self.quit_application,
                outputs=None
            )

        return app

    def cleanup(self):
        """Clean up resources when app shuts down"""
        self.controller.cleanup()

# Single instance enforcement functions
def get_pid_file_path():
    """Get the path to the PID file"""
    return "/tmp/multicam_controller.pid"

def is_process_running(pid):
    """Check if a process with given PID is running"""
    try:
        os.kill(pid, 0)  # Send signal 0 to check if process exists
        return True
    except OSError:
        return False

def check_single_instance():
    """Ensure only one instance of the main app is running"""
    # Skip PID check if this is a subprocess (e.g., OAK server)
    # Check if we're being called to run a script (OAK server case)
    if len(sys.argv) > 1 and (
        'run_multicam_server.py' in ' '.join(sys.argv) or
        '--port' in sys.argv or
        '--videos-dir' in sys.argv
    ):
        # This is a subprocess call (like OAK server), skip PID check
        return

    pid_file = get_pid_file_path()
    current_pid = os.getpid()

    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                existing_pid = int(f.read().strip())

            if is_process_running(existing_pid):
                print(f"❌ MultiCam Controller is already running (PID: {existing_pid})")
                print("Please close the existing instance or use the Quit button in the web interface.")
                sys.exit(1)
            else:
                print(f"⚠️  Removing stale PID file (process {existing_pid} no longer running)")
                os.remove(pid_file)

        except (ValueError, IOError) as e:
            print(f"⚠️  Invalid PID file, removing: {e}")
            try:
                os.remove(pid_file)
            except OSError:
                pass

    # Create new PID file
    try:
        with open(pid_file, 'w') as f:
            f.write(str(current_pid))
        print(f"✅ Started MultiCam Controller (PID: {current_pid})")
    except IOError as e:
        print(f"⚠️  Could not create PID file: {e}")

def cleanup_pid_file():
    """Remove the PID file on shutdown"""
    pid_file = get_pid_file_path()
    try:
        if os.path.exists(pid_file):
            os.remove(pid_file)
            print("✅ PID file cleaned up")
    except OSError as e:
        print(f"⚠️  Could not remove PID file: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print(f"\n🛑 Received signal {signum}, shutting down...")
    cleanup_pid_file()
    os._exit(0)

def main():
    """Main entry point"""
    print("🎥 Starting MultiCam Controller Web Interface...")

    # Check for single instance
    check_single_instance()

    # Set up signal handlers for clean shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    app_instance = MultiCamApp()

    try:
        # Create and launch the Gradio interface
        app = app_instance.create_interface()

        print("🌐 Web interface will open in your browser")
        print("📱 Make sure iPhone multiCam apps are running on same WiFi network")
        print("🎥 OAK server started automatically (if OAK dependencies available)")
        print("\n⏹️  Press Ctrl+C to stop the server or use the Quit button in the interface\n")

        # Launch with auto-open browser
        app.launch(
            server_name="0.0.0.0",  # Allow access from other devices on network
            server_port=7860,
            share=False,
            inbrowser=True
        )

    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    finally:
        app_instance.cleanup()
        cleanup_pid_file()
        print("✅ Cleanup complete")

if __name__ == "__main__":
    main()