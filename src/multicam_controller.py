#!/usr/bin/env python3

import socket
import json
import time
import threading
import struct
import os
import subprocess
import sys
import asyncio
from pathlib import Path
from zeroconf import ServiceBrowser, Zeroconf

class MultiCamController:
    def __init__(self):
        self.discovered_devices = {}
        self.zeroconf = Zeroconf()
        self.browser = None
        self.last_downloaded_files = []
        self.debug = False
        self.oak_server_task = None
        self.oak_server_process = None
        self.oak_device = None
        self.oak_server_instance = None

    def discover_devices(self, timeout=5):
        """Discover multiCam devices on the network using Bonjour/mDNS"""
        print(f"Discovering multiCam devices for {timeout} seconds...")

        class MultiCamListener:
            def __init__(self, controller):
                self.controller = controller

            def remove_service(self, zeroconf, type, name):
                print(f"Service removed: {name}")
                if name in self.controller.discovered_devices:
                    del self.controller.discovered_devices[name]

            def add_service(self, zeroconf, type, name):
                info = zeroconf.get_service_info(type, name)
                if info:
                    ip = socket.inet_ntoa(info.addresses[0])
                    port = info.port
                    print(f"Found multiCam device: {name} at {ip}:{port}")
                    self.controller.discovered_devices[name] = {
                        'ip': ip,
                        'port': port,
                        'info': info
                    }

            def update_service(self, zeroconf, type, name):
                pass

        listener = MultiCamListener(self)
        self.browser = ServiceBrowser(self.zeroconf, "_multicam._tcp.local.", listener)

        # Wait for discovery
        time.sleep(timeout)

        if self.discovered_devices:
            print(f"\nDiscovered {len(self.discovered_devices)} device(s):")
            for name, device in self.discovered_devices.items():
                print(f"  - {name}: {device['ip']}:{device['port']}")
        else:
            print("No multiCam devices found")

        return list(self.discovered_devices.values())

    def send_command(self, device_ip, device_port, command, timestamp=None, file_id=None):
        """Send a command to a multiCam device"""
        try:
            # Create command message
            message = {
                "command": command,
                "timestamp": timestamp or time.time(),
                "deviceId": "controller"
            }

            if file_id:
                message["fileId"] = file_id

            # Convert to JSON
            json_data = json.dumps(message).encode('utf-8')

            # Connect and send
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)  # Longer timeout for file transfers

            if self.debug:
                print(f"Connecting to {device_ip}:{device_port}...")
            sock.connect((device_ip, device_port))

            if self.debug:
                print(f"Sending command: {command}")
                if file_id:
                    print(f"   File ID: {file_id}")
            sock.send(json_data)

            # Handle different response types
            if command == "GET_VIDEO":
                return self._handle_file_download(sock, device_ip, file_id)
            else:
                # Wait for JSON response - use larger buffer and handle chunked responses for LIST_FILES
                response_data = b""

                # For LIST_FILES, we may need to receive in chunks due to large responses
                if command == "LIST_FILES":
                    sock.settimeout(10)  # Set timeout for receiving data
                    while True:
                        try:
                            chunk = sock.recv(8192)
                            if not chunk:
                                break
                            response_data += chunk
                            # Check if we have a complete JSON response
                            try:
                                decoded = response_data.decode('utf-8')
                                json.loads(decoded)
                                break  # Complete JSON received
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                continue  # Keep receiving
                        except socket.timeout:
                            break
                else:
                    response_data = sock.recv(4096)

                if response_data:
                    response_json = json.loads(response_data.decode('utf-8'))
                    if self.debug:
                        print(f"Response: {json.dumps(response_json, indent=2)}")

                    # Extract file ID from stop recording response
                    if command == "STOP_RECORDING" and "fileId" in response_json and response_json["fileId"]:
                        if self.debug:
                            print(f"File ID received: {response_json['fileId']}")
                        return response_json["fileId"]

                    # Return response data for further processing
                    return response_json

                sock.close()
                return True

        except Exception as e:
            print(f"Error sending command to {device_ip}:{device_port}: {e}")
            return False

    def _handle_file_download(self, sock, device_ip, file_id):
        """Handle file download from device"""
        try:
            print(f"Receiving file data...")

            # Read header size (4 bytes, big-endian uint32)
            header_size_data = sock.recv(4)
            if len(header_size_data) != 4:
                print("Failed to read header size")
                return False

            header_size = struct.unpack('>I', header_size_data)[0]
            print(f"Header size: {header_size} bytes")

            # Read header data
            header_data = b""
            while len(header_data) < header_size:
                chunk = sock.recv(header_size - len(header_data))
                if not chunk:
                    break
                header_data += chunk

            # Parse header JSON
            header_info = json.loads(header_data.decode('utf-8'))
            file_name = header_info["fileName"]
            file_size = header_info["fileSize"]

            print(f"File: {file_name}")
            print(f"Size: {file_size:,} bytes ({file_size / 1024 / 1024:.1f} MB)")

            # Create downloads directory
            downloads_dir = os.path.expanduser("~/Downloads/multiCam")
            os.makedirs(downloads_dir, exist_ok=True)

            # Create unique filename with device IP
            device_name = device_ip.replace('.', '_')
            local_filename = f"{device_name}_{file_name}"
            local_path = os.path.join(downloads_dir, local_filename)

            # Download file data
            print(f"Downloading to: {local_path}")
            bytes_received = 0

            with open(local_path, 'wb') as f:
                while bytes_received < file_size:
                    chunk_size = min(8192, file_size - bytes_received)
                    chunk = sock.recv(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_received += len(chunk)

                    # Progress indicator
                    progress = (bytes_received / file_size) * 100
                    print(f"\rProgress: {progress:.1f}% ({bytes_received:,}/{file_size:,} bytes)", end='')

            print(f"\nFile downloaded successfully: {local_path}")
            sock.close()
            return local_path

        except Exception as e:
            print(f"Error downloading file: {e}")
            sock.close()
            return False

    def send_command_to_all(self, command, timestamp=None, sync_delay=3.0):
        """Send a command to all discovered devices"""
        if not self.discovered_devices:
            print("No devices discovered. Run discovery first.")
            return

        # For START_RECORDING, calculate a future timestamp for synchronization
        if command == "START_RECORDING" and timestamp is None:
            sync_timestamp = time.time() + sync_delay
            print(f"Broadcasting synchronized {command} to {len(self.discovered_devices)} device(s)")
            print(f"Scheduled start time: {sync_timestamp} (in {sync_delay} seconds)")
        else:
            sync_timestamp = timestamp or time.time()
            if self.debug:
                print(f"Broadcasting {command} to {len(self.discovered_devices)} device(s) at timestamp {sync_timestamp}")

        # Send to all devices simultaneously using threads
        threads = []
        results = {}

        def send_and_store_result(device_name, device_ip, device_port):
            result = self.send_command(device_ip, device_port, command, sync_timestamp)
            results[device_name] = result

        for name, device in self.discovered_devices.items():
            thread = threading.Thread(
                target=send_and_store_result,
                args=(name, device['ip'], device['port'])
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Return file IDs if stopping recording
        if command == "STOP_RECORDING":
            print(f"\nDebug: Results from stop command: {results}")
            file_ids = {name: result for name, result in results.items() if isinstance(result, str)}
            if file_ids:
                print(f"\nRecorded files available for download:")
                for device_name, file_id in file_ids.items():
                    print(f"  {device_name}: {file_id}")
            else:
                print(f"\nNo file IDs received. Results: {results}")
            return file_ids

        return results

    def list_files_on_all_devices(self):
        """List all recorded files on all devices"""
        if not self.discovered_devices:
            print("No devices discovered. Run discovery first.")
            return

        print(f"Listing files on {len(self.discovered_devices)} device(s)...\n")

        total_files = 0
        total_size = 0

        for device_name, device in self.discovered_devices.items():
            try:
                print(f"{device_name} ({device['ip']}:{device['port']}):")
                response = self.send_command(device['ip'], device['port'], "LIST_FILES")

                if isinstance(response, dict) and 'files' in response:
                    files = response['files']
                    if files:
                        print(f"   Found {len(files)} file(s):")
                        device_total_size = 0

                        for file_info in files:
                            file_size_mb = file_info['fileSize'] / (1024 * 1024)
                            device_total_size += file_info['fileSize']
                            creation_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                                        time.localtime(file_info['creationDate']))

                            print(f"   • {file_info['fileName']}")
                            print(f"     ID: {file_info['fileId']}")
                            print(f"     Size: {file_size_mb:.1f} MB")
                            print(f"     Created: {creation_time}")
                            print()

                        print(f"   Device total: {device_total_size / (1024 * 1024):.1f} MB")
                        total_files += len(files)
                        total_size += device_total_size
                    else:
                        print("   No files found")
                else:
                    print("   Failed to get file list")

                print()

            except Exception as e:
                print(f"   Error: {e}")
                print()

        if total_files > 0:
            print(f"Summary: {total_files} total files, {total_size / (1024 * 1024):.1f} MB total")
        else:
            print("No files found on any device")

    def manual_connect(self, ip, port=8080):
        """Manually connect to a device without discovery"""
        print(f"Manual connection to {ip}:{port}")
        self.discovered_devices[f"manual-{ip}"] = {
            'ip': ip,
            'port': port
        }

    def download_file(self, device_name, file_id):
        """Download a specific file from a device"""
        if device_name not in self.discovered_devices:
            print(f"Device '{device_name}' not found")
            return False

        device = self.discovered_devices[device_name]
        return self.send_command(device['ip'], device['port'], "GET_VIDEO", file_id=file_id)

    def download_all_files(self, file_ids):
        """Download all files from the given file_ids dictionary"""
        if not file_ids:
            print("No files to download")
            return []

        print(f"Downloading {len(file_ids)} files...")
        downloaded_files = []

        for device_name, file_id in file_ids.items():
            if device_name in self.discovered_devices:
                device = self.discovered_devices[device_name]
                print(f"\nDownloading from {device_name}...")
                file_path = self.send_command(device['ip'], device['port'], "GET_VIDEO", file_id=file_id)
                if file_path:
                    downloaded_files.append(file_path)

        if downloaded_files:
            self.last_downloaded_files = downloaded_files
            print(f"\nDownloaded {len(downloaded_files)} files successfully")

        return downloaded_files

    def get_device_status(self):
        """Get status from all devices"""
        return self.send_command_to_all("DEVICE_STATUS")

    def start_oak_server(self, port=8081, videos_dir="./oak_videos"):
        """Start the OAK server as a subprocess"""
        if self.oak_server_process is not None:
            print("OAK server is already running")
            return True

        try:
            # Path to the OAK server script
            # Handle PyInstaller bundle vs development paths
            if getattr(sys, 'frozen', False):
                # Running in PyInstaller bundle
                if hasattr(sys, '_MEIPASS'):
                    # onefile bundle or onedir temp extract
                    base_path = Path(sys._MEIPASS)
                else:
                    # onedir bundle - look relative to executable
                    base_path = Path(sys.executable).parent
                    # For macOS app bundles, also try Resources directory
                    if not (base_path / "OAK-Controller-Rpi").exists():
                        resources_path = base_path.parent / "Resources"
                        if resources_path.exists():
                            base_path = resources_path
            else:
                # Running in development
                base_path = Path(__file__).parent.parent

            oak_script_path = base_path / "OAK-Controller-Rpi" / "run_multicam_server.py"

            if not oak_script_path.exists():
                print(f"Error: OAK server script not found at {oak_script_path}")
                return False

            # Ensure videos directory exists
            Path(videos_dir).mkdir(parents=True, exist_ok=True)

            print(f"Starting OAK server on port {port}...")

            # Start the OAK server process with visible logs
            print("🔍 Starting OAK server with visible logs...")

            # In PyInstaller bundle, we need to use python directly, not sys.executable
            if getattr(sys, 'frozen', False):
                # In bundle - use python from the pixi environment
                # Look for python in common pixi locations
                import shutil
                python_cmd = shutil.which("python") or shutil.which("python3") or "python3"

                # Try to find pixi environment python
                possible_pixi_paths = [
                    "/Users/angusemmett/code/multiCamController/.pixi/envs/default/bin/python",
                    str(Path(__file__).parent.parent.parent / ".pixi/envs/default/bin/python"),
                ]

                for pixi_python in possible_pixi_paths:
                    if Path(pixi_python).exists():
                        python_cmd = pixi_python
                        print(f"🐍 Using pixi Python: {python_cmd}")
                        break
                else:
                    print(f"⚠️  Using system Python: {python_cmd}")
            else:
                # In development - use sys.executable
                python_cmd = sys.executable

            self.oak_server_process = subprocess.Popen([
                python_cmd,
                str(oak_script_path),
                "--port", str(port),
                "--videos-dir", videos_dir
            ],
            # Allow logs to be visible in the terminal
            stdout=None,  # Inherit stdout from parent
            stderr=None,  # Inherit stderr from parent
            text=True
            )

            # Wait a moment for the server to start
            time.sleep(3)  # Increased wait time

            # Check if the process is still running
            if self.oak_server_process.poll() is None:
                print(f"✅ OAK server started successfully on port {port}")
                print("🌐 OAK device should be discoverable via mDNS like other multiCam devices")
                print(f"📋 OAK server PID: {self.oak_server_process.pid}")
                return True
            else:
                print(f"❌ OAK server failed to start (process exited)")
                self.oak_server_process = None
                return False

        except Exception as e:
            print(f"Error starting OAK server: {e}")
            return False

    def stop_oak_server(self):
        """Stop the OAK server process"""
        if self.oak_server_process is not None:
            try:
                print("Stopping OAK server...")
                self.oak_server_process.terminate()

                # Wait for graceful shutdown
                try:
                    self.oak_server_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't stop gracefully
                    self.oak_server_process.kill()
                    self.oak_server_process.wait()

                self.oak_server_process = None
                print("OAK server stopped")
                return True

            except Exception as e:
                print(f"Error stopping OAK server: {e}")
                return False
        else:
            print("OAK server is not running")
            return True

    def is_oak_server_running(self):
        """Check if the OAK server is currently running"""
        return self.oak_server_process is not None and self.oak_server_process.poll() is None

    def cleanup(self):
        """Clean up resources"""
        # Stop OAK server first
        self.stop_oak_server()

        if self.browser:
            self.browser.cancel()
        self.zeroconf.close()