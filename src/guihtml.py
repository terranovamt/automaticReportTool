import os
import re
import json
import socket
import threading
import webbrowser
import http.server
import socketserver
import urllib.parse
from collections import defaultdict
from multiprocessing import Process, Queue


PORT = 8000
MAX_PORT = 8800
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.abspath(os.path.join(BASE_DIR, "web"))
POST_FILE = os.path.abspath(os.path.join(BASE_DIR, "./resource/post.json"))
DATA_FILE = os.path.abspath(os.path.join(BASE_DIR, "./resource/datalist.json"))


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        if self.path == "/":
            self.path = "/html/index.html"
            load_data()
        elif self.path == "/index.html":
            self.path = "/html/index.html"
        elif self.path == "/about":
            self.path = "/html/about.html"
        elif self.path == "/loading":
            self.path = "/html/loading.html"
        elif self.path == "/log":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            with open("./src/run.log", "r") as logfile:
                data = logfile.read()
                self.wfile.write(data.encode())
            return
        elif self.path == "/post.json":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            with open(POST_FILE, "r") as jsonfile:
                data = jsonfile.read()
                self.wfile.write(data.encode())
            return
        elif self.path == "/datalist.json":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            with open(DATA_FILE, "r") as jsonfile:
                data = jsonfile.read()
                self.wfile.write(data.encode())
            return
        else:
            if not os.path.isfile(os.path.join(WEB_DIR, self.path.lstrip("/"))):
                self.path = "/html/404.html"
        return super().do_GET()

    def do_POST(self):
        try:
            if self.path == "/submit":
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                if not post_data:
                    self.send_response(400)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"Empty POST data")
                    return

                data = self.parse_post_data(post_data)

                # Check if there is at least one item with 'Run' set to '1'
                if not any(item.get("Run") == "1" for item in data.get("data", [])):
                    self.send_response(400)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"Select at least one item with Run set to 1")
                    return
                # Send a successful response
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(b"Successfully processed the data")
                # Process the data
                self.queue.put(data)

            else:
                self.send_response(404)
                self.end_headers()

        except Exception as e:
            print(f"Error: {e}")

    def parse_post_data(self, post_data):
        data_str = post_data.decode("utf-8")
        parsed_data = urllib.parse.parse_qs(data_str)
        data = json.loads(parsed_data["data"][0])
        return data

    def log_message(self, format, *args):
        pass  # Override to suppress logging


def load_data():
    process = Process(target=stdf_api)
    process.start()
    print("Reload data")


class ThreadingSimpleServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


def stdf_api():
    result = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    )
    base_path = "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF"

    for code_folder in os.listdir(base_path):
        code_folder_path = os.path.join(base_path, code_folder)
        if os.path.isdir(code_folder_path):
            for flow_folder in os.listdir(code_folder_path):
                flow_folder_path = os.path.join(code_folder_path, flow_folder)
                if os.path.isdir(flow_folder_path) and flow_folder.startswith(
                    ("EWS1", "EWS2", "EWS3", "EWSDIE", "FT1", "FT2")
                ):
                    for lot_folder in os.listdir(flow_folder_path):
                        lot_folder_path = os.path.join(flow_folder_path, lot_folder)
                        if os.path.isdir(lot_folder_path) and re.match(
                            r"^[A-Za-z0-9]+$", lot_folder
                        ):
                            for wafer_folder in os.listdir(lot_folder_path):
                                wafer_folder_path = os.path.join(
                                    lot_folder_path, wafer_folder
                                )
                                if os.path.isdir(wafer_folder_path) and re.match(
                                    rf"^{lot_folder}_\d+$", wafer_folder
                                ):
                                    type_folders = [
                                        f
                                        for f in os.listdir(wafer_folder_path)
                                        if os.path.isdir(
                                            os.path.join(wafer_folder_path, f)
                                        )
                                    ]
                                    result[code_folder][flow_folder][lot_folder][
                                        wafer_folder
                                    ] = type_folders

    # Convert defaultdict to regular dict
    result = {
        code: {
            flow: {lot: dict(wafer) for lot, wafer in lots.items()}
            for flow, lots in flows.items()
        }
        for code, flows in result.items()
    }

    # Create the structured JSON
    structured_result = {"CODE": list(result.keys())}
    for code, flows in result.items():
        structured_result[code] = {"FLOW": list(flows.keys())}
        for flow, lots in flows.items():
            structured_result[code][flow] = {"LOT": list(lots.keys())}
            for lot, wafers in lots.items():
                structured_result[code][flow][lot] = {"WAFER": list(wafers.keys())}
                for wafer, types in wafers.items():
                    structured_result[code][flow][lot][wafer] = {"TYPE": types}

    # Save the JSON to a file
    with open("./src/resource/datalist.json", "w") as json_file:
        json.dump(structured_result, json_file, indent=4)

    print("Reload Done")


def start_server(port, queue):
    Handler.queue = queue
    with ThreadingSimpleServer(("", port), Handler) as httpd:
        ip = socket.gethostbyname(socket.gethostname())
        print(f"Serving at {ip}:{port}")
        httpd.serve_forever()


def find_available_port(start_port, max_port, queue):
    port = start_port
    while port <= max_port:
        try:
            start_server(port, queue)
            break
        except OSError as e:
            if e.errno == socket.errno.EADDRINUSE:
                print(f"Port {port} is already in use. Trying port {port + 2}...")
                port += 2
            else:
                raise


def process_data(queue):
    while True:
        data = queue.get()
        if data is None:
            break
        report_generator(data)


def report_generator(data):
    with open(POST_FILE, "w") as jsonfile:
        json.dump(data, jsonfile, indent=4)
    debug = False

    from core import generate

    generate(data)
    update_run_status()


def update_run_status():
    with open(POST_FILE, "r") as jsonfile:
        data = json.load(jsonfile)

    # Iterate through the 'data' list and update 'Run' field
    for item in data.get("data", []):
        if "Run" in item and item["Run"] == "1":
            item["Run"] = "0"

    # Write the updated data back to the JSON file
    with open(POST_FILE, "w") as jsonfile:
        json.dump(data, jsonfile, indent=4)


def guihtml():
    if not os.path.isfile(POST_FILE):
        with open(POST_FILE, "w") as jsonfile:
            json.dump([], jsonfile)

    queue = Queue()
    server_thread = threading.Thread(
        target=find_available_port, args=(PORT, MAX_PORT, queue)
    )
    server_thread.daemon = True
    server_thread.start()

    webbrowser.open(f"http://localhost:{PORT}")

    process_thread = Process(target=process_data, args=(queue,))
    process_thread.start()
    load_data()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Shutting down server.")
        queue.put(None)
        process_thread.join()
