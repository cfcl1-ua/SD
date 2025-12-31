from flask import Flask, send_from_directory
import threading
import webbrowser
import time

app = Flask(__name__)

@app.route("/")
def index():
    return send_from_directory(".", "index.html")

@app.route("/charging_points.json")
def cps():
    return send_from_directory(".", "charging_points.json")

def open_browser():
    time.sleep(1)
    try:
        webbrowser.open("http://localhost:8000/")
    except:
        pass

if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    app.run(host="0.0.0.0", port=8000)
