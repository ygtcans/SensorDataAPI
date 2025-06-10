# main.py
#
# This file serves as the main entry point for the FastAPI application.
# It sets up and starts the industrial sensor data API, including both
# traditional RESTful HTTP endpoints and a real-time WebSocket data stream.
# It also initializes and manages the sensor data simulator.

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from data_producer.sensor_simulator import SensorSimulator
from data_producer.models import MachineState
from dotenv import load_dotenv
import os
import asyncio
from typing import List, Dict, Any
import json

# Load environment variables from a .env file.
# This is crucial for securely managing configurations like API keys.
load_dotenv()

# Initialize the FastAPI application.
# Provide metadata for automatic API documentation (e.g., Swagger UI).
app = FastAPI(
    title="Industrial Sensor API",
    version="1.0.0"
)

# Initialize and start the sensor data simulator.
# This component continuously generates simulated sensor readings for multiple machines.
simulator = SensorSimulator()
simulator.start()

# Retrieve the API key from environment variables for request authentication.
API_KEY = os.getenv("API_KEY")

# --- WebSocket Specific Additions ---

# Maintain a list of all active WebSocket connections.
# This allows the server to broadcast messages to all connected clients.
active_websocket_connections: List[WebSocket] = []

async def send_latest_data_to_websocket_clients():
    """
    Asynchronous background task to periodically broadcast the latest sensor data
    to all currently connected WebSocket clients.

    This ensures real-time data streaming without requiring clients to poll.
    """
    while True:
        # Fetch the most recent sensor data from the simulator.
        latest_data = simulator.get_latest_data()
        
        # Serialize the Python dictionary data into a JSON string for WebSocket transmission.
        message = json.dumps(latest_data)

        # Iterate through a copy of the active connections and attempt to send the data.
        # A copy is used to prevent issues if connections are removed during iteration.
        for connection in active_websocket_connections.copy():
            try:
                # Send the JSON message over the WebSocket connection.
                await connection.send_text(message)
            except WebSocketDisconnect:
                # Handle client disconnection gracefully.
                # Remove the disconnected client from the active connections list.
                print(f"WebSocket client disconnected. Total active connections: {len(active_websocket_connections)}")
                active_websocket_connections.remove(connection)
            except Exception as e:
                # Catch and log any other errors during data transmission to a specific client.
                # Remove the problematic connection to prevent further errors.
                print(f"Error sending data to WebSocket client: {e}")
                active_websocket_connections.remove(connection)

        # Pause the task for a specified duration before sending the next batch of data.
        # Adjust this delay to control the frequency of data pushes to clients.
        await asyncio.sleep(1) # Sends data every 1 second

@app.on_event("startup")
async def startup_event():
    """
    FastAPI lifecycle event handler.
    Executes a task when the application starts up.
    """
    # Create and run the data broadcasting function as a non-blocking background task.
    # This allows the API to serve HTTP requests concurrently with WebSocket streaming.
    asyncio.create_task(send_latest_data_to_websocket_clients())
    print("Background WebSocket data sender initiated.")

@app.websocket("/ws/sensordata")
async def websocket_endpoint(websocket: WebSocket, api_key: str):
    """
    Establishes a WebSocket connection for real-time sensor data streaming.

    Clients connect to this endpoint to receive continuous updates as data becomes available.
    Includes API key authentication for secure access.

    Args:
        websocket (WebSocket): The WebSocket connection object managed by FastAPI.
        api_key (str): The API key provided by the client for authentication.
    """
    try:
        # Authenticate the incoming WebSocket connection using the provided API key.
        verify_api_key(api_key) 
        
        # Accept the WebSocket connection after successful authentication.
        await websocket.accept()
        # Add the newly accepted connection to the list of active clients.
        active_websocket_connections.append(websocket)
        print(f"New WebSocket client connected. Total active connections: {len(active_websocket_connections)}")

        # Keep the connection alive indefinitely.
        # This loop primarily listens for disconnection events or messages from the client.
        # For a data streaming service, clients typically only receive data,
        # but `receive_text()` helps manage the connection's lifecycle.
        while True:
            await websocket.receive_text() 

    except HTTPException as e:
        # Handle authentication failures specifically for WebSocket connections.
        # Close the connection with a policy violation code (1008).
        print(f"WebSocket authentication failed: {e.detail}")
        await websocket.close(code=1008, reason="Authentication failed")
    except WebSocketDisconnect:
        # Log graceful client disconnections.
        print("WebSocket client disconnected gracefully.")
    except Exception as e:
        # Log any other unexpected errors that occur during the WebSocket connection.
        print(f"WebSocket connection error: {e}")
    finally:
        # Ensure the WebSocket connection is removed from the active list
        # when it closes, either gracefully or due to an error.
        if websocket in active_websocket_connections:
            active_websocket_connections.remove(websocket)
        print(f"WebSocket connection closed. Total active connections: {len(active_websocket_connections)}")

# --- Core Authentication Function ---

def verify_api_key(api_key: str):
    """
    Authenticates incoming requests by validating the provided API key.

    Raises:
        HTTPException: If the API key is invalid, returning a 401 Unauthorized status.
    """
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

# --- Standard REST API Endpoints ---
# These endpoints continue to coexist with the WebSocket functionality,
# providing traditional HTTP-based data retrieval.

@app.get("/")
def root():
    """
    Root endpoint for the API.
    Returns a welcome message, useful for basic API health checks.
    """
    return {"message": "Welcome to the Industrial Sensor API"}

@app.get("/sensordata")
def get_sensor_data(api_key: str):
    """
    Retrieves the most recent sensor data for all simulated machines.

    Args:
        api_key (str): The API key for authenticating the request.

    Returns:
        dict: A dictionary where keys are machine IDs and values are their latest sensor readings.
    """
    verify_api_key(api_key)
    data = simulator.get_latest_data()
    return data

@app.get("/status")
def get_factory_status(api_key: str):
    """
    Provides a high-level summary of the factory's operational status.

    Includes aggregated machine states, active error counts, and overall efficiency.

    Args:
        api_key (str): The API key for authenticating the request.

    Returns:
        dict: A summary object containing various factory status metrics.
    """
    verify_api_key(api_key)
    states_summary = simulator.get_machine_states_summary()
    error_summary = simulator.get_error_summary()
    latest_data = simulator.get_latest_data()
    # Safely get a sample timestamp if any machine data exists.
    sample_timestamp = latest_data.get(list(latest_data.keys())[0], {}).get("timestamp") if latest_data else None
    return {
        "timestamp": sample_timestamp,
        "total_machines": len(simulator.machines),
        "machine_states": states_summary,
        "active_errors": error_summary,
        "overall_efficiency": round((states_summary.get('active', 0) / len(simulator.machines)) * 100, 1) if simulator.machines else 0.0
    }

@app.get("/machine/{machine_id}")
def get_machine_data(machine_id: str, api_key: str):
    """
    Retrieves the most recent sensor data for a specific machine.

    Args:
        machine_id (str): The unique identifier of the machine.
        api_key (str): The API key for authenticating the request.

    Returns:
        dict: The latest sensor data for the specified machine.

    Raises:
        HTTPException: 404 Not Found if the `machine_id` does not exist.
    """
    verify_api_key(api_key)
    data = simulator.get_latest_data()
    if machine_id not in data:
        raise HTTPException(status_code=404, detail=f"Machine {machine_id} not found")
    return data[machine_id]

@app.post("/machine/{machine_id}/force-state")
def force_machine_state(machine_id: str, state: str, api_key: str):
    """
    Allows forcing a specific state change for a given machine.
    Primarily used for testing, debugging, or simulation control.

    Args:
        machine_id (str): The unique identifier of the machine.
        state (str): The desired new state (e.g., "active", "idle", "error", "maintenance").
        api_key (str): The API key for authenticating the request.

    Returns:
        dict: A confirmation message indicating the state change.

    Raises:
        HTTPException: 400 Bad Request if the `state` is invalid,
                       or 404 Not Found if the `machine_id` does not exist.
    """
    verify_api_key(api_key)
    try:
        # Convert the string state to the MachineState enum.
        new_state = MachineState(state.lower())
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid state: {state}")
    
    success = simulator.force_state_change(machine_id, new_state)
    if not success:
        raise HTTPException(status_code=404, detail=f"Machine {machine_id} not found")
    return {"message": f"Machine {machine_id} state changed to {state}"}

@app.get("/errors")
def get_error_details(api_key: str):
    """
    Fetches a detailed list of all currently active machine errors across the factory.

    Args:
        api_key (str): The API key for authenticating the request.

    Returns:
        dict: An object containing the total count of errors and a list of detailed error records.
    """
    verify_api_key(api_key)
    data = simulator.get_latest_data()
    errors = []
    for machine_id, machine_data in data.items():
        if machine_data.get('state') == 'error':
            errors.append({
                "machine_id": machine_id,
                "error_code": machine_data.get('error_code'),
                "error_description": machine_data.get('error_description'),
                "product_type": machine_data.get('product_type'),
                "timestamp": machine_data.get('timestamp'),
                "temperature": machine_data.get('temperature'),
                "pressure": machine_data.get('pressure'),
                "energy_consumption": machine_data.get('energy_consumption'),
                "vibration": machine_data.get('vibration')
            })
    return {
        "total_errors": len(errors),
        "errors": errors
    }

@app.post("/simulation/speed")
def set_simulation_speed(speed: float, api_key: str):
    """
    Sets the speed multiplier for the sensor data simulation.
    This allows controlling how fast data is generated (e.g., 2.0 for twice as fast).

    Args:
        speed (float): The desired simulation speed multiplier (e.g., 1.0 for real-time, 5.0 for 5x speed).
                       Valid range is typically between 0.1 and 10.0.
        api_key (str): The API key for authenticating the request.

    Returns:
        dict: A confirmation message indicating the new simulation speed.

    Raises:
        HTTPException: 400 Bad Request if the `speed` value is outside the acceptable range.
    """
    verify_api_key(api_key)
    if not 0.1 <= speed <= 10.0: # Using 'not X <= speed <= Y' is more readable.
        raise HTTPException(status_code=400, detail="Speed must be between 0.1 and 10.0")
    simulator.set_simulation_speed(speed)
    return {"message": f"Simulation speed set to {speed}x"}