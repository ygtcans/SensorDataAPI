# sensor_simulator.py
# This file defines the SensorSimulator class, which is responsible for managing
# a collection of Machine instances and simulating their sensor data generation
# in a continuous, threaded manner. It aims to mimic a real-world industrial
# environment with multiple machines operating in different states.

import threading
import time
import random
from datetime import datetime, timezone, timedelta # Added timedelta for potential use
from data_producer.machine import Machine # Assuming machine.py is in a 'data_producer' package
from data_producer.models import ProductType, MachineState # Assuming models.py is in the same package
from typing import Dict, List, Tuple # Added List for type hinting if needed later

class SensorSimulator:
    """
    Manages multiple Machine instances and simulates their sensor data generation over time.
    This class orchestrates the behavior of several machines, updating their states
    and generating sensor readings in a background thread to simulate a continuous
    operational environment.
    """

    def __init__(self, machine_count: int = 10):
        """
        Initializes the SensorSimulator with a specified number of machines.

        :param machine_count: The number of machines to simulate. Defaults to 10.
        """
        self.machines: Dict[str, Machine] = {}  # Dictionary to store machine instances, keyed by machine_id.
        self.latest_snapshot: Dict[str, dict] = {}  # Stores the most recent sensor data for each machine.
        self._lock = threading.Lock()  # A lock to ensure thread-safe access to shared data (machines, latest_snapshot).
        self._running = False  # Flag to control the main simulation loop.

        # Simulation settings
        self.update_interval: int = 5  # Interval in seconds between simulation updates for each machine.
        self.simulation_speed: float = 1.0  # Multiplier for simulation time; 1.0 is real-time. >1 is faster, <1 is slower.

        # Energy profile multipliers by product type.
        # This dictionary defines how energy consumption varies based on the product being manufactured.
        energy_profile: Dict[str, float] = {
            ProductType.POLYETHYLENE.value: 1.1,
            ProductType.POLYPROPYLENE.value: 1.0,
            ProductType.PVC.value: 1.3,
            ProductType.POLYSTYRENE.value: 1.05,
            ProductType.ABS.value: 0.95,
        }

        # Create and configure the specified number of machine instances.
        product_types: List[ProductType] = list(ProductType) # Get a list of all available product types.

        for i in range(machine_count):
            # Assign a product type to the machine, cycling through the available types.
            product: ProductType = product_types[i % len(product_types)]
            machine_id: str = f"Machine_{i+1}" # Create a unique ID for each machine.

            # Define product-specific operational ranges for temperature.
            temp_ranges: Dict[str, Tuple[float, float]] = {
                ProductType.POLYETHYLENE.value: (85, 125),
                ProductType.POLYPROPYLENE.value: (80, 120),
                ProductType.PVC.value: (90, 135),
                ProductType.POLYSTYRENE.value: (75, 115),
                ProductType.ABS.value: (82, 122)
            }

            # Define product-specific operational ranges for pressure.
            pressure_ranges: Dict[str, Tuple[float, float]] = {
                ProductType.POLYETHYLENE.value: (0.4, 0.9),
                ProductType.POLYPROPYLENE.value: (0.35, 0.85),
                ProductType.PVC.value: (0.45, 0.95),
                ProductType.POLYSTYRENE.value: (0.3, 0.8),
                ProductType.ABS.value: (0.38, 0.88)
            }

            # Get the specific temperature and pressure ranges for the current product, with defaults.
            temp_range: Tuple[float, float] = temp_ranges.get(product.value, (80, 130))
            pressure_range: Tuple[float, float] = pressure_ranges.get(product.value, (0.3, 1.0))
            # Assign a slightly varied maximum vibration level for each machine.
            max_vibration: float = random.uniform(0.6, 0.8)

            # Instantiate the Machine object.
            machine = Machine(machine_id, product, temp_range, pressure_range,
                              energy_profile, max_vibration)

            # Initialize machines with a distribution of states to simulate a more realistic factory floor.
            # Weighted towards ACTIVE and IDLE states.
            initial_states: List[MachineState] = ([MachineState.ACTIVE] * 7 +
                                                  [MachineState.IDLE] * 2 +
                                                  [MachineState.MAINTENANCE] * 1) # 70% Active, 20% Idle, 10% Maintenance
            machine.current_state = random.choice(initial_states)
            if machine.current_state == MachineState.ERROR: # Ensure error state has a code if chosen initially
                machine._assign_error_code()


            # Stagger the 'last_state_change' timestamp slightly to prevent all machines
            # from trying to change state simultaneously at the very beginning.
            # This introduces a bit of desynchronization.
            machine.last_state_change = datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, self.update_interval * 5))

            self.machines[machine_id] = machine # Add the configured machine to the simulator's collection.

    def start(self):
        """
        Starts the sensor data simulation in a new background thread.
        If the simulation is already running, this method does nothing.
        """
        if not self._running:
            self._running = True
            print(f"Starting sensor simulation with {len(self.machines)} machines...")
            print(f"Update interval: {self.update_interval}s, Simulation speed: {self.simulation_speed}x")
            # Create a daemon thread that will run the _update_loop method.
            # Daemon threads automatically exit when the main program exits.
            simulation_thread = threading.Thread(target=self._update_loop, daemon=True)
            simulation_thread.start()

    def stop(self):
        """
        Stops the sensor data simulation.
        Sets the _running flag to False, causing the background thread to terminate its loop.
        """
        self._running = False
        print("Sensor simulation stopping... Please wait for the current update cycle to complete.")
        # Note: The thread will complete its current iteration before stopping.

    def _update_loop(self):
        """
        The main loop for the simulation, running in a separate thread.
        Periodically updates the state and sensor data for each machine.
        """
        while self._running:
            try:
                current_time_utc = datetime.now(timezone.utc)
                # Determine the current work shift based on the hour of the day.
                current_shift = self._get_shift(current_time_utc.hour)

                # Acquire the lock to ensure exclusive access to shared machine data.
                with self._lock:
                    for machine_id, machine in self.machines.items():
                        # Update the state of the machine (e.g., ACTIVE, IDLE, ERROR).
                        machine.update_state(current_time_utc, current_shift)
                        # Generate new sensor data based on the machine's current state.
                        data = machine.generate_sensor_data(current_time_utc)
                        # Store the latest data for this machine in the snapshot.
                        self.latest_snapshot[machine_id] = data

                # Pause the loop according to the update interval and simulation speed.
                # A higher simulation_speed results in a shorter sleep time.
                time.sleep(self.update_interval / self.simulation_speed)

            except Exception as e:
                # Log any errors that occur within the simulation loop to prevent it from crashing.
                print(f"Error in simulation loop: {e}")
                # Brief pause after an error to avoid rapid error logging.
                time.sleep(1)
        print("Simulation loop has ended.")


    def get_latest_data(self) -> Dict[str, dict]:
        """
        Retrieves a snapshot of the latest sensor data for all machines.
        This method is thread-safe.

        :return: A dictionary where keys are machine_ids and values are their latest sensor data.
        """
        with self._lock:
            # Return a copy of the snapshot to prevent external modification of the internal state.
            return dict(self.latest_snapshot)

    def get_machine_states_summary(self) -> Dict[str, int]:
        """
        Provides a summary of the current states of all machines (e.g., how many are ACTIVE, IDLE, etc.).
        This method is thread-safe.

        :return: A dictionary where keys are state names (str) and values are the counts of machines in that state.
        """
        with self._lock:
            summary: Dict[str, int] = {state.value: 0 for state in MachineState} # Initialize counts for all possible states
            for data in self.latest_snapshot.values():
                current_machine_state = data.get('state')
                if current_machine_state in summary:
                    summary[current_machine_state] += 1
            return summary

    def get_error_summary(self) -> Dict[str, int]:
        """
        Provides a summary of current errors across all machines, categorized by error code.
        This method is thread-safe.

        :return: A dictionary where keys are error codes (str) and values are the counts of machines exhibiting that error.
        """
        with self._lock:
            error_counts: Dict[str, int] = {}
            for data in self.latest_snapshot.values():
                # Check if the machine is in an ERROR state and has an error_code.
                if data.get('state') == MachineState.ERROR.value and data.get('error_code'):
                    error_code = data['error_code']
                    error_counts[error_code] = error_counts.get(error_code, 0) + 1
            return error_counts

    def force_state_change(self, machine_id: str, new_state: MachineState) -> bool:
        """
        Manually forces a specific machine to a new state. Useful for testing or specific scenarios.
        This method is thread-safe.

        :param machine_id: The ID of the machine to modify.
        :param new_state: The MachineState to set for the machine.
        :return: True if the state change was successful, False if the machine_id was not found.
        """
        with self._lock:
            if machine_id in self.machines:
                machine = self.machines[machine_id]
                machine.current_state = new_state
                machine.last_state_change = datetime.now(timezone.utc) # Update timestamp for the change.

                # If the new state is ERROR, assign an appropriate error code.
                if new_state == MachineState.ERROR:
                    machine._assign_error_code() # Internal method of Machine class
                else:
                    # Clear any existing error codes if the machine is no longer in an ERROR state.
                    machine.error_code = None
                    machine.error_description = None

                print(f"Forced machine '{machine_id}' to state: {new_state.value}")
                # Update the snapshot immediately to reflect this forced change
                self.latest_snapshot[machine_id] = machine.generate_sensor_data(datetime.now(timezone.utc))
                return True
            else:
                print(f"Machine '{machine_id}' not found for state change.")
                return False

    def set_simulation_speed(self, speed: float):
        """
        Adjusts the speed of the simulation.
        The speed is clamped between 0.1x and 10.0x.

        :param speed: The desired simulation speed multiplier (1.0 is real-time).
        """
        # Clamp the speed to a reasonable range to prevent extreme values.
        self.simulation_speed = max(0.1, min(10.0, speed))
        print(f"Simulation speed set to {self.simulation_speed}x")

    @staticmethod
    def _get_shift(hour: int) -> str:
        """
        Determines the current work shift based on the hour of the day (24-hour format).
        This is a static method as it doesn't depend on the instance's state.

        :param hour: The current hour (0-23).
        :return: A string representing the shift ("day", "evening", or "night").
        """
        if 6 <= hour < 14:  # Day shift: 6:00 AM to 1:59 PM
            return "day"
        elif 14 <= hour < 22:  # Evening shift: 2:00 PM to 9:59 PM
            return "evening"
        else:  # Night shift: 10:00 PM to 5:59 AM
            return "night"

    def print_status(self):
        """
        Prints a summary of the current simulation status to the console,
        including machine state counts and error summaries.
        This is useful for monitoring the simulation.
        """
        # Retrieve current state and error summaries.
        # These methods are thread-safe due to their internal locking.
        states_summary = self.get_machine_states_summary()
        errors_summary = self.get_error_summary()

        # Format and print the status report.
        print("\n" + "="*50)
        print("FACTORY SIMULATION STATUS")
        print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
        print("="*50)
        print(f"Total Machines: {len(self.machines)}")
        print(f"Simulation Speed: {self.simulation_speed}x")
        print(f"Update Interval: {self.update_interval}s")
        print("-" * 50)
        print("Machine States:")
        for state_name, count in states_summary.items():
            print(f"  {state_name.capitalize()}: {count}")

        if errors_summary:
            print("\nCurrent Errors:")
            for error_code, count in errors_summary.items():
                # Assuming ERROR_CODES is accessible or you fetch description differently
                # from data_producer.models import ERROR_CODES (if not already imported globally)
                # error_desc = ERROR_CODES.get(error_code, "Unknown error")
                # print(f"  {error_code} ({error_desc}): {count} machines")
                print(f"  {error_code}: {count} machines") # Simpler version if ERROR_CODES not directly used here
        elif states_summary.get(MachineState.ERROR.value, 0) > 0:
             print("\nNo specific error codes reported, but machines are in error state.")
        else:
            print("\nNo errors reported.")

        print("="*50 + "\n")
