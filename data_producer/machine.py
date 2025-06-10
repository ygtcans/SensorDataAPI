# machine.py
# This file defines the Machine class, which simulates the behavior and data generation
# of an industrial machine. It manages state transitions, error occurrences,
# and sensor data generation based on the machine's current state and configuration.

import random
from datetime import datetime, timezone
from typing import Dict, Tuple
from data_producer.models import MachineState, ProductType, ERROR_CODES # Assuming models.py is in a 'data_producer' package

class Machine:
    """
    Simulates an industrial machine, including its state, performance metrics,
    and sensor data generation.
    """
    def __init__(self, machine_id: str, product_type: ProductType,
                 temp_range: Tuple[float, float], pressure_range: Tuple[float, float],
                 energy_profile: Dict[str, float], max_vibration: float = 0.7):
        """
        Initializes a new Machine instance.

        :param machine_id: Unique identifier for the machine.
        :param product_type: The type of product this machine is configured to produce.
        :param temp_range: A tuple (min_temp, max_temp) defining the operational temperature range.
        :param pressure_range: A tuple (min_pressure, max_pressure) defining the operational pressure range.
        :param energy_profile: A dictionary mapping product types (as strings) to energy consumption multipliers.
        :param max_vibration: The maximum allowable vibration level before it might indicate an issue.
        """
        self.machine_id = machine_id  # Unique identifier for this machine instance.
        self.product_type = product_type  # Type of product this machine processes.
        self.temp_range = temp_range  # Optimal operating temperature range (min, max).
        self.pressure_range = pressure_range  # Optimal operating pressure range (min, max).
        # Energy multiplier specific to the product type, defaults to 1.0 if not in profile.
        self.energy_multiplier = energy_profile.get(product_type.value, 1.0)
        self.max_vibration = max_vibration  # Maximum normal vibration level.

        # Machine state and history attributes
        self.current_state = MachineState.IDLE  # Initial state of the machine.
        self.last_state_change = datetime.now(timezone.utc)  # Timestamp of the last state change.
        self.last_update_time = datetime.now(timezone.utc)  # Initialize last update time
        self.error_code = None  # Current error code, if any.
        self.error_description = None  # Description corresponding to the current error_code.

        # Performance metrics
        self.uptime_hours = 0  # Cumulative uptime in hours since the last maintenance.
        self.maintenance_cycle = random.randint(200, 400)  # Hours of uptime before maintenance is typically needed.
        self.total_runtime = 0 # Total runtime of the machine (could be used for overall wear and tear tracking)

        # State probabilities (intended for a general, realistic distribution, not directly used for transitions here)
        # Note: The actual state transitions are governed by the transition_matrix in _calculate_next_state.
        self.state_probabilities = {
            MachineState.ACTIVE: 0.70,      # Target probability for being in ACTIVE state.
            MachineState.IDLE: 0.20,        # Target probability for being in IDLE state.
            MachineState.MAINTENANCE: 0.07, # Target probability for being in MAINTENANCE state.
            MachineState.ERROR: 0.03        # Target probability for being in ERROR state.
        }

        # Minimum state durations in minutes to prevent rapid state flapping.
        self.min_state_duration = {
            MachineState.ACTIVE: 15,        # Minimum duration for ACTIVE state.
            MachineState.IDLE: 5,           # Minimum duration for IDLE state.
            MachineState.MAINTENANCE: 60,   # Minimum duration for MAINTENANCE state.
            MachineState.ERROR: 10          # Minimum duration for ERROR state.
        }

    def _should_change_state(self, current_time: datetime) -> bool:
        """
        Determines if the machine should consider changing its state based on minimum duration and a time-increasing probability.

        :param current_time: The current UTC datetime.
        :return: True if a state change should be considered, False otherwise.
        """
        # Calculate time elapsed in the current state, in minutes.
        time_in_state = (current_time - self.last_state_change).total_seconds() / 60
        min_duration = self.min_state_duration[self.current_state]

        # Ensure the machine stays in the current state for at least its minimum duration.
        if time_in_state < min_duration:
            return False

        # The probability of changing state increases with time spent in the current state, capped at 10%.
        # This introduces a soft time-out mechanism for states.
        change_probability = min(0.1, time_in_state / 1000) # Example: after 1000 mins (16.6hrs), prob reaches 0.1 if not capped.
        return random.random() < change_probability

    def _calculate_next_state(self, shift: str) -> MachineState:
        """
        Calculates the next potential state of the machine based on the current state,
        operational shift, and predefined transition probabilities.

        :param shift: A string indicating the current work shift (e.g., "day", "evening", "night").
        :return: The calculated next MachineState.
        """
        # Shift-based adjustments to likelihood of certain states.
        shift_modifiers = {
            "day": {"active": 1.2, "maintenance": 1.5},      # Day shift: more active, higher chance of scheduled maintenance.
            "evening": {"active": 1.0, "maintenance": 0.8},  # Evening shift: baseline activity.
            "night": {"active": 0.6, "maintenance": 0.3}     # Night shift: less active, lower chance of maintenance.
        }

        # Check if maintenance is due based on uptime.
        if self.uptime_hours > self.maintenance_cycle:
            if random.random() < 0.3:  # 30% chance to transition to MAINTENANCE if due.
                return MachineState.MAINTENANCE

        # Transition probabilities matrix: P(next_state | current_state)
        # Defines the likelihood of moving from the current_state to various other states.
        transition_matrix = {
            MachineState.ACTIVE: {
                MachineState.ACTIVE: 0.85,
                MachineState.IDLE: 0.10,
                MachineState.ERROR: 0.03,
                MachineState.MAINTENANCE: 0.02
            },
            MachineState.IDLE: {
                MachineState.ACTIVE: 0.70,
                MachineState.IDLE: 0.25,
                MachineState.ERROR: 0.02,
                MachineState.MAINTENANCE: 0.03
            },
            MachineState.ERROR: { # From ERROR, higher chance to go to MAINTENANCE or attempt ACTIVE
                MachineState.ACTIVE: 0.60,
                MachineState.IDLE: 0.20,
                MachineState.ERROR: 0.05, # Chance to remain in error if not resolved
                MachineState.MAINTENANCE: 0.15
            },
            MachineState.MAINTENANCE: { # After MAINTENANCE, usually goes to ACTIVE or IDLE
                MachineState.ACTIVE: 0.70,
                MachineState.IDLE: 0.25,
                MachineState.ERROR: 0.02, # Small chance of error post-maintenance
                MachineState.MAINTENANCE: 0.03 # Small chance to stay in maintenance (e.g. extended work)
            }
        }

        # Apply shift modifications to the transition probabilities.
        probabilities = transition_matrix[self.current_state].copy() # Get base probabilities for the current state.
        modifier = shift_modifiers.get(shift, {}) # Get modifiers for the current shift.

        if "active" in modifier and MachineState.ACTIVE in probabilities:
            probabilities[MachineState.ACTIVE] *= modifier["active"]
        if "maintenance" in modifier and MachineState.MAINTENANCE in probabilities:
            probabilities[MachineState.MAINTENANCE] *= modifier["maintenance"]

        # Normalize probabilities to ensure they sum to 1 after modification.
        total_prob = sum(probabilities.values())
        if total_prob == 0: # Avoid division by zero if all probabilities become zero (edge case)
             return self.current_state # Stay in current state if no valid transitions
        probabilities = {state: prob / total_prob for state, prob in probabilities.items()}

        # Make a weighted random choice for the next state based on the (modified) probabilities.
        rand_val = random.random()
        cumulative_prob = 0
        for state, prob in probabilities.items():
            cumulative_prob += prob
            if rand_val <= cumulative_prob:
                return state

        return self.current_state # Fallback, should ideally be covered by normalization and choice.

    def update_state(self, current_time: datetime, shift: str):
        """
        Updates the machine's current state if conditions for a state change are met.
        Also handles associated logic like resetting uptime after maintenance or assigning error codes.

        :param current_time: The current UTC datetime.
        :param shift: A string indicating the current work shift.
        """
        if self._should_change_state(current_time):
            new_state = self._calculate_next_state(shift)

            if new_state != self.current_state:
                # If the machine was in MAINTENANCE and is now changing state,
                # reset uptime and set a new maintenance cycle.
                if self.current_state == MachineState.MAINTENANCE:
                    self.uptime_hours = 0
                    self.maintenance_cycle = random.randint(200, 400) # Reset maintenance interval

                self.current_state = new_state
                self.last_state_change = current_time

                # If the new state is ERROR, assign a specific error code.
                if new_state == MachineState.ERROR:
                    self._assign_error_code()
                else:
                    # Clear error codes if not in ERROR state.
                    self.error_code = None
                    self.error_description = None

        # Update uptime if the machine is currently in ACTIVE state.
        time_elapsed_hours = (current_time - self.last_update_time).total_seconds() / 3600
        if self.current_state == MachineState.ACTIVE:
            self.uptime_hours += time_elapsed_hours
            self.total_runtime += time_elapsed_hours
        self.last_update_time = current_time

    def _assign_error_code(self):
        """
        Assigns a realistic error code to the machine when it enters an ERROR state.
        The likelihood of specific errors can depend on the product type being processed.
        """
        # Error probabilities weighted by product type.
        # This simulates certain products being more prone to specific types of failures.
        product_error_weights = {
            ProductType.POLYETHYLENE.value: {"E101": 0.4, "E102": 0.3, "E103": 0.2, "E104": 0.1, "E105": 0.0}, # Made E105 0 for example
            ProductType.PVC.value: {"E101": 0.5, "E102": 0.2, "E103": 0.2, "E105": 0.1, "E104": 0.0},
            ProductType.POLYPROPYLENE.value: {"E102": 0.4, "E104": 0.3, "E101": 0.2, "E103": 0.1, "E105": 0.0},
            ProductType.POLYSTYRENE.value: {"E104": 0.4, "E102": 0.3, "E101": 0.2, "E105": 0.1, "E103": 0.0},
            ProductType.ABS.value: {"E103": 0.4, "E104": 0.3, "E101": 0.2, "E102": 0.1, "E105": 0.0}
        }

        # Default weights if the product type is not specifically listed or if error codes are missing.
        default_weights = {"E101": 0.3, "E102": 0.3, "E103": 0.2, "E104": 0.1, "E105": 0.1}
        specific_weights = product_error_weights.get(self.product_type.value, default_weights)
        
        # Ensure all global ERROR_CODES keys are present in weights, assign 0 if missing
        weights_for_choice = {code: specific_weights.get(code, 0.0) for code in ERROR_CODES.keys()}

        # Filter out errors with zero probability to avoid issues with random.choices if all are zero
        possible_errors = {code: weight for code, weight in weights_for_choice.items() if weight > 0}
        if not possible_errors: # If all error weights are zero for this product
            # Fallback to default_weights or a generic error
            possible_errors = {code: weight for code, weight in default_weights.items() if weight > 0}
            if not possible_errors: # If even default weights are all zero (should not happen with current setup)
                 self.error_code = list(ERROR_CODES.keys())[0] # Assign first available error
                 self.error_description = ERROR_CODES[self.error_code]
                 return

        # Choose an error code based on the defined weights.
        self.error_code = random.choices(list(possible_errors.keys()),
                                         weights=list(possible_errors.values()))[0]
        self.error_description = ERROR_CODES[self.error_code]


    def generate_sensor_data(self, timestamp: datetime) -> dict:
        """
        Generates a dictionary of simulated sensor data based on the machine's current state.

        :param timestamp: The UTC datetime for which to generate the sensor data.
        :return: A dictionary containing various sensor readings and machine status information.
        """
        # Calculate base temperature and pressure from the middle of their defined ranges.
        base_temp = (self.temp_range[0] + self.temp_range[1]) / 2
        base_pressure = (self.pressure_range[0] + self.pressure_range[1]) / 2

        # Initialize sensor value variations based on the current machine state.
        temp_variation = 0.0
        pressure_variation = 0.0
        energy = 0.0
        vibration = 0.0
        production_rate = 0

        # State-based modifications to sensor values
        if self.current_state == MachineState.ACTIVE:
            temp_variation = random.uniform(-5, 10)      # Temperature slightly fluctuates around optimal.
            pressure_variation = random.uniform(-0.05, 0.1) # Pressure slightly fluctuates.
            energy = random.uniform(0.8, 1.2) * self.energy_multiplier # Normal energy consumption.
            vibration = random.uniform(0.1, 0.4)     # Normal vibration levels.
            production_rate = random.randint(15, 25) # Normal production rate.

        elif self.current_state == MachineState.IDLE:
            temp_variation = random.uniform(-10, -5)     # Temperature drops when idle.
            pressure_variation = random.uniform(-0.1, -0.05) # Pressure drops when idle.
            energy = random.uniform(0.1, 0.3) * self.energy_multiplier # Low energy consumption.
            vibration = random.uniform(0.01, 0.1)    # Minimal vibration.
            production_rate = 0                      # No production.

        elif self.current_state == MachineState.MAINTENANCE:
            temp_variation = random.uniform(-15, -10)    # Temperature significantly lower (cool down).
            pressure_variation = random.uniform(-0.15, -0.1) # Pressure significantly lower.
            energy = random.uniform(0.05, 0.2) * self.energy_multiplier # Very low energy, for tools or diagnostics.
            vibration = random.uniform(0.0, 0.05)   # Almost no vibration.
            production_rate = 0                     # No production.

        else:  # ERROR state
            # Generate abnormal sensor values based on the specific error code.
            if self.error_code == "E101":  # Overtemperature
                temp_variation = random.uniform(15, 25) # Significantly higher temperature.
                pressure_variation = random.uniform(-0.05, 0.05) # Pressure might be normal or slightly off.
            elif self.error_code == "E102":  # Pressure drop
                temp_variation = random.uniform(-5, 5) # Temperature might be normal.
                pressure_variation = random.uniform(-0.3, -0.15) # Significant pressure drop.
            elif self.error_code == "E103":  # Energy spike
                temp_variation = random.uniform(5, 15) # Temperature might rise due to energy issue.
                pressure_variation = random.uniform(0.05, 0.15) # Pressure might rise.
            elif self.error_code == "E104":  # Vibration anomaly
                temp_variation = random.uniform(-5, 5) # Temperature might be normal.
                pressure_variation = random.uniform(-0.05, 0.05) # Pressure might be normal.
            else:  # E105 - Cooling failure (or any other unspecified error)
                temp_variation = random.uniform(10, 20) # Temperature rises due to cooling failure.
                pressure_variation = random.uniform(-0.1, 0.1) # Pressure might fluctuate.

            # General error state values
            energy = random.uniform(0.3, 1.5) * self.energy_multiplier # Energy consumption can be erratic.
            # Vibration is high if it's a vibration error, otherwise moderately high.
            vibration = random.uniform(0.5, self.max_vibration) if self.error_code == "E104" else random.uniform(0.1, 0.4)
            production_rate = random.randint(0, 10) # Production severely impacted or stopped.

        # Calculate final sensor values, ensuring they are not negative.
        temperature = max(0, base_temp + temp_variation)
        pressure = max(0, base_pressure + pressure_variation)

        # Construct the complete sensor data payload with all required fields.
        # This structure is important for consumers of this data (e.g., APIs, databases).
        data = {
            "timestamp": timestamp.isoformat(),              # ISO 8601 formatted timestamp.
            "machine_id": self.machine_id,                   # Identifier of the machine.
            "state": self.current_state.value,               # Current operational state.
            "temperature": round(temperature, 2),            # Current temperature in Celsius.
            "pressure": round(pressure, 3),                  # Current pressure in Bar or PSI.
            "energy_consumption": round(energy, 3),          # Current energy consumption in kWh or similar unit.
            "vibration": round(vibration, 3),                # Current vibration level (e.g., in g or mm/s).
            "humidity": round(random.uniform(45, 65), 2),    # Ambient humidity near the machine (%).
            "production_rate": production_rate,              # Units produced per minute or hour.
            "raw_material_quality": round(random.uniform(0.7, 1.0), 2), # Quality score of current raw material.
            "operator_override": random.random() < 0.05,     # Boolean, 5% chance of manual operator intervention.
            "cooling_status": "FAIL" if self.error_code == "E105" else "OK", # Status of the cooling system.
            "product_type": self.product_type.value,         # Type of product being processed.
            "uptime_hours": round(self.uptime_hours, 1),     # Machine uptime since last maintenance.
            # Always include error fields for a consistent API response structure, even if null.
            "error_code": self.error_code if self.current_state == MachineState.ERROR else None,
            "error_description": self.error_description if self.current_state == MachineState.ERROR else None
        }

        return data