# models.py
# This file defines data models and enumerations used throughout the data producer application.
# It includes definitions for machine states, product types, and a mapping of error codes to descriptions.

from enum import Enum

class MachineState(str, Enum):
    """
    Enumeration representing the possible operational states of a machine.
    Inherits from str and Enum to allow string comparisons and enum properties.
    """
    IDLE = "idle"                # Represents the machine being powered on but not actively processing.
    ACTIVE = "active"            # Represents the machine actively processing or producing.
    MAINTENANCE = "maintenance"  # Represents the machine being under scheduled or unscheduled maintenance.
    ERROR = "error"              # Represents the machine being in an error state, unable to function correctly.

class ProductType(str, Enum):
    """
    Enumeration representing the different types of products a machine can produce.
    Inherits from str and Enum for similar reasons as MachineState.
    """
    POLYETHYLENE = "Polyethylene"    # Polyethylene plastic type.
    POLYPROPYLENE = "Polypropylene"  # Polypropylene plastic type.
    PVC = "PVC"                      # Polyvinyl Chloride plastic type.
    POLYSTYRENE = "Polystyrene"      # Polystyrene plastic type.
    ABS = "ABS"                      # Acrylonitrile Butadiene Styrene plastic type.

# Dictionary mapping error codes to human-readable descriptions.
# This provides a centralized way to manage and retrieve error messages.
ERROR_CODES = {
    "E101": "Overtemperature detected",      # Error code for excessive temperature.
    "E102": "Pressure drop detected",        # Error code for a significant drop in pressure.
    "E103": "Energy spike detected",         # Error code for an unexpected surge in energy consumption.
    "E104": "Vibration anomaly detected",    # Error code for abnormal vibration levels.
    "E105": "Cooling failure"                # Error code for a failure in the cooling system.
}