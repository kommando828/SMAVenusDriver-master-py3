#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
bms_state_machine.py: 
State machine to control the charge profile of the SMA SunnyIsland inverters with Victron Venus OS.
"""

__author__      = "github usernames: jaedog"
__copyright__   = "Copyright 2020"
__license__     = "MIT"
__version__     = "0.1"

import logging
from statemachine import StateMachine, State
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
# Uncomment for debugging:
# logger.setLevel(logging.INFO)


class BMSChargeStateMachine(StateMachine):
    idle = State("Idle", initial=True)
    bulk_charge = State("ConstCurCharge")
    absorption_charge = State("ConstVoltCharge")
    float_charge = State("FloatCharge")
    canceled = State("CancelCharge")

    # Define transitions
    bulk = idle.to(bulk_charge)
    absorb = bulk_charge.to(absorption_charge)
    floating = absorption_charge.to(float_charge)
    rebulk = bulk_charge.from_(absorption_charge, float_charge)
    cancel = canceled.from_(bulk_charge, absorption_charge, float_charge)

    # "cycle" is a union of the main charge states
    cycle = bulk | absorb | floating

    def on_enter_idle(self):
        if hasattr(self.model, "on_enter_idle"):
            self.model.on_enter_idle()

    def on_enter_bulk_charge(self):
        if hasattr(self.model, "on_enter_bulk_charge"):
            self.model.on_enter_bulk_charge()

    def on_enter_absorption_charge(self):
        if hasattr(self.model, "on_enter_absorption_charge"):
            self.model.on_enter_absorption_charge()

    def on_enter_float_charge(self):
        if hasattr(self.model, "on_enter_float_charge"):
            self.model.on_enter_float_charge()


class BMSChargeModel:
    def __init__(self, bulk_current, absorb_voltage, float_voltage, min_absorb_time, rebulk_voltage):
        # Configuration parameters
        self.bulk_current = bulk_current
        self.absorb_voltage = absorb_voltage
        self.float_voltage = float_voltage
        self.min_absorb_time = min_absorb_time  # in minutes
        self.rebulk_voltage = rebulk_voltage

        self.original_bulk_current = bulk_current

        # Dynamic measurements
        self.actual_voltage = 0.0
        self.actual_current = 0.0
        self.set_current = 0.0

        # PD loop variables
        self.last_error = 0.0
        self.last_voltage = 0.0

        # State callback pointer – defaults to idle check
        self.check_state = self.check_idle_state
        self.state_changed = False

    # State entry callbacks
    def on_enter_idle(self):
        self.check_state = self.check_idle_state

    def on_enter_bulk_charge(self):
        self.check_state = self.check_bulk_charge_state

    def on_enter_absorption_charge(self):
        self.check_state = self.check_absorption_charge_state
        self.start_absorb_time = datetime.now()

    def on_enter_float_charge(self):
        self.check_state = self.check_float_charge_state

    # Method to update battery data (voltage, current)
    def update_battery_data(self, voltage, current):
        self.actual_voltage = round(voltage, 2)
        self.actual_current = round(current, 1)

    def update_state(self):
        """Calls the current state's check function and returns its value."""
        return self.check_state()

    # State logic methods
    def check_idle_state(self):
        # In idle state, no automatic transitions
        return 0

    def check_bulk_charge_state(self):
        self.set_current = self.bulk_current
        if self.actual_voltage >= self.absorb_voltage:
            self.last_voltage = self.actual_voltage
            return 1  # Trigger transition to absorption charge state
        return 0

    def compute_pd_adjustment(self, target_voltage):
        """Compute the PD-loop adjustment for set_current."""
        # Control constants
        P = 100.0
        D = 20.0
        error = target_voltage - self.actual_voltage
        adjustment = P * error + D * (error - self.last_error)
        logger.info(f"PD Loop: Error={error:.2f}, Last Error={self.last_error:.2f}, Adjustment={adjustment:.2f}")
        self.last_error = error
        return adjustment

    def do_current_logic(self, target_voltage):
        # Adjust set_current using PD loop
        # If set_current is higher than actual current, start with actual current
        if self.set_current > self.actual_current:
            self.set_current = self.actual_current

        adjustment = self.compute_pd_adjustment(target_voltage)
        self.set_current += adjustment

        # Enforce a minimum charge current threshold
        if self.set_current < 0.6:
            self.set_current = 0.6

        # Cap the charge current to the bulk current limit
        if self.set_current > self.bulk_current:
            self.set_current = self.bulk_current

        self.set_current = round(self.set_current, 1)
        logger.info(f"Actual Current: {self.actual_current:.1f}A, Set Current: {self.set_current:.1f}A, "
                    f"Last Voltage: {self.last_voltage:.2f}V, Actual Voltage: {self.actual_voltage:.2f}V")
        self.last_voltage = self.actual_voltage

    def check_absorption_charge_state(self):
        # On first entry after state change, do nothing
        if self.state_changed:
            return 0

        # If voltage falls below rebulk threshold, signal transition to bulk
        if self.actual_voltage < self.rebulk_voltage:
            return -1

        # If minimum absorption time has passed, signal transition to float charge state
        if datetime.now() - self.start_absorb_time > timedelta(minutes=self.min_absorb_time):
            return 1

        self.do_current_logic(self.absorb_voltage)
        return 0

    def check_float_charge_state(self):
        # If voltage falls below rebulk threshold, signal transition back to bulk
        if self.actual_voltage < self.rebulk_voltage:
            return -1

        self.do_current_logic(self.float_voltage)
        return 0


class BMSChargeController:
    def __init__(self, bulk_current, absorb_voltage, float_voltage, min_absorb_time, rebulk_voltage):
        self.model = BMSChargeModel(bulk_current, absorb_voltage, float_voltage, min_absorb_time, rebulk_voltage)
        self.state_machine = BMSChargeStateMachine(self.model)

    def __str__(self):
        return (f"BMS Charge Config, Bulk Current: {self.model.bulk_current}A, "
                f"Absorb Voltage: {self.model.absorb_voltage}V, "
                f"Min Absorb Time: {self.model.min_absorb_time} min, "
                f"Float Voltage: {self.model.float_voltage}V")

    def update_battery_data(self, voltage, current):
        self.model.update_battery_data(voltage, current)
        return self.check_state()

    def update_required_bulk_current(self, current):
        if current is None:
            self.model.bulk_current = self.model.original_bulk_current
        else:
            self.model.bulk_current = current

    def start_charging(self):
        if self.state_machine.current_state == self.state_machine.idle:
            self.state_machine.cycle()
            return True
        return False

    def is_charging(self):
        cs = self.state_machine.current_state
        return cs in (self.state_machine.bulk_charge, 
                      self.state_machine.absorption_charge, 
                      self.state_machine.float_charge)

    def stop_charging(self):
        print("stop_charging")
        self.state_machine.cancel()

    def check_state(self):
        # Flag that a state change check is in progress
        self.model.state_changed = True

        val = self.model.update_state()
        if val == 0:
            self.model.state_changed = False
        elif val == 1:
            self.state_machine.cycle()  # transition to next state
        elif val == -1:
            self.state_machine.rebulk()  # transition to bulk state

        return val

    def get_charge_current(self):
        return self.model.set_current

    def get_state(self):
        return self.state_machine.current_state.value
