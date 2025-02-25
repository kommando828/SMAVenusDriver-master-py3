#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from bms_state_machine import BMSChargeController

def main():
    # Battery parameters for a 48V 16S LiFePO4 battery.
    # Absorption: 58V (or 56.4V for longer life)
    # Float: 54.4V
    # Restart bulk voltage: Float - 0.8V (max of 54V)
    # Inverter Cut-off: 42.8V-48V (depending on load/voltage drop)
    bms_controller = BMSChargeController(
        charge_bulk_current=160,
        charge_absorb_voltage=58.4,
        charge_float_voltage=54.4,
        time_min_absorb=0.5,  # 0.5 minutes (30 seconds) for simulation
        rebulk_voltage=53.6
    )
    
    # Start the charge cycle (from idle state)
    started = bms_controller.start_charging()
    print(f"{bms_controller}, Start Charging: {started}")

    # Initialize simulated battery voltage and a counter for simulation purposes.
    bat_voltage = 42.8
    counter = 0

    while True:
        # In a real system these would be actual measurements.
        simulated_current = 0.0  
        state_changed = bms_controller.update_battery_data(bat_voltage, simulated_current)
        state = bms_controller.get_state()
        charge_current = bms_controller.get_charge_current()
        
        print(f"Battery Voltage: {bat_voltage:.2f}V, "
              f"Charge Current: {charge_current:.1f}A, "
              f"Charge State: {state}, "
              f"State Changed: {state_changed}")
        
        time.sleep(1)

        # If a state change was triggered, reset voltage to a predetermined value.
        if state_changed:
            if state == "absorb_chg":
                bat_voltage = 58.2
            elif state == "float_chg":
                bat_voltage = 56.1

        # Update the simulated battery voltage based on the current charge state.
        if state == "bulk_chg":
            bat_voltage += 1.8
        elif state == "absorb_chg":
            # In absorption, adjust voltage according to the current and a small drift.
            if charge_current > 0:
                bat_voltage += charge_current * 0.1
            elif charge_current == 0:
                bat_voltage -= 0.01

            counter += 1
            if counter > 15:
                bat_voltage = 54.0  # reset voltage for simulation purposes
                counter = 0
        elif state == "float_chg":
            counter += 1
            if counter > 5:
                bat_voltage = 53.0
            if charge_current > 0:
                bat_voltage += charge_current * 0.1
            elif charge_current == 0:
                bat_voltage -= 0.03

if __name__ == '__main__':
    main()
