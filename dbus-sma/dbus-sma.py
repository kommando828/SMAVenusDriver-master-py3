#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dbus-sma.py: Driver to integrate SMA SunnyIsland inverters
with Victron Venus OS.

Authors: github usernames: madsci1016, jaedog
Copyright 2020
License: MIT
Version: 1.1
"""

import os
import signal
import sys
import argparse
import socket
import logging
import yaml
import time
from datetime import datetime, timedelta
from timeit import default_timer as timer

# Third-party and DBus imports
import serial
import can
from can.bus import BusState
from dbus.mainloop.glib import DBusGMainLoop
import dbus

# import gobject -> Replaced in py3, but the new import also depends on the pyobject version installed.
#from gi.repository import GObject as gobject
from gi.repository import GLib

# Victron packages (from velib_python)
script_path = os.path.abspath(__file__) if '__file__' in globals() else os.getcwd()
sys.path.insert(1, os.path.join(os.path.dirname(script_path), 'ext', 'velib_python'))
from vedbus import VeDbusService
from ve_utils import get_vrm_portal_id, exit_on_error
from dbusmonitor import DbusMonitor
from settingsdevice import SettingsDevice

# Import BMS state machine classes
from bms_state_machine import BMSChargeStateMachine, BMSChargeModel, BMSChargeController

# Ignore terminal resize signals to prevent exceptions
signal.signal(signal.SIGWINCH, signal.SIG_IGN)

# Set software version and logger
softwareVersion = '1.1'
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# CAN bus configuration: adjust channel and type as needed.
canBusChannel = "can5"
canBusType = "socketcan"

# Driver information for DBus registration.
driver = {
    'name': "SMA SunnyIsland",
    'servicename': "smasunnyisland",
    'instance': 261,
    'id': 2754,
    'version': 476,
    'serial': "SMABillConnect",
    'connection': "com.victronenergy.vebus.smasunnyisland"
}

# CAN message and frame constants.
CAN_tx_msg = {
    "BatChg": 0x351,
    "BatSoC": 0x355,
    "BatVoltageCurrent": 0x356,
    "AlarmWarning": 0x35a,
    "BMSOem": 0x35e,
    "BatData": 0x35f
}
CANFrames = {
    "ExtPwr": 0x300,
    "InvPwr": 0x301,
    "OutputVoltage": 0x304,
    "Battery": 0x305,
    "Relay": 0x306,
    "Bits": 0x307,
    "LoadPwr": 0x308,
    "ExtVoltage": 0x309
}

# Global dictionaries for SMA readings.
sma_line1 = {"OutputVoltage": 0, "ExtPwr": 0, "InvPwr": 0, "ExtVoltage": 0, "ExtFreq": 0.00, "OutputFreq": 0.00}
sma_line2 = {"OutputVoltage": 0, "ExtPwr": 0, "InvPwr": 0, "ExtVoltage": 0}
sma_battery = {"Voltage": 0, "Current": 0}
sma_system = {"State": 0, "ExtRelay": 0, "ExtOk": 0, "Load": 0}

# Command packets to turn SMA on or off.
SMA_ON_MSG = can.Message(
    arbitration_id=0x35C,
    data=[0b00000001, 0, 0, 0],
    is_extended_id=False
)
SMA_OFF_MSG = can.Message(
    arbitration_id=0x35C,
    data=[0b00000010, 0, 0, 0],
    is_extended_id=False
)


def get_signed_number(number: int, bit_length: int) -> int:
    """
    Return the signed integer representation of 'number' assuming it is of 'bit_length' bits.
    """
    mask = (2 ** bit_length) - 1
    if number & (1 << (bit_length - 1)):
        return number | ~mask
    else:
        return number & mask


def int_to_bytes(integer: int) -> (int, int):
    """
    Convert an integer into a tuple of two bytes (high, low).
    """
    high, low = divmod(integer, 0x100)
    return high, low


class BMSData:
    """
    BMSData holds configuration and dynamic values for the battery management system.
    """
    def __init__(self, max_battery_voltage, min_battery_voltage, low_battery_voltage,
                 charge_bulk_amps, max_discharge_amps, charge_absorb_voltage, charge_float_voltage,
                 time_min_absorb, rebulk_voltage):
        self.max_battery_voltage = max_battery_voltage
        self.min_battery_voltage = min_battery_voltage
        self.low_battery_voltage = low_battery_voltage
        self.charge_bulk_amps = charge_bulk_amps
        self.max_discharge_amps = max_discharge_amps
        self.charge_absorb_voltage = charge_absorb_voltage
        self.charge_float_voltage = charge_float_voltage
        self.time_min_absorb = time_min_absorb
        self.rebulk_voltage = rebulk_voltage

        # Dynamic values
        self.charging_state = ""  # BMS charge state machine state
        self.state_of_charge = 42.0  # initial SoC
        self.actual_battery_voltage = 0.0
        self.req_discharge_amps = max_discharge_amps
        self.battery_current = 0.0
        self.pv_current = 0.0

    def __str__(self):
        return ("BMS Data, MaxV: {}V, MinV: {}V, LowV: {}V, BulkA: {}A, AbsorbV: {}V, FloatV: {}V, "
                "MinuteAbsorb: {}, RebulkV: {}V").format(
            self.max_battery_voltage, self.min_battery_voltage, self.low_battery_voltage,
            self.charge_bulk_amps, self.charge_absorb_voltage, self.charge_float_voltage,
            self.time_min_absorb, self.rebulk_voltage)


class SmaDriver:
    """
    SmaDriver integrates SMA SunnyIsland inverters with Victron Venus OS via DBus and CAN bus.
    """
    def __init__(self):
        self.driver_start_time = datetime.now()
        self._cfg = self.get_config_data()
        cfg_bms = self._cfg['BMSData']

        self._bms_data = BMSData(
            max_battery_voltage=cfg_bms['max_battery_voltage'],
            min_battery_voltage=cfg_bms['min_battery_voltage'],
            low_battery_voltage=cfg_bms['low_battery_voltage'],
            charge_bulk_amps=cfg_bms['charge_bulk_amps'],
            max_discharge_amps=cfg_bms['max_discharge_amps'],
            charge_absorb_voltage=cfg_bms['charge_absorb_voltage'],
            charge_float_voltage=cfg_bms['charge_float_voltage'],
            time_min_absorb=cfg_bms['time_min_absorb'],
            rebulk_voltage=cfg_bms['rebulk_voltage']
        )

        self.bms_controller = BMSChargeController(
            charge_bulk_current=self._bms_data.charge_bulk_amps,
            charge_absorb_voltage=self._bms_data.charge_absorb_voltage,
            charge_float_voltage=self._bms_data.charge_float_voltage,
            time_min_absorb=self._bms_data.time_min_absorb,
            rebulk_voltage=self._bms_data.rebulk_voltage
        )
        ret = self.bms_controller.start_charging()

        # Set up the DBus main loop
        DBusGMainLoop(set_as_default=True)

        self._can_bus = None
        self._safety_off = False  # flag for safety shutdown due to low battery

        logger.debug("Initializing CAN bus...")
        try:
            #self._can_bus = can.interface.Bus(bustype=canBusType, channel=canBusChannel, bitrate=500000)
            self._can_bus = can.Bus(interface="socketcan", channel=canBusChannel, bitrate=500000)
        except can.CanError as e:
            logger.error(e)

        logger.debug("CAN bus initialization complete.")

        # Initialize DBus settings using SettingsDevice.
        self._init_dbus_settings()

        # Dummy DBus tree for DbusMonitor (can be removed once DbusMonitor is more generic)
        dummy = {'code': None, 'whenToLog': 'configChange', 'accessLevel': None}
        dbus_tree = {'com.victronenergy.system': {
            '/Dc/Battery/Soc': dummy,
            '/Dc/Battery/Current': dummy,
            '/Dc/Battery/Voltage': dummy,
            '/Dc/Pv/Current': dummy,
            '/Ac/PvOnOutput/L1/Power': dummy,
            '/Ac/PvOnOutput/L2/Power': dummy,
        }}

        self._dbusmonitor = self._create_dbus_monitor(dbus_tree, valueChangedCallback=self._dbus_value_changed)
        self._dbusservice = self._create_dbus_service()
        self._setup_dbus_paths()

        self._changed = True

        # Create timers (in milliseconds) for CAN transmit, energy update, and CAN parsing.
        GLib.timeout_add(2000, exit_on_error, self._can_bus_txmit_handler)
        GLib.timeout_add(2000, exit_on_error, self._energy_handler)
        GLib.timeout_add(20, exit_on_error, self._parse_can_data_handler)

    def __del__(self):
        if self._can_bus:
            self._can_bus.shutdown()
            self._can_bus = None
            logger.debug("CAN bus shutdown.")

    def run(self):
        """Start and run the main loop."""
        logger.info("Starting mainloop; responding to events only.")
        #self._mainloop = gobject.MainLoop()
        self._mainloop = GLib.MainLoop()
        try:
            self._mainloop.run()
        except KeyboardInterrupt:
            self._mainloop.quit()

    def _create_dbus_monitor(self, *args, **kwargs):
        return DbusMonitor(*args, **kwargs)

    def _create_dbus_service(self):
        dbusservice = VeDbusService(driver['connection'])
        dbusservice.add_mandatory_paths(
            processname=__file__,
            processversion=softwareVersion,
            connection=driver['connection'],
            deviceinstance=driver['instance'],
            productid=driver['id'],
            productname=driver['name'],
            firmwareversion=driver['version'],
            hardwareversion=driver['version'],
            connected=1
        )
        return dbusservice

    def _init_dbus_settings(self):
        # Add AC input and other settings required by dbus-systemcalc-py.
        self._dbussettings = SettingsDevice(
            bus=dbus.SystemBus(),
            supportedSettings={
                'acinput': ['/Settings/SystemSetup/AcInput1', 1, 0, 0],
                'hub4mode': ['/Settings/CGwacs/Hub4Mode', 3, 0, 0],
                'gridmeter': ['/Settings/CGwacs/RunWithoutGridMeter', 1, 0, 0],
                'acsetpoint': ['/Settings/CGwacs/AcPowerSetPoint', 0, 0, 0],
                'maxchargepwr': ['/Settings/CGwacs/MaxChargePower', 0, 0, 0],
                'maxdischargepwr': ['/Settings/CGwacs/MaxDischargePower', 0, 0, 0],
                'maxchargepercent': ['/Settings/CGwacs/MaxChargePercentage', 0, 0, 0],
                'maxdischargepercent': ['/Settings/CGwacs/MaxDischargePercentage', 0, 0, 0],
                'essMode': ['/Settings/CGwacs/BatteryLife/State', 0, 0, 0],
            },
            eventCallback=None
        )

    def _setup_dbus_paths(self):
        # Add various DBus paths to expose SMA/charge data.
        self._dbusservice.add_path('/Serial', value=12345)
        self._dbusservice.add_path('/State', 0)
        self._dbusservice.add_path('/Mode', 3)
        self._dbusservice.add_path('/Ac/PowerMeasurementType', 0)
        self._dbusservice.add_path('/Hub4/AssistantId', 5)
        self._dbusservice.add_path('/Hub4/DisableCharge', value=0, writeable=True)
        self._dbusservice.add_path('/Hub4/DisableFeedIn', value=0, writeable=True)
        self._dbusservice.add_path('/Hub4/DoNotFeedInOverVoltage', value=0, writeable=True)
        self._dbusservice.add_path('/Hub4/L1/AcPowerSetpoint', value=0, writeable=True)
        self._dbusservice.add_path('/Hub4/L2/AcPowerSetpoint', value=0, writeable=True)
        self._dbusservice.add_path('/Hub4/Sustain', value=0, writeable=True)
        self._dbusservice.add_path('/Hub4/L1/MaxFeedInPower', value=0, writeable=True)
        self._dbusservice.add_path('/Hub4/L2/MaxFeedInPower', value=0, writeable=True)
        # Inverter/charger and system energy paths...
        paths = {
            '/Ac/Out/L1/P': -1, '/Ac/Out/L2/P': -1,
            '/Ac/Out/L1/I': -1, '/Ac/Out/L2/I': -1,
            '/Ac/Out/L1/V': -1, '/Ac/Out/L2/V': -1,
            '/Ac/Out/L1/F': -1, '/Ac/Out/L2/F': -1,
            '/Ac/Out/P': -1,
            '/Ac/ActiveIn/L1/P': -1, '/Ac/ActiveIn/L2/P': -1, '/Ac/ActiveIn/P': -1,
            '/Ac/ActiveIn/L1/V': -1, '/Ac/ActiveIn/L2/V': -1,
            '/Ac/ActiveIn/L1/F': -1, '/Ac/ActiveIn/L2/F': -1,
            '/Ac/ActiveIn/L1/I': -1, '/Ac/ActiveIn/L2/I': -1,
            '/Ac/ActiveIn/Connected': 1,
            '/Ac/ActiveIn/ActiveInput': 0,
            '/VebusError': 0,
            '/Dc/0/Voltage': -1, '/Dc/0/Power': -1, '/Dc/0/Current': -1,
            '/Ac/NumberOfPhases': 2,
            '/Alarms/GridLost': 0,
            '/VebusChargeState': 0,
            '/Energy/GridToDc': 0, '/Energy/GridToAcOut': 0,
            '/Energy/DcToAcOut': 0, '/Energy/AcIn1ToInverter': 0,
            '/Energy/AcIn1ToAcOut': 0, '/Energy/InverterToAcOut': 0,
            '/Energy/Time': timer()
        }
        for path, value in paths.items():
            self._dbusservice.add_path(path, value=value)

    def _dbus_value_changed(self, dbusServiceName, dbusPath, dict, changes, deviceInstance):
        self._changed = True

    def _parse_can_data_handler(self):
        """
        Called every 20 ms. Read CAN messages until a message with one of the desired arbitration IDs is received,
        then update SMA variables.
        """
        try:
            msg = None
            # Read messages until a desired one is found
            while True:
                msg = self._can_bus.recv(1)
                if msg is None:
                    sma_system["State"] = 0
                    logger.info("No message received from Sunny Island")
                    return True
                if msg.arbitration_id in (
                    CANFrames["ExtPwr"], CANFrames["InvPwr"], CANFrames["LoadPwr"],
                    CANFrames["OutputVoltage"], CANFrames["ExtVoltage"],
                    CANFrames["Battery"], CANFrames["Relay"], CANFrames["Bits"]
                ):
                    break

            # Process the received message
            if msg.arbitration_id == CANFrames["ExtPwr"]:
                sma_line1["ExtPwr"] = get_signed_number(msg.data[0] + msg.data[1] * 256, 16) * 100
                sma_line2["ExtPwr"] = get_signed_number(msg.data[2] + msg.data[3] * 256, 16) * 100
            elif msg.arbitration_id == CANFrames["InvPwr"]:
                sma_line1["InvPwr"] = get_signed_number(msg.data[0] + msg.data[1] * 256, 16) * 100
                sma_line2["InvPwr"] = get_signed_number(msg.data[2] + msg.data[3] * 256, 16) * 100
                self._updatedbus()
            elif msg.arbitration_id == CANFrames["LoadPwr"]:
                sma_system["Load"] = get_signed_number(msg.data[0] + msg.data[1] * 256, 16) * 100
                self._updatedbus()
            elif msg.arbitration_id == CANFrames["OutputVoltage"]:
                sma_line1["OutputVoltage"] = float(get_signed_number(msg.data[0] + msg.data[1] * 256, 16)) / 10
                sma_line2["OutputVoltage"] = float(get_signed_number(msg.data[2] + msg.data[3] * 256, 16)) / 10
                sma_line1["OutputFreq"] = float(msg.data[6] + msg.data[7] * 256) / 100
                self._updatedbus()
            elif msg.arbitration_id == CANFrames["ExtVoltage"]:
                sma_line1["ExtVoltage"] = float(get_signed_number(msg.data[0] + msg.data[1] * 256, 16)) / 10
                sma_line2["ExtVoltage"] = float(get_signed_number(msg.data[2] + msg.data[3] * 256, 16)) / 10
                sma_line1["ExtFreq"] = float(msg.data[6] + msg.data[7] * 256) / 100
                self._updatedbus()
            elif msg.arbitration_id == CANFrames["Battery"]:
                sma_battery["Voltage"] = float(msg.data[0] + msg.data[1] * 256) / 10
                sma_battery["Current"] = float(get_signed_number(msg.data[2] + msg.data[3] * 256, 16)) / 10
                self._updatedbus()
            elif msg.arbitration_id == CANFrames["Bits"]:
                sma_system["ExtRelay"] = 1 if msg.data[2] & 128 else 0
                if msg.data[2] & 64:
                    sma_system["ExtOk"] = 0  # Grid OK
                else:
                    # Latch grid down over two messages
                    sma_system["ExtOk"] = 2 if sma_system["ExtOk"] == 1 else 1
        except KeyboardInterrupt:
            self._mainloop.quit()
        except can.CanError as e:
            logger.error(e)
        except Exception as e:
            logger.error("Exception occurred: {}: {}".format(type(e).__name__, e))
        return True

    def _updatedbus(self):
        """
        Update DBus service paths with the latest SMA data.
        """
        self._dbusservice["/Ac/ActiveIn/L1/P"] = sma_line1["ExtPwr"]
        self._dbusservice["/Ac/ActiveIn/L2/P"] = sma_line2["ExtPwr"]
        self._dbusservice["/Ac/ActiveIn/L1/V"] = sma_line1["ExtVoltage"]
        self._dbusservice["/Ac/ActiveIn/L2/V"] = sma_line2["ExtVoltage"]
        self._dbusservice["/Ac/ActiveIn/L1/F"] = sma_line1["ExtFreq"]
        self._dbusservice["/Ac/ActiveIn/L2/F"] = sma_line1["ExtFreq"]
        if sma_system["ExtOk"] in (0, 2):
            self._dbusservice["/Alarms/GridLost"] = sma_system["ExtOk"]

        if sma_line1["ExtVoltage"] != 0:
            self._dbusservice["/Ac/ActiveIn/L1/I"] = int(sma_line1["ExtPwr"] / sma_line1["ExtVoltage"])
        if sma_line2["ExtVoltage"] != 0:
            self._dbusservice["/Ac/ActiveIn/L2/I"] = int(sma_line2["ExtPwr"] / sma_line2["ExtVoltage"])
        self._dbusservice["/Ac/ActiveIn/P"] = sma_line1["ExtPwr"] + sma_line2["ExtPwr"]

        self._dbusservice["/Dc/0/Voltage"] = sma_battery["Voltage"]
        self._dbusservice["/Dc/0/Current"] = -sma_battery["Current"]
        self._dbusservice["/Dc/0/Power"] = -sma_battery["Current"] * sma_battery["Voltage"]

        line1_inv_outpwr = sma_line1["ExtPwr"] + sma_line1["InvPwr"]
        line2_inv_outpwr = sma_line2["ExtPwr"] + sma_line2["InvPwr"]

        # Adjust line power based on reported load to compensate rounding.
        total_inv = line1_inv_outpwr + line2_inv_outpwr
        if sma_system["Load"] == total_inv + 100:
            line1_inv_outpwr += 50
            line2_inv_outpwr += 50
        elif sma_system["Load"] == total_inv - 100:
            line1_inv_outpwr -= 50
            line2_inv_outpwr -= 50

        self._dbusservice["/Ac/Out/L1/P"] = line1_inv_outpwr
        self._dbusservice["/Ac/Out/L2/P"] = line2_inv_outpwr
        self._dbusservice["/Ac/Out/P"] = sma_system["Load"]
        self._dbusservice["/Ac/Out/L1/F"] = sma_line1["OutputFreq"]
        self._dbusservice["/Ac/Out/L2/F"] = sma_line1["OutputFreq"]
        self._dbusservice["/Ac/Out/L1/V"] = sma_line1["OutputVoltage"]
        self._dbusservice["/Ac/Out/L2/V"] = sma_line2["OutputVoltage"]

        inverter_on = 0
        if sma_line1["OutputVoltage"] > 5:
            self._dbusservice["/Ac/Out/L1/I"] = int(line1_inv_outpwr / sma_line1["OutputVoltage"])
            inverter_on += 1
        if sma_line2["OutputVoltage"] > 5:
            self._dbusservice["/Ac/Out/L2/I"] = int(line2_inv_outpwr / sma_line2["OutputVoltage"])
            inverter_on += 1

        if sma_system["ExtRelay"]:
            self._dbusservice["/Ac/ActiveIn/Connected"] = 1
            self._dbusservice["/Ac/ActiveIn/ActiveInput"] = 0
        else:
            self._dbusservice["/Ac/ActiveIn/Connected"] = 0
            self._dbusservice["/Ac/ActiveIn/ActiveInput"] = 240

        # Determine charge state for DBus reporting.
        vebusChargeState = 0
        sma_system["State"] = 0
        if inverter_on > 0:
            sma_system["State"] = 9  # Inverting state
            if self._bms_data.battery_current > 0:
                if self._bms_data.charging_state == "bulk_chg":
                    vebusChargeState = 1
                    sma_system["State"] = 3
                elif self._bms_data.charging_state == "absorb_chg":
                    vebusChargeState = 2
                    sma_system["State"] = 4
                elif self._bms_data.charging_state == "float_chg":
                    vebusChargeState = 3
                    sma_system["State"] = 5

        self._dbusservice["/VebusChargeState"] = vebusChargeState
        self._dbusservice["/State"] = sma_system["State"]

    def _energy_handler(self):
        """Called on a timer to update energy consumption values."""
        energy_sec = timer() - self._dbusservice["/Energy/Time"]
        self._dbusservice["/Energy/Time"] = timer()

        if self._dbusservice["/Dc/0/Power"] > 0:
            # Energy from grid to battery
            self._dbusservice["/Energy/GridToAcOut"] += self._dbusservice["/Ac/Out/P"] * energy_sec * 0.00000028
            self._dbusservice["/Energy/GridToDc"] += self._dbusservice["/Dc/0/Power"] * energy_sec * 0.00000028
        else:
            # Energy from battery to output
            self._dbusservice["/Energy/DcToAcOut"] += self._dbusservice["/Ac/Out/P"] * energy_sec * 0.00000028

        self._dbusservice["/Energy/AcIn1ToAcOut"] = self._dbusservice["/Energy/GridToAcOut"]
        self._dbusservice["/Energy/AcIn1ToInverter"] = self._dbusservice["/Energy/GridToDc"]
        self._dbusservice["/Energy/InverterToAcOut"] = self._dbusservice["/Energy/DcToAcOut"]
        self._dbusservice["/Energy/Time"] = timer()
        return True

    def _execute_grid_solar_charge_logic(self):
        """
        Implements SMA grid logic based on the current time and state-of-charge (SoC).
        Returns the target charge current (amps).
        """
        charge_amps = None
        now = datetime.now()

        if sma_system["ExtRelay"] == 1:
            cfg_grid = self._cfg["GridLogic"]
            cfg_safety = self._cfg["SafetyLogic"]

            if cfg_grid["start_hour"] <= now.hour <= cfg_grid["end_hour"]:
                if now.hour >= cfg_grid["mid_hour"] and self._bms_data.state_of_charge < 49.0:
                    charge_amps = cfg_grid["mid_hour_current"]
                else:
                    charge_amps = cfg_grid["current"]
            else:
                charge_amps = cfg_grid["offtime_current"]

            if self._bms_data.state_of_charge < cfg_safety["after_blackout_min_soc"]:
                charge_amps = cfg_safety["after_blackout_charge_amps"]

            charge_amps -= self._bms_data.pv_current
            if charge_amps < 0.0:
                charge_amps = 0.0

        logger.info("Grid Logic: Time: {}, On Grid: {}, Charge amps: {}"
                    .format(now, sma_system["ExtRelay"], charge_amps))
        return charge_amps

    def _can_bus_txmit_handler(self):
        """
        Called on a two-second timer to send CAN messages.
        Logs status and sends a series of messages to the CAN bus.
        """
        out_load_msg = "SMA: System Load: {}, Driver runtime: {}".format(
            sma_system["Load"], datetime.now() - self.driver_start_time)
        out_ext_msg = ("SMA: External, Line 1: {}V, Line 2: {}V, Line 1 Pwr: {}W, Line 2 Pwr: {}W, Freq: {}"
                       .format(sma_line1["ExtVoltage"], sma_line2["ExtVoltage"],
                               sma_line1["ExtPwr"], sma_line2["ExtPwr"], sma_line1["ExtFreq"]))
        out_inv_msg = ("SMA: Inverter, Line 1: {}V, Line 2: {}V, Line 1 Pwr: {}W, Line 2 Pwr: {}W, Freq: {}"
                       .format(sma_line1["OutputVoltage"], sma_line2["OutputVoltage"],
                               sma_line1["InvPwr"], sma_line2["InvPwr"], sma_line1["OutputFreq"]))
        out_batt_msg = "SMA: Batt Voltage: {}, Batt Current: {}".format(
            sma_battery["Voltage"], sma_battery["Current"])

        logger.info(out_load_msg)
        logger.info(out_ext_msg)
        logger.info(out_inv_msg)
        logger.info(out_batt_msg)

        # Retrieve DBus values
        soc = self._dbusmonitor.get_value('com.victronenergy.system', '/Dc/Battery/Soc')
        volt = self._dbusmonitor.get_value('com.victronenergy.system', '/Dc/Battery/Voltage')
        current = self._dbusmonitor.get_value('com.victronenergy.system', '/Dc/Battery/Current')
        pv_current = self._dbusmonitor.get_value('com.victronenergy.system', '/Dc/Pv/Current') or 0.0

        if soc is None or volt is None:
            logger.error("DBusMonitor returning None for: SOC: {}, Volt: {}, Current: {}, PVCurrent: {}"
                         .format(soc, volt, current, pv_current))
            return True

        # Update BMS data
        self._bms_data.state_of_charge = soc
        self._bms_data.actual_battery_voltage = volt
        self._bms_data.battery_current = current
        self._bms_data.pv_current = pv_current

        # Update requested bulk current based on grid-solar logic.
        self.bms_controller.update_req_bulk_current(self._execute_grid_solar_charge_logic())

        # Update BMS state using the current battery voltage and inverted battery current (since SMA reports inverted)
        is_state_changed = self.bms_controller.update_battery_data(
            self._bms_data.actual_battery_voltage, -sma_battery["Current"]
        )
        self._bms_data.charging_state = self.bms_controller.get_state()
        charge_current = self.bms_controller.get_charge_current()

        logger.info("BMS Send, SoC: {:.1f}%, Batt Voltage: {:.2f}V, Batt Current: {:.2f}A, Charge State: {}, Req Charge: {}A, Req Discharge: {}A, PV Cur: {}"
                    .format(self._bms_data.state_of_charge, self._bms_data.actual_battery_voltage,
                            self._bms_data.battery_current, self._bms_data.charging_state,
                            charge_current, self._bms_data.req_discharge_amps, self._bms_data.pv_current))

        # Low battery safety logic
        cfg_safety = self._cfg["SafetyLogic"]
        if sma_system["ExtOk"] == 0 and self._bms_data.actual_battery_voltage < self._bms_data.low_battery_voltage:
            self._bms_data.state_of_charge = 1.0

        if not self._safety_off:
            if sma_system["ExtOk"] == 2 and soc < cfg_safety["min_soc_inv_off"]:
                self._can_bus.send(SMA_OFF_MSG)
                if sma_system["State"] == 0:
                    self._safety_off = True
        else:
            if sma_system["ExtOk"] == 0 or soc >= cfg_safety["min_soc_inv_off"]:
                self._can_bus.send(SMA_ON_MSG)
                if sma_system["State"] != 0:
                    self._safety_off = False

        # Pack values for CAN messages.
        SoC_HD = int(self._bms_data.state_of_charge * 100)
        SoC_HD_H, SoC_HD_L = int_to_bytes(SoC_HD)
        Req_Charge_H, Req_Charge_L = int_to_bytes(int(charge_current * 10))
        Req_Discharge_H, Req_Discharge_L = int_to_bytes(int(self._bms_data.req_discharge_amps * 10))
        Max_V_H, Max_V_L = int_to_bytes(int(self._bms_data.max_battery_voltage * 10))
        Min_V_H, Min_V_L = int_to_bytes(int(self._bms_data.min_battery_voltage * 10))

        msg = can.Message(
            arbitration_id=CAN_tx_msg["BatChg"],
            data=[Max_V_L, Max_V_H, Req_Charge_L, Req_Charge_H, Req_Discharge_L, Req_Discharge_H, Min_V_L, Min_V_H],
            is_extended_id=False
        )
        msg2 = can.Message(
            arbitration_id=CAN_tx_msg["BatSoC"],
            data=[int(self._bms_data.state_of_charge), 0x00, 0x64, 0x00, SoC_HD_L, SoC_HD_H],
            is_extended_id=False
        )
        msg3 = can.Message(
            arbitration_id=CAN_tx_msg["BatVoltageCurrent"],
            data=[0x00, 0x00, 0x00, 0x00, 0xf0, 0x00],
            is_extended_id=False
        )
        msg4 = can.Message(
            arbitration_id=CAN_tx_msg["AlarmWarning"],
            data=[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            is_extended_id=False
        )
        msg5 = can.Message(
            arbitration_id=CAN_tx_msg["BMSOem"],
            data=[0x42, 0x41, 0x54, 0x52, 0x49, 0x55, 0x4d, 0x20],
            is_extended_id=False
        )
        msg6 = can.Message(
            arbitration_id=CAN_tx_msg["BatData"],
            data=[0x03, 0x04, 0x0a, 0x04, 0x76, 0x02, 0x00, 0x00],
            is_extended_id=False
        )

        try:
            for m in (msg, msg2, msg3, msg4, msg5, msg6):
                while self._can_bus.recv(0.01) is not None:
                    time.sleep(0.01)
                self._can_bus.send(m)
                time.sleep(1)
        except can.CanError as e:
            logger.error("CAN BUS Transmit error (is controller missing?): %s", e)
        except KeyboardInterrupt:
            pass

        return True

    def get_config_data(self):
        """
        Reads and returns configuration data from a YAML file.
        Exits if the file cannot be read.
        """
        try:
            dir_path = os.path.dirname(os.path.realpath(__file__))
            with open(os.path.join(dir_path, "dbus-sma.yaml"), "r") as yamlfile:
                config = yaml.load(yamlfile, Loader=yaml.FullLoader)
                return config
        except Exception:
            logger.info("dbus-sma.yaml file not found or incorrect.")
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=('Driver for SMA SunnyIsland inverters integration '
                     'with Victron Venus OS via D-Bus service.'))
    parser.add_argument('-s', '--serial', help='tty')
    parser.add_argument("-d", "--debug", help="set logging level to debug", action="store_true")

    args = parser.parse_args()

    print(f"-------- dbus_SMADriver, v{softwareVersion} is starting up --------")
    # Uncomment and use a logging setup if desired.
    # logger = setup_logging(args.debug)

    smadriver = SmaDriver()
    smadriver.run()
    # Ensure resources are cleaned up.
    del smadriver

    print(f"-------- dbus_SMADriver, v{softwareVersion} is shutting down --------")
    sys.exit(1)