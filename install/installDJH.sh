#!/bin/bash
set -euo pipefail

# For testing purposes, you can uncomment the following lines:
# ROOT_DIR="/tmp/venus"
# mkdir -p "${ROOT_DIR}/data/etc"
# mkdir -p "${ROOT_DIR}/var/log"
# mkdir -p "${ROOT_DIR}/service"
# mkdir -p "${ROOT_DIR}/etc/udev/rules.d"
ROOT_DIR=""

echo
echo "Please ensure your socketcan-enabled canable USB adapter is plugged into the Venus."
echo "If your canable.io adapter is still using the factory-default slcan firmware, exit this script and install the 'candlelight' firmware."
echo
echo "This script requires internet access to install dependencies and software."
echo
read -p "Install SMA Sunny Island driver (with virtual BMS) on Venus OS at your own risk? [Y to proceed] " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
  read -p "Install dependencies (pip and Python libraries)? [Y to proceed] " -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "==== Updating package list and installing Python3 dependencies ===="
    opkg update
    opkg install python3-misc python3-distutils python3-numbers python3-html python3-ctypes python3-pkgutil
    opkg install python3-unittest python3-difflib gcc binutils python3-dev python3-unixadmin python3-xmlrpc

    echo "==== Downloading and installing pip for Python3 ===="
    wget https://bootstrap.pypa.io/get-pip.py
    python3 get-pip.py
    rm get-pip.py

    echo "==== Installing required Python packages via pip3 ===="
    pip3 install python-can python-statemachine pyyaml
  fi

  echo "==== Downloading driver and library ===="
  wget -O master.zip https://github.com/madsci1016/SMAVenusDriver/archive/master.zip
  unzip -qo master.zip
  rm master.zip

  echo "==== Installing udev rule for the canable device ===="
  local_udev_dir="${ROOT_DIR}/etc/udev/rules.d"
  mkdir -p "$local_udev_dir"
  template_udev_file="SMAVenusDriver-master/install/99-candlelight.rules"
  udev_file="${local_udev_dir}/99-candlelight.rules"
  cp "$template_udev_file" "$udev_file"

  # Grab the details of the canable USB adapter
  value=$(usb-devices | grep -A2 "canable.io" || true)
  var=$(echo "$value" | cut -d'=' -f2)
  set -- $var
  serial=$4

  error_msg="WARNING: Unable to automatically update 99-candlelight.rules with the device serial number."

  if [ ${#serial} -eq 24 ]; then
    echo "Found canable.io serial number: $serial"
    sed -i "s/000000000000000000000000/$serial/g" "$udev_file"
    if diff "$template_udev_file" "$udev_file" > /dev/null 2>&1; then
      echo "$error_msg"
    fi
  else
    echo "$error_msg"
  fi

  echo "==== Installing SMA SI driver ===="
  DBUS_NAME="dbus-smaDJH"
  DBUS_SMA_DIR="${ROOT_DIR}/data/etc/${DBUS_NAME}"

  mkdir -p "${ROOT_DIR}/var/log/${DBUS_NAME}"
  mkdir -p "${DBUS_SMA_DIR}"
  cp -R SMAVenusDriver-master/dbus-sma/* "${DBUS_SMA_DIR}/"

  # Replace the inverter SVG with a custom yellow Sunny Island version
  cp SMAVenusDriver-master/assets/overview-inverter.svg "${ROOT_DIR}/opt/victronenergy/themes/ccgx/images"

  chmod +x "${DBUS_SMA_DIR}/dbus-smaDJH.py"
  chmod +x "${DBUS_SMA_DIR}/service/run"
  chmod +x "${DBUS_SMA_DIR}/service/log/run"
  ln -s "${ROOT_DIR}/opt/victronenergy/vrmlogger/ext/" "${DBUS_SMA_DIR}/ext"
  ln -s "${DBUS_SMA_DIR}/service" "${ROOT_DIR}/service/${DBUS_NAME}"

  # Clean up the extracted driver files
  rm -rf SMAVenusDriver-master/

  echo
  echo "Installation complete. Please reboot the Venus OS device to finalize the installation."
fi
