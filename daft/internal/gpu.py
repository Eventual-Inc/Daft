from __future__ import annotations

import subprocess
import xml.etree.ElementTree as ET


def cuda_device_count():
    """Returns the number of CUDA devices detected by nvidia-smi command"""
    try:
        nvidia_smi_output = subprocess.check_output(["nvidia-smi", "-x", "-q"])
    except Exception:
        return 0
    root = ET.fromstring(nvidia_smi_output.decode("utf-8"))
    attached_gpus = root.find("attached_gpus")
    if attached_gpus is None:
        return 0
    return int(attached_gpus.text)
