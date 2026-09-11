#!/usr/bin/env python3
"""Compatibility entrypoint for the current cabinet party-set report.

The original V5 frozen-prose report and Org map are preserved historical audit
products. The current report must never restore their period-based sample or
assert that the active manuscript is frozen.
"""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent/'Processing/decomposition'))
from cabinet_party_set_report import main
if __name__=='__main__':main()
