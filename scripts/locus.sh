#!/bin/bash
# Run src files 
python3 dca/application_src.py
python3 dca/charge_src.py
python3 dca/inspection_src.py
python3 dca/license_src.py
python3 doa/inspection_src.py
python3 doe/pharmacy_src.py
python3 doh/inspection_src.py
python3 doh/license_src.py
python3 dos/license_src.py
python3 dot/application_src.py
python3 dot/inspection_src.py
python3 liq/license_src.py
# Merge
python3 record_linkage.py
# Industry assign
