## Source Files

# Add BBLs to / clean files
application_add_bbl.py
inspection_add_bbl.py
charge_add_bbl.py
# Merge inspection with application with charges ##### ERROR 20 DUPLICATES      # Clean revocation file
insp_app_chrg.py                                                                revocation_clean.py
# Merge inspection/application/charges with fnf                                 # Merge revocation with lob
insp_app_chrg_fnf.py                                                            lob_rev.py


# Merge inspection/application/charges/fnf with lob/revocation
dca_merge.py
# Flatten residual lob and merge with inspection/application/charges/fnf/lob/revocation
dca_merge_lob.py
# inspection/application/charges/fnf/lob/revocation add fnf
dca_merge_lob_fnf.py
# delete lbo col values
dca_final_clean.py

