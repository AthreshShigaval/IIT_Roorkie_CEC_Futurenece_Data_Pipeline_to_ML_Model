"""
include/nammamart/masking.py
----------------------------
PII masking used by the ETL pipeline. include/sql/elt/stg_customers.sql applies the
same rules in SQL so both pipelines publish identical masked values.

  email  priya.hegde199@example.com -> pr***@example.com   (invalid email -> NULL)
  phone  9774349021                 -> 97******21          (fewer than 4 digits -> NULL)
  name   Priya Hegde                -> Priya H.
"""

import re

import pandas as pd

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def mask_email(email):
    if email is None or pd.isna(email) or not EMAIL_RE.match(email):
        return None
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(phone):
    if phone is None or pd.isna(phone):
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 4:
        return None
    return digits[:2] + "*" * (len(digits) - 4) + digits[-2:]


def mask_name(full_name):
    if full_name is None or pd.isna(full_name) or not full_name.strip():
        return None
    parts = full_name.split()
    return parts[0] if len(parts) == 1 else f"{parts[0]} {parts[-1][0]}."
