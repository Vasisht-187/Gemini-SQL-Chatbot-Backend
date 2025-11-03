import re
import sqlparse


ALLOWED_STATEMENTS = {"SELECT"}


BLACKLIST_TOKENS = {";","--", "/*", "*/", "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "GRANT", "REVOKE", "TRUNCATE", "EXEC", "CALL", "SYSTEM_USER"}

# --- MODIFIED: Added 'CustCustDemographics' ---
ALLOWED_TABLES = {
    "Employee", "EmployeeTerritory", "Product", "Category",
    "Supplier", "Customer", "SalesOrder", "OrderDetail",
    "Region", "Territory", "Shipper", "CustomerDemographics",
    "CustCustDemographics" # <-- Add this table from your SQL file
}

def is_safe_sql(sql_text: str) -> (bool, str):
    sql_norm = sql_text.strip()
    
    if ";" in sql_norm:
        return False, "Semicolons/multiple statements detected."
    
    parsed = sqlparse.parse(sql_norm)
    if not parsed:
        return False, "Unable to parse SQL."
    stmt = parsed[0]
    first = stmt.token_first(skip_cm=True)
    if not first:
        return False, "Empty statement."
    first_value = first.value.upper()
    if first_value not in ALLOWED_STATEMENTS:
        return False, f"Only SELECT statements are allowed. Found: {first_value}"
    
    upper = sql_norm.upper()
    for tok in BLACKLIST_TOKENS:
        if tok in upper:
            return False, f"Disallowed token found: {tok}"
    
    table_names = re.findall(r"\bFROM\s+([`\"]?)(\w+)\1|\bJOIN\s+([`\"]?)(\w+)\3", sql_norm, flags=re.IGNORECASE)
    
    for tup in table_names:
        name = tup[1] or tup[3]
        if name and name not in ALLOWED_TABLES:
            # Add a secondary check for capitalization, just in case
            if name.capitalize() not in ALLOWED_TABLES:
                return False, f"Table not allowed: {name}"
    return True, "ok"