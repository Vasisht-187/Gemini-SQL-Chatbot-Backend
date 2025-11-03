import os
import json
import re
from google import genai

_client = genai.Client()

def _format_history(chat_history):
    """Formats the history for the prompt."""
    if not chat_history:
        return ""
    
    formatted = "\nChat History:\n"
    for item in chat_history:
        role = "User" if item.get("role") == "user" else "Assistant"
        content = item.get("content", "")
        formatted += f"{role}: {content}\n"
    return formatted

# --- We define the exact schema as text for the LLM's context ---
_SCHEMA = """
-- Here is the database schema:
CREATE TABLE Category (
  categoryId INT NOT NULL,
  categoryName VARCHAR(15) NOT NULL,
  description TEXT NULL
);
CREATE TABLE Region (
  regionId INT NOT NULL,
  regiondescription VARCHAR(50) NOT NULL
);
CREATE TABLE Territory (
  territoryId VARCHAR(20) NOT NULL,
  territorydescription VARCHAR(50) NOT NULL,
  regionId INT NOT NULL -- Foreign Key to Region
);
CREATE TABLE CustomerDemographics (
  customerTypeId INT NOT NULL,
  customerDesc TEXT NULL
);
CREATE TABLE Customer (
  custId INT NOT NULL, -- This is the primary key, NOT customerId
  companyName VARCHAR(40) NOT NULL,
  contactName VARCHAR(30) NULL,
  city VARCHAR(15) NULL,
  region VARCHAR(15) NULL,
  country VARCHAR(15) NULL,
  phone VARCHAR(24) NULL
);
CREATE TABLE CustCustDemographics (
  custId INT NOT NULL, -- Foreign Key to Customer
  customerTypeId INT NOT NULL -- Foreign Key to CustomerDemographics
);
CREATE TABLE Employee (
  employeeId INT NOT NULL,
  lastname VARCHAR(20) NOT NULL,
  firstname VARCHAR(10) NOT NULL,
  title VARCHAR(30) NULL,
  city VARCHAR(15) NULL,
  country VARCHAR(15) NULL,
  mgrId INT NULL -- Foreign key to Employee.employeeId
);
CREATE TABLE EmployeeTerritory (
  employeeId  INT NOT NULL, -- Foreign Key to Employee
  territoryId VARCHAR(20) NOT NULL -- Foreign Key to Territory
);
CREATE TABLE Supplier (
  supplierId INT NOT NULL,
  companyName VARCHAR(40) NOT NULL,
  contactName VARCHAR(30) NULL,
  city VARCHAR(15) NULL,
  country VARCHAR(15) NULL
);
CREATE TABLE Product (
  productId INT NOT NULL,
  productName VARCHAR(40) NOT NULL,
  supplierId INT NULL, -- Foreign key to Supplier
  categoryId INT NULL, -- Foreign key to Category
  unitPrice DECIMAL(10, 2) NULL,
  discontinued CHAR(1) NOT NULL
);
CREATE TABLE Shipper (
  shipperId INT NOT NULL,
  companyName VARCHAR(40) NOT NULL
);
CREATE TABLE SalesOrder (
  orderId INT NOT NULL,
  custId INT NOT NULL, -- Foreign key to Customer
  employeeId INT NULL, -- Foreign key to Employee
  orderDate DATETIME NULL,
  shippedDate DATETIME NULL,
  shipperid INT NOT NULL, -- Foreign key to Shipper
  freight DECIMAL(10, 2) NULL,
  shipCity VARCHAR(15) NULL,
  shipCountry VARCHAR(15) NULL
);
CREATE TABLE OrderDetail (
   orderDetailId INT NOT NULL,
   orderId INT NOT NULL, -- Foreign key to SalesOrder
   productId INT NOT NULL, -- Foreign key to Product
   unitPrice DECIMAL(10, 2) NOT NULL,
   quantity SMALLINT NOT NULL,
   discount DECIMAL(10, 2) NOT NULL
);
"""
# --- End of schema block ---


SQL_PROMPT_TEMPLATE = """
You are a SQL-generation assistant that only outputs JSON. 
**You are generating SQL for a MySQL database.**
You are given a database schema to use.

{schema}

Task:
1) Analyze the "Chat History" (if present) and the new "User question" to understand the full request.
2) **Determine if the question is a data request about Northwind OR a general/meta question.**
3) If it's a data request, translate it into a SQL SELECT statement and parameters.
4) If it's a general question or out of scope, set "sql" to null and provide a helpful text response.
5) **Suggest a chart** if the data is suitable for visualization (e.g., aggregations, trends).

Output ONLY valid JSON with the following keys:
- "sql": The SQL query string (or null). Use %s for parameters.
- "params": An array of parameters.
- "explain": A short English explanation for the user OR the conversational reply.
- "chart": An object with "type" and "config".
    - "type": One of "bar", "pie", "line", "table", "none".
    - "config": An object with "x_key" (label/x-axis) and "y_key" (value/y-axis).
               For "pie", use "x_key" for the name and "y_key" for the value.
               For "line", use "x_key" for the x-axis and "y_key" for the y-axis.

Constraints:
- **You MUST generate MySQL-compatible SQL.** For example, to format dates, use `DATE_FORMAT()`, not `strftime()`.
- **Use the provided schema table and column names EXACTLY.** Pay close attention to `custId`, `regiondescription`, etc.
- **If the question is not a request for data from the Northwind database (e.g., it's a greeting, "how are you?", "what can you do?", or about a different database), set "sql" to null and "explain" to a friendly, helpful reply that clarifies your role.**
- **For any relative date question (e.g., "last quarter", "last month", "last year"), you MUST calculate it relative to the latest `orderDate` in the `SalesOrder` table using a subquery. Do not use `NOW()` or `CURRENT_DATE()` as the data is historic.**
- MUST be a single SELECT statement. No semicolons.

- Example 1 (Query):
{{
  "sql": "SELECT categoryName, COUNT(*) as productCount FROM Product p JOIN Category c ON p.categoryId = c.categoryId GROUP BY c.categoryName ORDER BY productCount DESC",
  "params": [],
  "explain": "Here is a count of products in each category, ordered by the count.",
  "chart": {{"type": "bar", "config": {{"x_key": "categoryName", "y_key": "productCount"}}}}
}}
- Example 2 (Correct Column Name):
{{
  "sql": "SELECT country, COUNT(custId) AS customerCount FROM Customer GROUP BY country ORDER BY customerCount DESC",
  "params": [],
  "explain": "Here is a count of customers from each country, ordered from most to least.",
  "chart": {{"type": "bar", "config": {{"x_key": "country", "y_key": "customerCount"}}}}
}}
- Example 3 (MySQL Date Formatting):
{{
  "sql": "SELECT DATE_FORMAT(orderDate, '%Y-%m') AS orderMonth, COUNT(orderId) AS totalOrders FROM SalesOrder GROUP BY orderMonth ORDER BY orderMonth",
  "params": [],
  "explain": "This query counts the total number of orders placed for each month, ordered chronologically.",
  "chart": {{"type": "line", "config": {{"x_key": "orderMonth", "y_key": "totalOrders"}}}}
}}
- Example 4 (Meta-Question / Chit-Chat):
{{
  "sql": null,
  "params": [],
  "explain": "I'm an assistant who can help you query the Northwind database. You can ask me questions like 'How many customers are in Germany?' or 'Show me the 5 most expensive products.'",
  "chart": {{"type": "none", "config": {{}}}}
}}
- Example 5 (Out-of-Scope):
{{
  "sql": null,
  "params": [],
  "explain": "I'm sorry, I can only answer questions about the Northwind database schema provided to me.",
  "chart": {{"type": "none", "config": {{}}}}
}}
- Example 6 (Relative Date Query - "last 3 months"):
{{
  "sql": "SELECT e.firstname, e.lastname, SUM(od.unitPrice * od.quantity) AS totalSales FROM Employee e JOIN SalesOrder so ON e.employeeId = so.employeeId JOIN OrderDetail od ON so.orderId = od.orderId WHERE so.orderDate >= DATE_SUB((SELECT MAX(orderDate) FROM SalesOrder), INTERVAL 3 MONTH) GROUP BY e.employeeId, e.firstname, e.lastname ORDER BY totalSales DESC LIMIT 5",
  "params": [],
  "explain": "Here are the top 5 employees with the highest sales in the last 3 months, calculated relative to the latest order in the database.",
  "chart": {{"type": "bar", "config": {{"x_key": "lastname", "y_key": "totalSales"}}}}
}}
{history_block}
User question: {user_question}
"""

def ask_gemini_for_sql(user_question: str, chat_history: list = None, model="gemini-2.5-flash"):
    history_block = _format_history(chat_history)
    
    # This .format() call will now work correctly
    prompt = SQL_PROMPT_TEMPLATE.format(
        schema=_SCHEMA, # <-- We pass the schema text as context
        user_question=user_question,
        history_block=history_block
    )
    
    response = _client.models.generate_content(model=model, contents=prompt)
    text = getattr(response, "text", "").strip()
    print("Gemini raw text:", text)

    # Clean markdown code fences
    cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", text)
    cleaned = re.sub(r"```$", "", cleaned.strip())
    cleaned = cleaned.strip()

    # Find the JSON blob
    match = re.search(r"\{.*\}", cleaned, re.S)
    if not match:
        raise ValueError("Gemini did not return JSON:\n" + cleaned)

    json_text = match.group(0)
    try:
        parsed = json.loads(json_text)
        return parsed
    except json.JSONDecodeError as e:
        print("Failed to parse JSON:", json_text)
        raise e