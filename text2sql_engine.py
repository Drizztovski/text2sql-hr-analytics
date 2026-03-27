"""
text2sql_engine.py — Text-to-SQL Engine (Student Version with TODOs)
=====================================================================
Converts natural language questions into SQL queries using Google Gemini,
then executes them safely against a SQLite database.

Students will complete the TODO sections to build the full pipeline.

Usage:
    from text2sql_engine import Text2SQLEngine
    engine = Text2SQLEngine(conn, api_key='YOUR_KEY')
    result = engine.ask('What is the average salary by department?')
"""

import sqlite3
import pandas as pd
import re
from anthropic import Anthropic
try:
    import google.genai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    genai = None
import os
from dotenv import load_dotenv


# ============================================================
# PART A: Schema Context Builder
# ============================================================

def get_schema_for_prompt(conn):
    """
    Generate a concise schema description formatted for AI prompts.

    This is CRITICAL — the AI can only write correct SQL if it understands
    the database structure. The better the schema context, the better the SQL.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.

    Returns
    -------
    str
        Schema description optimized for inclusion in an AI prompt.
    """
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn
    )

    schema_parts = []

    for table_name in tables['name']:
        cols = pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)

        # TODO 1: Build a CREATE TABLE statement for each table
        # Include each column's name and type, marking primary keys.
        create_statement = f"CREATE TABLE {table_name} (\n"
        
        col_definitions = []
        for _, col in cols.iterrows():
            col_type = col['type'] or 'TEXT'
            pk_marker = ' PRIMARY KEY' if col['pk'] else ''
            col_definitions.append(f"    {col['name']} {col_type}{pk_marker}")
        
        create_statement += ',\n'.join(col_definitions)
        create_statement += "\n);"
        
        schema_parts.append(create_statement)
        schema_parts.append("")  # Blank line for readability

        # TODO 2: Add sample data so the AI can see actual values
        # Query 2-3 rows from each table and append as a SQL comment.
        sample_data = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 3", conn)
        
        if not sample_data.empty:
            schema_parts.append(f"-- Sample data from {table_name}:")
            schema_parts.append("-- " + sample_data.to_string(index=False).replace('\n', '\n-- '))
            schema_parts.append("")  # Blank line

    # TODO 3: Add relationship hints at the end of the schema
    # Tell the AI which columns link tables together so it writes correct JOINs.
    schema_parts.append("=" * 60)
    schema_parts.append("FOREIGN KEY RELATIONSHIPS")
    schema_parts.append("=" * 60)
    schema_parts.append("")
    
    # Define the relationships
    relationships = [
        "employees.dept_id → departments.dept_id",
        "employees.title_id → job_titles.title_id",
        "employees.manager_id → employees.emp_id (self-reference)",
        "salary_history.emp_id → employees.emp_id",
        "performance_reviews.emp_id → employees.emp_id",
        "performance_reviews.reviewer_id → employees.emp_id",
        "training_records.emp_id → employees.emp_id",
        "projects.dept_id → departments.dept_id",
        "project_assignments.project_id → projects.project_id",
        "project_assignments.emp_id → employees.emp_id",
    ]
    
    for rel in relationships:
        schema_parts.append(f"  {rel}")
    
    schema_parts.append("")

    return "\n".join(schema_parts)


# ============================================================
# PART B: SQL Safety Validator
# ============================================================

def validate_sql(sql):
    """
    Validate that AI-generated SQL is safe to execute.

    This is a CRITICAL security function. AI models can sometimes generate
    dangerous queries (DROP TABLE, DELETE, etc.) that could destroy data.

    Parameters
    ----------
    sql : str
        SQL query to validate.

    Returns
    -------
    tuple (bool, str)
        (is_safe, message) — True if safe, False with reason if not.

    Rules
    -----
    1. Query must start with SELECT or WITH (for CTEs)
    2. Must not contain: DROP, DELETE, UPDATE, INSERT, ALTER, TRUNCATE
    3. Must not contain multiple statements (no semicolons mid-query)
    """
    sql_clean = sql.strip()

    if not sql_clean:
        return False, "Empty query."

    sql_upper = sql_clean.upper()

    # TODO 4: Check that the query starts with SELECT or WITH
    if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
        return False, "Query must start with SELECT or WITH"

    # TODO 5: Check for dangerous keywords using regex word boundaries
    dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'TRUNCATE', 'CREATE', 'REPLACE']
    
    for keyword in dangerous_keywords:
        # Use word boundaries \b to match whole words only
        if re.search(rf'\b{keyword}\b', sql_upper):
            return False, f"Dangerous keyword detected: {keyword}"

    # TODO 6: Check for multiple statements (stacked query injection)
    # Allow semicolon at the very end, but not in the middle
    if ';' in sql_clean[:-1]:  # Check everywhere except last character
        return False, "Multiple statements not allowed (semicolon detected)"

    return True, "Query is safe."


# ============================================================
# PART C: Response Parser
# ============================================================

def extract_sql_from_response(response_text):
    """
    Extract a SQL query from an AI model's response.

    The AI might return SQL in several formats:
    - Wrapped in ```sql ... ``` code blocks
    - With explanations before/after
    - As plain text

    Parameters
    ----------
    response_text : str
        Raw text response from the AI model.

    Returns
    -------
    str
        Extracted SQL query, cleaned up.
    """
    # TODO 7: Try to extract SQL from markdown code blocks first
    # Look for ```sql ... ``` or ``` ... ``` patterns
    # Try ```sql ... ``` first
    sql_block = re.search(r'```sql\s*\n?(.*?)\n?```', response_text, re.DOTALL | re.IGNORECASE)
    if sql_block:
        return sql_block.group(1).strip()
    
    # Try generic ``` ... ``` blocks
    code_block = re.search(r'```\s*\n?(.*?)\n?```', response_text, re.DOTALL)
    if code_block:
        return code_block.group(1).strip()

    # TODO 8: Try to find a SELECT or WITH statement in the text
    # Handle both with and without a trailing semicolon
    # Look for SELECT statement
    select_match = re.search(r'(SELECT\b.*?)(?:;|\n\n|$)', response_text, re.DOTALL | re.IGNORECASE)
    if select_match:
        return select_match.group(1).strip()
    
    # Look for WITH statement (CTE)
    with_match = re.search(r'(WITH\b.*?)(?:;|\n\n|$)', response_text, re.DOTALL | re.IGNORECASE)
    if with_match:
        return with_match.group(1).strip()

    # Last resort: return the full text stripped
    return response_text.strip()


# ============================================================
# PART D: SQL Generator (Core AI Integration)
# ============================================================

def generate_sql(question, client, schema_info, model='claude-sonnet-4-20250514', provider='claude'):
    """
    Use Claude to convert a natural language question into a SQL query.

    Parameters
    ----------
    question : str
        Natural language question about the data.
    client : Anthropic
        Configured Anthropic client.
    schema_info : str
        Database schema description (from get_schema_for_prompt).
    model : str
        Claude model to use.

    Returns
    -------
    str
        Generated SQL query.
    """
    # TODO 9: Build a detailed prompt that includes:
    #   1. A role assignment for the AI
    #   2. The full database schema
    #   3. Clear rules (SELECT only, SQLite syntax, aliases, rounding, no explanations)
    #   4. The user's question
    prompt = prompt = f"""You are an expert SQL developer working with a SQLite database for TechCorp HR Analytics.

🚨🚨🚨 MANDATORY CTE SYNTAX CHECK 🚨🚨🚨
BEFORE writing ANY query with CTEs:
1. Does your query have multiple SELECT statements with parentheses?
2. If YES → Your query MUST start with the word WITH
3. If your query looks like: SELECT ... ) SELECT ... → YOU FORGOT WITH - ADD IT!

Example WRONG (will fail):
SELECT ... FROM ... ) SELECT ... FROM ...

Example CORRECT:
WITH cte_name AS (SELECT ... FROM ...) SELECT ... FROM cte_name

DATABASE SCHEMA:
{schema_info}

RULES FOR SQL GENERATION:
1. Use ONLY SELECT or WITH statements (no INSERT, UPDATE, DELETE, DROP, etc.)
2. Use SQLite syntax (not MySQL or PostgreSQL)
3. Always use table aliases in JOINs (e.g., 'e' for employees, 'd' for departments)
4. When calculating percentages or averages, use ROUND(value, 2) for readability
5. Use clear column aliases in SELECT (e.g., 'COUNT(*) AS employee_count')
6. For date operations, use SQLite functions: date(), strftime(), julianday()
7. Return ONLY the SQL query - no explanations, no markdown, no preamble
8. End the query with a semicolon
9. STRONGLY PREFER simple queries with subqueries in FROM/WHERE clauses instead of CTEs
10. If you MUST use CTEs: Start query with WITH, then list all CTEs with commas, then final SELECT

USER QUESTION:
{question}

⚠️ FINAL CHECK BEFORE RESPONDING:
- Count the SELECT statements in your query
- If more than one SELECT → Does it start with WITH?
- If NO → STOP and add WITH at the start!

SQL QUERY:"""

    # TODO 10: Call the Claude API and extract the SQL from the response
    # Handle API errors gracefully with try/except
    try:
        if provider.lower() == 'gemini':
            # Gemini API call
            response = client.generate_content(prompt)
            sql = response.text
        else:
            # Claude API call
            response = client.messages.create(
                model=model,
                max_tokens=2048,
                system="CRITICAL SQL RULE: If your query uses CTEs (Common Table Expressions), it MUST start with the keyword WITH. A query that looks like 'SELECT ... ) SELECT ...' is INVALID - it needs 'WITH cte_name AS (SELECT ...) SELECT ...' instead. Prefer simple queries with inline subqueries over CTEs when possible.",
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            # Extract the SQL from Claude's response
            sql = response.content[0].text
        
        # Use our parser to clean up the response
        sql = extract_sql_from_response(sql)
        
        return sql
        
    except Exception as e:
        print(f"SQL generation error: {e}")
        return f"-- Error: Could not generate SQL: {str(e)}"


# ============================================================
# PART E: Safe Query Executor
# ============================================================

def execute_generated_sql(sql, conn):
    """
    Validate and execute AI-generated SQL.

    Parameters
    ----------
    sql : str
        SQL query to execute.
    conn : sqlite3.Connection

    Returns
    -------
    tuple (bool, pd.DataFrame or str)
        (success, result_dataframe) or (False, error_message)
    """
    # TODO 11: First validate the SQL using validate_sql()
    # If not valid, return a tuple indicating failure with the reason
    is_safe, message = validate_sql(sql)
    
    if not is_safe:
        return False, f"SQL validation failed: {message}"

    # TODO 12: Execute the validated query and return the results
    # Handle execution errors gracefully
    try:
        # Execute the query and get results as a DataFrame
        result_df = pd.read_sql_query(sql, conn)
        return True, result_df
        
    except Exception as e:
        # If execution fails, return error message
        return False, f"SQL execution error: {str(e)}"


# ============================================================
# PART F: Visualization Code Generator
# ============================================================

def generate_visualization_code(question, sql, df, client, model='claude-sonnet-4-20250514', provider='claude'):
    """
    Use Claude to generate Python visualization code for query results.

    Given the original question, the SQL that produced the data, and the
    resulting DataFrame, ask the AI to write matplotlib/seaborn code that
    creates an appropriate visualization.

    Parameters
    ----------
    question : str
        The original natural language question.
    sql : str
        The SQL query that produced the data.
    df : pd.DataFrame
        The query results.
    client : Anthropic
    model : str

    Returns
    -------
    str
        Python code string that creates a matplotlib figure.
    """
    # TODO 13: Build a prompt that asks the AI to generate visualization code.
    # Include the question, SQL, DataFrame preview, and rules for the code:
    #   - Use matplotlib/seaborn, assume df is already defined
    #   - Proper labels, title, tight_layout, appropriate chart type
    #   - Store figure in variable called `fig`, do NOT call plt.show()
    #   - Return ONLY the Python code
    prompt = f"""You are a data visualization expert. Generate Python code to visualize query results.

MULTI-CHART DETECTION RULES:
Detect if this is a DISTRIBUTION query that would benefit from multiple perspectives:

CREATE MULTI-CHART LAYOUT (3 subplots) when:
- Question contains keywords: "distribution", "spread", "range", "variance"
- OR DataFrame has a single numeric column with 20+ rows
- OR question asks to "show" or "analyze" a single numeric variable

For distribution queries, create 3 complementary charts:
1. Histogram (left) - shows distribution shape and bins
2. Box plot (middle) - shows quartiles, median, and outliers  
3. Violin plot (right) - shows density at different values

Multi-chart code structure:
```python
fig, axes = plt.subplots(1, 3, figsize=(18, 7))

# Histogram
axes[0].hist(df['column'], bins=20, color='steelblue', edgecolor='black', linewidth=1.2)
axes[0].set_title('Histogram\\n(Distribution Shape)', fontsize=14, fontweight='bold')

# Box plot
axes[1].boxplot(df['column'], patch_artist=True, ...)
axes[1].set_title('Box Plot\\n(Quartiles & Outliers)', fontsize=14, fontweight='bold')

# Violin plot
axes[2].violinplot([df['column']], showmeans=True, showmedians=True, ...)
axes[2].set_title('Violin Plot\\n(Density)', fontsize=14, fontweight='bold')

plt.suptitle('Overall Title', fontsize=18, fontweight='bold', y=1.02)
```

USE SINGLE CHART for:
- Category comparisons (bar/horizontal bar charts)
- Grouped data (grouped bars, stacked bars)
- Time series (line charts)
- Relationships (scatter plots)
- Any question NOT about distribution/spread

ORIGINAL QUESTION:
{question}

SQL QUERY USED:
{sql}

DATAFRAME PREVIEW (first 5 rows):
{df.head().to_string(index=False)}

DATAFRAME INFO:
- Shape: {df.shape[0]} rows, {df.shape[1]} columns
- Columns: {', '.join(df.columns.tolist())}

RULES FOR VISUALIZATION CODE:
1. Use matplotlib and/or seaborn (import as needed: import matplotlib.pyplot as plt, import seaborn as sns)
2. The DataFrame is already defined as 'df' - do NOT redefine it
3. Choose an appropriate chart type based on the data (bar, line, scatter, pie, etc.)
4. Include descriptive title, axis labels, and any necessary formatting
5. Use plt.tight_layout() for proper spacing
6. Store the figure in a variable called 'fig' using: fig = plt.gcf()
7. Do NOT call plt.show() - just create the figure
8. Return ONLY the Python code - no explanations, no markdown formatting

STYLE REQUIREMENTS (MUST FOLLOW):
- ALWAYS start with: sns.set_style("darkgrid")
- Figure size: ALWAYS use figsize=(12, 7)
- Title: ALWAYS use fontsize=16, fontweight='bold'
- X-axis labels: If text labels are long, use rotation=45, ha='right'
- Tick marks: CRITICAL - ALWAYS include this exact line: plt.tick_params(axis='both', which='both', length=6, width=1.5)
- Bar edges: ALWAYS use edgecolor='black', linewidth=1.2 for all bar charts
- Grid: The darkgrid style provides this automatically
- Axis labels: Use fontsize=12

COLOR GUIDELINES:
- Choose colors that are visually appealing and work well on darkgrid background
- For single-color charts: 
  - Prefer vibrant colors: 'steelblue', 'coral', 'mediumseagreen', 'darkorange', 'mediumpurple'
  - Default to 'steelblue' if unsure
- For multi-color charts (2-5 categories):
  - Use seaborn color palettes: color=sns.color_palette('Set2', n_colors=X)
  - Best palettes for darkgrid: 'Set2', 'husl', 'Paired', 'Dark2'
  - OR use a curated list: color=['steelblue', 'coral', 'mediumseagreen', 'gold', 'mediumpurple']
- For many categories (6+):
  - Use: color=sns.color_palette('tab10', n_colors=X)
  - Or: color=sns.color_palette('husl', n_colors=X)
- NEVER use 'palette' parameter directly with .plot() or plt.bar() - it causes errors
- Avoid very light colors (yellow, light pink) - they don't show well on dark background
- Prefer medium-to-saturated colors for visibility

RECOMMENDED COLOR COMBINATIONS:
- 2 categories: ['steelblue', 'coral']
- 3 categories: ['steelblue', 'coral', 'mediumseagreen']
- 4 categories: ['steelblue', 'coral', 'mediumseagreen', 'gold']
- 5+ categories: color=sns.color_palette('Set2', n_colors=5)

CRITICAL EDGE RULE:
- EVERY bar chart must have: edgecolor='black', linewidth=1.2
- This makes bars stand out clearly against the background

EXAMPLE COLOR USAGE:
- Single-color bar chart: color='steelblue', edgecolor='black', linewidth=1.2
- 2-color grouped bars: color=['steelblue', 'coral'], edgecolor='black', linewidth=1.2
- Multi-color bar chart (3-5 bars): color=sns.color_palette('Set2', n_colors=4), edgecolor='black', linewidth=1.2
- Many categories (6+): color=sns.color_palette('husl', n_colors=8), edgecolor='black', linewidth=1.2
- Line charts: color='steelblue', linewidth=2.5, marker='o', markersize=6
- Scatter plots: color='coral', alpha=0.6 for overlapping points
- Stacked bars: Use Set2 or Paired palettes for clear distinction between segments

MANDATORY CODE ELEMENTS (Must appear in EVERY visualization):
1. sns.set_style("darkgrid")  # At the start
2. figsize=(12, 7)  # In plt.figure() or plt.subplots()
3. fontsize=16, fontweight='bold'  # In plt.title()
4. rotation=45, ha='right'  # In plt.xticks() if labels are text
5. plt.tick_params(axis='both', which='both', length=6, width=1.5)  # ALWAYS include this
6. edgecolor='black', linewidth=1.2  # For all bar charts
7. plt.tight_layout()  # Before fig = plt.gcf()

PYTHON CODE:"""

    # TODO 14: Call the API and extract the Python code from the response.
    try:
        if provider.lower() == 'gemini':
            # Gemini API call
            response = client.generate_content(prompt)
            code = response.text
        else:
            # Claude API call
            response = client.messages.create(
                model=model,
                max_tokens=2048,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            # Extract the code from Claude's response
            code = response.content[0].text
        
        # Use our parser to clean up the response (removes markdown, etc.)
        code = extract_python_from_response(code)
        
        return code
        
    except Exception as e:
        return f"# Error generating visualization: {str(e)}"


def extract_python_from_response(response_text):
    """Extract Python code from an AI response (similar to SQL extraction)."""
    # TODO 15: Extract Python code from markdown code blocks
    code_block = re.search(r'```(?:python)?\s*\n?(.*?)\n?```', response_text, re.DOTALL)
    if code_block:
        return code_block.group(1).strip()

    return response_text.strip()


# ============================================================
# PART G: The Complete Text2SQL Engine (ties everything together)
# ============================================================

class Text2SQLEngine:
    """
    Complete Text-to-SQL pipeline: Question → SQL → Results → Visualization.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.
    api_key : str
        Anthropic Claude API key.
    model : str
        Claude model name (default: 'claude-sonnet-4-20250514').
    """

    def __init__(self, conn, api_key, model='claude-sonnet-4-20250514', provider='claude'):
        self.conn = conn
        self.model = model
        self.provider = provider.lower()
        self.history = []

        # TODO 16: Build the schema context and initialize the API client
        # Store the schema, create the client, and test the connection.
        # Build the schema context (uses TODOs 1-3)
        print("Building schema context...")
        self.schema_info = get_schema_for_prompt(conn)
        print(f"✓ Schema loaded: {len(self.schema_info)} characters")
        
        # Initialize the appropriate API client based on provider
        if self.provider == 'gemini':
            if not GEMINI_AVAILABLE:
                raise ImportError("google-generativeai not installed. Run: pip install google-generativeai")
            print("Initializing Gemini API...")
            self.client = genai.Client(api_key=api_key)
            print(f"✓ Gemini API configured with model: {model}")
        else:
            # Default to Claude
            print("Initializing Claude API...")
            self.client = Anthropic(api_key=api_key)
            
            # Test the connection with a simple API call
            try:
                test_response = self.client.messages.create(
                    model=self.model,
                    max_tokens=10,
                    messages=[{"role": "user", "content": "Say 'ready'"}]
                )
                print(f"✓ Claude API connected: {test_response.content[0].text.strip()}")
            except Exception as e:
                print(f"⚠️ Claude API test failed: {str(e)}")
                print("Continuing anyway - will fail on first query if credentials are invalid.")

    def ask(self, question, show_sql=True, interpret=True, visualize=False):
        """
        Ask a natural language question and get SQL + results + interpretation.

        Parameters
        ----------
        question : str
            Natural language question about the data.
        show_sql : bool
            Whether to print the generated SQL.
        interpret : bool
            Whether to generate an AI interpretation.
        visualize : bool
            Whether to generate visualization code.

        Returns
        -------
        dict with keys: question, sql, data, interpretation, viz_code, success
        """
        result = {
            'question': question,
            'sql': None,
            'data': None,
            'interpretation': None,
            'viz_code': None,
            'success': False
        }

        if not hasattr(self, 'client') or self.client is None:
            print("Error: Claude API not configured.")
            return result

        # TODO 17: Implement the full ask() pipeline:
        # 1. Generate SQL from the question
        print(f"\n🤔 Question: {question}")
        result['sql'] = generate_sql(question, self.client, self.schema_info, self.model, self.provider)
        
        # 2. Optionally print the SQL
        if show_sql:
            print(f"\n📝 Generated SQL:\n{result['sql']}\n")
        
        # 3. Validate and execute — handle failures
        success, data_or_error = execute_generated_sql(result['sql'], self.conn)
        
        if not success:
            # Execution failed
            print(f"❌ Error: {data_or_error}")
            result['success'] = False
            self.history.append(result)
            return result
        
        # Success! We have data
        result['data'] = data_or_error
        result['success'] = True
        print(f"✓ Query returned {len(result['data'])} rows\n")
        
        # 4. If interpret=True and data exists, generate interpretation
        if interpret and result['data'] is not None and not result['data'].empty:
            print("🧠 Generating interpretation...")
            result['interpretation'] = self._interpret_results(question, result['data'])
            print(f"\n💡 Insight: {result['interpretation']}\n")
        
        # 5. If visualize=True and data exists, generate visualization code
        if visualize and result['data'] is not None and not result['data'].empty:
            print("📊 Generating visualization code...")
            result['viz_code'] = generate_visualization_code(
                question, 
                result['sql'], 
                result['data'], 
                self.client, 
                self.model,
                self.provider
            )
            print("✓ Visualization code generated\n")
        
        # 6. Save to history and return
        self.history.append(result)
        return result

    def _interpret_results(self, question, data):
        """Generate a business-friendly interpretation of query results."""
        prompt = f"""You are a data analyst at a technology company. A user asked: "{question}"

Here are the query results:
{data.to_string(index=False)}

Provide a brief (3-4 sentence) business interpretation. Include:
- One key insight from the data
- One actionable recommendation
- Reference specific numbers from the results."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except Exception as e:
            return f"(Could not generate interpretation: {e})"

    def get_sql_only(self, question):
        """Generate SQL without executing it. Useful for learning."""
        if not hasattr(self, 'client') or self.client is None:
            return "-- Error: Claude API not configured"
        return generate_sql(question, self.client, self.schema_info, self.model)

    def execute_custom_sql(self, sql):
        """Execute a manually written SQL query safely."""
        success, result = execute_generated_sql(sql, self.conn)
        if success:
            return result
        else:
            print(result)
            return pd.DataFrame()

    def show_schema(self):
        """Print the database schema."""
        print(self.schema_info)

    def show_history(self):
        """Show all questions asked in this session."""
        if not self.history:
            print("No questions asked yet.")
            return

        print(f"\nQuestion History ({len(self.history)} questions)")
        print("=" * 60)
        for i, item in enumerate(self.history, 1):
            status = "✓" if item['success'] else "✗"
            print(f"{i}. [{status}] {item['question']}")
            if item['sql']:
                sql_preview = item['sql'].replace('\n', ' ')[:80]
                print(f"   SQL: {sql_preview}...")
