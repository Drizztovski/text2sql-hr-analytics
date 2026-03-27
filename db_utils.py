"""
db_utils.py — Database Utility Functions for the Text-to-SQL Project
=====================================================================
Helper module for loading CSV data into SQLite and inspecting schemas.

This module is PROVIDED — you do not need to modify it.

Usage:
    from db_utils import load_csv_to_db, get_schema_info, execute_query
"""

import sqlite3
import pandas as pd
import os


def load_csv_to_db(csv_dir, db_path=':memory:'):
    """
    Load all CSV files from a directory into a SQLite database.

    Each CSV file becomes a table (filename without .csv = table name).
    Returns the database connection.
    """
    conn = sqlite3.connect(db_path, check_same_thread=False)
    csv_files = sorted([f for f in os.listdir(csv_dir) if f.endswith('.csv')])

    if not csv_files:
        print(f"Warning: No CSV files found in '{csv_dir}'")
        return conn

    print(f"Loading {len(csv_files)} CSV files into database...")

    for csv_file in csv_files:
        table_name = csv_file.replace('.csv', '')
        file_path = os.path.join(csv_dir, csv_file)
        df = pd.read_csv(file_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"  ✓ {table_name}: {len(df)} rows, {len(df.columns)} columns")

    conn.execute("PRAGMA foreign_keys = ON")
    print(f"\nDatabase ready! {len(csv_files)} tables loaded.")
    return conn


def get_schema_info(conn):
    """
    Get a comprehensive schema description optimized for AI prompts.
    
    Includes: tables, columns, data types, primary keys, foreign keys,
    sample data, relationships, and common query patterns.
    
    This enhanced version provides complete context needed for accurate
    SQL generation by AI models.
    """
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn
    )

    schema_parts = [
        "=" * 80,
        "DATABASE SCHEMA - TechCorp HR Analytics",
        "=" * 80,
        ""
    ]

    # ========================================================================
    # PART 1: Detailed table schemas with foreign keys
    # ========================================================================
    
    for table_name in tables['name']:
        cols = pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)
        row_count = pd.read_sql_query(
            f"SELECT COUNT(*) as cnt FROM {table_name}", conn
        )['cnt'][0]

        schema_parts.append(f"TABLE: {table_name} ({row_count} rows)")
        schema_parts.append("-" * 80)

        # Column information with enhanced formatting
        for _, col in cols.iterrows():
            pk = " PRIMARY KEY" if col['pk'] else ""
            nullable = "" if col['notnull'] else " (nullable)"
            col_type = col['type'] or 'TEXT'
            
            schema_parts.append(
                f"  {col['name']:25s} {col_type:10s}{pk}{nullable}"
            )

        schema_parts.append("")
        
    # ========================================================================
    # PART 2: Foreign Key Relationships (CRITICAL for JOINs!)
    # ========================================================================
    
    schema_parts.append("=" * 80)
    schema_parts.append("FOREIGN KEY RELATIONSHIPS")
    schema_parts.append("=" * 80)
    schema_parts.append("")
    
    # Define the foreign key relationships for TechCorp HR database
    relationships = [
        ("employees", "dept_id", "departments", "dept_id", "Many-to-One", 
         "Many employees belong to one department"),
        
        ("employees", "title_id", "job_titles", "title_id", "Many-to-One",
         "Many employees share the same job title"),
        
        ("employees", "manager_id", "employees", "emp_id", "Self-Reference",
         "Employees report to other employees (manager hierarchy)"),
        
        ("salary_history", "emp_id", "employees", "emp_id", "Many-to-One",
         "Each employee has multiple salary history records"),
        
        ("performance_reviews", "emp_id", "employees", "emp_id", "Many-to-One",
         "Each employee has multiple performance reviews"),
        
        ("performance_reviews", "reviewer_id", "employees", "emp_id", "Many-to-One",
         "Reviews are conducted by employees (usually managers)"),
        
        ("training_records", "emp_id", "employees", "emp_id", "Many-to-One",
         "Each employee has multiple training records"),
        
        ("projects", "dept_id", "departments", "dept_id", "Many-to-One",
         "Each project belongs to one department"),
        
        ("project_assignments", "project_id", "projects", "project_id", "Many-to-Many",
         "Junction table: projects have many employees"),
        
        ("project_assignments", "emp_id", "employees", "emp_id", "Many-to-Many",
         "Junction table: employees work on many projects"),
    ]
    
    for from_table, from_col, to_table, to_col, rel_type, description in relationships:
        schema_parts.append(
            f"{from_table}.{from_col} → {to_table}.{to_col}"
        )
        schema_parts.append(f"  Type: {rel_type}")
        schema_parts.append(f"  Note: {description}")
        schema_parts.append("")
    
    # ========================================================================
    # PART 3: Sample Data (First 3 rows per table)
    # ========================================================================
    
    schema_parts.append("=" * 80)
    schema_parts.append("SAMPLE DATA (First 3 rows per table)")
    schema_parts.append("=" * 80)
    schema_parts.append("")

    for table_name in tables['name']:
        sample = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 3", conn)
        schema_parts.append(f"{table_name}:")
        schema_parts.append(sample.to_string(index=False))
        schema_parts.append("")
    
    # ========================================================================
    # PART 4: Common Query Patterns (Teach AI how to JOIN)
    # ========================================================================
    
    schema_parts.append("=" * 80)
    schema_parts.append("COMMON QUERY PATTERNS")
    schema_parts.append("=" * 80)
    schema_parts.append("")
    
    schema_parts.append("-- Get employee details with department and title names:")
    schema_parts.append("SELECT e.*, d.dept_name, j.title_name")
    schema_parts.append("FROM employees e")
    schema_parts.append("JOIN departments d ON e.dept_id = d.dept_id")
    schema_parts.append("JOIN job_titles j ON e.title_id = j.title_id")
    schema_parts.append("")
    
    schema_parts.append("-- Get manager hierarchy (self-join):")
    schema_parts.append("SELECT e.first_name, e.last_name,")
    schema_parts.append("       m.first_name AS manager_first, m.last_name AS manager_last")
    schema_parts.append("FROM employees e")
    schema_parts.append("LEFT JOIN employees m ON e.manager_id = m.emp_id")
    schema_parts.append("")
    
    schema_parts.append("-- Get project team members (many-to-many via junction table):")
    schema_parts.append("SELECT p.project_name, e.first_name, e.last_name, pa.role_on_project")
    schema_parts.append("FROM projects p")
    schema_parts.append("JOIN project_assignments pa ON p.project_id = pa.project_id")
    schema_parts.append("JOIN employees e ON pa.emp_id = e.emp_id")
    schema_parts.append("")
    
    schema_parts.append("-- Get salary history with employee names:")
    schema_parts.append("SELECT e.first_name, e.last_name, sh.old_salary, sh.new_salary,")
    schema_parts.append("       sh.change_date, sh.reason")
    schema_parts.append("FROM salary_history sh")
    schema_parts.append("JOIN employees e ON sh.emp_id = e.emp_id")
    schema_parts.append("")
    
    # ========================================================================
    # PART 5: Important Notes for AI
    # ========================================================================
    
    schema_parts.append("=" * 80)
    schema_parts.append("IMPORTANT NOTES")
    schema_parts.append("=" * 80)
    schema_parts.append("")
    schema_parts.append("1. MANAGER HIERARCHY:")
    schema_parts.append("   - employees.manager_id references employees.emp_id (self-join)")
    schema_parts.append("   - C-level executives have NULL manager_id (no manager)")
    schema_parts.append("   - Use LEFT JOIN when including managers to keep all employees")
    schema_parts.append("")
    
    schema_parts.append("2. MANY-TO-MANY RELATIONSHIPS:")
    schema_parts.append("   - employees ↔ projects via project_assignments junction table")
    schema_parts.append("   - Always JOIN through project_assignments, never direct")
    schema_parts.append("")
    
    schema_parts.append("3. CATEGORICAL COLUMNS - Valid Values:")
    schema_parts.append("   - employees.status: 'Active', 'Inactive'")
    schema_parts.append("   - employees.work_mode: 'Remote', 'Hybrid', 'On-site'")
    schema_parts.append("   - employees.performance_band: 'Exceeds', 'Meets', 'Needs Improvement', 'Unsatisfactory'")
    schema_parts.append("   - projects.status: 'Active', 'Completed', 'On Hold'")
    schema_parts.append("   - salary_history.reason: 'Promotion', 'Annual Review', 'Merit Increase', 'Market Adjustment'")
    schema_parts.append("   - performance_reviews.rating: 'Exceeds Expectations', 'Meets Expectations', 'Needs Improvement', 'Unsatisfactory'")
    schema_parts.append("   - training_records.status: 'Completed', 'In Progress', 'Planned'")
    schema_parts.append("   - job_titles.career_level: 'Entry', 'Mid', 'Senior', 'Lead', 'Manager', 'Director', 'VP', 'C-Level'")
    schema_parts.append("")
    
    schema_parts.append("4. DATE FORMATS:")
    schema_parts.append("   - All dates stored as TEXT in format: 'YYYY-MM-DD'")
    schema_parts.append("   - Use SQLite date functions: date(), strftime(), julianday()")
    schema_parts.append("")
    
    schema_parts.append("5. SALARY RANGES:")
    schema_parts.append("   - employees.salary: $45,000 - $180,000 (current salary)")
    schema_parts.append("   - job_titles.min_salary / max_salary: defines band for each role")
    schema_parts.append("")
    
    schema_parts.append("=" * 80)

    return "\n".join(schema_parts)


def get_table_info(table_name, conn):
    """Get detailed information about a specific table."""
    cols = pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)
    row_count = pd.read_sql_query(
        f"SELECT COUNT(*) as cnt FROM {table_name}", conn
    )['cnt'][0]

    print(f"Table: {table_name} ({row_count} rows)")
    print("-" * 40)

    result = cols[['name', 'type', 'notnull', 'pk']].copy()
    result.columns = ['Column', 'Type', 'Not Null', 'Primary Key']
    result['Not Null'] = result['Not Null'].map({1: 'Yes', 0: 'No'})
    result['Primary Key'] = result['Primary Key'].map({1: 'Yes', 0: 'No'})
    return result


def execute_query(sql, conn, params=None):
    """
    Execute a SQL query and return results as a DataFrame.
    A safe wrapper around pd.read_sql_query with error handling.
    """
    try:
        if params:
            return pd.read_sql_query(sql, conn, params=params)
        return pd.read_sql_query(sql, conn)
    except Exception as e:
        print(f"Query Error: {e}")
        print(f"SQL: {sql}")
        return pd.DataFrame()


def list_tables(conn):
    """List all tables in the database with row counts."""
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn
    )

    results = []
    for table_name in tables['name']:
        count = pd.read_sql_query(
            f"SELECT COUNT(*) as rows FROM {table_name}", conn
        )['rows'][0]
        results.append({'table': table_name, 'rows': count})

    return pd.DataFrame(results)


def get_foreign_keys(conn):
    """
    Discover and display all foreign key relationships in the database.
    Useful for understanding how tables connect.
    """
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn
    )

    relationships = []
    for table_name in tables['name']:
        fks = pd.read_sql_query(f"PRAGMA foreign_key_list({table_name})", conn)
        if not fks.empty:
            for _, fk in fks.iterrows():
                relationships.append({
                    'from_table': table_name,
                    'from_column': fk['from'],
                    'to_table': fk['table'],
                    'to_column': fk['to']
                })

    if relationships:
        return pd.DataFrame(relationships)
    else:
        print("No formal foreign keys defined (relationships exist by naming convention).")
        # Show implied relationships
        implied = [
            ("employees", "dept_id", "departments", "dept_id"),
            ("employees", "title_id", "job_titles", "title_id"),
            ("employees", "manager_id", "employees", "emp_id"),
            ("projects", "dept_id", "departments", "dept_id"),
            ("project_assignments", "project_id", "projects", "project_id"),
            ("project_assignments", "emp_id", "employees", "emp_id"),
            ("salary_history", "emp_id", "employees", "emp_id"),
            ("performance_reviews", "emp_id", "employees", "emp_id"),
            ("training_records", "emp_id", "employees", "emp_id"),
        ]
        return pd.DataFrame(implied, columns=['from_table', 'from_column', 'to_table', 'to_column'])


def generate_business_insight(result, client):
    
    

    """
    Generate structured business insights from a Text2SQL engine result.
    
    Uses Claude to analyze query results and provide actionable insights
    in a consistent format.
    
    Parameters
    ----------
    result : dict
        Result dictionary from Text2SQLEngine.ask() containing:
        - 'question': The original question
        - 'sql': The generated SQL
        - 'data': The DataFrame of results
        - 'success': Whether query succeeded
    client : Anthropic
        Configured Claude API client
    
    Returns
    -------
    str
        Formatted business insight with Key Findings, Observations, 
        Recommendations, and Single Highest Impact
    
    Example
    -------
    >>> from anthropic import Anthropic
    >>> client = Anthropic(api_key="your-key")
    >>> result = engine.ask("What is the average salary by department?")
    >>> insight = generate_business_insight(result, client)
    >>> print(insight)
    """
    
    if not result.get('success') or result.get('data') is None:
        return "Cannot generate insights - query did not return data."
    
    # Build the prompt
    prompt = f"""You are a senior business analyst at TechCorp. Analyze the following query results and provide structured business insights.

ORIGINAL QUESTION:
{result['question']}

SQL QUERY USED:
{result['sql']}

QUERY RESULTS:
{result['data'].to_string(index=False)}

DATA SUMMARY:
- Rows returned: {len(result['data'])}
- Columns: {', '.join(result['data'].columns.tolist())}

Generate a professional business insight report with EXACTLY this structure:

BUSINESS INSIGHT - [Brief Title Based on Question]

Key Findings:
- [2-4 bullet points with specific numbers from the data]
- [Focus on what stands out - highs, lows, outliers]
- [Include employee counts, percentages, or dollar amounts where relevant]

Observations:
- [2-4 bullet points analyzing patterns or relationships]
- [Connect different data points]
- [Note any concerning or positive trends]

Recommendations:
- [2-4 specific, actionable recommendations]
- [Each should address a finding or observation]
- [Be concrete - suggest what HR/leadership should DO]

Single Highest Impact:
- [ONE action that would have the biggest positive impact]
- [Be specific and justify why this matters most]

RULES:
- Use actual numbers from the data (don't make up values)
- Keep each bullet point to 1-2 sentences
- Be professional but direct
- Focus on actionable insights, not just describing the data
- The "Single Highest Impact" must be your #1 priority recommendation
"""
    
    try:
        response = client.messages.create(
            model='claude-sonnet-4-20250514',
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return response.content[0].text.strip()
        
    except Exception as e:
        return f"Error generating business insight: {str(e)}"
    
    
def safe_visualize(result, show_code=False):
    """
    Safely execute visualization code with intelligent fallback.
    
    If the AI-generated visualization fails, automatically creates
    an appropriate chart based on the data shape.
    
    Parameters
    ----------
    result : dict
        Result dictionary from Text2SQLEngine.ask() containing:
        - 'viz_code': AI-generated visualization code
        - 'data': The DataFrame to visualize
        - 'question': Original question (for fallback title)
    show_code : bool
        If True, prints the generated code before executing
    
    Returns
    -------
    None
        Displays the visualization using plt.show()
    
    Example
    -------
    >>> result = engine.ask("What is the average salary?", visualize=True)
    >>> safe_visualize(result)
    """
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    if not result.get('viz_code') or result.get('data') is None:
        print("⚠️ No visualization code or data available")
        return
    
    df = result['data']
    
    if show_code:
        print("\n" + "="*80)
        print("GENERATED VISUALIZATION CODE:")
        print("="*80)
        print(result['viz_code'])
        print("="*80 + "\n")
    
    # Try executing the AI-generated code
    try:
        exec(result['viz_code'])
        plt.show()
        print("✓ Visualization generated successfully")
        
    except Exception as e:
        print(f"\n⚠️ AI-generated visualization failed: {e}")
        print("🔄 Creating intelligent fallback visualization...\n")
        
        # Smart fallback based on data shape
        _create_fallback_visualization(df, result.get('question', 'Query Results'))


def _create_fallback_visualization(df, question):
    """
    Create an intelligent fallback visualization based on data shape.
    
    Analyzes the DataFrame structure and creates an appropriate chart:
    - 2 columns (category + value): bar chart
    - Multiple numeric columns: grouped bar chart
    - Single numeric column: histogram
    - Large dataset: sample and visualize
    """
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    # ALWAYS close any existing figures first
    plt.close('all')
    
    # Create new figure with guaranteed size
    sns.set_style("darkgrid")
    fig, ax = plt.subplots(figsize=(12, 7))
    
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    text_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    
    # Debug: print what we're working with
    print(f"📊 Data shape: {df.shape}")
    print(f"📊 Numeric columns: {numeric_cols}")
    print(f"📊 Text columns: {text_cols}")
    
    try:
        # Case 1: Two columns (category + value) - Bar Chart
        if len(df.columns) == 2 and len(numeric_cols) >= 1:
            if len(text_cols) >= 1:
                category_col = text_cols[0]
                value_col = numeric_cols[0]
            else:
                # Both numeric - use first as category
                category_col = df.columns[0]
                value_col = df.columns[1]
            
            # Limit to top 20 if too many categories
            plot_df = df.nlargest(20, value_col) if len(df) > 20 else df
            
            ax.bar(range(len(plot_df)), plot_df[value_col],
                   color='steelblue', edgecolor='black', linewidth=1.2)
            ax.set_xticks(range(len(plot_df)))
            ax.set_xticklabels(plot_df[category_col].astype(str), rotation=45, ha='right')
            ax.set_xlabel(str(category_col), fontsize=12)
            ax.set_ylabel(str(value_col), fontsize=12)
            ax.set_title(f'{value_col} by {category_col}', fontsize=16, fontweight='bold')
            print(f"✓ Created 2-column bar chart")
        
        # Case 2: One text column + multiple numeric columns - Grouped Bar
        elif len(text_cols) >= 1 and len(numeric_cols) > 1:
            category_col = text_cols[0]
            
            # Limit to top 15 categories
            plot_df = df.head(15) if len(df) > 15 else df
            
            x = range(len(plot_df))
            width = 0.8 / len(numeric_cols)
            colors = ['steelblue', 'coral', 'lightgreen', 'gold', 'orchid']
            
            for i, col in enumerate(numeric_cols[:5]):  # Max 5 numeric columns
                offset = (i - len(numeric_cols)/2) * width + width/2
                ax.bar([p + offset for p in x], plot_df[col],
                       width=width, label=col,
                       color=colors[i % len(colors)],
                       edgecolor='black', linewidth=1.2)
            
            ax.set_xticks(x)
            ax.set_xticklabels(plot_df[category_col].astype(str), rotation=45, ha='right')
            ax.set_xlabel(str(category_col), fontsize=12)
            ax.set_ylabel('Value', fontsize=12)
            ax.set_title(f'Comparison by {category_col}', fontsize=16, fontweight='bold')
            ax.legend(fontsize=10)
            print(f"✓ Created grouped bar chart")
        
        # Case 3: Single numeric column - Histogram
        elif len(numeric_cols) == 1 and len(df) > 5:
            col = numeric_cols[0]
            ax.hist(df[col], bins=min(20, len(df)//2), 
                   color='steelblue', edgecolor='black', linewidth=1.2)
            ax.set_xlabel(str(col), fontsize=12)
            ax.set_ylabel('Frequency', fontsize=12)
            ax.set_title(f'Distribution of {col}', fontsize=16, fontweight='bold')
            print(f"✓ Created histogram")
        
        # Case 4: Multiple numeric columns, no text - first column as bar
        elif len(numeric_cols) >= 2:
            col = numeric_cols[0]
            value_col = numeric_cols[1]
            plot_df = df.head(20) if len(df) > 20 else df
            
            ax.bar(range(len(plot_df)), plot_df[value_col],
                   color='steelblue', edgecolor='black', linewidth=1.2)
            ax.set_xlabel(str(col), fontsize=12)
            ax.set_ylabel(str(value_col), fontsize=12)
            ax.set_title(f'{value_col} by Row', fontsize=16, fontweight='bold')
            print(f"✓ Created numeric bar chart")
        
        # Case 5: All else fails - simple bar of first numeric column
        elif len(numeric_cols) > 0:
            col = numeric_cols[0]
            plot_df = df.head(20) if len(df) > 20 else df
            
            ax.bar(range(len(plot_df)), plot_df[col],
                   color='steelblue', edgecolor='black', linewidth=1.2)
            ax.set_xlabel('Row Index', fontsize=12)
            ax.set_ylabel(str(col), fontsize=12)
            ax.set_title(f'{col} Distribution', fontsize=16, fontweight='bold')
            print(f"✓ Created simple bar chart")
        
        # Case 6: No numeric data at all
        else:
            ax.text(0.5, 0.5, 'No numeric data available to visualize',
                   ha='center', va='center', fontsize=14, transform=ax.transAxes)
            ax.set_title('Fallback Visualization', fontsize=16, fontweight='bold')
            print(f"⚠️ No numeric data to visualize")
        
        # ALWAYS add these at the end
        ax.tick_params(axis='both', which='both', length=6, width=1.5)
        plt.tight_layout()
        plt.show()
        print("✓ Fallback visualization displayed")
        
    except Exception as fallback_error:
        print(f"❌ Fallback visualization error: {fallback_error}")
        print(f"Data info:")
        print(df.info())
        print(f"\nFirst few rows:")
        print(df.head())
        
        # Last resort - just show the data
        plt.close('all')
        fig, ax = plt.subplots(figsize=(12, 7))
        ax.text(0.5, 0.5, f'Visualization failed\nSee data table above',
               ha='center', va='center', fontsize=14, transform=ax.transAxes)
        ax.set_title('Error: Could Not Visualize', fontsize=16, fontweight='bold')
        plt.tight_layout()
        plt.show()
        
        
def get_table_schema(conn, table_name):
    """
    Get schema information for a specific table.
    
    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection
    table_name : str
        Name of the table to inspect
    
    Returns
    -------
    list of dict
        List of column information dictionaries with keys:
        - 'name': Column name
        - 'type': SQLite data type
        - 'notnull': Whether column is NOT NULL (1 or 0)
        - 'pk': Whether column is PRIMARY KEY (1 or 0)
    
    Example
    -------
    >>> schema = get_table_schema(conn, 'employees')
    >>> print(schema[0])
    {'name': 'emp_id', 'type': 'INTEGER', 'notnull': 1, 'pk': 1}
    """
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = cursor.fetchall()
    
    # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
    return [
        {
            'name': col[1],
            'type': col[2],
            'notnull': col[3],
            'pk': col[5]
        }
        for col in columns
    ]