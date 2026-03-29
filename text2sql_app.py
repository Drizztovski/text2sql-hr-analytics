"""
text2sql_app.py — TechCorp HR Analytics Streamlit Application
=============================================================================
A web interface that allows users to query an HR database using plain English.
Converts natural language questions into SQL via AI, executes them safely
against a SQLite database, and returns results with auto-generated
visualizations and business insights.

Features:
    - Dual AI provider support: Anthropic Claude and Google Gemini
    - Conversation memory for contextual follow-up questions
    - SQL history sidebar with one-click re-run
    - Custom SQL editor with safety validation
    - CSV export for all query results
    - Interactive ER diagram and schema explorer

Run with:  streamlit run text2sql_app.py
"""

import streamlit as st
import streamlit_mermaid as stmd
import pandas as pd
import sqlite3
import os
import sys
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# Make sure we can import from the current directory
sys.path.insert(0, os.path.dirname(__file__))

from db_utils import load_csv_to_db, get_schema_info, list_tables, get_table_schema, generate_business_insight

# Import from our completed text2sql_engine.py
from text2sql_engine import (
    Text2SQLEngine,
    get_schema_for_prompt,
    generate_sql,
    execute_generated_sql,
    validate_sql,
    generate_visualization_code,
    extract_sql_from_response,
)


# ============================================================
# PAGE CONFIGURATION
# ============================================================
st.set_page_config(
    page_title="TechCorp HR Analytics",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏢 TechCorp HR Analytics")
st.caption("Ask questions about your company data in plain English")


# ============================================================
# SESSION STATE INITIALIZATION
# ============================================================

if 'messages' not in st.session_state:
    st.session_state.messages = []

if 'engine' not in st.session_state:
    st.session_state.engine = None

if 'pending_question' not in st.session_state:
    st.session_state.pending_question = None

if 'sql_history' not in st.session_state:
    st.session_state.sql_history = []

if 'conversation_memory' not in st.session_state:
    st.session_state.conversation_memory = []
    

# ============================================================
# SIDEBAR: Configuration & Schema Explorer
# ============================================================

with st.sidebar:
    # Configuration section - collapsible
    with st.expander("⚙️ Configuration", expanded=True):
        # AI Provider Selection
        api_provider = st.selectbox(
            "AI Provider",
            ["Claude (Anthropic - Paid)", "Gemini (Google - Free)"],
            help="Choose your AI provider. Gemini offers a free tier!"
        )

        # API Key input (changes based on provider)
        if api_provider.startswith("Claude"):
            api_key = st.text_input(
                "Anthropic API Key",
                type="password",
                help="Enter your Anthropic API key",
            )
            model_name = "claude-sonnet-4-20250514"
        else:
            api_key = st.text_input(
                "Gemini API Key (Free!)",
                type="password",
                help="Get your free key at https://aistudio.google.com/apikey",
            )
            model_name = "gemini-2.5-flash"

        if not api_key:
            st.warning(f"Please enter your {api_provider.split()[0]} API key to get started.")
        
        # Engine initialization status (shown right here in config section!)
        if api_key and st.session_state.get('engine') is not None:
            st.success(f"✓ Engine initialized with {api_provider.split()[0]}!")

    st.divider()

    @st.cache_resource
    def get_database_connection():
        """
        Load all CSV files from the data/ directory into an in-memory SQLite database.

        Cached with st.cache_resource so the database is only loaded once per
        session, not on every Streamlit rerun.

        Returns:
            sqlite3.Connection: Active connection to the in-memory HR database.
        """
        csv_dir = os.path.join(os.path.dirname(__file__), 'data')
        return load_csv_to_db(csv_dir, db_path=':memory:')

    conn = get_database_connection()

    st.header("📋 Database Schema")
    st.caption("Reference these tables when asking questions")

    with st.expander("📊 Table Details", expanded=True):
        tables_df = list_tables(conn)
        
        # Extract table names from the DataFrame
        for _, row in tables_df.iterrows():
            table_name = row['table']
            row_count = row['rows']
            
            # Get schema info for this specific table
            schema = get_table_schema(conn, table_name)
            
            # Create expander for each table
            with st.expander(f"**{table_name}** ({row_count:,} rows)"):
                st.caption("Columns:")
                for col_info in schema:
                    st.text(f"  • {col_info['name']} ({col_info['type']})")

    st.divider()
    
    # Mermaid ER Diagram
    st.subheader("🗺️ Database Relationships")
    
    # Define the Mermaid ER diagram
    mermaid_diagram = """
    erDiagram
        employees ||--o{ performance_reviews : receives
        employees ||--o{ project_assignments : assigned_to
        employees ||--o{ salary_history : has
        employees ||--o{ training_records : completes
        employees }o--|| departments : belongs_to
        employees }o--|| job_titles : has
        projects ||--o{ project_assignments : includes
        
        employees {
            int employee_id PK
            string first_name
            string last_name
            int department_id FK
            int job_title_id FK
            date hire_date
        }
        
        departments {
            int department_id PK
            string department_name
            int employee_count
        }
        
        job_titles {
            int job_title_id PK
            string title
            decimal min_salary
            decimal max_salary
        }
        
        performance_reviews {
            int review_id PK
            int employee_id FK
            date review_date
            int rating
        }
        
        project_assignments {
            int assignment_id PK
            int employee_id FK
            int project_id FK
            date start_date
            date end_date
        }
        
        projects {
            int project_id PK
            string project_name
            string status
        }
        
        salary_history {
            int salary_id PK
            int employee_id FK
            decimal salary
            date effective_date
        }
        
        training_records {
            int training_id PK
            int employee_id FK
            string course_name
            date completion_date
        }
    """
    
    # Display the diagram 
    stmd.st_mermaid(mermaid_diagram)
    
    st.caption("📊 Visual representation of table relationships")

    st.divider()

    # SQL History Sidebar
    st.subheader("🕐 SQL History")
    if st.session_state.sql_history:
        for i, entry in enumerate(reversed(st.session_state.sql_history[-20:])):
            with st.expander(f"Q: {entry['question'][:50]}..."):
                st.code(entry['sql'], language="sql")
                st.caption(f"🕐 {entry['timestamp']}")
                if st.button("▶ Re-run this query", key=f"rerun_{i}"):
                    st.session_state.pending_question = entry['question']
                    st.rerun()
    else:
        st.caption("No queries yet. Ask a question to get started!")
# ============================================================
# MAIN AREA: Chat Interface
# ============================================================


def build_conversation_context(messages, max_exchanges=3):
    """
    Build a conversation history string from the last N Q&A pairs.

    Prepends recent question/SQL pairs to the current prompt so the AI
    can handle follow-up questions like "show me just the top 3" without
    losing context from previous queries. Limited to the last max_exchanges
    to avoid exceeding token limits.

    Args:
        messages (list): The full st.session_state.messages list containing
            all chat history dicts with role, content, question, and sql keys.
        max_exchanges (int): Maximum number of past Q&A pairs to include.
            Defaults to 3.

    Returns:
        str: Formatted context string ready to prepend to the current prompt,
            or empty string if no prior exchanges exist.
    """
    
    # Pull only assistant messages that have a question and SQL
    exchanges = [
        m for m in messages
        if m.get("role") == "assistant" and m.get("question") and m.get("sql")
    ]
    # Take the last max_exchanges only (token management)
    recent = exchanges[-max_exchanges:]
    if not recent:
        return ""
    context_lines = ["PREVIOUS QUESTIONS IN THIS CONVERSATION:"]
    for i, m in enumerate(recent, 1):
        context_lines.append(f"{i}. User asked: {m['question']}")
        context_lines.append(f"   SQL used: {m['sql'][:120]}...")
    context_lines.append("")
    return "\n".join(context_lines)


# Initialize the engine when an API key is provided
if api_key and st.session_state.engine is None:
    try:
        # Initialize engine with the selected provider
        if api_provider.startswith("Claude"):
            st.session_state.engine = Text2SQLEngine(conn, api_key=api_key, model=model_name)
        else:
            st.session_state.engine = Text2SQLEngine(conn, api_key=api_key, model=model_name, provider="gemini")
        
        # Success message now shown in the Configuration expander above
    except Exception as e:
        st.sidebar.error(f"Error initializing engine: {str(e)}")

# Show a helpful message if no API key is provided
if not api_key:
    st.info("👈 Please enter your API key in the sidebar to get started.")
    st.stop()  # Stop execution until API key is provided


tab1, tab2 = st.tabs(["💬 Chat", "🔧 Custom SQL"])

with tab1:

    # Render all messages from chat history with their role and associated data
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])
            
            # If this is an assistant message with additional data, show it
            if message["role"] == "assistant" and "data" in message:
                
                # Show SQL query in expandable section
                if "sql" in message and message["sql"]:
                    with st.expander("📝 View SQL Query"):
                        st.code(message["sql"], language="sql")
                
                # Show data table
                if message["data"] is not None and not message["data"].empty:
                    st.dataframe(message["data"], width='stretch')
                    if message.get("csv"):
                        st.download_button(
                            label="📥 Download Results as CSV",
                            data=message["csv"],
                            file_name=f"query_results_{message.get('question', 'data')[:20].replace(' ', '_')}.csv",
                            mime="text/csv",
                            key=f"download_history_{st.session_state.messages.index(message)}"
                        )
                
                # Show visualization if available
                if "viz_code" in message and message["viz_code"]:
                    try:
                        # Provide the DataFrame to the viz code
                        df = message["data"]
                        
                        # Execute the visualization code
                        exec(message["viz_code"])
                        fig = plt.gcf()
                        st.pyplot(fig)
                        plt.close(fig)
                    except Exception as e:
                        st.warning(f"Could not display visualization: {str(e)}")
        
                # Show business insight if available
                if "insight" in message and message["insight"]:
                    st.info(message["insight"])

    # Suggested starter questions (only show when chat is empty)
    if len(st.session_state.messages) == 0:
        st.markdown("### 💡 Try asking:")
        
        # Create columns for question buttons
        col1, col2 = st.columns(2)
        
        suggested_questions = [
            "How many employees are in each department?",
            "What is the average salary by department?",
            "Who are the top 5 highest paid employees?",
            "Show me the distribution of performance ratings",
            "What's the trend in hiring over the past 3 years?",
            "Which departments have the highest average salaries?"
        ]
        
        # Display questions in two columns
        for idx, question in enumerate(suggested_questions):
            col = col1 if idx % 2 == 0 else col2
            with col:
                if st.button(question, key=f"suggested_{idx}", use_container_width=True):
                    # Set the question in session state to be processed
                    st.session_state.pending_question = question
                    st.rerun()
        
        st.divider()

    # Process pending question from button click
    if 'pending_question' in st.session_state and st.session_state.pending_question:
        prompt = st.session_state.pending_question
        st.session_state.pending_question = None  # Clear it
        
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
        
        # Generate response with the engine
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                # Build conversation context for follow-up question support
                context = build_conversation_context(st.session_state.messages)
                augmented_prompt = f"{context}{prompt}" if context else prompt
                result = st.session_state.engine.ask(augmented_prompt, visualize=True, interpret=False)
            
            if result['success']:
                st.write("Here's what I found:")
                
                # Show SQL query
                with st.expander("📝 View SQL Query"):
                    st.code(result['sql'], language="sql")
                
                # Show data table
                if result['data'] is not None and not result['data'].empty:
                    st.dataframe(result['data'], width='stretch')
                    st.caption(f"📊 {len(result['data'])} rows returned")
                else:
                    st.info("No data returned from query.")
                
                # Show visualization
                if result.get('viz_code'):
                    try:
                        df = result['data']
                        exec(result['viz_code'])
                        fig = plt.gcf()
                        st.pyplot(fig)
                        plt.close(fig)
                    except Exception as e:
                        st.warning(f"Could not display visualization: {str(e)}")
                
                # Auto-generate Business Insights for Claude users
                if result['data'] is not None and not result['data'].empty:
                    if st.session_state.engine.provider != 'gemini':
                        with st.spinner("Generating business insights..."):
                            insight_text = generate_business_insight(result, st.session_state.engine.client)
                            st.info(insight_text)
                            result['insight'] = insight_text
                    else:
                        st.caption("💡 Tip: Business insights available with Claude provider")
                
                # Save to chat history
                message_data = {
                    "role": "assistant",
                    "content": "Here's what I found:",
                    "sql": result['sql'],
                    "data": result['data'],
                    "csv": result['data'].to_csv(index=False) if result['data'] is not None and not result['data'].empty else None,
                    "viz_code": result.get('viz_code'),
                    "question": prompt
                }
                if 'insight' in result:
                    message_data['insight'] = result['insight']
                st.session_state.messages.append(message_data)
                # Record to SQL history
                st.session_state.sql_history.append({
                    "question": prompt,
                    "sql": result['sql'],
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                st.rerun()  # Force refresh to hide suggested questions
                
            else:
                error_msg = f"❌ Error: {result.get('error', 'Unknown error')}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })
                st.rerun()  # Force refresh to hide suggested questions

    # Accept user input and process it through the full engine pipeline
    if prompt := st.chat_input("Ask a question about your HR data..."):
        
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
        
        # Generate response with the engine
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                # Build conversation context for follow-up question support
                context = build_conversation_context(st.session_state.messages)
                augmented_prompt = f"{context}{prompt}" if context else prompt
                result = st.session_state.engine.ask(augmented_prompt, visualize=True, interpret=False)
            
            # Display the response
            if result['success']:
                st.write("Here's what I found:")
                
                # Show SQL query in expandable section
                with st.expander("📝 View SQL Query"):
                    st.code(result['sql'], language="sql")
                
                # Show data table
                if result['data'] is not None and not result['data'].empty:
                    st.dataframe(result['data'], width='stretch')
                    
                    # Show row count
                    st.caption(f"📊 {len(result['data'])} rows returned")
                else:
                    st.info("No data returned from query.")
                
                # Show visualization if available
                if result.get('viz_code'):
                    try:
                        # Provide the DataFrame to the viz code
                        df = result['data']
                        
                        # Execute the visualization code
                        exec(result['viz_code'])
                        fig = plt.gcf()
                        st.pyplot(fig)
                        plt.close(fig)
                    except Exception as e:
                        st.warning(f"Could not display visualization: {str(e)}")
                            
                # Auto-generate Business Insights for Claude users
                if result['data'] is not None and not result['data'].empty:
                    if st.session_state.engine.provider != 'gemini':
                        with st.spinner("Generating business insights..."):
                            insight_text = generate_business_insight(result, st.session_state.engine.client)
                            st.info(insight_text)
                            
                            # Will save with message below
                            result['insight'] = insight_text
                    else:
                        st.caption("💡 Tip: Business insights available with Claude provider")
                
                # Save assistant response to chat history
                message_data = {
                    "role": "assistant",
                    "content": "Here's what I found:",
                    "sql": result['sql'],
                    "data": result['data'],
                    "csv": result['data'].to_csv(index=False) if result['data'] is not None and not result['data'].empty else None,
                    "viz_code": result.get('viz_code'),
                    "question": prompt
                }
                
                # Add insight if generated
                if 'insight' in result:
                    message_data['insight'] = result['insight']
                
                st.session_state.messages.append(message_data)
                # Record to SQL history
                st.session_state.sql_history.append({
                    "question": prompt,
                    "sql": result['sql'],
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                st.rerun()
                
            else:
                # Error occurred
                error_msg = f"❌ Error: {result.get('error', 'Unknown error')}"
                st.error(error_msg)
                
                # Save error to chat history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })


# ============================================================
# Custom SQL Tab
# ============================================================

with tab2:
    st.markdown("### 🔧 Custom SQL Editor")
    st.caption("Write and execute your own SQL queries directly against the HR database.")
    
    # Text area for SQL input
    custom_sql = st.text_area(
        "Enter your SQL query:",
        height=150,
        placeholder="SELECT * FROM employees LIMIT 10;",
        help="Only SELECT queries are allowed. DROP, DELETE, UPDATE and other dangerous operations are blocked."
    )
    
    # Run button
    if st.button("▶ Run Query", type="primary", key="run_custom_sql"):
        if not custom_sql.strip():
            st.warning("Please enter a SQL query first.")
        else:
            # Validate before executing
            is_valid, validation_msg = validate_sql(custom_sql)
            if not is_valid:
                st.error(f"❌ Query blocked: {validation_msg}")
            else:
                with st.spinner("Running query..."):
                    success, result = execute_generated_sql(custom_sql, conn)
                
                if success and result is not None and not result.empty:
                    st.success(f"✅ Query returned {len(result)} rows")
                    st.dataframe(result, width='stretch')
                    
                    # CSV export for custom SQL results
                    csv = result.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results as CSV",
                        data=csv,
                        file_name=f"custom_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key="download_custom_sql"
                    )
                elif success and (result is None or result.empty):
                    st.info("Query executed successfully but returned no results.")
                else:
                    st.error(f"❌ Query failed: {result}")
    
    st.divider()
    st.caption("💡 Tip: Use the Database Schema in the sidebar to explore available tables and columns.")


