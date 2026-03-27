# Session 14: Project 2 — HR Analytics with AI-Assisted SQL

## Overview

Build an **AI-powered data analytics tool** that converts plain English questions into SQL queries, executes them against a real HR database, and automatically generates visualizations — all wrapped in a Streamlit web app.

**Estimated time: 6-10 hours**

---

## Quick Start

### 1. Install Dependencies

```bash
pip install google-genai pandas matplotlib seaborn streamlit
```

### 2. Get a Gemini API Key

1. Go to [Google AI Studio](https://aistudio.google.com/apikey)
2. Click "Create API Key"
3. Copy the key — you'll paste it into the notebook

### 3. Open the Notebook

```bash
jupyter notebook Session_14_Text2SQL_Project.ipynb
```

### 4. Work Through Parts 1-7

| Part | What You Do | Time |
|------|-------------|------|
| **Part 1** | Load the database, explore the schema | 30 min |
| **Part 2** | Complete 17 TODOs in `text2sql_engine.py` | 2-3 hours |
| **Part 3** | Answer 12 business questions | 1-2 hours |
| **Part 4** | Build an LLM-powered auto-visualizer | 1-1.5 hours |
| **Part 5** | Practice prompt engineering | 45 min |
| **Part 6** | Build a Streamlit web app (TODOs in `text2sql_app.py`) | 1-1.5 hours |
| **Part 7** | Reflection | 30 min |

### 5. Run the Streamlit App

```bash
streamlit run text2sql_app.py
```

---

## Project Structure

```
session_14/
├── README.md                          ← You are here
├── Session_14_Text2SQL_Project.ipynb   ← Main project notebook
│
├── data/                              ← HR database (8 CSV files, 1,300+ rows)
│   ├── departments.csv                  8 departments
│   ├── job_titles.csv                   38 job titles with salary bands
│   ├── employees.csv                    120 employees
│   ├── salary_history.csv               308 salary change records
│   ├── projects.csv                     20 company projects
│   ├── project_assignments.csv          138 employee-project assignments
│   ├── performance_reviews.csv          293 annual reviews (2022-2024)
│   └── training_records.csv             418 training enrollments
│
├── schema/                            ← Database documentation
│   ├── SCHEMA.md                        Full schema with ER diagram
│   └── er_diagram.mermaid               Mermaid ER diagram (view at mermaid.live)
│
├── db_utils.py                        ← PROVIDED: Database utilities (do not modify)
├── text2sql_engine.py                 ← YOUR WORK: 17 TODOs to complete
├── text2sql_app.py                    ← YOUR WORK: Streamlit app with TODOs
│
└── solution/                          ← Reference solutions (try first!)
    ├── text2sql_engine.py               Complete engine with all TODOs filled
    └── text2sql_app.py                  Complete Streamlit app with extensions
```

---

## The Database

TechCorp is a mid-size technology company with 120 employees across 8 departments. The database models real HR data:

| Table | Rows | Description |
|-------|------|-------------|
| `departments` | 8 | Organizational units with budgets |
| `job_titles` | 38 | Roles with salary bands and career levels |
| `employees` | 120 | Employee records (name, salary, status, work mode) |
| `salary_history` | 308 | Every salary change with dates and reasons |
| `projects` | 20 | Company projects with budgets and status |
| `project_assignments` | 138 | Who works on which project (many-to-many) |
| `performance_reviews` | 293 | Annual ratings 2022-2024 |
| `training_records` | 418 | Course enrollments and completions |

See `schema/SCHEMA.md` for the complete ER diagram and column descriptions.

---

## What You're Building

### The Text-to-SQL Pipeline

```
 "What is the average salary    →  AI builds the  →  Safety    →  Execute   →  Results
  by department?"                   SQL query         validator     against      + chart
                                                      checks it    SQLite       + insight
```

### Key Components

1. **Schema Context Builder** — Gives the AI a complete picture of the database
2. **SQL Safety Validator** — Blocks dangerous queries (DROP, DELETE, etc.)
3. **Response Parser** — Extracts clean SQL from AI responses
4. **SQL Generator** — Converts English to SQL using Google Gemini
5. **Auto-Visualizer** — Generates matplotlib/seaborn code for results
6. **Streamlit App** — Chat interface for non-technical users

---

## TODO Summary

### text2sql_engine.py (17 TODOs)

| TODO | Function | What to Implement |
|------|----------|-------------------|
| 1-3 | `get_schema_for_prompt()` | Build schema description for AI |
| 4-6 | `validate_sql()` | Safety checks for generated SQL |
| 7-8 | `extract_sql_from_response()` | Parse SQL from AI responses |
| 9-10 | `generate_sql()` | Build prompt and call Gemini API |
| 11-12 | `execute_generated_sql()` | Validate then execute |
| 13-15 | `generate_visualization_code()` | AI-generated chart code |
| 16-17 | `Text2SQLEngine` class | Wire everything together |

### text2sql_app.py (5 main + 5 optional TODOs)

| TODO | What to Implement |
|------|-------------------|
| 18-19 | Database connection and schema display |
| 20-21 | Session state and engine initialization |
| 22-23 | Chat history display and query processing |
| A-E | Optional: history, custom SQL, memory, export, suggestions |

---

## Tips

- **Work in order**: The TODOs build on each other (1 → 2 → 3 → ... → 17)
- **Test as you go**: The notebook has test cells after each section
- **Read the docstrings**: Each TODO has detailed hints in the comments
- **Use the solution**: If you're stuck for more than 15 minutes, peek at `solution/`
- **Restart kernel**: After editing `.py` files, restart the Jupyter kernel or use `importlib.reload()`

---

## Prerequisites

- **Sessions 11-12**: SQL fundamentals (SELECT, JOIN, GROUP BY, subqueries)
- **Session 13**: Python-SQL integration (sqlite3, pd.read_sql_query) + Streamlit basics
- **Python**: Functions, string formatting, try/except, regex basics
