## Task 1 — Aggregation: top_departments

import sqlite3





def top_departments(db_path):
    conn=sqlite3.connect(db_path)
    cursor=conn.cursor()

    query="""
    select d.name,sum(e.salary) as total_salary
    from employees as e
    join departments as d on e.dept_id=d.dept_id
    group by d.name
    order by total_salary DESC
    limit 3;

"""
    cursor.execute(query)
    results=cursor.fetchall()
    conn.close()
    return results


## Task 2 — JOIN: employees_with_projects


def employees_with_projects(db_path):
    conn=sqlite3.connect(db_path)
    cursor=conn.cursor()

    
    query="""
    select e.name,p.name as project_name
    from employees as e
    inner join project_assignments as pa on e.emp_id=pa.emp_id
    inner join projects p ON pa.project_id = p.project_id;
   
"""
    cursor.execute(query)
    results=cursor.fetchall()
    conn.close()
    return results




## Task 3 — Window Function: salary_rank_by_department

def salary_rank_by_department(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
    SELECT 
        e.name,
        d.name,
        e.salary,
        RANK() OVER (
            PARTITION BY e.dept_id
            ORDER BY e.salary DESC
        ) AS rank
    FROM employees e
    JOIN departments d ON e.dept_id = d.dept_id
    ORDER BY d.name, rank;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    conn.close()

    return results



