from datetime import datetime

#################################### AWS RDS Queries ###################################
SELECT_EMP_SAL = """
select * from finance.emp_sal
"""

SELECT_EMP_DETAIL = """
select * from hr.emp_details
"""

################################## Snowflake DWH Queries ###############################
SELECT_DWH_EMP_DIM = """
select * from airflow_iti.dimensions.employee_dim
"""

def INSERT_INTO_DWH_EMP_DIM(rows_to_insert):
    sql = f"""
    INSERT INTO airflow_iti.dimensions.employee_dim (emp_id, name, marital_status, num_children, address, phone_number, job, hire_date, salary, effective_start_date, effective_end_date, active)
    VALUES {rows_to_insert}"""
    
    return sql

def UPDATE_DWH_EMP_DIM(ids_to_update):
    sql = f"""
    UPDATE airflow_iti.dimensions.employee_dim
    SET effective_end_date = '{datetime.now().date().strftime("%Y-%m-%d")}',
    active = FALSE
    WHERE emp_id IN ({ids_to_update}) AND active = TRUE """
    return sql
