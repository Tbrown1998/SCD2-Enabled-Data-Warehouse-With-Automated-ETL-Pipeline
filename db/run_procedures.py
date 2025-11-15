# db/run_procedures.py

import logging

def run_stored_procedure(conn, proc_name):
    """
    Execute a stored procedure in PostgreSQL.
    Supports CALL proc_name() and SELECT proc_name().
    """
    cur = conn.cursor()
    try:
        sql = f"CALL {proc_name}();"
        cur.execute(sql)
        logging.info(f"=== Successfully executed procedure: {proc_name} ===")
    except Exception as e:
        logging.error(f"=== Error executing procedure {proc_name}: {e} ===")
        conn.rollback()
    finally:
        cur.close()


def run_procedure_sequence(conn, procedures):
    """
    Run a list of procedures in order.
    """
    for proc in procedures:
        run_stored_procedure(conn, proc)
