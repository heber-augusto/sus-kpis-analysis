from delta.tables import *

def vacuum_tables_from_database(spark_session, database_name, retention_hours=24):
    table_list_df = spark_session.sql(f"SHOW TABLES FROM {database_name};")
    for table_row in table_list_df.collect():
        table_name = f"{table_row['namespace']}.{table_row['tableName']}"
        try:
            deltaTable = DeltaTable.forName(spark_session, table_name)    # Hive metastore-based tables
            deltaTable.vacuum(retentionHours=retention_hours)
            print(f'vacuum completed for table {table_name}')
        except:
            print(f'error during vacuum for table {table_name}')
            continue
