from delta.tables import *

def table_exists(spark, database, table):
    try:
        delta_table = DeltaTable.forName(spark, f"{database}.{table}")
        return True
    except:
        return False

def vacuum_table(spark_session, database_name, table_name, retention_hours=24):
    table_address = f"{database_name}.{table_name}"
    try:
        deltaTable = DeltaTable.forName(spark_session, table_address)    # Hive metastore-based tables
        deltaTable.vacuum(retentionHours=retention_hours)
        print(f'vacuum completed for table {table_address}')
    except:
        print(f'error during vacuum for table {table_address}')
        pass


def vacuum_tables_from_database(spark_session, database_name, retention_hours=24):
    table_list_df = spark_session.sql(f"SHOW TABLES FROM {database_name};")
    for table_row in table_list_df.collect():
        vacuum_table(
            spark_session = spark_session, 
            database_name = database_name, 
            table_name = table_row['tableName'], 
            retention_hours = retention_hours)
