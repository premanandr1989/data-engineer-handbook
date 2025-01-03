from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import StreamTableEnvironment, TableConfig, DataTypes
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.window import Session
import os, requests
# env = StreamExecutionEnvironment.get_execution_environment()
# t_env = StreamTableEnvironment.create(env)

# Create source table
def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events_ip'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            host VARCHAR,
            url VARCHAR,
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

# create aggregated table
def create_aggregated_events_sink_postgres(t_env):
    table_name = 'host_ip_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            avg_events_per_session DOUBLE,
            total_sessions BIGINT,
            total_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create postgres source table
        source_table = create_processed_events_sink_postgres(t_env)

        # aggregated_table = create_aggregated_events_sink_postgres(t_env)
        # Create postgres aggregated sink table
        aggregated_sink_table = create_aggregated_events_sink_postgres(t_env)

        host_stats = t_env.sql_query(f"""
                    WITH sessionized_events as (
                            SELECT 
                                ip,
                                host,
                                SESSION_START(timestamp, INTERVAL '5' MINUTES) as session_start,
                                SESSION_END(timestamp, INTERVAL '5' MINUTES) as session_end,
                                COUNT(*) as events_per_session
                            FROM {source_table}
                            GROUP BY ip, host, SESSION(timestamp, INTERVAL '5' MINUTES)
                            )
                            Select
                                host,
                                avg(events_per_session) as avg_events_per_session,
                                count(*) as total_sessions,
                                SUM(events_per_session) as total_events
                            FROM sessionized_events
                    """)
        host_stats.execute_insert(aggregated_sink_table)

    except Exception as e:
        print("Writing records from Flink to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()