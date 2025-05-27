from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    # table_id = 'your-project.your_dataset.your_table_name'
    
    table_id = 'gcp-data-pipeline-460310.uber_data_engineering_yt.fact_table'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            # DataFrame(data['fact_table']),
            (df['fact_table']),
            # (df(value)),
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

    table_id = 'gcp-data-pipeline-460310.uber_data_engineering_yt.datetime_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            # DataFrame(data['fact_table']),
            (df['datetime_dim']),
            # (df(value)),
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

    table_id = 'gcp-data-pipeline-460310.uber_data_engineering_yt.passenger_count_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            # DataFrame(data['fact_table']),
            (df['passenger_count_dim']),
            # (df(value)),
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

    table_id = 'gcp-data-pipeline-460310.uber_data_engineering_yt.trip_distance_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            # DataFrame(data['fact_table']),
            (df['trip_distance_dim']),
            # (df(value)),
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

    table_id = 'gcp-data-pipeline-460310.uber_data_engineering_yt.rate_code_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            # DataFrame(data['fact_table']),
            (df['rate_code_dim']),
            # (df(value)),
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

    table_id = 'gcp-data-pipeline-460310.uber_data_engineering_yt.pickup_location_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            # DataFrame(data['fact_table']),
            (df['pickup_location_dim']),
            # (df(value)),
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

    table_id = 'gcp-data-pipeline-460310.uber_data_engineering_yt.dropoff_location_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            # DataFrame(data['fact_table']),
            (df['dropoff_location_dim']),
            # (df(value)),
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

    table_id = 'gcp-data-pipeline-460310.uber_data_engineering_yt.payment_type_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            # DataFrame(data['fact_table']),
            (df['payment_type_dim']),
            # (df(value)),
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )



#automate table creation
    # config_path = path.join(get_repo_path(), 'io_config.yaml')
    # config_profile = 'default'
    
    # for key, value in df:
    
    #     table_id = 'gcp-data-pipeline-460310.uber_data_engineering_yt.{}'.format(key)

    #     BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
    #         # DataFrame(data['fact_table']),
    #         # (df['fact_table']),
    #         (df(value)),
    #         table_id,
    #         if_exists='replace',  # Specify resolution policy if table name already exists
    #     )
