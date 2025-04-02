test_envs = [
    {
        'name': "test",
        'active': 0,
        'env': {
            'server_cpu': 16,
            'client_cpu': 16,
            'network': 0,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': 1,
            'target': 'csv',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcclient',
            'tables': ['lineitem_sf10'],
            'table': 'lineitem_sf10'
        }
    },
    {
        'name': "testb",
        'active': 0,
        'env': {
            'server_cpu': 8,
            'client_cpu': 2,
            'network': 500,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': 1,
            'target': 'csv',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcclient',
            'tables': ['lineitem_sf10'],
            'table': 'lineitem_sf10'
        }
    },
    {
        'name': "figurePandasPG",
        'active': 0,
        'env': {
            'server_cpu': 16,
            'client_cpu': 16,
            'network': 0,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'postgres',
            'src_format': 1,
            'target': 'pandas',
            'target_format': 2,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcpython',
            'tables': ['lineitem_sf10', 'ss13husallm', 'iotm', 'inputeventsm']
        }
    },
    {
        'name': "figure_8b",
        'active': 0,
        'env': {
            'server_cpu': 16,
            'client_cpu': 16,
            'network': 0,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': 1,
            'target': 'pandas',
            'target_format': 2,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcpython',
            'tables': ['lineitem_sf10', 'ss13husallm', 'iotm', 'inputeventsm']
        }
    },
    {
        'name': "figure_parquet",
        'active': 0,
        'env': {
            'server_cpu': 16,
            'client_cpu': 16,
            'network': 0,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'parquet',
            'src_format': 2,
            'target': 'pandas',
            'target_format': 2,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcpython',
            'tables': ['lineitem_sf10', 'ss13husallm', 'iotm', 'inputeventsm']
        }
    },
    {
        'name': "figure_parquet_csv",
        'active': 0,
        'env': {
            'server_cpu': 16,
            'client_cpu': 16,
            'network': 0,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'parquet',
            'src_format': 2,
            'target': 'csv',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcclient',
            'tables': ['lineitem_sf10', 'ss13husallm', 'iotm', 'inputeventsm']
        }
    }
]
