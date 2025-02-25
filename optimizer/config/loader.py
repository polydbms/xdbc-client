# Upper bounds (in MB/s)
upper_bounds = {
    'postgres_postgres': {
        2: {
            'read': 1000,
            'write': 5000
        }
    },
    'spark_postgres': {
        2: {
            'read': 1000,
            'write': 7000
        }
    },
    'pandas_csv': {
        2: {
            'read': 5000,
            'write': 5000
        }
    },
    'csv_csv': {
        1: {
            'read': 5000,
            'write': 5000
        },
        2: {
            'read': 5000,
            'write': 3000
        }
    },
    'csv_postgres': {
        1: {
            'read': 1000,
            'write': 5000
        },
        2: {
            'read': 1000,
            'write': 1400
        }
    },
    'pandas_postgres': {
        # analytics
        1: {
            'read': 1000,
            'write': 5000
        },
        # storage
        2: {
            'read': 1000,
            'write': 5000
        }
    }
}

default_config = {
    'read_par': 1,
    'deser_par': 1,
    'comp_par': 1,
    'send_par': 1,
    'rcv_par': 1,
    'decomp_par': 1,
    'ser_par': 1,
    'write_par': 1,
    'compression_lib': 'nocomp',
    'buffer_size': 1024,
    'server_buffpool_size': 1024 * 4 * 3,
    'client_buffpool_size': 1024 * 4 * 3,
    'format': 1,
    'skip_ser': 0,
    'skip_deser': 0
}
parquet_config = {
    'read_par': 1,
    'deser_par': 1,
    'comp_par': 1,
    'send_par': 1,
    'rcv_par': 1,
    'decomp_par': 1,
    'ser_par': 1,
    'write_par': 1,
    'compression_lib': 'nocomp',
    'buffer_size': 65536,
    'server_buffpool_size': 65536 * 4 * 3,
    'client_buffpool_size': 65536 * 4 * 3,
    'format': 1,
    'skip_ser': 0,
    'skip_deser': 0
}
