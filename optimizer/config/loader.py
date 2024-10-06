throughput_data = {
    # TODO: check
    'pandas_csv': {
        2: {
            'read': 2707,
            'deser': 903,
            'comp': 77407,
            # 'send': 3026,
            'send': 5000,
            'rcv': 1944,
            'decomp': 15972,
            'write': 853
        }
    },
    'csv_csv': {
        # analytics
        1: {
            'read': 2583,
            'deser': 881,
            'comp': 77407,
            'send': 4241,
            'rcv': 4241,
            'decomp': 22712,
            'write': 20701,
        },
        # storage
        2: {
            'read': 2707,
            'deser': 903,
            'comp': 77407,
            # 'send': 3026,
            'send': 5000,
            # 'rcv': 3026,
            'rcv': 5000,
            'decomp': 22712,
            'write': 219,
        }
    },
    'csv_postgres': {
        1: {
            'read': 176,
            'deser': 805,
            'comp': 25250,
            'send': 2381,
            'rcv': 2381,
            'decomp': 10272,
            'write': 13974
        }
    },
    'pandas_postgres': {
        2:
            {
                'read': 176,
                'deser': 805,
                'comp': 25250,
                'send': 2381,
                'rcv': 1944,
                'decomp': 15972,
                'write': 853
            }
    }
}

# Upper bounds (in MB/s)
upper_bounds = {
    'pandas_csv': {
        2: {
            'read': 5000,
            'write': 5000,
            'network': 5000
        }
    },
    'csv_csv': {
        # analytics
        1: {
            'read': 5000,
            'write': 5000,
            'send': 3000,
            'rcv': 3000
        },
        # storage
        2: {
            'read': 5000,
            'write': 2000,
            'send': 5000,
            'rcv': 5000
        }
    },
    'csv_postgres': {
        # analytics
        1: {
            'read': 800,
            'write': 5000,
            'send': 3000
        },
        # storage
        2: {
            'read': 850,
            'write': 1400,
            'send': 3000
        }
    },
    'pandas_postgres': {
        # analytics
        1: {
            'read': 800,
            'write': 5000,
            'network': 3000
        },
        # storage
        2: {
            'read': 700,
            'write': 5000,
            'network': 3000
        }
    }
}

default_config = {
    'read_par': 1,
    'deser_par': 8,
    'comp_par': 4,
    'send_par': 1,
    'rcv_par': 1,
    'decomp_par': 4,
    'write_par': 16,
    'compression_lib': 'zstd'
}

comp_throughputs = {
    'read_par': {1: 2583},
    'deserialization_par': {1: 881},
    'compression_nocomp_par': {1: 77407},
    'compression_snappy_par': {1: 688},
    'compression_lzo_par': {1: 610},
    'compression_lz4_par': {1: 622},
    'compression_zstd_par': {1: 405},
    'send_par': {1: 4241},
    'receive_par': {1: 4241},
    'decompression_nocomp_par': {1: 22712},
    'decompression_snappy_par': {1: 1426},
    'decompression_lzo_par': {1: 710},
    'decompression_lz4_par': {1: 1625},
    'decompression_zstd_par': {1: 1240},
    'write_par': {1: 20701},
}
