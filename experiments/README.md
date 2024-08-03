# XDBC Client

## Setting the env

- Build postgres container (instructions in the xdb repo under ``docker/postgres``)
- Build xdbc-client and xdbc-server containers through their respective ``make`` files
- Start the xdbc & postgres environment: ``docker compose -f docker-xdbc.yml``
- Load data & create test table with ``create_test_table_and_import.sql``
- Start the "traffic controller": ``docker compose -f docker-tc.yml``

## Run experiments

- Stard xdbc-client and xdbc-server
- If you want to only
  run: ``./run_experiments.sh $SERVER_PATH/experiments $CLIENT_PATH/experiments $BUILD_OPT_SERVER $BUILD_OPT_CLIENT``
- For example: ``./run_experiments.sh /workspace/xdbc-server/experiments /xdbc-client/experiments 0 0``
- If you want to rebuild set ``$BUILD_OPT_x`` to `1`