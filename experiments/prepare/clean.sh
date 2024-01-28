rm -f /dev/shm/lineitem_sf10_*.csv
rm -f /dev/shm/test_10000000_*.csv
#docker exec xdbcserver bash -c "rm -f /dev/shm/lineitem_sf10_*.csv"
docker exec xdbcclient bash -c "rm -f /dev/shm/output*.csv"
cp ~/lineitem_sf10.csv /dev/shm/lineitem_sf10.csv
cp ~/test_10000000.csv /dev/shm/test_10000000.csv