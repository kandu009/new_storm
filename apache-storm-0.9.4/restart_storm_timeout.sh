echo "cleaning up current release directory"
cd /home/kandur/research/storm_fault_tolerance/new_storm/custom_release/; rm -rf *

echo "copying the new release tar"
cp /home/kandur/research/storm_fault_tolerance/new_storm/apache-storm-0.9.4/storm-dist/binary/target/apache-storm-0.9.4.tar.gz /home/kandur/research/storm_fault_tolerance/new_storm/custom_release/

echo "untar the storm release file"
cd /home/kandur/research/storm_fault_tolerance/new_storm/custom_release/; tar xvzf apache-storm-0.9.4.tar.gz

echo "building the release storm jar"
cd /home/kandur/research/storm_fault_tolerance/new_storm/custom_release/apache-storm-0.9.4/examples/storm-starter/; mvn assembly:assembly

echo "copying the config file storm.yaml"
/bin/cp /home/kandur/.storm/storm.yaml /home/kandur/research/storm_fault_tolerance/new_storm/custom_release/apache-storm-0.9.4/conf/storm.yaml

echo "restarting zookeeper"
cd /home/kandur/research/zookeeper-3.4.6; ./bin/zkServer.sh restart 
