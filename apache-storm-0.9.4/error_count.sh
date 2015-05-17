echo "splitter-edge-failures"
cd /home/kandur/research/storm_fault_tolerance/new_storm/custom_release/apache-storm-0.9.4/logs; grep -R 'has failed to get an acknowledgement' . | grep 'sb_eb_ses' | wc -l

echo "edge-centre-failures"
cd /home/kandur/research/storm_fault_tolerance/new_storm/custom_release/apache-storm-0.9.4/logs; grep -R 'has failed to get an acknowledgement' . | grep 'eb_cb_ecs' | wc -l

echo "centre-print-failures"
cd /home/kandur/research/storm_fault_tolerance/new_storm/custom_release/apache-storm-0.9.4/logs; grep -R 'has failed to get an acknowledgement' . | grep 'cb_pb_cas' | wc -l