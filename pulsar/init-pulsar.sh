#!/bin/bash
echo "sleeping for 18 seconds..."
sleep 18s
echo "tenants create tenant-a..."
/pulsar/bin/pulsar-admin tenants create tenant-a
echo "namespaces create tenant-a/entity-demo..."
/pulsar/bin/pulsar-admin namespaces create tenant-a/entity-demo
echo "namespaces list tenant-a..."
/pulsar/bin/pulsar-admin namespaces list tenant-a
echo "namespaces set-retention tenant-a/entity-demo --size 10G --time -1..."
/pulsar/bin/pulsar-admin namespaces set-retention tenant-a/entity-demo --size 10G --time -1
echo "topics create-partitioned-topic persistent://tenant-a/entity-demo/user-events --partitions 3..."
/pulsar/bin/pulsar-admin topics create-partitioned-topic persistent://tenant-a/entity-demo/user-events --partitions 3
echo "topics list tenant-a/entity-demo...."
/pulsar/bin/pulsar-admin topics list tenant-a/entity-demo
echo "topics get-partitioned-topic-metadata persistent://tenant-a/entity-demo/user-events..."
/pulsar/bin/pulsar-admin topics get-partitioned-topic-metadata persistent://tenant-a/entity-demo/user-events
echo "topics partitioned-lookup persistent://tenant-a/entity-demo/user-events..."
/pulsar/bin/pulsar-admin topics partitioned-lookup persistent://tenant-a/entity-demo/user-events