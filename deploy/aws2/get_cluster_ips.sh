#!/bin/bash

aws --profile=NEX ec2 describe-instances \
    --filter="Name=tag:ec2_pod_instance_name,Values=data,persist" \
    --query Reservations[*].Instances[*].PrivateIpAddress | \
    # Remove json formating
    sed -e 's/\[//g' | sed -e 's/\]//g' | sed -e 's/\"//g' | sed -e 's/,//g' | \
    # Remove space and newlines
    tr -d " " | grep -v "^$" > ip.list
