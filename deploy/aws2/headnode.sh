#!/bin/bash

###
#  Log in to headnode by querying AWS for the IP address


IP_ADDR=$(aws --profile NEX ec2 \
              describe-instances \
              --filter "Name=tag:ec2_pod_instance_name,Values=master" \
              --filter "Name=tag:ec2_pod,Values=kotfic_NEX" \
              --query Reservations[0].Instances[0].PublicIpAddress \
              --output text)

ssh -i ../keys/aws-NEX.pem ubuntu@$IP_ADDR $*
