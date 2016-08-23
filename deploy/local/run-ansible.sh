#!/bin/bash

ANSIBLE_BIN=$(which ansible-playbook)
${ANSIBLE_BIN:?"Could not find ansible bin"} 1>/dev/null 2>&1

$ANSIBLE_BIN -e "ansible_user=vagrant" -i .vagrant/provisioners/ansible/inventory/vagrant_ansible_inventory "$@"
