## Commands

### Launch
Assuming you have set up a file with local overrides in ```local_vars.yml```
```bash
 ansible-playbook -e @local_vars.yml launch.yml
```

### Provision
Assuming you have set up a file with local overrides in ```local_vars.yml```
```bash
ansible-playbook -i cumulus.py  -e @local_vars.yml provision.yml
```

## Useful Tags

- **provision:** Only run provisioning tasks
- **provision_{users|nfs|hdfs|...}:** Run provisioning tasks for just that role

- **restart:** Start/restart all services and daemons
- **restart_{nfs|hdfs|...}:** Start/restart a specific daemon
- **update_hdfs_configs:** Re-distribute files like core-site.xml and hdfs-site.xml after they have been changed locally
