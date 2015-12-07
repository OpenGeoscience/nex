## Common

- Create a folder called ```keys``` under deployment/

- Generate key pair for use by sparkstandalone roles called ```master``` and ```master.pub``` and place them in the ```keys/``` folder

## Local Vagrant Deployment

- Run ```git submodule init --update``` to ensure gobig repo is available

- For local copy netCDF data from aws/1997_files.txt into /data/NEX-GDDP-1997/

## AWS Deployment

- place your AWS IAM key-file in a file called keys/aws-NEX.pem

- Make sure ```AWS_ACCESS_KEY_ID``` and ```AWS_SECRET_ACCESS_KEY``` are set as environment variables

- To deploy and provision cluster defined in ```pod_config.yml```  run ```ansible-playbook -b site.yml```

- To set up libraries and development environment run ```ansible-playbook local.yml``` after running ```ansible-playbook -b site.yml```

- To download, convert and import NEX-GDDP data from 1997 into the running hdfs server run ```ansible-playbook import.yml```  (Note this may take some time, e.g., ~10 to 15 minutes)

## Notes

- Use ```pssh -h ~/ip.list -i [command]``` to run a shell command on all datanodes
