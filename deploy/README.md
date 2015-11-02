## Common

- Create a folder called ```keys``` under deployment/

- Generate key pair for use by sparkstandalone roles called ```master``` and ```master.pub``` and place them in the ```keys/``` folder

## Local Vagrant Deployment

- Run ```git submodule init --update``` to ensure gobig repo is available

- For local copy netCDF data from aws/1997_files.txt into /data/NEX-GDDP-1997/

## AWS Deployment

- place your AWS IAM account credentials in a file called keys/aws-NEX.pem

- Make sure ```AWS_ACCESS_KEY_ID``` and ```AWS_SECRET_ACCESS_KEY``` are set as environment variables


- To deploy and provision cluster defined in ```pod_config.yml```  run ```ansible-playbook -b site.yml```

- To set up libraries and development environment run ```ansible-playbook local.yml``` after running ```ansible-playbook -b site.yml```

- To download files from 1997 run the following command:
  ```parallel --slf <(cat /home/ubuntu/ip.list | sed -e 's/^/1\//g') --joblog /data/tmp/log -a /home/ubuntu/1997_http_files.txt wget --timeout=10 -P /data/tmp/ {}```
  (Should take around 60 to 70 seconds)



## Notes

- Use ```pssh -h ~/ip.list -i [command]``` to run a shell command on all datanodes
