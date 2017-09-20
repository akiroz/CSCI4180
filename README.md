## CSCI4180 Assignment 1
Hadoop Setup and Word Count

## Prerequisites 

- ansible

## Deploy Hadoop

- Launch 3 VMs on OpenStack with the access key set to `./keys/ubuntu.pub`.
- Edit `./ansible/hosts.yml` with the correct ports according to your VM IPs.

```
$ ## add VM host keys to known_hosts
$ ssh -i keys/ubuntu -p 122XX ubuntu@137.189.89.214 exit
$ ssh -i keys/ubuntu -p 122YY ubuntu@137.189.89.214 exit
$ ssh -i keys/ubuntu -p 122ZZ ubuntu@137.189.89.214 exit
$ ## Run deployment script
$ cd ansible
$ ansible-playbook main.yml
$ ## Access VMs with everything setup
$ ssh -i keys/vm1-hadoop -p 122XX hadoop@137.189.89.214
$ ssh -i keys/vm2-hadoop -p 122YY hadoop@137.189.89.214
$ ssh -i keys/vm3-hadoop -p 122ZZ hadoop@137.189.89.214
```

## Run WordCount on Hadoop

TODO: write this....
