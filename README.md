## CSCI4180 Assignments

## Prerequisites 

Deploy:

- ssh
- ansible

Build:

- java
- gradle

## Deploy Hadoop

- Connect to the CSE VPN
- Launch 3 VMs on OpenStack with the access key set to `./keys/ubuntu.pub`.
- Edit `./ansible/hosts.yml` with the correct ports according to your VM IPs.
- Run deployment script

```
$ cd ansible
$ ansible-playbook main.yml
```

- Access VMs

```
$ ssh -i keys/vm1-hadoop -p 122XX hadoop@137.189.89.214
$ ssh -i keys/vm2-hadoop -p 122YY hadoop@137.189.89.214
$ ssh -i keys/vm3-hadoop -p 122ZZ hadoop@137.189.89.214
```

- Start Hadoop

```
hadoop@vm1:~$ ./hadoop-init.sh
```

## Run programs on Hadoop

- Copy data to VM

```
$ scp -i keys/vm1-hadoop -P 122XX -r data/KJV12.TXT hadoop@137.189.89.214:
```

- Import data to HDFS

```
hadoop@vm1:~$ hadoop fs -mkdir /KJV12
hadoop@vm1:~$ hadoop fs -put KJV12.TXT /KJV12
```

- Compile all programs into an uberjar & upload it to the cluster

```
$ gradle shadowJar
$ scp -i keys/vm1-hadoop -P 122XX build/libs/assg1-all.jar hadoop@137.189.89.214:
```

- Run program

```
hadoop@vm1:~$ ./hadoop-run-job.sh assg1-all.jar <PROGRAM> <IN DIR> <ARGS...>
```

## Helper Script

```
$ ./compile-and-run.sh <PROGRAM> <IN DIR> <ARGS...>
```

