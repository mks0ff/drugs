# Drugs Project
 
how to check json file

list all containers :
 ```bash
 docker ps
 ```
 
connect on the worker node : 
 ```bash
 docker exec -it {{container_id}} /bin/bash
 ```
 
display json file content for drug by id (A01AD) and Name (diphenhydramine) : 
 ```bash
 cat /tmp/Drugs/*/*/*/Id=A01AD/Name=diphenhydramine/*.json
 ```