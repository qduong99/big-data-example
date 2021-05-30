# Install Docker
First install Docker on your machine using the instructions from official website:
https://docs.docker.com/install/

After installing it, check if Docker is installed correctly opening your terminal and typing the following command:

> $ docker -v
>
>  Docker version 20.10.6, build 370c289

# Check out sample project from github

https://github.com/qduong99/big-data-example

pull github project to C:\workspace\MIU\big-data-example ( or any path you want - make sure to change below path to match with your project)

JAVA JDK 8 

# Part 1 . Setup (Window) - Run MapReduce/Spark in local mode.
1. add HADOOP_HOME variable to environment variables

value = C:\workspace\MIU\big-data-example\HADOOP, HADOOP folder data from this GitHub.

![img_15.png](img_15.png)

![img_1.png](img_1.png)

- Restart Intellij IDE, now you can run the mapreduce / spark locally [Debuggable]

![img_14.png](img_14.png)

# Part 2 . Pseudo Mode - Single Cluster Hadoop Instance [Cloudera] 
1. expose daemon docker port 2375 to intellij : Open Docker For Desktop -> Setting :
   
   ![img_13.png](img_13.png)
   ![img_3.png](img_3.png)

   Click to "Apply & Restart"

   - [intellij] in Services tab, click on Docker 
     
   ![img_4.png](img_4.png)
   ![img_16.png](img_16.png)
   
   - click on play icon on Services and Docker connected to Intellij
     
     ![img_5.png](img_5.png)
     
2. run Docker images :
- by command 
   > docker compose -f docker\docker-compose.yml up -d

- or run it from intellij IDE 

![img_19.png](img_19.png)

![img_6.png](img_6.png)

- popup appears on window , allow Docker to share with your local file --> press "Share It"

![img_7.png](img_7.png)
- Cloudera installed

![img_8.png](img_8.png)

- now you can submit jars file to execute on map-reduce   
3. build jars file and public to share folder, run both "jar" then "publish" tasks

  ![img_10.png](img_10.png)


2. submit the jars to hadoop.

- in "Services" tab in Intellij, right click on cloudera image, click on "create terminal"
  
  ![img_11.png](img_11.png)

- execute the hadoop command to run map-reduce :
> hadoop fs -mkdir input
>
> hadoop fs -put /DATA/input/dataFile.txt input
>
> hadoop fs -cat input/dataFile.txt

> hadoop jar /DATA/jars/BD.jar edu.miu.mapreduce.WordCount input outputHDFS
>
> hadoop fs -ls outputHDFS
>
> hadoop fs -cat outputHDFS/part-r-00000


3. submit the spark jars to hadoop. 

> hadoop jar /DATA/jars/BD.jar edu.miu.mapreduce.SparkWordCount input outputHDFS2
>


