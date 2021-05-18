
# Part 1 . Setup (Window) - Run MapReduce/Spark in local mode.
1. add HADOOP_HOME variable to environment variables

value = C:\workspace\MIU\BD\HADOOP, HADOOP folder data from this GitHub. 
![img.png](img.png)

![img_1.png](img_1.png)

- Restart Intellij IDE, now you can run the mapreduce / spark locally [Debuggable]



# Part 2 . Pseudo Mode - Single Cluster Hadoop Instance [Cloudera] 
1. expose daemon docker port 2375 to intellij
   ![img_13.png](img_13.png)
   ![img_3.png](img_3.png)

   Click to "Apply & Restart"

   - [intellij] in Services tab, click on Docker  
     ![img_4.png](img_4.png)
   - click on play icon on Services and Docker connected to Intellij
     ![img_5.png](img_5.png)
     
2. run Docker images :
- by command 
   > docker compose -f docker\docker-compose.yml up -d

- or run it from intellij IDE 
![img_2.png](img_2.png)

![img_6.png](img_6.png)

- popup appears on window , allow Docker to share with your local file --> press "Share It"
![img_7.png](img_7.png)
- after Cloudera installed
![img_8.png](img_8.png)

- now you can submit jars file to execute on map-reduce   
3. upgrade docker cloudera image to java 8 [spark need to write code on java 8]
    - folow the guide line in this file : docker\upgrade-java8-cloudera.txt

    - check to make sure cloudera run as java 8 version
    > [root@quickstart /]# java -version
   
4. submit the spark jars to hadoop.
- build jars file and public to share folder, run both "jar" and "publish" tasks
![img_10.png](img_10.png)
  
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

