# ETL-Task
This repository has been created to develop a simple ETL task. This task has been designed to test the ability of PAIR Finance applicants. You can find the task description in Docs/Task.pdf .<br />
<br />
This task is totally about:<br />
  * Working with docker<br />
  * ETL process<br />
  * Scheduling and ...<br />
<br />
To Develop and test the task, I used :

-Programing language: Python 3.8<br />
-OS: Ubuntu 22<br />
and PyCharm IDEA Community Edition<br />

# About project
1-One of the most important steps in system development (as a data engineer) is to make the infrastructure fault tolerant. One of the influential issues in this field is functionality. For this reason, **packaging** has been tried in this project. You can find packages and utils in the **analytics/bin/-** directory.<br />

2-Although the pre-processing and schema registry is one of the important steps, due to data transfer from DB to DB, this problem has caused less concern. However, in some cases, casting has been used for this purpose.<br />

3-For scheduling, three solutions can be chosen.<br />
- Cron tab: ```RUN crontab -l | { cat; echo "* * * * * bash /root/get_date.sh"; } | crontab -```<br />
- Python Schedule library: 
```
import schedule
schedule.every(1).hour.do(job)
schedule.run_pending()
``` 
- and the simplest one that is implemented in the code using sleep().<br />

4-**Pandas** and **Numpy** libraries could also be used to work with the data, but it was preferred not to use them due to the complexity-less of the calculations.

# Project structure
<pre>
<br />
* You can find file path, etc. in config.
* You can find sample inputs/outputs in the data directory.
|
|--->main
|     |--->Dockerfile
|     |--->main.py
|--->analytics
|     |--->Dockerfile
|     |--->analytics.py
|     |--->bin
|            |--->dbUtils
|            |--->utils
|            |--->config (set initial values like PATH)
|--->Docs (include **result.png** )
|--->docker-compose.yml
</pre>
# Future improvement
1. To improve monitoring, we can use **logger** library and insert logs into a database.
2. It is better to extend dbUtils as a standard package and make **analytics.py** cleaner.
3. With generating dimension (with DIM tables), we can access to some more statistics quickly, like the number of unique devices, which device is turned off (unexpectedly), etc.
# How to run 
I had to manage the docker because of some fatal error. So, I did the steps listed below:
1. Change Python:3-slim to Python:3.8-slim
2. Cahnge dockerfile headers and add some new libraries like **cryptography**
3. Finally, I used this command to run: ``` sudo docker-compose up --build ```

# Output
**Images are stored in docs directory**
