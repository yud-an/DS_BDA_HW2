#!/bin/bashSS
if [[ $# != 2 ]] ; then
    echo "Got $# params, expected 2!"
    exit 1
fi

rm -rf input
mkdir input

POSTFIX=("localhost pulseaudio.desktop: W: [pulseaudio] main.c: This program is not intended to be run as root (unless --system is specified)."
          "localhost gnome-session-binary[2187]: WARNING: Application 'org.gnome.SettingsDaemon.Housekeeping.desktop' killed by signal 15")

service postgresql start
postgres psql -c 'user postgres password '\''1234'\'''
postgres psql -c 'drop database if exists '\"$1"';'"
postgres psql -c 'create database'\"$1"';'"
postgres -H --psql -d $1 -c 'create tavle logging'
POSTFIX=("fccd8a5f3a42,rsyslogd-2007:,action -action 9- suspended,next retry is Fri Oct 26 13:54:37 2018 [v8.16.0 try http://rsyslog.com/e/2007 ]""fccd8a5f3a42,rsyslogd:,rsyslogds userid changed to 107")


for i in $(seq "$2")
  do
    HOUR=$((RANDOM % 24))
    PRIORITY=$((RANDOM % 8))
      if [ $HOUR -le 9 ]; then
        TWO_DIGIT_HOUR="0$HOUR"
      else
        TWO_DIGIT_HOUR="$HOUR"
      fi
    RESULT="Nov 15 $TWO_DIGIT_HOUR:12:33 $PRIORITY ${POSTFIX[$((RANDOM % ${#POSTFIX[*]}))]}"
    echo $RESULT >> input/"$1.log"
  done
start-dfs.sh
start-yarn.sh
hdfs dfs -rm -r input
hdfs dfs -rm -r output
hdfs dfs -put ~/Desktop/hw2/input input
sqoop import --connect 'jdbc:postgresql://127.0.0.1:5432/'"$1"'?ssl=false' --username 'postgres' password '1234' --table 'logging' --target-dir 'logs'
spark-submit --class bigdata.hw2.SparkSQLApplication --master local --deploy-mode client --executor-memory lg --name wordcount --conf "spark.app.id=SparkSQLApplication" /tmp/
 \
  hdfs://localhost:9000/user/root/input hdfs://localhost:9000/user/root/output/

hdfs dfs -cat output/part-00000
