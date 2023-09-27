echo "starting spark env setup ";

echo "installing and downloading packages";

apt-get update  > /dev/null
apt-get install openjdk-8-jdk-headless -qq > /dev/null
wget -q https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
tar xf spark-3.4.1-bin-hadoop3.tgz

wget -q https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

echo "setting enviroment variables";

export PYTHONHASHSEED=1234
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_HOME="${1}spark-3.4.1-bin-hadoop3"
export SPARK_VERSION=3.4.1

mv "${1}gcs-connector-hadoop3-latest.jar" $SPARK_HOME/jars
