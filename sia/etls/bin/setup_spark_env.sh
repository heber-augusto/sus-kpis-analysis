echo "starting spark env setup ";

echo "installing and downloading packages";

apt-get update  > /dev/null
apt-get install openjdk-8-jdk-headless -qq > /dev/null
wget -q https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
tar xf spark-3.4.1-bin-hadoop3.tgz

wget -q https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

echo "setting enviroment variables";

mv "${1}gcs-connector-hadoop3-latest.jar" "${1}/spark-3.4.1-bin-hadoop3/jars"
