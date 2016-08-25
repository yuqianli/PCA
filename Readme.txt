To build:
mvn package

To run:
[user01@mapr1node PCA]$ /opt/mapr/spark/spark-1.2.1/bin/spark-submit --class edu.sjsu.cs286.PCA --master local[1] ./target/PCA-1.0-SNAPSHOT.jar ./iris.txt 2 /user/user01/PCA/output
