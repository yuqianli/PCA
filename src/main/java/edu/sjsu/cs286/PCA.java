package edu.sjsu.cs286;

import java.util.LinkedList;

import org.apache.spark.api.java.*;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class PCA {
  public static void main(String[] args) throws FileNotFoundException {
    SparkConf conf = new SparkConf().setAppName("PCA");
    SparkContext sc = new SparkContext(conf);
	
	if (args.length < 3) {
      System.err.println(
        "Usage: PCA <input_file> <principal_components> <output_file>");
      System.exit(1);
    }
	
	Scanner file = new Scanner(new File(args[0]));
    double[][] array = new double [150][4];
    
    if(file.hasNextLine()){   
        for (int i=0; i < array.length; i++)
        {    
           String line = file.nextLine();
            Scanner scanner = new Scanner(line);
            scanner.useDelimiter(",");
            
                for (int j=0; j < array[i].length ; j++)
                { 
                    if(scanner.hasNextDouble()){
                        array[i][j]= scanner.nextDouble();
                         }   
                }          
        }        
    }
	
    LinkedList<Vector> rowsList = new LinkedList<Vector>();
    for (int i = 0; i < array.length; i++) {
      Vector currentRow = Vectors.dense(array[i]);
      rowsList.add(currentRow);
    }
    JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);

    // Create a RowMatrix from JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());

    // Compute the top 3 principal components.
    Matrix pc = mat.computePrincipalComponents(Integer.parseInt(args[1]));
    RowMatrix projected = mat.multiply(pc);
	RDD<Vector> output  = projected.rows();
    output.saveAsTextFile(args[2]);
  }
}
