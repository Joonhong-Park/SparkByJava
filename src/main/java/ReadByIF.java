import input.FlightDataInputFormat;
import input.FlightDataWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SQLContext;


public class ReadByIF {
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage : ReadByIF <input file path>");
            System.exit(1);
        }

        SparkConf sparkconf = new SparkConf().setMaster("yarn").setAppName("InputFormat");

        CSVtoDataFrame(sparkconf, args);
    }

    public static void CSVtoDataFrame(SparkConf sparkconf, String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(sparkconf);

        Configuration conf = new Configuration();
        
        JavaPairRDD<LongWritable, FlightDataWritable> lines = sc.newAPIHadoopFile(args[0], FlightDataInputFormat.class, LongWritable.class, FlightDataWritable.class, conf);

        JavaRDD<FlightDataWritable> fulldata = lines.map(v1 -> v1._2);

//        FlightDataWritable head = fulldata.first();
//
//        JavaRDD<FlightDataWritable> data_nohead = fulldata.filter(v1 -> v1 != head);

//        StructType schema = new StructType();
//        schema.add(DataTypes.createStructField(head.getDEST_COUNTRY_NAME(), DataTypes.StringType, true));
//        schema.add(DataTypes.createStructField(head.getORIGIN_COUNTRY_NAME(), DataTypes.StringType, true));
//        schema.add(DataTypes.createStructField(head.getCount(), DataTypes.StringType, true));
//
//        SQLContext sqc = new SQLContext(sc);
//        Dataset<Row> df = sqc.createDataFrame();



        for (Tuple2<LongWritable, FlightDataWritable> line : lines.take(10)) {
            FlightDataWritable fdw = line._2;
            System.out.println(fdw.getORIGIN_COUNTRY_NAME() + " >> " + fdw.getDEST_COUNTRY_NAME() + " :: " + fdw.getCount());
        }

    }
}
