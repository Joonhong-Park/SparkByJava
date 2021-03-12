import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCSV {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage : ReadCSV <input file path>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("Java Spark Read CSV")
                .master("yarn")
                .getOrCreate();

        CSVtoDataFrame(spark, args);
    }

    private static void CSVtoDataFrame(SparkSession spark, String[] args) throws Exception {

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(args[0]);

        df.show();
        df.printSchema();
        DataFrameWriter<Row> write = df.write();
        write.save("spark_output");
    }
}
