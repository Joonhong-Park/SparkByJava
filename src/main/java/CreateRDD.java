import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

public class CreateRDD {

    public static void main(String[] args) throws Exception {

        if (args.length < 1){
            System.err.println("Usage : ReadCSV <input file path>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("Java Spark Read CSV")
                .getOrCreate();

        CSVtoRDD(spark,args);

    }

    private static void CSVtoRDD(SparkSession spark, String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> rdd = sc.textFile(args[0]);

        JavaRDD<String[]> rdd_split = rdd.map(line -> line.split(",",0));
        JavaRDD<Row> rddOfRows = rdd_split.map(RowFactory::create);

        rddOfRows.take(10).forEach(System.out::println);

        //List<String> take = rdd.take(10);
        //System.out.println("take = " + take);
    }

}
