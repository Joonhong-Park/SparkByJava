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
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;
import scala.Tuple2;


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

        // JavaRDD 객체 생성
        JavaRDD<Row> fulldata = lines.map(v1 -> {
            FlightDataWritable flightDataWritable = v1._2;
            String count = flightDataWritable.getCount();
            String dest_country_name = flightDataWritable.getDEST_COUNTRY_NAME();
            String origin_country_name = flightDataWritable.getORIGIN_COUNTRY_NAME();
            return new GenericRow(new Object[]{dest_country_name, origin_country_name, count});
        });

        // 헤더 정보 제거
        Row header = fulldata.first();

        /**
         * Scala Code : var onlydata = fulldata.filter(v1 => v1 != header)
         * Java는 !=로 값을 비교할 수 없으므로 equals()를 사용하여
         * 아래와 같이 사용하며 주석의 내용과 같은 의미임
         */

        JavaRDD<Row> onlydata = fulldata.filter(v1 -> {
            return !v1.equals(header);
                });

//        JavaRDD<Row> onlydata = fulldata.filter(v1 -> {
//            if (v1.equals(header)) {
//                return false;
//            }else{
//                return true;
//            }
//        });

        StructField field1 = DataTypes.createStructField("DEST_COUNTRY_NAME", DataTypes.StringType, true);
        StructField field2 = DataTypes.createStructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, true);
        StructField field3 = DataTypes.createStructField("Count", DataTypes.StringType, true);

        /**
         * Schema 생성방법 1
         */
//        StructType schema = new StructType();
//
//        schema.add(field1);
//        schema.add(field2);
//        schema.add(field3);

        /**
         * Schema 생성방법 2
         */
        StructType schema = new StructType(
                new StructField[]{
                        field1,
                        field2,
                        field3
                }
        );

        /**
         * Schema 생성방법 3
         * Schema 내용들을 담은 Class를 만들어 아래와 같이 DF 생성가능
         */

//        Dataset<Row> df = spark.createDataFrame(fulldata, Schema.class);


        SQLContext spark = new SQLContext(sc);

        Dataset<Row> df = spark.createDataFrame(onlydata, schema);

        df.show();

//        for (Tuple2<LongWritable, FlightDataWritable> line : lines.take(10)) {
//            FlightDataWritable fdw = line._2;
//            System.out.println(fdw.getORIGIN_COUNTRY_NAME() + " >> " + fdw.getDEST_COUNTRY_NAME() + " :: " + fdw.getCount());
//        }

    }
}
