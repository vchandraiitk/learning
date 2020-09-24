import org.apache.spark.sql.SparkSession;

public class SparkContext {

    public static void main(String[] args) {
        SparkSession spark1 = SparkSession
                .builder()
                .appName("Java Spark SQL basic example2")
                .config("spark.master", "local")
                .getOrCreate();
        spark1.stop();

        SparkSession spark2 = SparkSession
                .builder()
                .appName("Java Spark SQL basic example1")
                .config("spark.master", "local")
                .getOrCreate();

        System.out.println(spark1.hashCode());
        System.out.println(spark2.hashCode());
        System.out.println(spark1.sparkContext().hashCode());
        System.out.println(spark2.sparkContext().hashCode());



    }
}
