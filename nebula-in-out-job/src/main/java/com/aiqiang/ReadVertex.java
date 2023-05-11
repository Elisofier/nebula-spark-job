package com.aiqiang;

import com.facebook.thrift.protocol.TCompactProtocol;
import com.vesoft.nebula.connector.NebulaConnectionConfig;
import com.vesoft.nebula.connector.ReadNebulaConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.vesoft.nebula.connector.connector.package$;

/**
 * @author aiqiang
 * @version 1.0.0
 * @date 2023/4/20
 */
public class ReadVertex {
    // 创建SparkConf对象

    private static final String SPARK_MASTER = "local";
    private static final String META_ADDRESS = "nebula-metad01:30101";
    private static final String SPACES = "version2";
    static String TAG_NAME = "account";

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        Class<?>[] classes = {TCompactProtocol.class};
        sparkConf.registerKryoClasses(classes);

        SparkContext sparkContext = new SparkContext(SPARK_MASTER, "read-" + TAG_NAME,
                sparkConf);
        SparkSession sparkSession = new SparkSession(sparkContext);

        readData(sparkSession);

        sparkSession.close();
        System.exit(0);

    }

    private static void readData(SparkSession sparkSession) {

        NebulaConnectionConfig nebulaConnectionConfig = NebulaConnectionConfig
                .builder()
                .withMetaAddress(META_ADDRESS)
                .withConenctionRetry(2)
                .withTimeout(600000)
                .build();

        readVertex(sparkSession, nebulaConnectionConfig);
    }

    private static void readVertex(SparkSession sparkSession,
                                   NebulaConnectionConfig nebulaConnectionConfig) {

        ReadNebulaConfig readNebulaConfig = ReadNebulaConfig
                .builder()
                .withSpace(SPACES)
                .withLabel(TAG_NAME)
                .withNoColumn(false)
                .build();
        DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
        Dataset<Row> dataset = package$.MODULE$.NebulaDataFrameReader(dataFrameReader)
                .nebula(nebulaConnectionConfig, readNebulaConfig).loadVerticesToDF();
        System.out.println("Vertices schema");
        dataset.printSchema();
        dataset.show(20);
        System.out.println("Vertices nums:" + dataset.count());
        // 将DataFrame写入本地JSON文件
        dataset.write().json("/home/nebula-data/entity_" + TAG_NAME);
    }

}
