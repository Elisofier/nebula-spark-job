package com.aiqiang;

import com.facebook.thrift.protocol.TCompactProtocol;
import com.vesoft.nebula.connector.NebulaConnectionConfig;
import com.vesoft.nebula.connector.ReadNebulaConfig;
import com.vesoft.nebula.connector.connector.package$;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadEdge {

    private static final String SPARK_MASTER = "local";

    private static final String META_ADDRESS = "nebula-metad01:30101";

    private static final String SPACES = "version2";

    private static final String EDGE_NAME = "work_in";

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        Class<?>[] classes = {TCompactProtocol.class};
        sparkConf.registerKryoClasses(classes);

        SparkContext sparkContext = new SparkContext(SPARK_MASTER, "read_" + EDGE_NAME,
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

        readEdges(sparkSession, nebulaConnectionConfig);
    }


    private static void readEdges(SparkSession sparkSession,
                                  NebulaConnectionConfig nebulaConnectionConfig) {
        ReadNebulaConfig readNebulaConfig = ReadNebulaConfig
                .builder()
                .withSpace(SPACES)
                .withLabel(EDGE_NAME)
                .withNoColumn(true)
                .build();
        DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
        Dataset<Row> dataset = package$.MODULE$.NebulaDataFrameReader(dataFrameReader)
                .nebula(nebulaConnectionConfig, readNebulaConfig).loadEdgesToDF();
        System.out.println("Edge schema");
        dataset.printSchema();
        dataset.show(20);
        System.out.println("Edge nums:" + dataset.count());
        // 将DataFrame写入本地JSON文件
        dataset.write().json("/data/nebula-data/edge_" + EDGE_NAME);
    }


}
