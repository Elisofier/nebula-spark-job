package com.aiqiang;

import com.facebook.thrift.protocol.TCompactProtocol;
import com.vesoft.nebula.connector.NebulaConnectionConfig;
import com.vesoft.nebula.connector.WriteNebulaEdgeConfig;
import com.vesoft.nebula.connector.WriteNebulaVertexConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import com.vesoft.nebula.connector.connector.package$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


/**
 * @author aiqiang
 * @version 1.0.0
 * @date 2023/4/20
 */
public class WriteEdge {

    private static final String SPARK_MASTER = "local";
    private static final String META_ADDRESS = "nebula01:29559";
    private static final String GRAPH_ADDRESS = "nebula01:29669";
    private static final String SPACES = "data_graph";

    private static final String EDGE_FILE_PATH = "/data/nebula-data/edge/edge_have";

    private static final String EDGE_NAME = "have";

    private static final String EDGE_SRC = "_srcId";

    private static final String EDGE_DST = "_dstId";

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        Class<?>[] classes = {TCompactProtocol.class};
        sparkConf.registerKryoClasses(classes);

        SparkContext sparkContext = new SparkContext(SPARK_MASTER, "Write_" + EDGE_NAME,
                sparkConf);
        SparkSession sparkSession = new SparkSession(sparkContext);
        List<String> jsonFiles = findJsonFiles(EDGE_FILE_PATH);
        for (String jsonFile : jsonFiles) {
//            writeData(sparkSession, jsonFile);
            System.out.println(jsonFile);
        }

        sparkSession.close();
        System.exit(0);

    }

    private static List<String> findJsonFiles(String directoryPath) {
        List<String> jsonFiles = new ArrayList<>();
        File directory = new File(directoryPath);

        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("The provided path is not a directory.");
        }

        File[] files = directory.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    jsonFiles.add(file.getAbsolutePath());
                } else if (file.isDirectory()) {
                    jsonFiles.addAll(findJsonFiles(file.getAbsolutePath()));
                }
            }
        }

        return jsonFiles;
    }

    private static void writeData(SparkSession sparkSession, String filePath) {

        // build connection config
        NebulaConnectionConfig nebulaConnectionConfig = NebulaConnectionConfig
                .builder()
                .withMetaAddress(META_ADDRESS)
                .withGraphAddress(GRAPH_ADDRESS)
                .withConenctionRetry(2)
                .build();


        //写入edge数据
        System.out.println("Start to write nebula data [edge]");
        Dataset<Row> edgeDataset = sparkSession.read().json(filePath);
        edgeDataset.show();
        edgeDataset.persist(StorageLevel.MEMORY_ONLY_SER());
        WriteNebulaEdgeConfig writeNebulaEdgeConfig = WriteNebulaEdgeConfig
                .builder()
                .withSpace(SPACES)
                .withEdge(EDGE_NAME)
                .withSrcIdField(EDGE_SRC)
                .withDstIdField(EDGE_DST)
                .withRankField("_rank")
                .withSrcAsProperty(false)
                .withDstAsProperty(false)
                .withRankAsProperty(false)
                .withBatch(1000)
                .build();
        DataFrameWriter<Row> edgeDataFrameWriter = new DataFrameWriter<>(edgeDataset);
        package$.MODULE$.NebulaDataFrameWriter(edgeDataFrameWriter)
                .nebula(nebulaConnectionConfig, writeNebulaEdgeConfig).writeEdges();
        System.out.println("End to write nebula data [edge]");

    }


}
