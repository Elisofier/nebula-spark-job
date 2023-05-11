package com.aiqiang;


import com.facebook.thrift.protocol.TCompactProtocol;
import com.vesoft.nebula.connector.NebulaConnectionConfig;
import com.vesoft.nebula.connector.WriteNebulaVertexConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import com.vesoft.nebula.connector.connector.package$;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class WriteVertex {

    private static final String SPARK_MASTER = "local";
    private static final String META_ADDRESS = "nebula01:29559";
    private static final String GRAPH_ADDRESS = "nebula01:29669";
    private static final String SPACES = "data_graph";
    private static final String TAG_DATA_FILE_PATH = "/home/nebula-data/account1";
    private static final String TAG_NAME = "account";
    private static final String TAG_VID_FIELD = "_vertexId";

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        Class<?>[] classes = {TCompactProtocol.class};
        sparkConf.registerKryoClasses(classes);

        SparkContext sparkContext = new SparkContext(SPARK_MASTER, "Write_" + TAG_NAME,
                sparkConf);
        SparkSession sparkSession = new SparkSession(sparkContext);
        List<String> jsonFiles = findJsonFiles(TAG_DATA_FILE_PATH);
        for (String jsonFile : jsonFiles) {
            System.out.println(jsonFile);
//            writeData(sparkSession, jsonFile);
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

        NebulaConnectionConfig nebulaConnectionConfig = NebulaConnectionConfig
                .builder()
                .withMetaAddress(META_ADDRESS)
                .withGraphAddress(GRAPH_ADDRESS)
                .withConenctionRetry(2)
                .build();

        //写入vertex数据
        System.out.println("Start to write nebula data [vertex]");
        Dataset<Row> vertexDataset = sparkSession.read().json(filePath);
        vertexDataset.show();
        vertexDataset.persist(StorageLevel.MEMORY_ONLY_SER());
        WriteNebulaVertexConfig writeNebulaVertexConfig = WriteNebulaVertexConfig
                .builder()
                .withSpace(SPACES)
                .withTag(TAG_NAME)
                .withVidField(TAG_VID_FIELD)
                .withVidAsProp(false)
                .withBatch(1000)
                .build();
        DataFrameWriter<Row> vertexDataFrameWriter = new DataFrameWriter<>(vertexDataset);
        package$.MODULE$.NebulaDataFrameWriter(vertexDataFrameWriter)
                .nebula(nebulaConnectionConfig, writeNebulaVertexConfig).writeVertices();
        System.out.println("End to write nebula data [vertex]");

    }


}
