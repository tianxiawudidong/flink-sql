package com.ifchange.flink.sql.hive;

import org.apache.flink.table.filesystem.PartitionCommitPolicy;
import org.apache.flink.hive.shaded.parquet.example.data.Group;
import org.apache.flink.hive.shaded.parquet.hadoop.ParquetFileReader;
import org.apache.flink.hive.shaded.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.flink.hive.shaded.parquet.hadoop.ParquetReader;
import org.apache.flink.hive.shaded.parquet.hadoop.ParquetWriter;
import org.apache.flink.hive.shaded.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.flink.hive.shaded.parquet.hadoop.example.GroupReadSupport;
import org.apache.flink.hive.shaded.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.flink.hive.shaded.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.flink.hive.shaded.parquet.hadoop.util.HadoopInputFile;
import org.apache.flink.hive.shaded.parquet.schema.MessageType;
import org.apache.flink.table.filesystem.PartitionCommitPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * flink hive parquet file compact
 */
public class ParquetFileMergingCommitPolicy implements PartitionCommitPolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFileMergingCommitPolicy.class);

    @Override
    public void commit(Context context) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String partitionPath = context.partitionPath().getPath();

        List<Path> files = listAllFiles(fs, new Path(partitionPath), "part-");
        LOGGER.info("{} files in path {}", files.size(), partitionPath);

        MessageType schema = getParquetSchema(files, conf);
        if (schema == null) {
            return;
        }
        LOGGER.info("Fetched parquet schema: {}", schema.toString());

        Path result = merge(partitionPath, schema, files, fs);
        LOGGER.info("Files merged into {}", result.toString());
    }

    /**
     * 列出目录下所有的文件
     */
    private List<Path> listAllFiles(FileSystem fs, Path dir, String prefix) throws IOException {
        List<Path> result = new ArrayList<>();

        RemoteIterator<LocatedFileStatus> dirIterator = fs.listFiles(dir, false);
        while (dirIterator.hasNext()) {
            LocatedFileStatus fileStatus = dirIterator.next();
            Path filePath = fileStatus.getPath();
            if (fileStatus.isFile() && filePath.getName().startsWith(prefix)) {
                result.add(filePath);
            }
        }
        return result;
    }

    /**
     * 获取schema
     */
    private MessageType getParquetSchema(List<Path> files, Configuration conf) throws IOException {
        if (files.size() == 0) {
            return null;
        }

        HadoopInputFile inputFile = HadoopInputFile.fromPath(files.get(0), conf);
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        ParquetMetadata metadata = reader.getFooter();
        MessageType schema = metadata.getFileMetaData().getSchema();

        reader.close();
        return schema;
    }

    /**
     * 合并策略
     */
    private Path merge(String partitionPath, MessageType schema, List<Path> files, FileSystem fs) throws IOException {
        Path mergeDest = new Path(partitionPath + "/result-" + System.currentTimeMillis() + ".parquet");
        ParquetWriter<Group> writer = ExampleParquetWriter.builder(mergeDest)
            .withType(schema)
            .withConf(fs.getConf())
            .withWriteMode(Mode.CREATE)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build();

        for (Path file : files) {
            ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
                .withConf(fs.getConf())
                .build();
            Group data;
            while ((data = reader.read()) != null) {
                writer.write(data);
            }
            reader.close();
        }
        writer.close();

        for (Path file : files) {
            fs.delete(file, false);
        }

        return mergeDest;
    }

}
