/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.deltasharing.parquet;

import com.google.common.collect.ImmutableList;
import io.trino.deltasharing.DeltaSharingColumn;
import io.trino.deltasharing.models.DeltaFile;
import io.trino.parquet.*;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.*;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.*;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static org.apache.parquet.io.ColumnIOUtil.columnDefinitionLevel;
import static org.apache.parquet.io.ColumnIOUtil.columnRepetitionLevel;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static io.trino.deltasharing.parquet.ParquetTypeTranslator.fromParquetType;
import static org.joda.time.DateTimeZone.UTC;

public class ParquetPlugin
        implements FilePlugin
{
    @Override
    public List<DeltaSharingColumn> getFields(String path, String parquetFileDirectory, Function<String, InputStream> streamProvider,String prefix)
    {
        System.out.println("getFields =================================");
        path = getLocalPath(path, streamProvider,parquetFileDirectory,Optional.of(getFileName(path)),prefix);
        System.out.println(path);
        MessageType schema = getSchema(new File(path));
        return schema.getFields().stream()
                .map(field -> new DeltaSharingColumn(
                        field.getName(),
                        fromParquetType(field)))
                .collect(Collectors.toList());
    }

    public String getFileName(String path){
        final String regex = ".*\\/(.+)\\.parquet\\?.*";
        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(path);
        String fileName = null;
        if (matcher.find()) {
            System.out.println("Full match: " + matcher.group(0));

            for (int i = 1; i <= matcher.groupCount(); i++) {
                fileName =  matcher.group(i);
                System.out.println("Group " + i + ": " +fileName);
            }
        }
        return fileName;
    }

//    @Override
    public Iterable<Page> getPagesIterator(String path, Function<String, InputStream> streamProvider,String parquetFileDirectory,String prefix)
    {
        path = getLocalPath(path, streamProvider,parquetFileDirectory,Optional.of(getFileName(path)),prefix);
        ParquetReader reader = getReader(new File(path));

        ImmutableList.Builder<Type> trinoTypes = ImmutableList.builder();
        ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
        ImmutableList.Builder<Boolean> rowIndexChannels = ImmutableList.builder();

        // TODO assuming all columns are being read, populate the above lists
        MessageType schema = getSchema(new File(path));
        MessageColumnIO messageColumnIO = getColumnIO(schema, new MessageType(schema.getName(), schema.getFields()));
        schema.getFields().forEach(field -> {
            Type trinoType = fromParquetType(field);
            trinoTypes.add(trinoType);
            rowIndexChannels.add(false);
            internalFields.add(constructField(trinoType, messageColumnIO.getChild(field.getName())));
        });

        ParquetPageSource pageSource = new ParquetPageSource(reader, trinoTypes.build(), rowIndexChannels.build(), internalFields.build());
        List<Page> result = new LinkedList<>();
        Page page;
        while ((page = pageSource.getNextPage()) != null) {
            result.add(page.getLoadedPage());
        }
        return result;
    }
//    @Override
//    public Iterable<Page> getPagesIterator(String path, Function<String, InputStream> streamProvider)
//    {
//
//        List<String> filePaths = List.of("https://tf-benchmarking.s3.amazonaws.com/delta_2/dwh/test_hm/part-00000-e3bd56f7-7979-4f94-9bba-44d8496597f6-c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIA3GXK5AWRNOYRTVMP%2F20230422%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230422T195655Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJGMEQCIH9PXF7vwZaYSYX1OppCFm4OIwkJIo35Pv0MoDhVbkUOAiAjM0Vc%2FxLV%2Fh7ABx49GZ437aXwe%2FGTOjlnijNhIWBPaiqoAwjD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAIaDDc3MDM2NTUyMzM2MiIMajq0oBf0PergXGxPKvwCS8Poj3vu16vfPlFJP2NlKFynk4NSX6K1GY5smAS6wurEAdKSyiOH1jLT0w0yi%2B3yD6LxSrXnCa%2FmAk5p5I45KmSbm5FkCypp99nv3%2Fqa5eHkSa5J3TCLqoZxmFmzJWrkJNkOFAQS9RIGC2WDIxU2Z6Xh7SvJMdY4%2FEjEQEAlyKGya4pPYPQXOkAjOZlUUipuA9CJuXoxpmmd%2B3ppzJNtnIEaWpc9YrsSoWNnh3PY6aZgbKh3dCxGDUNQoDGgYNgzuzstfRot16qCcMkX%2BZuaWtkrv%2FjxjA1UCr5%2FEisrtcz5%2FUAZfwJStgumjCn%2BZHYO6RTRkSexiyv6a%2BHho1xTYgx0elvQE2qWxjrPK6bJeyW8W3uJdQQClWLag0anqyGxxQxAaBlye3Vd1nDmquHBUgnHFCEFFpzXL0FeqqYOlMT18sN8HF6JwdEp2NdTKrN9SiXA5U8AE8RHsU2XCRazcSx9EBX%2F%2Bx8HYo6DtE0VX6ZoWvfzHOJ4mAhwYsEwmMWQogY6pwG7icvmY8q5it7AGK2UtMaAR%2FBwW5y2%2B00%2FDr6Ia62q8e7wc38JUotLe9mBrn3WkMVfdRV2f6F8D2VixIV49IR3Zle4h3pHf0VfH6KHHI%2BsYRb0oi2wJrJfVg%2Fn%2BTvh6U1qd5jAT7wObjJPyW8RkXo24kGCZjaw%2F%2FqzYmtW8A3qZZvedytbI4DHr2kJOHLBxaMLg%2BCnxSz2oby6cTx4xGifqwgr54yk3g%3D%3D&X-Amz-Signature=92e9d3894e6a84252e83303b8482d8996a66991adf1143d84d422cf9fcc0209e",
//            "https://tf-benchmarking.s3.amazonaws.com/delta_2/dwh/test_hm/part-00000-e3bd56f7-7979-4f94-9bba-44d8496597f6-c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIA3GXK5AWRNOYRTVMP%2F20230422%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230422T195655Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJGMEQCIH9PXF7vwZaYSYX1OppCFm4OIwkJIo35Pv0MoDhVbkUOAiAjM0Vc%2FxLV%2Fh7ABx49GZ437aXwe%2FGTOjlnijNhIWBPaiqoAwjD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAIaDDc3MDM2NTUyMzM2MiIMajq0oBf0PergXGxPKvwCS8Poj3vu16vfPlFJP2NlKFynk4NSX6K1GY5smAS6wurEAdKSyiOH1jLT0w0yi%2B3yD6LxSrXnCa%2FmAk5p5I45KmSbm5FkCypp99nv3%2Fqa5eHkSa5J3TCLqoZxmFmzJWrkJNkOFAQS9RIGC2WDIxU2Z6Xh7SvJMdY4%2FEjEQEAlyKGya4pPYPQXOkAjOZlUUipuA9CJuXoxpmmd%2B3ppzJNtnIEaWpc9YrsSoWNnh3PY6aZgbKh3dCxGDUNQoDGgYNgzuzstfRot16qCcMkX%2BZuaWtkrv%2FjxjA1UCr5%2FEisrtcz5%2FUAZfwJStgumjCn%2BZHYO6RTRkSexiyv6a%2BHho1xTYgx0elvQE2qWxjrPK6bJeyW8W3uJdQQClWLag0anqyGxxQxAaBlye3Vd1nDmquHBUgnHFCEFFpzXL0FeqqYOlMT18sN8HF6JwdEp2NdTKrN9SiXA5U8AE8RHsU2XCRazcSx9EBX%2F%2Bx8HYo6DtE0VX6ZoWvfzHOJ4mAhwYsEwmMWQogY6pwG7icvmY8q5it7AGK2UtMaAR%2FBwW5y2%2B00%2FDr6Ia62q8e7wc38JUotLe9mBrn3WkMVfdRV2f6F8D2VixIV49IR3Zle4h3pHf0VfH6KHHI%2BsYRb0oi2wJrJfVg%2Fn%2BTvh6U1qd5jAT7wObjJPyW8RkXo24kGCZjaw%2F%2FqzYmtW8A3qZZvedytbI4DHr2kJOHLBxaMLg%2BCnxSz2oby6cTx4xGifqwgr54yk3g%3D%3D&X-Amz-Signature=92e9d3894e6a84252e83303b8482d8996a66991adf1143d84d422cf9fcc0209e",
//            "https://tf-benchmarking.s3.amazonaws.com/delta_2/dwh/test_hm/part-00000-e3bd56f7-7979-4f94-9bba-44d8496597f6-c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIA3GXK5AWRNOYRTVMP%2F20230422%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230422T195655Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJGMEQCIH9PXF7vwZaYSYX1OppCFm4OIwkJIo35Pv0MoDhVbkUOAiAjM0Vc%2FxLV%2Fh7ABx49GZ437aXwe%2FGTOjlnijNhIWBPaiqoAwjD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAIaDDc3MDM2NTUyMzM2MiIMajq0oBf0PergXGxPKvwCS8Poj3vu16vfPlFJP2NlKFynk4NSX6K1GY5smAS6wurEAdKSyiOH1jLT0w0yi%2B3yD6LxSrXnCa%2FmAk5p5I45KmSbm5FkCypp99nv3%2Fqa5eHkSa5J3TCLqoZxmFmzJWrkJNkOFAQS9RIGC2WDIxU2Z6Xh7SvJMdY4%2FEjEQEAlyKGya4pPYPQXOkAjOZlUUipuA9CJuXoxpmmd%2B3ppzJNtnIEaWpc9YrsSoWNnh3PY6aZgbKh3dCxGDUNQoDGgYNgzuzstfRot16qCcMkX%2BZuaWtkrv%2FjxjA1UCr5%2FEisrtcz5%2FUAZfwJStgumjCn%2BZHYO6RTRkSexiyv6a%2BHho1xTYgx0elvQE2qWxjrPK6bJeyW8W3uJdQQClWLag0anqyGxxQxAaBlye3Vd1nDmquHBUgnHFCEFFpzXL0FeqqYOlMT18sN8HF6JwdEp2NdTKrN9SiXA5U8AE8RHsU2XCRazcSx9EBX%2F%2Bx8HYo6DtE0VX6ZoWvfzHOJ4mAhwYsEwmMWQogY6pwG7icvmY8q5it7AGK2UtMaAR%2FBwW5y2%2B00%2FDr6Ia62q8e7wc38JUotLe9mBrn3WkMVfdRV2f6F8D2VixIV49IR3Zle4h3pHf0VfH6KHHI%2BsYRb0oi2wJrJfVg%2Fn%2BTvh6U1qd5jAT7wObjJPyW8RkXo24kGCZjaw%2F%2FqzYmtW8A3qZZvedytbI4DHr2kJOHLBxaMLg%2BCnxSz2oby6cTx4xGifqwgr54yk3g%3D%3D&X-Amz-Signature=92e9d3894e6a84252e83303b8482d8996a66991adf1143d84d422cf9fcc0209e");
//        List<Page> result = new LinkedList<>();
//        for (int i=0;i<filePaths.size();i++){
//            String path1 = filePaths.get(i);
//            System.out.println("Reading file " + path1);
//            String s=Integer.toString(i);
//            path = getLocalPath(path1, streamProvider,Optional.of(s));
//            ParquetReader reader = getReader(new File(path));
//
//            ImmutableList.Builder<Type> trinoTypes = ImmutableList.builder();
//            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
//            ImmutableList.Builder<Boolean> rowIndexChannels = ImmutableList.builder();
//
//            // TODO assuming all columns are being read, populate the above lists
//            MessageType schema = getSchema(new File(path));
//            MessageColumnIO messageColumnIO = getColumnIO(schema, new MessageType(schema.getName(), schema.getFields()));
//            schema.getFields().forEach(field -> {
//                Type trinoType = fromParquetType(field);
//                trinoTypes.add(trinoType);
//                rowIndexChannels.add(false);
//                internalFields.add(constructField(trinoType, messageColumnIO.getChild(field.getName())));
//            });
//
//            ParquetPageSource pageSource = new ParquetPageSource(reader, trinoTypes.build(), rowIndexChannels.build(), internalFields.build());
//
//            Page page;
//            while ((page = pageSource.getNextPage()) != null) {
//                result.add(page.getLoadedPage());
//            }
//        }
//
//
//        return result;
//    }

    public void CreateDirectoryIfNotExists(String directoryName){
        File directory = new File(directoryName);
        if (! directory.exists()){
            directory.mkdirs();
        }
    }

    public boolean FileExists(String fp){
        File filePath = new File(fp);
        return filePath.exists();
    }

    private String getLocalPath(String path, Function<String, InputStream> streamProvider,String parquetFileDirectory, Optional<String> fileName,String prefix)
    {
        if (path.startsWith("http://") || path.startsWith("https://") || path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("s3://")) {
            System.out.println("================================================get local path");
            try (AutoDeletingTempFile tempFile = new AutoDeletingTempFile()) {
                String filePath = parquetFileDirectory + prefix + "/";
                CreateDirectoryIfNotExists(filePath);
                System.out.println("================================================"+filePath);
                if (fileName.isPresent()){
                    filePath+=(fileName.get()+".parquet");
                }
                else{
                    filePath+=".parquet";
                }
                System.out.println("getLocalFile: "+filePath);
                if (!FileExists(filePath)){
                    // we can use StandardCopyOption.COPY_ATTRIBUTES but this also may download the
                    // parquet file as stream and replaces attributes of local parquet
                    // file to avoid network overhead adding this check
                    Files.copy(streamProvider.apply(path), Path.of(filePath), StandardCopyOption.REPLACE_EXISTING);
                }
                return filePath;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (path.startsWith("file:")) {
            return path.substring(5);
        }
        return path;
    }

    private MessageType getSchema(File file)
    {
        ParquetReaderOptions options = new ParquetReaderOptions();
        MessageType fileSchema = null;
        ParquetDataSource dataSource = null;
        try {
            dataSource = new FileParquetDataSource(file, options);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            fileSchema = fileMetaData.getSchema();
        }
        catch (Exception e) {
            handleException(file, dataSource, e);
        }
        return fileSchema;
    }

    private ParquetReader getReader(File file)
    {
        ParquetReaderOptions options = new ParquetReaderOptions();
        MessageType fileSchema;
        MessageType requestedSchema;
        MessageColumnIO messageColumn;
        ParquetReader parquetReader = null;
        ParquetDataSource dataSource = null;
        try {
            dataSource = new FileParquetDataSource(file, options);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            fileSchema = fileMetaData.getSchema();

            requestedSchema = new MessageType(fileSchema.getName(), fileSchema.getFields());
            messageColumn = getColumnIO(fileSchema, requestedSchema);

            long nextStart = 0;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                blocks.add(block);
                blockStarts.add(nextStart);
                nextStart += block.getRowCount();
            }
            parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumn,
                    blocks.build(),
                    blockStarts.build(),
                    dataSource,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    options);
        }
        catch (Exception e) {
            handleException(file, dataSource, e);
        }
        return parquetReader;
    }

    private void handleException(File file, ParquetDataSource dataSource, Exception e)
    {
        try {
            if (dataSource != null) {
                dataSource.close();
            }
        }
        catch (IOException ignored) {
        }
        if (e instanceof TrinoException) {
            throw (TrinoException) e;
        }
        if (e instanceof ParquetCorruptionException) {
            throw new TrinoException(HIVE_BAD_DATA, e);
        }
        if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                e instanceof FileNotFoundException) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
        }
        String message = format("Error opening Parquet file %s: %s", file, e.getMessage());
        throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
    }

    private static TrinoException handleException(Exception e)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read temporary data", e);
    }

    public static class AutoDeletingTempFile
            implements AutoCloseable
    {
        private final String directory;
        private final File file;

        public AutoDeletingTempFile()
                throws IOException
        {   directory = "/Users/cb-it-01-1834/chargebee/research/opensource/trino-storage/";
            file = File.createTempFile("trino-storage-", ".parquet");
        }
        public String getDirectory(){
            return directory;
        }
        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
                throws IOException
        {
            if (!file.delete()) {
                throw new IOException(format("Failed to delete temp file %s", file));
            }
        }
    }

    public static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnRepetitionLevel(columnIO);
        int definitionLevel = columnDefinitionLevel(columnIO);
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field rowField = fields.get(i);
                String name = rowField.getName().orElseThrow().toLowerCase(Locale.ENGLISH);
                Optional<Field> field = constructField(rowField.getType(), lookupColumnByName(groupColumnIO, name));
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0));
            Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            Optional<Field> field = constructField(arrayType.getElementType(), getArrayElementColumn(groupColumnIO.getChild(0)));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        RichColumnDescriptor column = new RichColumnDescriptor(primitiveColumnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
        return Optional.of(new PrimitiveField(type, repetitionLevel, definitionLevel, required, column, primitiveColumnIO.getId()));
    }
}
