package org.deltasharing;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import org.apache.calcite.rel.stream.Delta;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.deltasharing.reader.ParquetPageSourceFactory;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;
import javax.swing.text.StyledEditorKit;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.predicateMatches;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetOptimizedNestedReaderEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetOptimizedReaderEnabled;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.*;
//import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createParquetPageSource;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.*;
import static org.deltasharing.DeltaSharingErrorCode.*;
import static org.deltasharing.reader.ParquetPageSourceFactory.createParquetPageSource;

public class DeltaSharingPageSourceProvider implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;

    private final FileFormatDataSourceStats dataSourceStats;

    private final ParquetReaderOptions options;
//    private final DateTimeZone timeZone;
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;


    @Inject
    public DeltaSharingPageSourceProvider(TrinoFileSystemFactory fileSystemFactory,
                                          FileFormatDataSourceStats dataSourceStats,
                                          ParquetReaderConfig parquetReaderConfig) {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.dataSourceStats = requireNonNull(dataSourceStats, "dataSourceStats is null");
        this.options = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
//        this.timeZone = DateTimeZone.forID(TimeZone.getDefault().getID());
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
                                                ConnectorSession session,
                                                ConnectorSplit split,
                                                ConnectorTableHandle table,
                                                List<ColumnHandle> columns,
                                                DynamicFilter dynamicFilter) {

        DeltaSharingSplit deltaSharingSplit = (DeltaSharingSplit) split;
        Path path = new Path(deltaSharingSplit.getPath());
        DateTimeZone timeZone = DateTimeZone.forID(TimeZone.getDefault().getID());
        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());
        // just send regular columns to create parquet page source
        // for partition columns, separate blocks will be created
        List<HiveColumnHandle> regularColumns = hiveColumns.stream()
                .filter(columnHandle -> !columnHandle.isPartitionKey() && !columnHandle.isHidden())
                .collect(Collectors.toList());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(path.toString(), deltaSharingSplit.getFileSize());
        ConnectorPageSource dataPageSource = createPageSource(session, regularColumns, (DeltaSharingSplit) split, inputFile, dataSourceStats, options, timeZone);

        return dataPageSource;
    }

    private static ConnectorPageSource createPageSource(
            ConnectorSession session,
            List<HiveColumnHandle> columns,
            DeltaSharingSplit deltaSharingSplit,
            TrinoInputFile inputFile,
            FileFormatDataSourceStats dataSourceStats,
            ParquetReaderOptions options,
            DateTimeZone timeZone)
    {
        ParquetDataSource dataSource = null;
        Path path = new Path(deltaSharingSplit.getPath());
        long start = 0;
        long length = 100;
        Boolean useColumnNames = true;
        try {
            dataSource = new TrinoParquetDataSource(inputFile, options, dataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = getParquetMessageType(columns, useColumnNames, fileSchema);

            MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);

            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = options.isIgnoreStatistics()
                    ? TupleDomain.all()
                    : getParquetTupleDomain(descriptorsByPath, TupleDomain.none(), fileSchema, useColumnNames);

            TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, timeZone);

            long nextStart = 0;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
            ImmutableList.Builder<Optional<ColumnIndexStore>> columnIndexes = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                Optional<ColumnIndexStore> columnIndex = getColumnIndexStore(dataSource, block, descriptorsByPath, parquetTupleDomain, options);
                if (start <= firstDataPage && firstDataPage < start + length
                        && predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, columnIndex, Optional.empty(),
                        timeZone, DOMAIN_COMPACTION_THRESHOLD)) {
                    blocks.add(block);
                    blockStarts.add(nextStart);
                    columnIndexes.add(columnIndex);
                }
                nextStart += block.getRowCount();
            }

            Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);
            List<HiveColumnHandle> baseColumns = readerProjections.map(projection ->
                            projection.get().stream()
                                    .map(HiveColumnHandle.class::cast)
                                    .collect(toUnmodifiableList()))
                    .orElse(columns);
            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetDataSource finalDataSource = dataSource;
            ParquetPageSourceFactory.ParquetReaderProvider parquetReaderProvider = fields -> new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    fields,
                    blocks.build(),
                    blockStarts.build(),
                    finalDataSource,
                    timeZone,
                    newSimpleAggregatedMemoryContext(),
                    options.withBatchColumnReaders(isParquetOptimizedReaderEnabled(session))
                            .withBatchNestedColumnReaders(isParquetOptimizedNestedReaderEnabled(session)),
                    exception -> handleException(dataSourceId, exception),
                    Optional.of(parquetPredicate),
                    columnIndexes.build(),
                    Optional.empty());
            return createParquetPageSource(baseColumns, fileSchema, messageColumn, useColumnNames, parquetReaderProvider);
        }
        catch (IOException | RuntimeException e) {
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
                throw new TrinoException(DELTA_SHARING_BAD_DATA, e);
            }
            String message = "Error opening Hudi split %s (offset=%s, length=%s): %s".formatted(path, start, length, e.getMessage());
            throw new TrinoException(DELTA_SHARING_BAD_DATA, message, e);
        }
    }

    private static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(DELTA_SHARING_BAD_DATA, exception);
        }
        return new TrinoException(DELTA_SHARING_CURSOR_ERROR, format("Failed to read Parquet file: %s", dataSourceId), exception);
    }
}