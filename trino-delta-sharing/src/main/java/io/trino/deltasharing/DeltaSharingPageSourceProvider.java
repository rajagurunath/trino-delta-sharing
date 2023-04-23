package io.trino.deltasharing;


import com.google.inject.Inject;
import io.trino.deltasharing.parquet.ParquetPlugin;
import io.trino.spi.connector.*;
import io.trino.deltasharing.parquet.FilePlugin;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DeltaSharingPageSourceProvider
        implements ConnectorPageSourceProvider {
    private DeltaSharingClientV1 deltaSharingClientV1;

    @Inject
    public void StoragePageSourceProvider(DeltaSharingClientV1 deltaSharingClientV1) {
        this.deltaSharingClientV1 = requireNonNull(deltaSharingClientV1, "storageClient is null");
    }

    public DeltaSharingPageSourceProvider(DeltaSharingClientV1 deltaSharingClientV1) {
        this.deltaSharingClientV1 = deltaSharingClientV1;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {


        DeltaSharingSplit deltaSharingSplit = (DeltaSharingSplit) requireNonNull(split, "split is null");
        String parquetFileDirectory = deltaSharingClientV1.getParquetFileDirectory();
        ParquetPlugin plugin = new ParquetPlugin();
        String filePath = deltaSharingSplit.getParquetURL();
        DeltaSharingTableHandle deltaSharingTable = (DeltaSharingTableHandle) requireNonNull(table);
        SchemaTableName schemaTableName = deltaSharingTable.getSchemaTableName();
        return new FixedPageSource(plugin.getPagesIterator(filePath, path -> deltaSharingClientV1.getInputStream(session, path),parquetFileDirectory,schemaTableName.toString()));

    }
}
