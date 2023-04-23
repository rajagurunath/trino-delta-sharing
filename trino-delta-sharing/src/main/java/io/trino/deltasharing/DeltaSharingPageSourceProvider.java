package io.trino.deltasharing;


import com.google.inject.Inject;
import io.trino.deltasharing.parquet.ParquetPlugin;
import io.trino.deltasharing.parquet.StorageClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.RecordSet;
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

//        String schemaName = storageSplit.getSchemaName();
        String tableName = deltaSharingSplit.getTableName();
        FilePlugin plugin = new ParquetPlugin();

        return new FixedPageSource(plugin.getPagesIterator(tableName, path -> deltaSharingClientV1.getInputStream(session, path)));

    }
}
