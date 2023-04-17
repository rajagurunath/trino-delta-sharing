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

package io.trino.deltasharing;

import io.trino.deltasharing.models.DeltaFile;
import io.trino.spi.connector.*;
import io.trino.spi.type.Type;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class DeltaSharingRecordSetProviderV1
        implements ConnectorRecordSetProvider
{
    private final String defaultType;
    private final DeltaSharingMetadata metadata;

    @Inject
    public DeltaSharingRecordSetProviderV1(DeltaSharingConfig config, DeltaSharingMetadata metadata)
    {
        this.defaultType = config.getDefaultType();
        this.metadata = metadata;
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle connectorTransactionHandle,
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> list)
    {
        List<DeltaSharingColumnHandle> columnHandles = list.stream()
                .map(c -> (DeltaSharingColumnHandle) c)
                .collect(toList());
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(connectorSession, table);

        List<Integer> columnIndexes = columnHandles.stream()
                .map(column -> {
                    int index = 0;
                    for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                        if (columnMetadata.getName().equalsIgnoreCase(column.getName())) {
                            return index;
                        }
                        index++;
                    }
                    throw new IllegalStateException("Unknown column: " + column.getName());
                })
                .collect(toList());
        String SchemaName = tableMetadata.getTable().getSchemaName();
        String TableName = tableMetadata.getTable().getTableName();
        Iterable<List<?>> rows = getRows(SchemaName,TableName);
        Iterable<List<?>> mappedRows = StreamSupport.stream(rows.spliterator(), false)
                .map(row -> columnIndexes.stream()
                        .map(row::get)
                        .collect(toList())).collect(toList());

        List<Type> mappedTypes = columnHandles.stream()
                .map(DeltaSharingColumnHandle::getType)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }

    private Iterable<List<?>> getRows(String SchemaName,String TableName)
    {
        DeltaSharingClientV1 deltaSharingClientV1 = metadata.getDeltaSharingClient();
        List<DeltaFile> parquetFileUrls = deltaSharingClientV1.getTableData("delta_share1",SchemaName,TableName,List.of(""),"2","0");
        // TODO replace the list with an iterable that provides the data read from the data source for this connector
        System.out.println(parquetFileUrls);
        return List.of(
                List.of("x", defaultType, "my-name"));
    }
}
