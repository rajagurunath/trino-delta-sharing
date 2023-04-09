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

package org.deltasharing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class DeltaSharingMetadata
        implements ConnectorMetadata
{
//    public static final String SCHEMA_NAME = "default";
//    private final DeltaSharingClient deltaSharingClient;
    private final DeltaSharingClientV1 deltaSharingClient;

    @Inject
    public DeltaSharingMetadata(DeltaSharingClientV1 deltaSharingClient)
    {
        this.deltaSharingClient = requireNonNull(deltaSharingClient, "deltaSharingClient is null");
//        this.deltaSharingClientV1 = requireNonNull(deltaSharingClientV1,"deltaSharingClientV1 is null");
    }

    // TODO replace with the actual tables provided by this connector
//    public static final Map<String, List<ColumnMetadata>> columns = new ImmutableMap.Builder<String, List<ColumnMetadata>>()
//            .put("single_row", ImmutableList.of(
//                    new ColumnMetadata("id", VARCHAR),
//                    new ColumnMetadata("type", VARCHAR),
//                    new ColumnMetadata("name", VARCHAR)))
//            .build();
    public DeltaSharingClientV1 getDeltaSharingClient(){
        return deltaSharingClient;
    }
    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(deltaSharingClient.getSchemas());
    }
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        System.out.println("getTableHandle: " + schemaTableName);
        if (!schemaTableName.getSchemaName().equals(schemaTableName.getSchemaName())) {
            return null;
        }
        return new DeltaSharingTableHandle(schemaTableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        DeltaSharingTableHandle tableHandle = (DeltaSharingTableHandle) connectorTableHandle;
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        System.out.println("SchemaTableName: " + schemaTableName);
        List<ColumnMetadata> columnMetadataList = deltaSharingClient.getTableMetadata("delta_share1",schemaTableName.getSchemaName(),schemaTableName.getTableName());
        return new ConnectorTableMetadata(
                schemaTableName,
                columnMetadataList);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schemaName1 = schemaName.orElse("delta_schema1");
        List<String> tableList = deltaSharingClient.getTables("delta_share1",schemaName1);
        return tableList.stream().map(table-> new SchemaTableName(schemaName1,table)).collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        return getTableMetadata(connectorSession, connectorTableHandle).getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> new DeltaSharingColumnHandle(column.getName(), column.getType())));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle columnHandle)
    {
        DeltaSharingColumnHandle handle = (DeltaSharingColumnHandle) columnHandle;
        return new ColumnMetadata(handle.getName(), handle.getType());
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
//        return columns.entrySet().stream()
//                .map(entry -> TableColumnsMetadata.forTable(
//                        new SchemaTableName(prefix.getSchema().orElse(""), entry.getKey()),
//                        entry.getValue()))
//                .iterator()

        return listTables(session,prefix.getSchema()).stream()
                .map(entry-> TableColumnsMetadata.forTable(
                        entry,deltaSharingClient.getTableMetadata("delta_share1",
                                entry.getSchemaName(),entry.getTableName()))).iterator();
    }
}
