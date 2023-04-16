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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemModule;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.s3select.TrinoS3ClientFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.type.TypeManager;
import org.deltasharing.reader.ParquetPageSource;
import org.deltasharing.reader.ParquetPageSourceFactory;
import org.deltasharing.reader.ParquetReaderConfig;
import org.deltasharing.reader.TrinoParquetDataSource;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

public class DeltaSharingModule
        implements Module
{
    private final NodeManager nodeManager;
    private final TypeManager typeManager;

    public DeltaSharingModule(NodeManager nodeManager, TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(DeltaSharingConnector.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingClientV1.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingSplitManager.class).in(Scopes.SINGLETON);
//        binder.bind(ParquetPageSource.class).in(Scopes.SINGLETON);
        binder.bind(ParquetReaderConfig.class).in(Scopes.SINGLETON);
//        binder.bind(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
//        binder.bind(TrinoParquetDataSource.class).in(Scopes.SINGLETON);
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        binder.bind(TrinoS3ClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingSplitManager.class).in(Scopes.SINGLETON);
//        binder.bind(HdfsFileSystemFactory.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingConnector.class).in(Scopes.SINGLETON);
        binder.bind(TrinoFileSystemFactory.class).in(Scopes.SINGLETON);
//        binder.install(new HdfsFileSystemModule());
//        binder.bind(TrinoFileSystemFactory.class).to(HdfsFileSystemFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).in(Scopes.SINGLETON);
//        binder.bind(ConnectorPageSourceProvider.class).in(Scopes.SINGLETON);
//        binder.bind(ConnectorSplitManager.class).to(DeltaSharingSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(DeltaSharingPageSourceProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(io.trino.plugin.hive.parquet.ParquetReaderConfig.class);

//        binder.bind(DeltaSharingRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DeltaSharingConfig.class);
        configBinder(binder).bindConfig(HiveConfig.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(DeltaSharingTable.class));

    }
}
