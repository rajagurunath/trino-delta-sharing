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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.trino.deltasharing.parquet.StorageClient;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.authentication.HdfsAuthenticationModule;
import io.trino.plugin.hive.azure.HiveAzureModule;
import io.trino.plugin.hive.gcs.HiveGcsModule;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.s3.HiveS3Module;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.*;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public final class InternalDeltaSharingConnectorFactory
{
    private InternalDeltaSharingConnectorFactory() {}

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Optional<Module> metastoreModule,
            Module module)
    {
        ClassLoader classLoader = InternalDeltaSharingConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new MBeanServerModule(),
                    new HiveS3Module(),
                    new HiveAzureModule(),
                    new HiveGcsModule(),
                    new HdfsAuthenticationModule(),
                    new CatalogNameModule(catalogName),
                    binder -> {
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                        newSetBinder(binder, EventListener.class);
                    },
                    module);
            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

//            System.out.println(injector.getAllBindings().toString());
            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
//            HdfsFileSystemFactory hdfsFileSystemFactory = injector.getInstance(HdfsFileSystemFactory.class);
//            DeltaSharingPageSourceProvider connectorPageSource = injector.getInstance(DeltaSharingPageSourceProvider.class);
//            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
//            ConnectorPageSinkProvider connectorPageSink = injector.getInstance(ConnectorPageSinkProvider.class);
//            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
//            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(Key.get(new TypeLiteral<Set<SessionPropertiesProvider>>() {}));

//            Optional<ConnectorAccessControl> deltaAccessControl = injector.getInstance(Key.get(new TypeLiteral<Optional<ConnectorAccessControl>>() {}))
//                    // TODO: add the following when adding support for system tables
//                    //   .map(accessControl -> new SystemTableAwareAccessControl(accessControl, systemTableProviders))
//                    .map(accessControl -> new ClassLoaderSafeConnectorAccessControl(accessControl, classLoader));

//            Set<EventListener> eventListeners = injector.getInstance(Key.get(new TypeLiteral<Set<EventListener>>() {}))
//                    .stream()
//                    .map(listener -> new ClassLoaderSafeEventListener(listener, classLoader))
//                    .collect(toImmutableSet());

//            Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<Set<Procedure>>() {}));
//            Set<TableProcedureMetadata> tableProcedures = injector.getInstance(Key.get(new TypeLiteral<Set<TableProcedureMetadata>>() {}));
            DeltaSharingClientV1 deltaSharingClientV1 = new DeltaSharingClientV1();
            DeltaSharingMetadata deltaSharingMetadata = new DeltaSharingMetadata(deltaSharingClientV1);
            DeltaSharingSplitManager splitManager = new DeltaSharingSplitManager(context.getNodeManager(), deltaSharingClientV1);
//            ConnectorSplitManager splitManager = (ConnectorSplitManager)injector.getInstance(ConnectorSplitManager.class);
//            ConnectorPageSourceProvider connectorPageSource = (ConnectorPageSourceProvider)injector.getInstance(ConnectorPageSourceProvider.class);
//            HdfsEnvironment hdfsEnvironment = injector.getInstance(HdfsEnvironment.class);
            DeltaSharingPageSourceProvider deltaSharingPageSourceProvider = new DeltaSharingPageSourceProvider(deltaSharingClientV1);
            return new DeltaSharingConnector(
                    deltaSharingMetadata,
                    splitManager,
                    deltaSharingPageSourceProvider
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}