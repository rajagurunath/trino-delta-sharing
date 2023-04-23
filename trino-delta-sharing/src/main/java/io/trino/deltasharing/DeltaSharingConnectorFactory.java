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

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.connector.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.plugin.base.Versions.checkSpiVersion;
import static java.util.Objects.requireNonNull;

public class DeltaSharingConnectorFactory
        implements ConnectorFactory {
    public static final String CONNECTOR_NAME = "delta-sharing";
    private final Class<? extends Module> module;

    public DeltaSharingConnectorFactory(Class<? extends Module> module) {
        this.module = module;
    }

    @Override
    public String getName() {
        return CONNECTOR_NAME;
    }
//    @Override
//    public Connector create(String s, Map<String, String> requiredConfig, ConnectorContext context)
//    {
//        requireNonNull(requiredConfig, "requiredConfig is null");
//
//        // A plugin is not required to use Guice; it is just very convenient
//        Bootstrap app = new Bootstrap(
//                new JsonModule(),
//                new DeltaSharingModule(
//                        context.getNodeManager(),
//                        context.getTypeManager()));
//
//        Injector injector = app
//                .doNotInitializeLogging()
//                .setRequiredConfigurationProperties(requiredConfig)
//                .initialize();
//
//        return injector.getInstance(DeltaSharingConnector.class);
//    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkSpiVersion(context, this);
        ClassLoader classLoader = context.duplicatePluginClassLoader();
        try {
            Class<?> moduleClass = classLoader.loadClass(Module.class.getName());
            Object moduleInstance = classLoader.loadClass(module.getName()).getConstructor().newInstance();
            return (Connector) classLoader.loadClass(InternalDeltaSharingConnectorFactory.class.getName())
                    .getMethod("createConnector", String.class, Map.class, ConnectorContext.class, Optional.class, moduleClass)
                    .invoke(null, catalogName, config, context, Optional.empty(), moduleInstance);
        }
        catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            throwIfUnchecked(targetException);
            throw new RuntimeException(targetException);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

//    @Override
//    public Connector create(String s, Map<String, String> requiredConfig, ConnectorContext context) {
//        requireNonNull(requiredConfig, "requiredConfig is null");
//
//        ClassLoader classLoader = DeltaSharingConnectorFactory.class.getClassLoader();
//        // A plugin is not required to use Guice; it is just very convenient
//        Bootstrap app = new Bootstrap(
//                new JsonModule(),
//                new EventModule(),
//                new MBeanModule(),
//                new JsonModule(),
//                new HdfsModule(),
//                new HiveS3Module(),
//                new HiveGcsModule(),
//                new HiveAzureModule(),
//                new HdfsAuthenticationModule(),
//                new HdfsFileSystemModule(),
//                new MBeanServerModule(),
//                binder -> {
//                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
//                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
//                });
//        new DeltaSharingModule(
//                context.getNodeManager(),
//                context.getTypeManager());
//
//        Injector injector = app
//                .doNotInitializeLogging()
//                .setRequiredConfigurationProperties(requiredConfig)
//                .initialize();
//
////        DeltaSharingSplitManager splitManager = new DeltaSharingSplitManager(context.getNodeManager());
//        ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
//        ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
//
////        DeltaSharingPageSourceProvider connectorPageSource = injector.getInstance(DeltaSharingPageSourceProvider.class);
//        DeltaSharingMetadata deltaSharingMetadata = injector.getInstance(DeltaSharingMetadata.class);
//        return new DeltaSharingConnector(
//                deltaSharingMetadata,
//                new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
//                new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader)
//        );
//    }

}