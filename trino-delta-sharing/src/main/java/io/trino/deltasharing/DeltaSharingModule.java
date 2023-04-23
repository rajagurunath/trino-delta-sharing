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

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.deltasharing.parquet.FileParquetDataSource;
import io.trino.deltasharing.parquet.ParquetPlugin;
import io.trino.deltasharing.parquet.ParquetTypeTranslator;
import io.trino.deltasharing.parquet.StorageClient;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import javax.inject.Inject;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
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
        binder.bind(ConnectorPageSourceProvider.class).to(DeltaSharingPageSourceProvider.class).in(Scopes.SINGLETON);

        binder.bind(DeltaSharingConnector.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingClient.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingClientV1.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(StorageClient.class).in(Scopes.SINGLETON);
        binder.bind(ParquetPlugin.class).in(Scopes.SINGLETON);
        binder.bind(ParquetTypeTranslator.class).in(Scopes.SINGLETON);
//        binder.bind(DeltaSharingRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(FileParquetDataSource.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DeltaSharingConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(DeltaSharingTable.class));

    }
    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(new TypeSignature(value));
        }
    }
}
