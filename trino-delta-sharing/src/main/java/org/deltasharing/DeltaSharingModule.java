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
import io.trino.spi.NodeManager;
import io.trino.spi.type.TypeManager;

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
        binder.bind(DeltaSharingClient.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingClientV1.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSharingRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DeltaSharingConfig.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(DeltaSharingTable.class));

    }
}
