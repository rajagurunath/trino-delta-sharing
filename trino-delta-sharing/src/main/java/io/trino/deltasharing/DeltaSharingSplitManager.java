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
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.*;
import com.google.common.collect.ImmutableList;
import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DeltaSharingSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private DeltaSharingClientV1 deltaSharingClientV1;

    @Inject
    public DeltaSharingSplitManager(NodeManager nodeManager, DeltaSharingClientV1 deltaSharingClientV1)
    {   this.deltaSharingClientV1 = requireNonNull(deltaSharingClientV1, "deltaShringClientV1 is null");
        this.nodeManager = nodeManager;
    }
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        List<HostAddress> addresses = nodeManager.getRequiredWorkerNodes().stream()
                .map(Node::getHostAndPort)
                .collect(toList());
        DeltaSharingTableHandle deltaLakeTableHandle = (DeltaSharingTableHandle) table;
        List<DeltaFile> deltaFiles = deltaSharingClientV1.getTableData(
                deltaLakeTableHandle.getSchema(),
                deltaLakeTableHandle.getTable(),
                null,
                "",
                ""
                );
        ImmutableList.Builder<DeltaSharingSplit> deltaSharingSplitBuilder = ImmutableList.builder();
        deltaFiles.forEach(deltaFile -> deltaSharingSplitBuilder.add(new DeltaSharingSplit(deltaFile.url,addresses,deltaFile.id)));
        return new FixedSplitSource(deltaSharingSplitBuilder.build());


    }
}