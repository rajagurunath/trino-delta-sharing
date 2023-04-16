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

import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static org.deltasharing.DeltaSharingTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class DeltaSharingConnector
        implements Connector
{
    private final DeltaSharingMetadata metadata;
    private final ConnectorSplitManager splitManager;
//    private final DeltaSharingRecordSetProvider recordSetProvider;

    private final ConnectorPageSourceProvider pageSourceProvider;


    @Inject
    public DeltaSharingConnector(
            DeltaSharingMetadata metadata,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull((DeltaSharingSplitManager) splitManager, "splitManager is null");
//        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider,"pageSourceProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

//    @Override
//    public ConnectorRecordSetProvider getRecordSetProvider()
//    {
//        return recordSetProvider;
//    }
    public ConnectorPageSourceProvider getConnectorPageSource()
    {
        return pageSourceProvider;
    }

}
