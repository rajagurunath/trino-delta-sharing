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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.deltasharing.models.DeltaFile;

import java.util.List;
import java.util.Map;

public class DeltaSharingSplit
        implements ConnectorSplit
{
    private final DeltaSharingTableHandle tableHandle;
    private final List<HostAddress> addresses;

    private final DeltaFile deltaFile;

    @JsonCreator
    public DeltaSharingSplit(
            @JsonProperty("tableHandle") DeltaSharingTableHandle tableHandle,
            @JsonProperty("addresses") List<HostAddress> addresses,
            DeltaFile deltaFile)

    {
        this.tableHandle = tableHandle;
        this.addresses = addresses;
        this.deltaFile = deltaFile;
    }

    public DeltaFile getDeltaFile(){
        return deltaFile;
    }

    public Map<String,String> getPredicate(){
        return deltaFile.partitionValues;
    }


    public String getPath(){
        return deltaFile.url;
    }

    public Integer getFileSize(){
        return deltaFile.size;
    }
    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    @JsonProperty("addresses")
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    @Override
    public Object getInfo()
    {
        return "DeltaSharing split";
    }

    @JsonProperty("tableHandle")
    public DeltaSharingTableHandle getTableHandle()
    {
        return tableHandle;
    }
}
