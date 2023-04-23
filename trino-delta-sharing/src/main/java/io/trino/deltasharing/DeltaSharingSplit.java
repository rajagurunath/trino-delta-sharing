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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.deltasharing.models.DeltaFile;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import static java.util.Objects.requireNonNull;

public class DeltaSharingSplit
        implements ConnectorSplit
{
//    private final DeltaSharingTableHandle tableHandle;
    private final List<HostAddress> addresses;
//    private final DeltaFile deltaFile;
    private final String fileID;
    private final String url;

    @JsonCreator
    public DeltaSharingSplit(
//            @JsonProperty("tableHandle") DeltaSharingTableHandle tableHandle,
            @JsonProperty("url")  String url,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("fileID") String  fileID
            )
    {
//        this.tableHandle = tableHandle;
        this.addresses = addresses;
        this.fileID = requireNonNull(fileID,"fileID cannot be null");
        this.url = requireNonNull(url,"url cannot be null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

//    public String getTableName(){
//        return tableHandle.getSchemaTableName().getTableName();
//    }
    @Override
    @JsonProperty("addresses")
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("url", url)
                .put("fileID",fileID)
                .buildOrThrow();
    }
    @JsonProperty("fileID")
    public String getFileID(){
        return fileID;
    }

    @JsonProperty("url")
    public String getParquetURL(){
        return url;
    }


//    @JsonProperty("tableHandle")
//    public DeltaSharingTableHandle getTableHandle()
//    {
//        return tableHandle;
//    }
}
