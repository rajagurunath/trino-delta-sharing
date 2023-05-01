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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

public class DeltaSharingColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type type;

    private Optional<String>  Comment = Optional.of("");

    private final boolean nullable;

    @JsonCreator
    public DeltaSharingColumnHandle(String name, Type type,boolean nullable,Optional<String>  Comment)
    {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.Comment = Comment;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }


    @JsonProperty
    public Optional<String> getComment() {return Comment;}


    @JsonProperty
    public boolean isNullable() { return nullable;}

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeltaSharingColumnHandle that = (DeltaSharingColumnHandle) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
