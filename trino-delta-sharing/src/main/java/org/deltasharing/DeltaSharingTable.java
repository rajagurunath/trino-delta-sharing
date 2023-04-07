package org.deltasharing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class DeltaSharingTable
{
    private final String name;
    private final List<DeltaSharingColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final List<String> sources;

    @JsonCreator
    public DeltaSharingTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<DeltaSharingColumn> columns,
            @JsonProperty("sources") List<String> sources)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (DeltaSharingColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<DeltaSharingColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<String> getSources()
    {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}