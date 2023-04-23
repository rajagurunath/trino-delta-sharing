package io.trino.deltasharing;

import com.google.common.collect.ImmutableList;
import io.trino.orc.OrcWriteValidation;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class DeltaSharingProperties implements SessionPropertiesProvider {

    public static final String LOCATION_PROPERTY = "location";

    public static final String PARQUET_LOCATION_PROPERTY = "delta-sharing.parquetFileDirectory";
    private static List<PropertyMetadata<?>> tableProperties = null;

    private static String parquetDirectory;
    @Inject
    public DeltaSharingProperties(ConnectorSession session) {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        "/tmp/delta-sharing/",
                        false))
                .build();
        parquetDirectory = requireNonNull(session.getProperty(PARQUET_LOCATION_PROPERTY,String.class));

    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static String getLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(LOCATION_PROPERTY);
    }

    public static String getLocation(ConnectorSession session){
//        return parquetDirectory;
        return session.getProperty(PARQUET_LOCATION_PROPERTY,String.class);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return tableProperties;
    }
}
