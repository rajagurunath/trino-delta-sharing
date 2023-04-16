//package org.deltasharing;
//import com.google.common.base.Function;
//import com.google.common.base.Supplier;
//import com.google.common.base.Suppliers;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSet;
//import io.airlift.json.JsonCodec;
//import javax.inject.Inject;
//
//import java.io.IOException;
//import java.io.UncheckedIOException;
//import java.net.URI;
//import java.net.URL;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import static com.google.common.collect.Iterables.transform;
//import static com.google.common.collect.Maps.transformValues;
//import static com.google.common.collect.Maps.uniqueIndex;
//import static java.util.Objects.requireNonNull;
//
//public class DeltaSharingClient
//{
//    /**
//     * SchemaName -> (TableName -> TableMetadata)s
//     */
//    private final Supplier<Map<String, Map<String, DeltaSharingTable>>> schemas;
//
//    @Inject
//    public DeltaSharingClient(DeltaSharingConfig config, JsonCodec<Map<String, List<DeltaSharingTable>>> catalogCodec)
//    {
//        requireNonNull(catalogCodec, "catalogCodec is null");
//        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));
//    }
//
//    public Set<String> getSchemaNames()
//    {
//
//        System.out.println("delta guru getSchemaNames");
//
//        return schemas.get().keySet();
//    }
//
//    public Set<String> getTableNames(String schema)
//    {
//        requireNonNull(schema, "schema is null");
//        Map<String, DeltaSharingTable> tables = schemas.get().get(schema);
//        if (tables == null) {
//            return ImmutableSet.of();
//        }
//        return tables.keySet();
//    }
//
//    public DeltaSharingTable getTable(String schema, String tableName)
//    {
//        requireNonNull(schema, "schema is null");
//        requireNonNull(tableName, "tableName is null");
//        Map<String, DeltaSharingTable> tables = schemas.get().get(schema);
//        if (tables == null) {
//            return null;
//        }
//        return tables.get(tableName);
//    }
//
//    private static Supplier<Map<String, Map<String, DeltaSharingTable>>> schemasSupplier(JsonCodec<Map<String, List<DeltaSharingTable>>> catalogCodec, URI metadataUri)
//    {
//        return () -> {
//            try {
//                return lookupSchemas(metadataUri, catalogCodec);
//            }
//            catch (IOException e) {
//                throw new UncheckedIOException(e);
//            }
//        };
//    }
//
//
//    private static Map<String, Map<String, DeltaSharingTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<DeltaSharingTable>>> catalogCodec)
//            throws IOException
//    {
//        System.out.println("lookupSchemas");
//        URL result = metadataUri.toURL();
////        String json = Resources.toString(result, UTF_8);
////        String json = '{"dschema1":["dt1","dt2"]}';
//        String json= """
//                {
//                  "dschema": [
//                    {
//                      "name": "table1",
//                      "columns": [
//                        {
//                          "name": "col1",
//                          "type": "VARCHAR"
//                        }
//                      ],
//                      "sources": ["delta_share1"]
//                    }
//                  ]
//                }
//
//                """;
//        System.out.println(result.toString());
//        System.out.println(json);
//
//        Map<String, List<DeltaSharingTable>> catalog = catalogCodec.fromJson(json);
//
//        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
//    }
//
//    private static Function<List<DeltaSharingTable>, Map<String, DeltaSharingTable>> resolveAndIndexTables(URI metadataUri)
//    {
//        return tables -> {
//            Iterable<DeltaSharingTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
//            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, DeltaSharingTable::getName));
//        };
//    }
//
//    private static Function<DeltaSharingTable, DeltaSharingTable> tableUriResolver(URI baseUri)
//    {
//        return table -> {
////            List<URI> sources = ImmutableList.copyOf(transform(List.of(URI.create("http://localhost:8001/delta-sharing/shares/delta_share1/")), baseUri::resolve));
//            List<String> sources = List.of("table");
//
//            return new DeltaSharingTable(table.getName(), deltaSharingClient, table.getColumns(), sources);
//        };
//    }
//}