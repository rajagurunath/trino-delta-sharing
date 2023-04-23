package io.trino.deltasharing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.deltasharing.models.*;
import io.trino.deltasharing.services.DeltaSharingService;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.ResponseBody;
import org.apache.hadoop.fs.Path;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import io.trino.deltasharing.models.*;
import io.trino.deltasharing.services.*;
import io.trino.deltasharing.models.DeltaSharingSchemaResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import retrofit2.converter.gson.GsonConverterFactory;

import javax.net.ssl.HttpsURLConnection;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class DeltaSharingClientV1 {

    private static final String providerJSON = """
            {
              "shareCredentialsVersion": 1,
              "endpoint": "http://localhost:8001/delta-sharing/",
              "bearerToken":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhZG1pbiIsImV4cCI6MTY4MjgzMzY0NH0.4k-ESYVWhxW8YXjHhc5p_zYX9p2vrzY3LxRSQIUMxiw",
              "expirationTime": "2023-04-15T09:36:29Z"
            }
            """;
    static ObjectMapper mapper = new ObjectMapper();

    public DeltaSharingService getDeltaSharingService() {
        Retrofit retrofit = getRetroFitClient();
        return retrofit.create(DeltaSharingService.class);
    }

    public static MetaData.DeltaSharingProfileAdaptor getProfileAdaptor() {
        MetaData.DeltaSharingProfileAdaptor profileAdaptor;
        try {
            profileAdaptor = mapper.readValue(providerJSON, MetaData.DeltaSharingProfileAdaptor.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return profileAdaptor;
    }

    public DeltaSharingClientV1() throws JsonProcessingException {
    }

    public static Retrofit getRetroFitClient() {
        MetaData.DeltaSharingProfileAdaptor profileAdaptor = getProfileAdaptor();
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(new Interceptor() {
            @Override
            public okhttp3.Response intercept(Chain chain) throws IOException {
                Request newRequest = chain.request().newBuilder()
                        .addHeader("Authorization", "Bearer " + profileAdaptor.getBearerToken())
                        .build();
                return chain.proceed(newRequest);
            }
        }).build();
        Gson gson = new GsonBuilder()
                .setLenient()
                .create();

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(profileAdaptor.getEndpoint())
                .addConverterFactory(GsonConverterFactory.create(gson))
                .client(client)
                .build();
        return retrofit;
    }

    public List<String> getSchemas() {
        DeltaSharingService deltaSharingService = getDeltaSharingService();
        Call<DeltaSharingSchemaResponse> callSync = deltaSharingService.getSchemas("delta_share1");
        System.out.println("Starting");
        try {
            Response<DeltaSharingSchemaResponse> response = callSync.execute();
            DeltaSharingSchemaResponse apiResponse = response.body();
            assert apiResponse != null;
            List<String> schemaList = apiResponse.getSchemas();
            System.out.println(schemaList);
            return schemaList;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }


    public List<String> getTables(String share, String schema) {
        DeltaSharingService deltaSharingService = getDeltaSharingService();
        Call<DeltaSharingTableResponse> callSync = deltaSharingService.getTables(share, schema);
        try {
            Response<DeltaSharingTableResponse> response = callSync.execute();
            DeltaSharingTableResponse apiResponse = response.body();
            assert apiResponse != null;
            List<String> tableList = apiResponse.getTables();
            System.out.println(tableList);
            return tableList;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public List<String> getAllTables(String share) {
        DeltaSharingService deltaSharingService = getDeltaSharingService();
        Call<DeltaSharingTableResponse> callSync = deltaSharingService.getAllTables(share);
        try {
            Response<DeltaSharingTableResponse> response = callSync.execute();
            DeltaSharingTableResponse apiResponse = response.body();
            assert apiResponse != null;
            List<String> tableList = apiResponse.getTables();
            System.out.println(tableList);
            return tableList;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public String getTableVersion(String share, String schema, String table) {
        DeltaSharingService deltaSharingService = getDeltaSharingService();
        Call<Void> headerCallSync = deltaSharingService.getTableVersion(share, schema, table);
        String tableVersion = "";
        System.out.println("header Starting");
        try {
            Response<Void> response = headerCallSync.execute();
            System.out.println(response.toString());
            System.out.println(response.code());
            System.out.println(response.message());
            System.out.println(response.body());
//            assert apiResponse != null;
            tableVersion = response.headers().get("Delta-Table-Version");
            System.out.println("Delta-Table-Version: " + tableVersion);
            return tableVersion;
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return tableVersion;
    }

    public  List<ColumnMetadata> getTableMetadata(String share, String schema, String table) {
        DeltaSharingService deltaSharingService = getDeltaSharingService();
        String tableVersion = getTableVersion(share, schema, table);
        Call<ResponseBody> metadataCallSync = deltaSharingService.getTableMetadata(tableVersion, share, schema, table);
        System.out.println("Starting");
        try {
            Response<ResponseBody> response = metadataCallSync.execute();
            ResponseBody apiResponse = response.body();
            assert apiResponse != null;
            Gson gson = new Gson();
            try {
                assert response.body() != null;
                try (InputStream inputStream = response.body().byteStream();
                     Reader reader = new InputStreamReader(inputStream)) {
                    BufferedReader bufferedReader = new BufferedReader(reader);
                    String line1 = bufferedReader.readLine();
                    Protocol protocol1 = gson.fromJson(line1, Protocol.class);
                    System.out.println(protocol1.minReaderVersion);

                    System.out.println("after header");
                    String line2 = bufferedReader.readLine();

                    DeltaSharingMetadataModel metadata1 = gson.fromJson(line2, DeltaSharingMetadataModel.class);
                    System.out.println(metadata1.metaData);
                    System.out.println(metadata1.metaData.schemaString);
                    DeltaTableSchema deltaTableSchema = gson.fromJson(metadata1.metaData.schemaString, DeltaTableSchema.class);
                    return deltaTableSchema.fields.stream().map(
                            field -> new ColumnMetadata(field.name, DeltaTrinoColumn.convert(field.type))).collect(toList());

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
//            assert apiResponse != null;
//            System.out.println(apiResponse.metaData.schemaString);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    public List<DeltaFile> getTableData(String share, String schema, String table, List<String> predicates, String limitHint, String version) {
        DeltaSharingService deltaSharingService = getDeltaSharingService();
        String tableVersion = getTableVersion(share, schema, table);
        DeltaSharingQueryRequest deltaSharingQueryRequest = new DeltaSharingQueryRequest(predicates, limitHint, version);
        Call<ResponseBody> queryCallSync = deltaSharingService.getTableData(tableVersion, share,schema,table,deltaSharingQueryRequest);
        ArrayList<DeltaFile> parquetFileUrs = new ArrayList<DeltaFile>();
        System.out.println("Starting");
        try {

            Response<ResponseBody> response = queryCallSync.execute();
            ResponseBody apiResponse = response.body();
            assert apiResponse != null;
            Gson gson = new Gson();

            try {
                assert response.body() != null;
                try (InputStream inputStream = response.body().byteStream();
                     Reader reader = new InputStreamReader(inputStream)) {
                    BufferedReader bufferedReader = new BufferedReader(reader);
                    String line1 = bufferedReader.readLine();
                    Protocol protocol1 = gson.fromJson(line1, Protocol.class);
                    System.out.println(protocol1.minReaderVersion);
                    System.out.println("after header");
                    String line2 = bufferedReader.readLine();
                    DeltaSharingMetadataModel metadata1 = gson.fromJson(line2, DeltaSharingMetadataModel.class);
                    System.out.println(metadata1.metaData);
                    System.out.println(metadata1.metaData.schemaString);

                    String line;
                    System.out.println("table files");
                    while ((line = bufferedReader.readLine()) != null) {
                        DeltaSharingQuery deltaFileResponse = gson.fromJson(line, DeltaSharingQuery.class);
                        System.out.println(deltaFileResponse.file.url);
                        parquetFileUrs.add(deltaFileResponse.file);
                        // Process the ApiResponse object
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
//            assert apiResponse != null;
//            System.out.println(apiResponse.metaData.schemaString);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return parquetFileUrs;
    }

    public InputStream getInputStream(ConnectorSession session, String path)
    {
        try {
            if (path.startsWith("http://") || path.startsWith("https://")) {
                System.out.println("input stream ");
//                String path1 = "https://tf-benchmarking.s3.amazonaws.com/delta_2/dwh/test_hm/part-00000-e3bd56f7-7979-4f94-9bba-44d8496597f6-c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIA3GXK5AWRNOYRTVMP%2F20230422%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230422T180935Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJGMEQCIH9PXF7vwZaYSYX1OppCFm4OIwkJIo35Pv0MoDhVbkUOAiAjM0Vc%2FxLV%2Fh7ABx49GZ437aXwe%2FGTOjlnijNhIWBPaiqoAwjD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAIaDDc3MDM2NTUyMzM2MiIMajq0oBf0PergXGxPKvwCS8Poj3vu16vfPlFJP2NlKFynk4NSX6K1GY5smAS6wurEAdKSyiOH1jLT0w0yi%2B3yD6LxSrXnCa%2FmAk5p5I45KmSbm5FkCypp99nv3%2Fqa5eHkSa5J3TCLqoZxmFmzJWrkJNkOFAQS9RIGC2WDIxU2Z6Xh7SvJMdY4%2FEjEQEAlyKGya4pPYPQXOkAjOZlUUipuA9CJuXoxpmmd%2B3ppzJNtnIEaWpc9YrsSoWNnh3PY6aZgbKh3dCxGDUNQoDGgYNgzuzstfRot16qCcMkX%2BZuaWtkrv%2FjxjA1UCr5%2FEisrtcz5%2FUAZfwJStgumjCn%2BZHYO6RTRkSexiyv6a%2BHho1xTYgx0elvQE2qWxjrPK6bJeyW8W3uJdQQClWLag0anqyGxxQxAaBlye3Vd1nDmquHBUgnHFCEFFpzXL0FeqqYOlMT18sN8HF6JwdEp2NdTKrN9SiXA5U8AE8RHsU2XCRazcSx9EBX%2F%2Bx8HYo6DtE0VX6ZoWvfzHOJ4mAhwYsEwmMWQogY6pwG7icvmY8q5it7AGK2UtMaAR%2FBwW5y2%2B00%2FDr6Ia62q8e7wc38JUotLe9mBrn3WkMVfdRV2f6F8D2VixIV49IR3Zle4h3pHf0VfH6KHHI%2BsYRb0oi2wJrJfVg%2Fn%2BTvh6U1qd5jAT7wObjJPyW8RkXo24kGCZjaw%2F%2FqzYmtW8A3qZZvedytbI4DHr2kJOHLBxaMLg%2BCnxSz2oby6cTx4xGifqwgr54yk3g%3D%3D&X-Amz-Signature=98aa6bd4485b686ec480774f296cf4781eea3d252276fb872b2f553712df762a";
                //                FSDataInputStream stream = new FSDataInputStream(new InMemoryHttpInputStream(new URI(path1)));
                URL url = new URL(path);
                System.out.println("===================================");
                HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(2000);
                return connection.getInputStream();
//                return stream.getWrappedStream();
            }
            if (!path.startsWith("file:")) {
                path = "file:" + path;
            }

            return URI.create(path).toURL().openStream();
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to open stream for %s", path), e);
        }
    }


    public static void main(String[] args) throws JsonProcessingException {
        System.out.println("getting client");
        {

            DeltaSharingClientV1 client = new DeltaSharingClientV1();
            System.out.println(client.getSchemas());
            System.out.println(client.getTables("delta_share1","delta_schema1"));
            System.out.println(client.getAllTables("delta_share1"));
            System.out.println(client.getTableVersion("delta_share1","delta_schema1","test_student"));
            System.out.println(client.getTableMetadata("delta_share1","delta_schema1","test_student"));
            System.out.println(client.getTableData("delta_share1","delta_schema1","test_student",List.of(""),"200","1"));

        }
    }
}