package org.deltasharing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.deltasharing.models.*;
import org.deltasharing.services.*;
import org.deltasharing.models.DeltaSharingSchemaResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import retrofit2.converter.gson.GsonConverterFactory;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.stream.Collectors.toList;

public class DeltaSharingClientV1 {

    private static final String providerJSON = """
            {
              "shareCredentialsVersion": 1,
              "endpoint": "http://localhost:8000/delta-sharing/",
              "bearerToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhZG1pbiIsImV4cCI6MTY4MTU4Nzc3OH0.84FhExRV2xgWiaYGbUsFfRiRXAaxhCzXhLz4JHy21tw",
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

    public List<String> getTableData(String share, String schema, String table, List<String> predicates, String limitHint, String version) {
        DeltaSharingService deltaSharingService = getDeltaSharingService();
        String tableVersion = getTableVersion(share, schema, table);
        DeltaSharingQueryRequest deltaSharingQueryRequest = new DeltaSharingQueryRequest(predicates, limitHint, version);
        Call<ResponseBody> queryCallSync = deltaSharingService.getTableData(tableVersion, share,schema,table,deltaSharingQueryRequest);
        ArrayList<String> parquetFileUrs = new ArrayList<String>();

        System.out.println("Starting");
        try {

            Response<ResponseBody> response = queryCallSync.execute();
            ResponseBody apiResponse = response.body();
            assert apiResponse != null;
            Gson gson = new Gson();

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
                    parquetFileUrs.add(deltaFileResponse.file.url);
                    // Process the ApiResponse object
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


    public static void main(String[] args) throws JsonProcessingException {
        System.out.println("getting client");
        {

            DeltaSharingClientV1 client = new DeltaSharingClientV1();
            System.out.println(client.getSchemas());
            System.out.println(client.getTables("delta_share1","delta_schema1"));
            System.out.println(client.getAllTables("delta_share1"));
            System.out.println(client.getTableVersion("delta_share1","delta_schema1","test_hm"));
            System.out.println(client.getTableMetadata("delta_share1","delta_schema1","test_hm"));
            System.out.println(client.getTableData("delta_share1","delta_schema1","test_hm",List.of(""),"2","0"));

        }
    }
}