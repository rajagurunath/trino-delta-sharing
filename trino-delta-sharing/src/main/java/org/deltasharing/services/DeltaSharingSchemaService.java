package org.deltasharing.services;
import org.deltasharing.models.DeltaSharingSchemaResponse;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.Path;


public interface DeltaSharingSchemaService {

    @Headers("accept: application/json; charset=utf-8")
    @GET("shares/{share}/schemas")
    Call<DeltaSharingSchemaResponse> getSchemas(
            @Path("share") String share
    );

}
