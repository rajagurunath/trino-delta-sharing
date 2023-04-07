//package org.deltasharing;
//import retrofit2.Call;
//import retrofit2.http.GET;
//import retrofit2.http.Header;
//import retrofit2.http.Headers;
//import retrofit2.http.Path;
//import retrofit2.http.Query;
//public interface DeltaSharingService {
//    @Headers("accept: application/vnd.github.v3+json")
//    @GET("/repos/{owner}/{repo}/actions/artifacts")
//    Call<ArtifactsList> listArtifacts(
//            @Header("Authorization") String auth,
//            @Path("owner") String owner,
//            @Path("repo") String repo,
//            @Query("per_page") int perPage,
//            @Query("page") int page);
//
//    @Headers("accept: application/vnd.github.v3+json")
//    @GET("/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts")
//    Call<ArtifactsList> listRunArtifacts(
//            @Header("Authorization") String auth,
//            @Path("owner") String owner,
//            @Path("repo") String repo,
//            @Path("run_id") long runId,
//            @Query("per_page") int perPage,
//            @Query("page") int page);
//
//    @Headers("accept: application/vnd.github.v3+json")
//    @GET("/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip")
//    Call<ResponseBody> getArtifact(
//            @Header("Authorization") String auth,
//            @Path("owner") String owner,
//            @Path("repo") String repo,
//            @Path("artifact_id") long artifactId);
//}
