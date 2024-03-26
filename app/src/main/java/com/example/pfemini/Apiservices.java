package com.example.pfemini;

import retrofit2.Call;
import retrofit2.http.Field;
import retrofit2.http.FormUrlEncoded;
import retrofit2.http.POST;

public interface Apiservices {

    @FormUrlEncoded
    @POST("/authenticate")
    Call<LoginResponse> authenticateUser(
            @Field("username") String username,
            @Field("password") String password
    );
    @FormUrlEncoded
    @POST("change_password")
    Call<Changeresponse> changePassword(
            @Field("username") String username, // Include the session username
            @Field("old_password") String oldPassword,
            @Field("new_password") String newPassword
    );


    @FormUrlEncoded
    @POST("/add_user")
    Call<Addresponse> addUser(
            @Field("username") String username,
            @Field("email") String email,
            @Field("password") String password,
            @Field("role") String role
    );




}
