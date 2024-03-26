package com.example.pfemini;


import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.AsyncTask;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

public class login extends AppCompatActivity {
    private EditText editTextUsername, editTextPassword;
    private Button buttonLogin;
    private Apiservices apiService;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);
        editTextUsername = findViewById(R.id.editTextUsername);
        editTextPassword = findViewById(R.id.editTextPassword);
        buttonLogin = findViewById(R.id.buttonLogin);


        apiService = RetrofitClient.getClient().create(Apiservices.class);

        buttonLogin.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // Get username and password from EditText fields
                String username = editTextUsername.getText().toString().trim();
                String password = editTextPassword.getText().toString().trim();

                // Call the API service method to authenticate user
                Call<LoginResponse> call = apiService.authenticateUser(username, password);
                call.enqueue(new Callback<LoginResponse>() {
                    @Override
                    public void onResponse(Call<LoginResponse> call, Response<LoginResponse> response) {
                        if (response.isSuccessful()) {
                            // Handle successful response
                            LoginResponse loginResponse = response.body();
                            String status = loginResponse.getStatus();
                            String message = loginResponse.getMessage();
                            String role = loginResponse.getRole(); // Assuming you get the role from the server

                            if (status.equals("success")) {
                                // Authentication successful
                                Toast.makeText(login.this, message, Toast.LENGTH_SHORT).show();
                                // Save the username in shared preferences
                                SharedPreferences sharedPreferences = getSharedPreferences("MyPrefs", MODE_PRIVATE);
                                SharedPreferences.Editor editor = sharedPreferences.edit();
                                editor.putString("username", username);
                                editor.putString("role", role); // Save the user's role
                                editor.apply();

                                if ("mecano".equals(role)) {
                                    Intent intent = new Intent(login.this, MecanoActivity.class);
                                    startActivity(intent);
                                } else if ("driver".equals(role)) {
                                    Intent intent = new Intent(login.this, MainActivity.class);
                                    startActivity(intent);
                                } else if ("admin".equals(role)) {
                                    Intent intent = new Intent(login.this, Admin_activity.class);
                                    startActivity(intent);
                                } else {
                                    // Handle unknown role
                                    Toast.makeText(login.this, "Unknown role: " + role, Toast.LENGTH_SHORT).show();
                                }
                                finish();
                            } else {
                                // Authentication failed
                                Toast.makeText(login.this, message, Toast.LENGTH_SHORT).show();
                            }
                        }}
                            @Override
                    public void onFailure(Call<LoginResponse> call, Throwable t) {
                        // Handle network errors
                        Toast.makeText(login.this, "Network error. Please try again later.", Toast.LENGTH_SHORT).show();
                    }
                });
            }
        });
    }
}





