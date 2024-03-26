package com.example.pfemini;

import androidx.appcompat.app.AppCompatActivity;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

import android.os.Bundle;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

public class Passwordmodif extends AppCompatActivity {

    private EditText oldPasswordEditText, newPasswordEditText, confirmPasswordEditText;
    private Button savePasswordButton;
    private Apiservices apiService;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getSupportActionBar().setTitle("Change Password");
        setContentView(R.layout.activity_passwordmodif);
        getSupportActionBar().setDefaultDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setDisplayShowHomeEnabled(true);
        oldPasswordEditText = findViewById(R.id.oldPasswordEditText);
        newPasswordEditText = findViewById(R.id.newPasswordEditText);
        confirmPasswordEditText = findViewById(R.id.confirmPasswordEditText);
        savePasswordButton = findViewById(R.id.savePasswordButton);

        apiService = RetrofitClient.getClient().create(Apiservices.class);

        // Retrieve session username from intent extras
        String sessionUsername = getIntent().getStringExtra(profile.SESSION_USERNAME);

        savePasswordButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String oldPassword = oldPasswordEditText.getText().toString().trim();
                String newPassword = newPasswordEditText.getText().toString().trim();
                String confirmPassword = confirmPasswordEditText.getText().toString().trim();

                // Check if new password matches confirm password
                if (!newPassword.equals(confirmPassword)) {
                    Toast.makeText(Passwordmodif.this, "Passwords do not match", Toast.LENGTH_SHORT).show();
                    return;
                }

                // Call API service method to update password
                Call<Changeresponse> call = apiService.changePassword(sessionUsername, oldPassword, newPassword);
                call.enqueue(new Callback<Changeresponse>() {
                    @Override
                    public void onResponse(Call<Changeresponse> call, Response<Changeresponse> response) {
                        if (response.isSuccessful()) {
                            Changeresponse apiResponse = response.body();
                            if (apiResponse != null && apiResponse.getStatus().equals("success")) {
                                Toast.makeText(Passwordmodif.this, apiResponse.getMessage(), Toast.LENGTH_SHORT).show();
                                finish(); // Finish the activity after successfully changing the password
                            } else {
                                Toast.makeText(Passwordmodif.this, "Failed to change password", Toast.LENGTH_SHORT).show();
                            }
                        } else {
                            Toast.makeText(Passwordmodif.this, "Error occurred. Please try again.", Toast.LENGTH_SHORT).show();
                        }
                    }

                    @Override
                    public void onFailure(Call<Changeresponse> call, Throwable t) {
                        Toast.makeText(Passwordmodif.this, "Network error. Please try again later.", Toast.LENGTH_SHORT).show();
                    }
                });
            }
        });



    }


    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here
        if (item.getItemId() == android.R.id.home) {
            // Handle the back button click
            onBackPressed();
            return true;
        }
        return super.onOptionsItemSelected(item);
    }

}
