package com.example.pfemini;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.os.Handler;

public class loading extends AppCompatActivity {

    private static final int SPLASH_DURATION = 3000; // Duration of splash screen in milliseconds

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_loading);

        // Delayed navigation after SPLASH_DURATION milliseconds
        new Handler().postDelayed(new Runnable() {
            @Override
            public void run() {
                // Check if the user is already logged in
                SharedPreferences sharedPreferences = getSharedPreferences("MyPrefs", MODE_PRIVATE);
                String username = sharedPreferences.getString("username", "");
                String role = sharedPreferences.getString("role", "");

                Intent intent;
                if (!username.isEmpty()) {
                    // User is already logged in
                    if ("admin".equals(role)) {
                        // Redirect admin users to Admin_activity
                        intent = new Intent(loading.this, Admin_activity.class);
                    } else {
                        // Redirect other users to MainActivity
                        intent = new Intent(loading.this, MainActivity.class);
                    }
                } else {
                    // User is not logged in, navigate to LoginActivity
                    intent = new Intent(loading.this, login.class);
                }
                startActivity(intent);
                finish(); // Finish the loading activity so the user cannot navigate back to it
            }
        }, SPLASH_DURATION);
    }
}




