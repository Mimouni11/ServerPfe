package com.example.pfemini;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;

public class profile extends AppCompatActivity {

    private static final int AVATAR_SELECTION_REQUEST_CODE = 1;
    private static final String PREFS_NAME = "MyPrefs";
    private static final String KEY_SELECTED_AVATAR = "selectedAvatar";

    private Button modifyButton;
    private ImageView profileIcon;
    private int selectedAvatarResourceId;
    public static final String SESSION_USERNAME = "sessionUsername"; // Change to public
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getSupportActionBar().setTitle("Profile");

        setContentView(R.layout.activity_profile);
        getSupportActionBar().setDefaultDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setDisplayShowHomeEnabled(true);
        profileIcon = findViewById(R.id.profileIcon);

        SharedPreferences prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE);
        selectedAvatarResourceId = prefs.getInt(KEY_SELECTED_AVATAR, R.drawable.man);
        profileIcon.setImageResource(selectedAvatarResourceId);

        TextView usernameTextView = findViewById(R.id.usernameTextView);
        String username = prefs.getString("username", "");
        usernameTextView.setText(username);


        TextView modifyPasswordText = findViewById(R.id.modifyPasswordText);
        modifyPasswordText.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // Navigate to the password modification interface
                Intent intent = new Intent(profile.this, Passwordmodif.class);
                intent.putExtra(SESSION_USERNAME, username);
                startActivity(intent);
            }
        });

        TextView logoutText = findViewById(R.id.logoutText);
        logoutText.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // Perform logout action
                SharedPreferences.Editor editor = getSharedPreferences(PREFS_NAME, MODE_PRIVATE).edit();
                editor.remove("username"); // Remove the username from SharedPreferences
                editor.apply();

                // Redirect to the login activity
                Intent intent = new Intent(profile.this, login.class);
                startActivity(intent);

                // Finish the current activity
                finish();
            }
        });






        modifyButton = findViewById(R.id.modifyProfileButton);
        modifyButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(profile.this, Avatar_selection.class);
                startActivityForResult(intent, AVATAR_SELECTION_REQUEST_CODE);
            }
        });
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, @Nullable Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == AVATAR_SELECTION_REQUEST_CODE && resultCode == RESULT_OK) {
            if (data != null && data.hasExtra("selectedAvatar")) {
                selectedAvatarResourceId = data.getIntExtra("selectedAvatar", R.drawable.man);
                profileIcon.setImageResource(selectedAvatarResourceId);

                // Save the selected avatar resource ID to SharedPreferences
                SharedPreferences.Editor editor = getSharedPreferences(PREFS_NAME, MODE_PRIVATE).edit();
                editor.putInt(KEY_SELECTED_AVATAR, selectedAvatarResourceId);
                editor.apply();
            }
        }
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
