package com.example.pfemini;

import androidx.appcompat.app.AppCompatActivity;

import android.graphics.Rect;
import android.os.Bundle;
import android.view.MotionEvent;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.Toast;

import java.util.Random;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class Gere_utlisateur extends AppCompatActivity {

    private EditText usernameEditText, emailEditText, passwordEditText, roleEditText;
    private Button addButton;
    private Apiservices apiService;
    private Spinner roleSpinner;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_gere_utlisateur);

        usernameEditText = findViewById(R.id.username);
        emailEditText = findViewById(R.id.Mail);
        passwordEditText = findViewById(R.id.Password);
        roleSpinner = findViewById(R.id.roleSpinner);
        addButton = findViewById(R.id.buttonAddUser);
        populateRoleSpinner();
        apiService = RetrofitClient.getClient().create(Apiservices.class);

        addButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String username = usernameEditText.getText().toString().trim();
                String email = emailEditText.getText().toString().trim();
                String password = passwordEditText.getText().toString().trim();
                String role = roleSpinner.getSelectedItem().toString().trim();

                // Call the API service method to add a user
                Call<Addresponse> call = apiService.addUser(username, email, password, role);
                call.enqueue(new Callback<Addresponse>() {
                    @Override
                    public void onResponse(Call<Addresponse> call, Response<Addresponse> response) {
                        if (response.isSuccessful()) {
                            Addresponse addResponse = response.body();
                            String status = addResponse.getStatus();
                            String message = addResponse.getMessage();
                            Toast.makeText(Gere_utlisateur.this, message, Toast.LENGTH_SHORT).show();
                        } else {
                            Toast.makeText(Gere_utlisateur.this, "Error occurred. Please try again.", Toast.LENGTH_SHORT).show();
                        }
                    }

                    @Override
                    public void onFailure(Call<Addresponse> call, Throwable t) {
                        Toast.makeText(Gere_utlisateur.this, "Network error. Please try again later.", Toast.LENGTH_SHORT).show();
                    }
                });
            }
        });

        // Set onTouchListener for generating password when drawable is clicked
        passwordEditText.setOnTouchListener(new View.OnTouchListener() {
            @Override
            public boolean onTouch(View v, MotionEvent event) {
                if (event.getAction() == MotionEvent.ACTION_UP) {
                    // Calculate the bounds of the drawable
                    Rect bounds = passwordEditText.getCompoundDrawables()[2].getBounds();

                    // Check if touch event occurred within the bounds of the drawable
                    if (event.getRawX() >= (passwordEditText.getRight() - bounds.width())) {
                        // Click occurred on the drawable, generate and set password
                        String generatedPassword = generateRandomPassword(8); // Change length as needed
                        passwordEditText.setText(generatedPassword);
                        return true; // Consume the event
                    }
                }
                return false;
            }
        });
    }

    private String generateRandomPassword(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-_=+";
        StringBuilder password = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            password.append(characters.charAt(random.nextInt(characters.length())));
        }
        return password.toString();
    }

    private void populateRoleSpinner() {
        // Define an array of roles
        String[] roles = {"driver", "mecano", "chef"};

        // Create an ArrayAdapter using the string array and a default spinner layout
        ArrayAdapter<String> adapter = new ArrayAdapter<>(this, android.R.layout.simple_spinner_item, roles);

        // Specify the layout to use when the list of choices appears
        adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);

        // Apply the adapter to the spinner
        roleSpinner.setAdapter(adapter);
    }

}
