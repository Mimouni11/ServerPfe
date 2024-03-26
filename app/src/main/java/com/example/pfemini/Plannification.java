package com.example.pfemini;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;

public class Plannification extends AppCompatActivity {

    private LinearLayout editTextContainer;
    private Button addButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        editTextContainer = findViewById(R.id.editTextContainer);
        addButton = findViewById(R.id.addButton);

        addButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                addEditText();
            }
        });
    }

    private void addEditText() {
        EditText newEditText = new EditText(this);
        LinearLayout.LayoutParams params = new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
        );
        params.setMargins(0, 16, 0, 0); // Set margins to create space between EditText fields
        newEditText.setLayoutParams(params);
        newEditText.setHint("New EditText");
        editTextContainer.addView(newEditText);
    }
}