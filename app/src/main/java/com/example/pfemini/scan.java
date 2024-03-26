package com.example.pfemini;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import android.app.DownloadManager;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

import com.google.zxing.integration.android.IntentIntegrator;
import com.google.zxing.integration.android.IntentResult;

public class scan extends AppCompatActivity {
    Button scaner;
    TextView text;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_scan);
        getSupportActionBar().setDefaultDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setDisplayShowHomeEnabled(true);

        scaner = findViewById(R.id.scanner);
        text = findViewById(R.id.text);

        scaner.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                IntentIntegrator intentIntegrator = new IntentIntegrator(scan.this);
                intentIntegrator.setOrientationLocked(true);
                intentIntegrator.setPrompt("scanner");
                intentIntegrator.setDesiredBarcodeFormats(IntentIntegrator.QR_CODE);
                intentIntegrator.initiateScan();

            }
        });
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, @Nullable Intent data) {

        IntentResult intentResult = IntentIntegrator.parseActivityResult(requestCode, resultCode, data);
        if (intentResult != null) {
            String contents = intentResult.getContents();
            if (contents != null) {
                // Check if the scanned content is a URL pointing to a PDF
                if (contents.endsWith(".pdf")) {
                    // Initiate PDF download
                    downloadPDF(contents);
                } else {
                    // Display scanned text
                    text.setText(contents);
                }
            }
        } else {
            super.onActivityResult(requestCode, resultCode, data);
        }


    }

    private void downloadPDF(String url) {
        // Create a DownloadManager.Request with the PDF URL
        DownloadManager.Request request = new DownloadManager.Request(Uri.parse(url));
        request.setTitle("PDF Download"); // Set the title of the download notification
        request.setDescription("Downloading PDF"); // Set the description of the download notification
        request.setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED);
        request.setDestinationInExternalPublicDir(Environment.DIRECTORY_DOWNLOADS, "downloaded_pdf.pdf");

        // Get the DownloadManager service and enqueue the request
        DownloadManager downloadManager = (DownloadManager) getSystemService(Context.DOWNLOAD_SERVICE);
        if (downloadManager != null) {
            downloadManager.enqueue(request);
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
