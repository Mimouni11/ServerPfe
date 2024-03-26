package com.example.pfemini;

import static android.content.ContentValues.TAG;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

import android.content.Context;
import android.content.pm.PackageManager;
import android.graphics.Bitmap;
import android.graphics.ImageFormat;
import android.hardware.camera2.CameraAccessException;
import android.hardware.camera2.CameraCaptureSession;
import android.hardware.camera2.CameraCharacteristics;
import android.hardware.camera2.CameraDevice;
import android.hardware.camera2.CameraManager;
import android.hardware.camera2.CaptureRequest;
import android.hardware.camera2.CaptureResult;
import android.hardware.camera2.TotalCaptureResult;
import android.hardware.camera2.params.StreamConfigurationMap;
import android.media.Image;
import android.media.ImageReader;
import android.os.Bundle;
import android.util.Log;
import android.util.Size;
import android.view.Surface;
import android.view.SurfaceHolder;
import android.view.SurfaceView;
import android.widget.Toast;
import android.Manifest;

import com.google.zxing.BinaryBitmap;
import com.google.zxing.MultiFormatReader;
import com.google.zxing.RGBLuminanceSource;
import com.google.zxing.Result;
import com.google.zxing.common.HybridBinarizer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class Qrscan extends AppCompatActivity implements SurfaceHolder.Callback {
    private static final int CAMERA_PERMISSION_REQUEST_CODE = 200;
    private SurfaceHolder surfaceHolder;
    private SurfaceView surfaceView;
    private ImageReader imageReader;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_qrscan);
        surfaceView = findViewById(R.id.surfaceView);
        surfaceHolder = surfaceView.getHolder();
        surfaceHolder.addCallback(this);
        if (checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
            requestCameraPermission();
        } else {
            initializeScanner();
        }
    }

    private void requestCameraPermission() {
        ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.CAMERA}, CAMERA_PERMISSION_REQUEST_CODE);
    }

    private void startScanning() {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.CAMERA}, CAMERA_PERMISSION_REQUEST_CODE);
            return;
        }

        CameraManager cameraManager = (CameraManager) getSystemService(Context.CAMERA_SERVICE);
        try {
            if (cameraManager != null) {
                String cameraId = cameraManager.getCameraIdList()[0];
                CameraCharacteristics characteristics = cameraManager.getCameraCharacteristics(cameraId);
                StreamConfigurationMap map = characteristics.get(CameraCharacteristics.SCALER_STREAM_CONFIGURATION_MAP);
                if (map == null) {
                    Log.e(TAG, "StreamConfigurationMap is null");
                    return;
                }

                Size[] outputSizes = map.getOutputSizes(SurfaceHolder.class);
                if (outputSizes == null || outputSizes.length == 0) {
                    Log.e(TAG, "Output sizes are null or empty");
                    return;
                }

                Size optimalSize = getOptimalPreviewSize(outputSizes, surfaceView.getWidth(), surfaceView.getHeight());
                surfaceHolder.setFixedSize(optimalSize.getWidth(), optimalSize.getHeight());

                imageReader = ImageReader.newInstance(optimalSize.getWidth(), optimalSize.getHeight(), ImageFormat.YUV_420_888, 2);
                imageReader.setOnImageAvailableListener(new ImageReader.OnImageAvailableListener() {
                    @Override
                    public void onImageAvailable(ImageReader reader) {
                        Image image = reader.acquireLatestImage();
                        if (image != null) {
                            processImage(image);
                            image.close();
                        }
                    }
                }, null);

                if (surfaceHolder.getSurface() != null && imageReader.getSurface() != null) {
                    cameraManager.openCamera(cameraId, new CameraDevice.StateCallback() {
                        @Override
                        public void onOpened(@NonNull CameraDevice camera) {
                            try {
                                camera.createCaptureSession(Arrays.asList(surfaceHolder.getSurface(), imageReader.getSurface()), new CameraCaptureSession.StateCallback() {
                                    @Override
                                    public void onConfigured(@NonNull CameraCaptureSession session) {
                                        try {
                                            CaptureRequest.Builder captureRequestBuilder = camera.createCaptureRequest(CameraDevice.TEMPLATE_PREVIEW);
                                            captureRequestBuilder.addTarget(surfaceHolder.getSurface());
                                            captureRequestBuilder.addTarget(imageReader.getSurface());
                                            session.setRepeatingRequest(captureRequestBuilder.build(), new CameraCaptureSession.CaptureCallback() {
                                                @Override
                                                public void onCaptureCompleted(@NonNull CameraCaptureSession session, @NonNull CaptureRequest request, @NonNull TotalCaptureResult result) {
                                                    // No need to process the result here since we're using ImageReader to capture images
                                                }
                                            }, null);
                                        } catch (CameraAccessException e) {
                                            e.printStackTrace();
                                        }
                                    }

                                    @Override
                                    public void onConfigureFailed(@NonNull CameraCaptureSession session) {
                                        Toast.makeText(Qrscan.this, "Failed to configure camera capture session", Toast.LENGTH_SHORT).show();
                                    }
                                }, null);
                            } catch (CameraAccessException e) {
                                e.printStackTrace();
                            }
                        }

                        @Override
                        public void onDisconnected(@NonNull CameraDevice camera) {
                            Toast.makeText(Qrscan.this, "Camera disconnected", Toast.LENGTH_SHORT).show();
                        }

                        @Override
                        public void onError(@NonNull CameraDevice camera, int error) {
                            Toast.makeText(Qrscan.this, "Camera error: " + error, Toast.LENGTH_SHORT).show();
                        }
                    }, null);
                } else {
                    Log.e(TAG, "Surface is null");
                }
            }
        } catch (CameraAccessException e) {
            e.printStackTrace();
        }
    }


    private Size getOptimalPreviewSize(Size[] sizes, int width, int height) {
        double targetRatio = (double) height / width;

        if (sizes == null) return null;

        Size optimalSize = null;
        double minDiff = Double.MAX_VALUE;

        for (Size size : sizes) {
            double ratio = (double) size.getWidth() / size.getHeight();
            if (Math.abs(ratio - targetRatio) < minDiff) {
                optimalSize = size;
                minDiff = Math.abs(ratio - targetRatio);
            }
        }

        return optimalSize;
    }

    private void initializeScanner() {
        startScanning();
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == CAMERA_PERMISSION_REQUEST_CODE) {
            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                initializeScanner();
            } else {
                Toast.makeText(this, "Camera permission denied", Toast.LENGTH_SHORT).show();
                finish(); // Close the activity if camera permission is denied
            }
        }
    }

    private void processImage(Image image) {
        ByteBuffer buffer = image.getPlanes()[0].getBuffer();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        int width = image.getWidth();
        int height = image.getHeight();

        int rotation = getWindowManager().getDefaultDisplay().getRotation();
        if (rotation == Surface.ROTATION_0 || rotation == Surface.ROTATION_180) {
            // Landscape orientation, no rotation needed
        } else {
            // Portrait orientation, rotate the image 90 degrees
            byte[] rotatedData = new byte[data.length];
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++)
                    rotatedData[x * height + height - y - 1] = data[x + y * width];
            }
            int tmp = width;
            width = height;
            height = tmp;
            data = rotatedData;
        }

        int[] intData = new int[data.length];
        // Convert byte array to int array
        for (int i = 0; i < data.length; i++) {
            intData[i] = data[i] & 0xFF;
        }

        RGBLuminanceSource source = new RGBLuminanceSource(width, height, intData);
        BinaryBitmap binaryBitmap = new BinaryBitmap(new HybridBinarizer(source));

        MultiFormatReader multiFormatReader = new MultiFormatReader();
        try {
            Result qrCodeResult = multiFormatReader.decode(binaryBitmap);
            String qrCodeText = qrCodeResult.getText();
            Log.d("QR Code", "Text: " + qrCodeText);
        } catch (Exception e) {
            Log.e("QR Code", "Error decoding QR code: " + e.getMessage());
        } finally {
            image.close();
        }
    }


    @Override
    public void surfaceCreated(@NonNull SurfaceHolder holder) {

    }

    @Override
    public void surfaceChanged(@NonNull SurfaceHolder holder, int format, int width, int height) {
        if (holder.getSurface() == null) {
            return;
        }

        double aspectRatio = (double) width / height;

        CameraManager cameraManager = (CameraManager) getSystemService(Context.CAMERA_SERVICE);
        try {
            if (cameraManager != null) {
                String cameraId = cameraManager.getCameraIdList()[0];
                CameraCharacteristics characteristics = cameraManager.getCameraCharacteristics(cameraId);
                StreamConfigurationMap map = characteristics.get(CameraCharacteristics.SCALER_STREAM_CONFIGURATION_MAP);
                Size[] outputSizes = map.getOutputSizes(SurfaceHolder.class);

                Size optimalSize = null;
                double minAspectRatioDiff = Double.MAX_VALUE;
                for (Size size : outputSizes) {
                    double previewAspectRatio = (double) size.getWidth() / size.getHeight();
                    double aspectRatioDiff = Math.abs(previewAspectRatio - aspectRatio);
                    if (aspectRatioDiff < minAspectRatioDiff) {
                        optimalSize = size;
                        minAspectRatioDiff = aspectRatioDiff;
                    }
                }

                if (optimalSize != null) {
                    surfaceHolder.setFixedSize(optimalSize.getWidth(), optimalSize.getHeight());
                }
            }
        } catch (CameraAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void surfaceDestroyed(@NonNull SurfaceHolder holder) {

    }
}
