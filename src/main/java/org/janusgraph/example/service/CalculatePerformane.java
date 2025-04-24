package org.janusgraph.example.service;

import java.io.File;

public class CalculatePerformane {

    // Function to calculate the time taken in seconds
    public static double calculateTimeInSeconds(long startTime, long endTime) {
        long durationInNano = endTime - startTime;  // Duration in nanoseconds
        return durationInNano / 1_000_000_000.0;  // Convert to seconds
    }

    // Function to calculate the size of the database directory
    public static long calculateDirectorySize(File directory) {
        long size = 0;
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                size += calculateDirectorySize(file);  // Recurse into subdirectories
            } else {
                size += file.length();  // Add file size
            }
        }
        return size;
    }

}
