package com.cs324a2.client;

import com.cs324a2.common.JobRequest;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses manual text and CSV files into job inputs. List jobs take
 * comma/whitespace separated ints (optional header); PRIMESUM takes start,end.
 */
public final class JobParser {

    private JobParser() {
    }

    /** Parses manual text like "2, 4 5\n11" into integers. */
    public static List<Integer> parseNumberList(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("numbers must not be empty");
        }
        String[] tokens = text.trim().split("[,;\\s]+");
        List<Integer> numbers = new ArrayList<>(tokens.length);
        for (String token : tokens) {
            if (token.isBlank()) {
                continue;
            }
            try {
                numbers.add(Integer.parseInt(token.trim()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid integer: '" + token + "'", e);
            }
        }
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be empty");
        }
        return List.copyOf(numbers);
    }

    /** Load numbers from a CSV. Single column or comma-separated, optional header. */
    public static List<Integer> loadNumberList(File file) throws IOException {
        List<Integer> numbers = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            boolean firstLine = true;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                // skip one header row like "value" or "numbers"
                if (firstLine && line.matches("(?i)[a-z_]+.*") && !line.matches("-?\\d.*")) {
                    firstLine = false;
                    // header with digits: fall through and try to parse
                    if (!line.matches(".*\\d.*")) {
                        continue;
                    }
                }
                firstLine = false;
                for (String token : line.split("[,;\\s]+")) {
                    if (token.isBlank()) {
                        continue;
                    }
                    try {
                        numbers.add(Integer.parseInt(token.trim()));
                    } catch (NumberFormatException e) {
                        throw new IOException("invalid integer '" + token + "' in " + file.getName(), e);
                    }
                }
            }
        }
        if (numbers.isEmpty()) {
            throw new IOException("no numbers found in " + file.getName());
        }
        return List.copyOf(numbers);
    }

    /** Load a PRIMESUM range from "start,end" (single row, header optional). */
    public static long[] loadPrimeSumRange(File file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            long[] range = null;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (line.matches("(?i)[a-z_,;\\s]+") && !line.matches(".*\\d.*")) {
                    continue; // header like "start,end"
                }
                if (range != null) {
                    throw new IOException("expected one 'start,end' range in " + file.getName()
                            + " but found another row: " + line);
                }
                String[] tokens = line.split("[,;\\s]+");
                if (tokens.length < 2) {
                    throw new IOException("expected 'start,end' in " + file.getName() + " but got: " + line);
                }
                try {
                    long start = Long.parseLong(tokens[0].trim());
                    long end = Long.parseLong(tokens[1].trim());
                    try {
                        JobRequest.checkRangeSpan(start, end);
                    } catch (IllegalArgumentException e) {
                        throw new IOException(e.getMessage() + " in " + file.getName(), e);
                    }
                    range = new long[]{start, end};
                } catch (NumberFormatException e) {
                    throw new IOException("invalid range '" + line + "' in " + file.getName(), e);
                }
            }
            if (range != null) return range;
        }
        throw new IOException("no start,end range found in " + file.getName());
    }
}
