package com.example.data;

public class CommonUtil {

    public static boolean isValidDate(String dateInISOFormat) {
        if (dateInISOFormat == null) {
            return false;
        } else if (dateInISOFormat.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return true;
        }

        return false;
    }
}
