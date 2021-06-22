package org.icgc.dcc.download.server.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class DateUtils {
    /**
     * This method checks if two epoch times are equal up to seconds digits by diving both by a 1000 (i.e. remove
     * milliseconds digits) and check for equality. Note that this method does not round the epoch times, since
     * both numbers should have the same digits except for the last three.
     *
     * The use case for this method is for tests that assert epoch time equality, but the milliseconds digits are not
     * available for both numbers. Primary example is the Hadoop-common library methods which return epoch times with
     * no millisecond digits but Java-8 FileSystem methods return epoch times with milliseconds.
     *
     * Java 8 (OpenJDK) FS method returns   :1624388695494L
     * Hadoop FS method returns             :1624388695000L
     *
     * @param epochTimeA
     * @param epochTimeB
     * @return
     */
    public static Boolean isEpochsEqualUpToSecondsDigits(Long epochTimeA, Long epochTimeB) {
        return epochTimeA / 1000 == epochTimeB / 1000;
    }
}
