package com.lohika.morning.spark.presentation.spark.driver.reader;

public interface DataFilesReader<T> {

    T readAllParticipants(final boolean includeOnlyPresentParticipants);

}
