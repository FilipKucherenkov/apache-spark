package org.fnk.etl;

public enum DatasetItem {
    RATINGS("u.data", "\t"),
    MOVIES("u.item", "|");

    public final String fileName;
    public final String delimiter;

    DatasetItem(String fileName, String delimiter){
        this.fileName = fileName;
        this.delimiter = delimiter;
    }
}
