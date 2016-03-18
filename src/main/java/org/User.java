package org;

import org.apache.avro.Schema;

/**
 * Created by kevin on 3/12/2016.
 */
public class User {

    private Schema.Type type = Schema.Type.RECORD;
    private String name;
    private int favoriteNumber;
    public void setName(String name) {
        this.name = name;
    }
    public void setFavoriteNumber(int favoriteNumber) {
        this.favoriteNumber = favoriteNumber;
    }
    public String getName() {
        return name;
    }
    public int getFavoriteNumber() {
        return favoriteNumber;
    }
    public Schema.Type getType() {
        return type;
    }
    public void setType(Schema.Type type) {
        this.type=type;
    }

}
