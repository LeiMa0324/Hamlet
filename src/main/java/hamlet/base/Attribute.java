package hamlet.base;

import lombok.Data;

/**
 * an attribute in the data schema
 */
@Data
public class Attribute {

    private final String name;
    public Attribute(String name){
        this.name = name;
    }
}
