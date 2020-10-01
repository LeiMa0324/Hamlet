package hamlet.base;

import lombok.Data;

import java.util.ArrayList;

@Data
public class DatasetSchema {
    private ArrayList<Attribute> attributes;

    public DatasetSchema(){
        this.attributes = new ArrayList<>();
    }

    public DatasetSchema(ArrayList<Attribute> attributes){
        this.attributes = attributes;
    }

    public void addAttribute(Attribute attribute){
        attributes.add(attribute);
    }

    public Attribute getAttributeByName(String attrName){
        for (Attribute attr: attributes){
            if (attr.getName().equals(attrName)){
                return attr;
            }
        }
        return null;
    }
}
