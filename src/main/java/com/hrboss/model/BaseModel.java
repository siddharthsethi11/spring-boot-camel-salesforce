package com.hrboss.model;

import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

/**
 * Base wrapper class for all model. All commons and shareable property should be defined in this class.
 * <p>
 * All subclasses are representation of MongoDB {@link BasicDBObject}.
 *
 * @author kwidjaja Created on 16/04/14.
 */
public abstract class BaseModel extends BasicDBObject implements Serializable {

    private static final long serialVersionUID = 3077846942765290287L;

    protected BaseModel() {
    }

    protected BaseModel(int size) {
        super(size);
    }

    protected BaseModel(String key, Object value) {
        super(key, value);
    }

    protected BaseModel(Map<String, Object> m) {
        super(m);
    }

    public String getId() {
        Object id = get("_id");
        if (id != null) {
            if (id instanceof String && ObjectId.isValid(id.toString())) {
                setId(id.toString());
            }

            return getObjectId("_id").toString();
        }

        return "";
    }

    public void setId(String id) {
        put("_id", new ObjectId(id));
    }

    public Date getInsertTimestamp() {
        if (containsField("insertAt")) {
            return getDate("insertAt");
        }

        return null;
    }

    public void setInsertTimestamp(Date insertTimestamp) {
        put("insertAt", insertTimestamp);
    }

    public Date getUpdatedTimestamp() {
        if (containsField("updateAt")) {
            return getDate("updateAt");
        }

        return null;
    }

    public void setUpdatedTimestamp(Date updatedTimestamp) {
        put("updateAt", updatedTimestamp);
    }

    @SuppressWarnings("unused")
    public Date geValidFrom() {
        if (get("validFrom") != null) {
            return getDate("validFrom");
        }

        return null;
    }

    @SuppressWarnings("unused")
    public void setValidFrom(Date validFrom) {
        put("validFrom", validFrom);
    }

    @SuppressWarnings("unused")
    public Date geValidTo() {
        if (get("validTo") != null) {
            return getDate("validTo");
        }

        return null;
    }

    @SuppressWarnings("unused")
    public void setValidTo(Date validTo) {
        put("validTo", validTo);
    }

}
