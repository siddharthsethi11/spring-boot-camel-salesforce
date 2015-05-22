/**
 * Copyright (c) 2013 HRBoss
 */
package com.hrboss.model;

import java.util.Map;

import com.mongodb.BasicDBObject;


/**
 * @author Nguyen Nhat Quang
 *
 */
public class RawData extends BaseModel {

    /**
     * 
     */
    private static final long serialVersionUID = 1832706616824597697L;

    public RawData() {

    }

    public RawData(int size) {

        super(size);
    }

    public RawData(String key, Object value) {

        super(key, value);
    }

    public RawData(Map<String, Object> m) {

        super(m);
    }

    public String getDatsetId() {

        return containsField("datasetId") ? getString("datasetId") : "";
    }

    public void setDatasetId(String datasetId) {

        put("datasetId", datasetId);
    }

    @SuppressWarnings("rawtypes")
    public BasicDBObject getData() {

        if (containsField("data")) {
            Object data = get("data");
            if (Map.class.isAssignableFrom(data.getClass())) {
                return new BasicDBObject((Map) data);
            }
        }

        return new BasicDBObject();
    }

    public void setData(BasicDBObject data) {
        if (data != null) {
            put("data", data);
        }
    }

}
