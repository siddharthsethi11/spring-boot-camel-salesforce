/**
 * Copyright (c) 2015 HRBoss
 */
package com.github.deeprot.model;

import java.util.Map;

import com.mongodb.BasicDBObject;

/**
 * @author Nguyen Nhat Quang
 *
 */
public class TablePermission extends BasicDBObject {

    /**
     * 
     */
    private static final long serialVersionUID = 6483798113335649855L;

    public TablePermission() {
    }

    public TablePermission(int size) {
        super(size);
    }

    public TablePermission(String key, Object value) {
        super(key, value);
    }

    public TablePermission(Map<String, Object> m) {
        super(m);
    }

    // grantor
    public String getGrantor() {
        return containsField("grantor") ? getString("grantor") : "";
    }

    public void setGrantor(String grantor) {
        put("grantor", grantor);
    }

    // grantee
    public String getGrantee() {
        return containsField("grantee") ? getString("grantee") : "";
    }

    public void setGrantee(String grantee) {
        put("grantee", grantee);
    }

    // privileges
    public String getPrivileges() {
        return containsField("privileges") ? getString("privileges") : "";
    }

    public void setPrivileges(String privileges) {
        put("privileges", privileges);
    }
}
