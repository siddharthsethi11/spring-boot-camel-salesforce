/**
 * Copyright (c) 2013 HRBoss
 */
package com.github.deeprot.model;

/**
 * The enumeration defines all types of databases
 * 
 * @author Nguyen Nhat Quang
 *
 */
public enum DatabaseType {

    SQLSERVER("SQLSERVER", "SQLServer"),
    MYSQL("MYSQL", "MySQL"),
    ORACLE("ORACLE", "Oracle"),
    POSTGRESQL("POSTGRESQL", "Postgre");

    private String name;
    private String description;

    private DatabaseType(String name, String description) {

        this.name = name;
        this.description = description;
    }

    public String getName() {

        return name;
    }

    public void setName(String name) {

        this.name = name;
    }

    public String getDescription() {

        return description;
    }

    public void setDescription(String description) {

        this.description = description;
    }

    public static DatabaseType getDatabaseType(String name) {

        for (DatabaseType dbType : values()) {
            if (dbType.getName().equals(name)) {
                return dbType;
            }
        }

        return null;
    }
    
    public boolean equals(String type) {
    	return this.getName().equals(type);
    }
}
