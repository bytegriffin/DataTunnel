package com.bytegriffin.datatunnel;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * https://mongodb.github.io/mongo-java-driver/3.6/
 */
public class TestMongoDB {
    //mongodb://user1:pwd1@host1/?authSource=db1&ssl=true
    private static String address = "mongodb://localhost:27017/test?authSource=db1&ssl=true";
    private static String tablename = "local";

    /***
     * 建立链接
     * @return
     */
    private static MongoDatabase getConn() {
        MongoClientURI mc = new MongoClientURI(address, getConfigBuilder());
        @SuppressWarnings("resource")
        MongoClient mongoClient = new MongoClient(mc);
        String databaseName = address.substring(address.lastIndexOf("/") + 1, address.indexOf("?") == -1 ? address.length() : address.indexOf("?"));
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        return database;
    }

    private static Builder getConfigBuilder() {
        return new MongoClientOptions.Builder()
                .connectTimeout(5000) // 链接超时时间
                .socketTimeout(5000) // read数据超时时间
                .readPreference(ReadPreference.primary()) // 最近优先策略
                .connectionsPerHost(30) // 每个地址最大请求数
                .maxWaitTime(1000 * 60 * 2) // 长链接的最大等待时间
                .threadsAllowedToBlockForConnectionMultiplier(50); // 一个socket最大的等待请求数
    }

    /**
     * 创建表
     */
    static void createTable() {
        MongoDatabase database = getConn();
        database.createCollection(tablename);
        System.out.println("创建表成功");
    }

    /**
     * 插入数据
     */
    static void insertData() {
        MongoDatabase database = getConn();
        MongoCollection<Document> collection = database.getCollection(tablename);
        Document doc = new Document();
        doc.append("column1", "value1");
        collection.insertOne(doc);
    }

    /**
     * 查询数据
     */
    static void query() {
        MongoDatabase database = getConn();
        MongoCollection<Document> collection = database.getCollection(tablename);

        collection.find();
    }

    public static void main(String[] args) {
        createTable();
    }
}
