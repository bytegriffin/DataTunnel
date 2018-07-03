package com.bytegriffin.datatunnel;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import redis.clients.jedis.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TestRedis {

    private static final String host_split = ",";
    private static final int default_port = 6379;
    private static String redisMode = "one";
    private static String redisAuth;
    private static String address = "localhost:6379";
    private static String tablename = "tablename1";
    private static String columnname = "column1";

    /**
     * 建立链接
     *
     * @return
     */
    private static JedisCommands getConnection() {
        JedisCommands commands = null;
        if ("one".equalsIgnoreCase(redisMode)) {
            commands = one(address, redisAuth);
        } else if ("sharded".equalsIgnoreCase(redisMode)) {
            commands = sharded(address, redisAuth);
        } else if ("cluster".equalsIgnoreCase(redisMode)) {
            commands = cluster(address, redisAuth);
        }
        return commands;
    }

    private static JedisPoolConfig config() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(1000);
        config.setMaxIdle(5);
        config.setMinIdle(1);
        config.setBlockWhenExhausted(true);
        config.setMaxWaitMillis(60 * 1000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);
        config.setTimeBetweenEvictionRunsMillis(60 * 1000);
        return config;
    }

    /**
     * 关闭链接
     *
     * @param jedis
     */
    public static void close(JedisCommands jedis) {
        if ("one".equalsIgnoreCase(redisMode)) {
            Jedis one = (Jedis) jedis;
            if (one != null) {
                one.close();
            }
        } else if ("sharded".equalsIgnoreCase(redisMode)) {
            ShardedJedis sharedJedis = (ShardedJedis) jedis;
            if (sharedJedis != null) {
                sharedJedis.close();
            }
        } else if ("cluster".equalsIgnoreCase(redisMode)) {
            try {
                JedisCluster jedisCluster = (JedisCluster) jedis;
                if (jedisCluster != null) {
                    jedisCluster.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 单机
     */
    @SuppressWarnings("resource")
    public static JedisCommands one(String address, String password) {
        String firstAddress = Splitter.on(host_split).trimResults().split(address).iterator().next();
        List<String> list = Splitter.on(":").trimResults().splitToList(firstAddress);
        String host = "";
        Integer port = null;
        if (list.size() == 1) {
            host = Strings.isNullOrEmpty(list.get(0)) ? null : list.get(0);
            port = default_port;
        } else if (list.size() == 2) {
            host = Strings.isNullOrEmpty(list.get(0)) ? null : list.get(0);
            port = Strings.isNullOrEmpty(list.get(1)) ? null : Integer.valueOf(list.get(1));
        } else {
            System.exit(1);
        }
        JedisPool jedisPool = null;
        if (Strings.isNullOrEmpty(password)) {
            jedisPool = new JedisPool(config(), host, port);
        } else {
            jedisPool = new JedisPool(config(), host, port, 2000, password);
        }
        try {
            return jedisPool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    /**
     * 分片 客户端集群
     */
    public static JedisCommands sharded(String addresses, String password) {
        List<String> list = Splitter.on(host_split).trimResults().splitToList(addresses);
        List<JedisShardInfo> shards = Lists.newArrayList();
        for (String addr : list) {
            List<String> hostAndPort = Splitter.on(":").trimResults().splitToList(addr);
            if (hostAndPort.size() == 1) {
                JedisShardInfo node = new JedisShardInfo(hostAndPort.get(0), default_port);
                if (!Strings.isNullOrEmpty(password)) {
                    node.setPassword(password);
                }
                shards.add(node);
            } else if (hostAndPort.size() == 2) {
                JedisShardInfo node = new JedisShardInfo(hostAndPort.get(0), Integer.valueOf(hostAndPort.get(1)));
                if (!Strings.isNullOrEmpty(password)) {
                    node.setPassword(password);
                }
                shards.add(node);
            }
        }
        if (shards == null || shards.isEmpty()) {
            System.exit(1);
        }
        @SuppressWarnings("resource")
        ShardedJedisPool shardedJedisPool = new ShardedJedisPool(config(), shards);
        ShardedJedis sharedJedis = null;
        try {
            sharedJedis = shardedJedisPool.getResource();
            return sharedJedis;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    /**
     * 集群 redis 3以上支持cluster
     */
    public static JedisCommands cluster(String addresses, String password) {
        List<String> list = Splitter.on(host_split).trimResults().splitToList(addresses);
        Set<HostAndPort> clusterNodes = Sets.newHashSet();
        for (String addr : list) {
            List<String> hostAndPort = Splitter.on(":").trimResults().splitToList(addr);
            if (hostAndPort.size() == 1) {
                clusterNodes.add(new HostAndPort(hostAndPort.get(0), default_port));
            } else if (hostAndPort.size() == 2) {
                clusterNodes.add(new HostAndPort(hostAndPort.get(0), Integer.valueOf(hostAndPort.get(1))));
            }
        }
        if (clusterNodes == null || clusterNodes.isEmpty()) {
            System.exit(1);
        }
        JedisCluster jedisCluster = null;
        try {
            if (Strings.isNullOrEmpty(redisAuth)) {
                jedisCluster = new JedisCluster(clusterNodes, 2000, 2000, 5, config());
            } else {
                jedisCluster = new JedisCluster(clusterNodes, 2000, 2000, 5, redisAuth, config());
            }
            return jedisCluster;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    /**
     * 查询
     *
     * @return
     */
    static void query() {
        JedisCommands jedis = getConnection();
        String str = jedis.hget(tablename, columnname);
        System.out.println(str);
    }

    /**
     * 插入数据
     */
    static void insertData() {
        JedisCommands jedis = getConnection();
        jedis.hset(tablename, columnname, "test value");
    }

    public static void main(String[] args) {
        query();
    }

}
