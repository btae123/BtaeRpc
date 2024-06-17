package com.rpc.registry.ZK;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Configuration
public class zkClient {

    @Value("127.0.0.1:2181") // 使用属性注入ZooKeeper地址，未配置时使用默认值
    private String zookeeperAddress;

    private static final int BASE_SLEEP_TIME = 1000; // 基础重试等待时间
    private static final int MAX_RETRIES = 3; // 最大重试次数

    private static CuratorFramework zkClient;
    /**
     * 创建并配置CuratorFramework客户端，作为Spring Bean管理。
     * @return CuratorFramework实例
     */
    @Bean(destroyMethod = "close")
    protected CuratorFramework getZkClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME, MAX_RETRIES);

        zkClient = CuratorFrameworkFactory.newClient(zookeeperAddress, retryPolicy);

        zkClient.start();
        try {
            if (!zkClient.blockUntilConnected(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Unable to connect to the ZooKeeper server within 30 seconds.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while connecting to the ZooKeeper server.", e);
        }

        return zkClient;
    }
}