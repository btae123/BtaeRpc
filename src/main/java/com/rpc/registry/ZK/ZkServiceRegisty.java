package com.rpc.registry.ZK;

import com.rpc.registry.Registy;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
@Slf4j
public final class ZkServiceRegisty implements Registy {
    @Autowired
    zkClient ZK;

    CuratorFramework ZkClient = ZK.getZkClient();
    public static final String ZK_REGISTER_ROOT_PATH = "/rpc-root";
    private static final Map<String, List<String>> SERVICE_ADDRESS_MAP = new ConcurrentHashMap<>();
    private static final Set<String> REGISTERED_PATH_SET = ConcurrentHashMap.newKeySet();

//    @Override
//    public void registyService(String path) {
//        try {
//            if (REGISTERED_PATH_SET.contains(path) || ZkClient.checkExists().forPath(path) != null) {
//                log.info("The node already exists. The node is:[{}]", path);
//            } else {
//                //eg: /my-rpc/github.javaguide.HelloService/127.0.0.1:9999
//                ZkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
//                log.info("The node was created successfully. The node is:[{}]", path);
//            }
//            REGISTERED_PATH_SET.add(path);
//        } catch (Exception e) {
//            log.error("create persistent node for path [{}] fail", path);
//        }
//    }

    @Override
    public void registyService(String serviceName){
        try {
            if(REGISTERED_PATH_SET.contains(serviceName)||ZkClient.checkExists().forPath(serviceName)!=null){
                log.info("The Node already exists. The node is:[{}]",serviceName);
            }else {
                ZkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(serviceName);
                log.info("The node was created successfully. The node is:[{}]", serviceName);
            }
        } catch (Exception e) {
            log.error("create persistent node for path [{}] fail", serviceName);
        }
    }
    @Override
    public List<String> getService(String serviceName) {
//        if (SERVICE_ADDRESS_MAP.containsKey(seviceName)) {
//            return SERVICE_ADDRESS_MAP.get(seviceName);
//        }
//        List<String> result = null;
//        String servicePath = ZK_REGISTER_ROOT_PATH + "/" + seviceName;
//        try {
//            result = ZkClient.getChildren().forPath(servicePath);
//            SERVICE_ADDRESS_MAP.put(seviceName, result);
//            registerWatcher(seviceName, ZkClient);
//        } catch (Exception e) {
//            log.error("get children nodes for path [{}] fail", servicePath);
//        }
//        return result;
        if(SERVICE_ADDRESS_MAP.containsKey(serviceName)){
            return SERVICE_ADDRESS_MAP.get(serviceName);
        }
        List<String> result = null;
        String servicePath = ZK_REGISTER_ROOT_PATH + '/' + serviceName;
        try {
            result = ZkClient.getChildren().forPath(servicePath);
            SERVICE_ADDRESS_MAP.putIfAbsent(serviceName,result);
            registerWatcher(serviceName,ZkClient);
        } catch (Exception e) {
            log.error("get children nodes for path [{}] fail", servicePath);
        }
        return result;
    }

    @Override
    public void clearRegistry(InetSocketAddress inetSocketAddress) {
        // 使用Collectors收集需要删除的路径
        Set<String> pathsToRemove = REGISTERED_PATH_SET.parallelStream()
                .filter(p -> p.endsWith(inetSocketAddress.toString()))
                .collect(Collectors.toSet());

        pathsToRemove.forEach(p -> {
            try {
                ZkClient.delete().forPath(p);
                REGISTERED_PATH_SET.remove(p);
            } catch (Exception e) {
                log.error("clear registry for path [{}] fail", p);
            }
        });

        // 确保所有删除操作已完成后再打印日志
        log.info("All attempted registered services clearance for server, remaining paths:[{}]", REGISTERED_PATH_SET.toString());
    }

    private static void registerWatcher(String rpcServiceName,CuratorFramework ZkClient) throws Exception {
        String servicePath = ZK_REGISTER_ROOT_PATH + "/" + rpcServiceName;
        PathChildrenCache pathChildrenCache = new PathChildrenCache(ZkClient, servicePath, true);
        PathChildrenCacheListener pathChildrenCacheListener = (curatorFramework, pathChildrenCacheEvent) -> {
            List<String> serviceAddresses = curatorFramework.getChildren().forPath(servicePath);
            SERVICE_ADDRESS_MAP.put(rpcServiceName, serviceAddresses);
        };
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        pathChildrenCache.start();
    }
}
