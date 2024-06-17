package com.rpc.registry;


import java.net.InetSocketAddress;
import java.util.List;

public interface Registy {
    void registyService(String path);
    List<String> getService(String seviceName);

    void clearRegistry(InetSocketAddress inetSocketAddress);
}
