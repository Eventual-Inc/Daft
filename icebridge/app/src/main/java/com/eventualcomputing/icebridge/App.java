
package com.eventualcomputing.icebridge;

import py4j.GatewayServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import java.util.Map;
import java.util.HashMap;


public class App {

    static public HashMap<java.lang.Integer, java.lang.Long> longMapConverter(
        Map<java.lang.Integer, java.lang.Integer> input
    ) {
        HashMap<java.lang.Integer, java.lang.Long> toRtn = new HashMap<java.lang.Integer, java.lang.Long>();
        for (Map.Entry<java.lang.Integer, java.lang.Integer> entry : input.entrySet()) {
            java.lang.Integer key = entry.getKey();
            java.lang.Integer value = entry.getValue();
            toRtn.put(key, Long.valueOf(value));
        }
        return toRtn;
 
    }


    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(new App());
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }
}
