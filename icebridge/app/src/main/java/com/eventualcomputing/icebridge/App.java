
package com.eventualcomputing.icebridge;

import py4j.GatewayServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class App {

    public void printA() {
        System.out.println("hi");
    }

    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(new App());
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }
}
