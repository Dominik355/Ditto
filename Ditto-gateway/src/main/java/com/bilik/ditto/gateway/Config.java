package com.bilik.ditto.gateway;


import static java.util.Objects.requireNonNull;

public class Config {

    public final String kubeNamespace;
    public final String dittoServiceName;
    public final int port;
    public final int backlog;

    public Config() {
        this.kubeNamespace = requireNonNull(getProperty("KUBE_NAMESPACE"), "variable KUBE_NAMESPACE can not be null");
        this.dittoServiceName = requireNonNull(getProperty("DITTO_SERVICE_NAME"), "variable DITTO_SERVICE_NAME can not be null");
        this.port = Integer.parseInt(requireNonNull(getProperty("PORT"), "variable PORT can not be null"));
        String backlogEnv = getProperty("BACKLOG");
        this.backlog = backlogEnv == null ? 0 : Integer.parseInt(backlogEnv);
    }

    private static String getProperty(String name) {
        var p = System.getProperty(name);
        return p != null ?
                p :
                System.getenv().get(name);
    }
}
