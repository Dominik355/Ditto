package com.bilik.ditto.gateway.endpoints;

import com.bilik.ditto.gateway.server.RequestMapping;
import com.bilik.ditto.gateway.server.objects.HttpMethod;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class ActuatorEndpoint {

    @RequestMapping(method = HttpMethod.GET, value = "liveness")
    public void liveness() {}

    @RequestMapping(method = HttpMethod.GET, value = "readiness")
    public void readiness() {}

    @RequestMapping(method = HttpMethod.GET, value = "threadDump")
    public String threadDump() {
        StringBuffer threadDump = new StringBuffer(System.lineSeparator());
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        ThreadInfo[] threadInfos;
        try {
            threadInfos = threadMXBean.dumpAllThreads(true, true);
        } catch (Exception e) {
            threadInfos = threadMXBean.dumpAllThreads(false, false);
        }

        for(ThreadInfo threadInfo : threadInfos) {
            threadDump.append(threadInfo.toString());
        }
        return threadDump.toString();
    }

}
