package cn.v5.lbrpc.common.client.core.loadbalancer;

import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by wy96fyw@gmail.com on 2017/1/26.
 */
public class ConsistentHashPolicyTest {
    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testGetPartitionKey() {
        ConsistentHashPolicy policy = new ConsistentHashPolicy();
        assertTrue(policy.getPartitionKey("{}safjalfa").compareTo(ByteBuffer.wrap("{}safjalfa".getBytes())) == 0);
        assertTrue(policy.getPartitionKey("safj{}alfa").compareTo(ByteBuffer.wrap("safj{}alfa".getBytes())) == 0);
        assertTrue(policy.getPartitionKey("safj{a}alfa").compareTo(ByteBuffer.wrap("a".getBytes())) == 0);
        assertTrue(policy.getPartitionKey("safj}{{alfa").compareTo(ByteBuffer.wrap("safj}{{alfa".getBytes())) == 0);
        assertTrue(policy.getPartitionKey("safj{alf{a").compareTo(ByteBuffer.wrap("safj{alf{a".getBytes())) == 0);
        assertTrue(policy.getPartitionKey("safj{a{lf}a}c").compareTo(ByteBuffer.wrap("a{lf".getBytes())) == 0);
        assertTrue(policy.getPartitionKey("safj{a{lf}{a}c").compareTo(ByteBuffer.wrap("a{lf".getBytes())) == 0);
    }

    @Test
    public void testQueryPlan() throws Exception {
        ConsistentHashPolicy policy = new ConsistentHashPolicy();
        String service = "testQueryPlan";
        String protocol = "thrift";
        Host host1 = new Host(Pair.create(service, protocol), new InetSocketAddress(8081));
        Host host2 = new Host(Pair.create(service, protocol), new InetSocketAddress(8082));
        Host host3 = new Host(Pair.create(service, protocol), new InetSocketAddress(8083));
        List<Host> all = new ArrayList<Host>(){{
            add(host1);
            add(host2);
            add(host3);
        }};
        policy.init(all);

        Iterator<Host> plan = policy.queryPlan(new SettableRequest(new String[]{"xvq{xxvjjq}sk"}));
        List<Host> res = new ArrayList<>();
        while (plan.hasNext()) {
            res.add(plan.next());
        }
        assertTrue(res.containsAll(all) && all.containsAll(res));

        plan = policy.queryPlan(new SettableRequest(new String[]{"c{xxvjjq}xv"}));
        List<Host> res1 = new ArrayList<>();
        while (plan.hasNext()) {
            res1.add(plan.next());
        }
        assertEquals(res, res1);

        Host host4 = new Host(Pair.create(service, protocol), new InetSocketAddress(8084));
        all.add(host4);
        policy.onAdd(host4);
        plan = policy.queryPlan(new SettableRequest(new String[]{"xvq{xxvjjq}sk"}));
        res = new ArrayList<>();
        while (plan.hasNext()) {
            res.add(plan.next());
        }
        assertTrue(res.containsAll(all) && all.containsAll(res));

        policy.onRemoval(host4);
        plan = policy.queryPlan(new SettableRequest(new String[]{"c{xxvjjq}xv"}));
        res = new ArrayList<>();
        while (plan.hasNext()) {
            res.add(plan.next());
        }
        assertTrue(res1.containsAll(res) && res.containsAll(res1));
    }

    private static class SettableRequest implements IRequest {
        Object[] args;

        public SettableRequest(Object[] args) {
            this.args = args;
        }

        @Override
        public void setStreamId(int id) {

        }

        @Override
        public void setHeader(String key, String value) {

        }

        @Override
        public String getService() {
            return null;
        }

        @Override
        public String getMethod() {
            return null;
        }

        @Override
        public Object[] getArgs() {
            return args;
        }

        public void setArgs(Object[] args) {
            this.args = args;
        }
    }
}