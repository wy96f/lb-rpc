package cn.v5.lbrpc.common.client.core.loadbalancer;

import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.utils.BiMultiValMap;
import cn.v5.lbrpc.common.utils.MurmurHash;
import cn.v5.lbrpc.common.utils.SortedBiMultiValMap;
import com.google.common.collect.AbstractIterator;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by wy96fyw@gmail.com on 2017/1/26.
 */
public class ConsistentHashPolicy implements LoadBalancingPolicy {
    private static ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

    private final Ring ring = new Ring();

    public ByteBuffer getPartitionKey(Object p) {
        String ps = p.toString();
        int start = -1;
        int end = -1;
        for (int i = 0; i < ps.length(); i++) {
            if (ps.charAt(i) == '{' && start == -1) {
                start = i;
            }
            if (ps.charAt(i) == '}' && start != -1) {
                end = i;
                break;
            }
        }
        String res = (end != -1 && end != start + 1) ? ps.substring(start + 1, end) : ps;
        return ByteBuffer.wrap(res.getBytes());
    }

    @Override
    public Iterator<Host> queryPlan(final IRequest request) {
        final Ring clonedRing = ring.cachedRing();
        final int tokenSize = clonedRing.getTokens().size();
        final int hostSize = clonedRing.getHosts().size();

        if (tokenSize <= 0) {
            return new AbstractIterator<Host>() {
                @Override
                protected Host computeNext() {
                    return endOfData();
                }
            };
        }
        final Set<Host> queried = new HashSet<>();
        int start;
        if (request.getArgs() == null || request.getArgs().length < 1) {
            start = threadLocalRandom.nextInt(tokenSize);
        } else {
            start = clonedRing.getFirstIdx(Ring.getToken(getPartitionKey(request.getArgs()[0])));
        }
        final int idx = start;

        return new AbstractIterator<Host>() {
            int cur = idx;

            @Override
            protected Host computeNext() {
                Host host = null;
                while (host == null && queried.size() < hostSize) {
                    host = clonedRing.getTokenToHosts().get((clonedRing.getTokens().get(cur)));
                    cur++;
                    if (cur >= tokenSize) cur = 0;
                    if (!queried.add(host) || host.isDown()) {
                        host = null;
                    }
                }
                return host == null ? endOfData() : host;
            }
        };
    }

    @Override
    public void init(Collection<Host> hosts) {
        for (Host host : hosts) {
            ring.addHost(host);
        }
    }

    @Override
    public void onUp(Host host) {

    }

    @Override
    public void onDown(Host host) {

    }

    @Override
    public void onAdd(Host host) {
        ring.addHost(host);
    }

    private static class Ring {
        private static final int NUM_TOKENS = 128;
        private static final Comparator<Host> hostComparator = new Comparator<Host>() {
            @Override
            public int compare(Host o1, Host o2) {
                return o1.hashCode() - o2.hashCode();
            }
        };

        private BiMultiValMap<Long, Host> tokenToHosts = new BiMultiValMap<Long, Host>();
        private List<Long> tokens = new ArrayList<>();
        private List<Host> hosts = new ArrayList<>();

        private AtomicReference<Ring> cachedRing = new AtomicReference<>();

        private static Long getToken(ByteBuffer key) {
            long[] res = new long[2];
            MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0, res);
            return normalize(res[0]);
        }

        private int getFirstIdx(Long token) {
            int index = Collections.binarySearch(tokens, token);
            if (index < 0) {
                index = (index + 1) * (-1);
                if (index >= tokens.size()) {
                    index = 0;
                }
            }
            return index;
        }

        private static Long normalize(long l) {
            return l == Long.MIN_VALUE ? Long.MAX_VALUE : l;
        }

        private static Long generateToken() {
            return normalize(threadLocalRandom.nextLong());
        }

        public Ring() {
        }

        private Ring(BiMultiValMap<Long, Host> tokenToHosts) {
            this.tokenToHosts = tokenToHosts;
            this.tokens = new ArrayList<>(tokenToHosts.keySet());
            this.hosts = new ArrayList<>(tokenToHosts.values());
        }

        public BiMultiValMap<Long, Host> getTokenToHosts() {
            return tokenToHosts;
        }

        public List<Long> getTokens() {
            return tokens;
        }

        public List<Host> getHosts() {
            return hosts;
        }

        public Ring cachedRing() {
            Ring r = cachedRing.get();
            if (r != null) {
                return r;
            }
            synchronized (this) {
                if ((r = cachedRing.get()) == null) {
                    r = new Ring(SortedBiMultiValMap.create(tokenToHosts, null, hostComparator));
                    cachedRing.set(r);
                    return r;
                }
                return r;
            }
        }

        private void invalidateCache() {
            synchronized (this) {
                cachedRing.set(null);
            }
        }

        public void addHost(Host host) {
            synchronized (this) {
                for (int i = 0; i < NUM_TOKENS; i++) {
                    Long token = generateToken();
                    if (!tokenToHosts.containsKey(token)) {
                        tokenToHosts.put(token, host);
                    } else {
                        i--;
                    }
                }
            }
            invalidateCache();
        }

        public void removeHost() {
            // TODO
            invalidateCache();
        }
    }
}
