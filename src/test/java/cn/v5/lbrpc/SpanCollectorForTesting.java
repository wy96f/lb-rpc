package cn.v5.lbrpc;

import com.github.kristofa.brave.EmptySpanCollectorMetricsHandler;
import com.github.kristofa.brave.SpanCollector;
import com.github.kristofa.brave.http.HttpSpanCollector;
import com.twitter.zipkin.gen.Span;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * Created by yangwei on 27/9/16.
 */
public class SpanCollectorForTesting implements SpanCollector {
    private final static SpanCollectorForTesting instance = new SpanCollectorForTesting();

    private final static Logger logger = Logger.getLogger(SpanCollectorForTesting.class.getName());

    private final ConcurrentLinkedQueue<Span> spans = new ConcurrentLinkedQueue<>();

    private final static SpanCollector httpSpanCollector = HttpSpanCollector.create("http://127.0.0.1:9411", new EmptySpanCollectorMetricsHandler());

    public static SpanCollectorForTesting getInstance() {
        return instance;
    }

    public void clear() {
        spans.clear();
    }

    @Override
    public void collect(Span span) {
        logger.info("yw: " + span.toString());
        spans.add(span);
        httpSpanCollector.collect(span);
    }

    public List<Span> getCollectedSpans() {
        return new ArrayList<>(spans);
    }

    @Override
    public void addDefaultAnnotation(String s, String s1) {
        throw new UnsupportedOperationException();
    }
}
