package cn.v5.lbrpc.common.client.core;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.util.List;

/**
 * Created by yangwei on 15-5-6.
 */
public abstract class CloseFuture extends AbstractFuture<Void> {
    public static CloseFuture immediateFuture() {
        CloseFuture future = new CloseFuture() {
            @Override
            public CloseFuture force() {
                return this;
            }
        };
        future.set(null);
        return future;
    }

    public abstract CloseFuture force();

    static class Forwarding extends CloseFuture {
        private final List<CloseFuture> futures;

        Forwarding(List<CloseFuture> futures) {
            this.futures = futures;

            Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<Void>>() {
                @Override
                public void onSuccess(List<Void> voids) {
                    onFuturesDone();
                }

                @Override
                public void onFailure(Throwable throwable) {
                    setException(throwable);
                }
            });
        }

        @Override
        public CloseFuture force() {
            for (CloseFuture future : futures) {
                future.force();
            }
            return this;
        }

        protected void onFuturesDone() {
            set(null);
        }
    }
}
