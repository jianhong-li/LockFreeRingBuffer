package com.jianhongl.buffer.base;

import com.jianhongl.buffer.LockFreeRingBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockFreeRingBufferTest {

    public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LockFreeRingBufferTest.class);

    /**
     * test integer  overflow
     */
    @Test
    public void testOverflow() {
        int y = 0;
        int x = Integer.MAX_VALUE;
        x = x + 1;
        Assert.assertEquals(Integer.MIN_VALUE, x);
        y = x & 0x7;
        Assert.assertEquals(0, y);
        y = (++x) & 0x07;
        Assert.assertEquals(1, y);
        y = (++x) & 0x07;
        Assert.assertEquals(2, y);
    }

    /**
     * Test method for {@link LockFreeRingBuffer#LockFreeRingBuffer(int)}.
     * Test if the buffer size is power of 2
     */
    @Test
    public void testBufferSize() {
        Exception exception = null;
        try {
            LockFreeRingBuffer<Integer> buffer = new LockFreeRingBuffer<>(100);
        } catch (Exception e) {
            exception = e;
        }
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception instanceof IllegalArgumentException);


        exception = null;
        try {
            LockFreeRingBuffer<Integer> buffer = new LockFreeRingBuffer<>(4);
        } catch (Exception e) {
            exception = e;
        }
        Assert.assertNull(exception);


        exception = null;
        try {
            LockFreeRingBuffer<Integer> buffer = new LockFreeRingBuffer<>(1024);
        } catch (Exception e) {
            exception = e;
        }
        Assert.assertNull(exception);
    }

    /**
     * Test method for {@link LockFreeRingBuffer#push(Object)}.
     * Test if the buffer is full
     */
    @Test
    public void testPush() {
        LockFreeRingBuffer<Integer> buffer = new LockFreeRingBuffer<>(4);
        Assert.assertTrue(buffer.push(1) >= 0);
        Assert.assertTrue(buffer.push(2) >= 0);
        Assert.assertTrue(buffer.push(3) >= 0);
        Assert.assertFalse(buffer.push(4) >= 0);
        Assert.assertFalse(buffer.push(5) >= 0);
        Assert.assertFalse(buffer.push(null) >= 0);
    }

    @Test
    public void testSingleThread() {
        int bufferSize = 8;
        int threadCnt = 8;
        logger.info("threadCnt:{},bufferSize:{}", threadCnt, bufferSize);
        LockFreeRingBuffer<ExclusiveData> ringBuffer = new LockFreeRingBuffer<>(bufferSize);
        // init buffer data
        for (int i = 0; i < bufferSize - 1; i++) {
            ringBuffer.push(new ExclusiveData(i));
        }

        for (int i = 0; i < 2000; i++) {
            ExclusiveData data = ringBuffer.pop();
            Assert.assertNotNull(data);
            Assert.assertEquals(data.index, (i % 7));
            ringBuffer.push(data);
        }
    }

    /**
     * 测试一个环形缓冲区中的数据不会被同时分配给两个以上的线程使用.
     */
    @Test
    public void testCaseRepeatGetOne() throws InterruptedException {
        int bufferSize = 8;
        int threadCnt = 8;
        int loopCnt = 100;
        logger.info("threadCnt:{},bufferSize:{}", threadCnt, bufferSize);

        // init buffer data
        LockFreeRingBuffer<ExclusiveData> ringBuffer = new LockFreeRingBuffer<>(bufferSize);
        for (int i = 0; i < bufferSize - 1; i++) {
            ringBuffer.push(new ExclusiveData(i));
        }
        // reset count after init
        // ringBuffer.resetCnt();

        boolean[] rs = new boolean[threadCnt];
        Arrays.fill(rs, true);

        CountDownLatch countDownLatch = new CountDownLatch(threadCnt);

        for (int i = 0; i < threadCnt; i++) {
            int _index = i;
            new Thread(() -> {

                try {
                    for (int j = 0; j < loopCnt; j++) {

                        ExclusiveData exclusiveData = ringBuffer.pop();
                        if (exclusiveData != null) {
//                            logger.info("thread: {} get data:{}", _index, exclusiveData.index);
                            boolean success = exclusiveData.fooWithLock();
                            int push = ringBuffer.push(exclusiveData);
//                            logger.info("thread: {} push [{}] to index :{}", _index, exclusiveData.index, push);
                            if (!success) {
                                rs[_index] = false;
                                break;
                            }
                        } else {
//                            logger.warn("thread: {} no buffer data", _index);
                        }
                    }

                } catch (Exception e) {
                    logger.error("run with exclusiveData Error:", e);
                }
                countDownLatch.countDown();
            }).start();
        }

        countDownLatch.await();

        for (boolean r : rs) {
            Assert.assertTrue(r);
        }

        System.out.println(ringBuffer);

        Assert.assertEquals(ringBuffer.getReadCnt() + bufferSize - 1, ringBuffer.getWriteCnt());
        Assert.assertEquals(loopCnt * (threadCnt - 1), ringBuffer.getReadCnt());
    }

    public class ExclusiveData {

        private int index;

        private int slot = -1;

        public ExclusiveData(int index) {
            this.index = index;
        }

        @Override
        public String toString() {
            return "ExclusiveData{" +
                    "index=" + index +
                    '}';
        }

        private Lock lock = new ReentrantLock();

        public boolean fooWithLock() {
            boolean locked = lock.tryLock();
            if (locked) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    //
                }
                lock.unlock();
                return true;
            } else {
                logger.error("tryLock Data error");
                return false;
            }
        }
    }
}
