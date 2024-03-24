package com.jianhongl.buffer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * <pre>
 * 算法说明:
 * 读与写都用指针的移动成功作为相关槽位的权限获取成功的标识.
 * - 读: 在移动指针前,先缓存指针下的值. 确保不是空后再移动指针值. 在尝试移动成功后,pRead 原位置的值表明已经读取成功了.
 * 读取完成,必须清理干净原来位置的数据.备 pWrite指针写入. 因为如果不清理. 在pWrite的时候就会遇到 slot 为 not null 的情况. 此时无法区分是本身有值, 还是读取后的遗留值. 此时如果 pWrite都无差别写,则会使 下一轮的 pRead 提前取到 pWrite move后还未写前的值. 因此必须要清理.
 * - 写: 在移动指针前, 先缓存下指针处的值. 确保为空时,再移动指针. 目的是使前面的 pRead 操作已经清理掉待 take 出的 value.
 * size: 必须是2^n 的正整数. 这样在算位置的时候可以使用位运算,加快速度.
 * 真正能存储的元素个数为 size-1. 因此. size=1时.不能存储元素. size=2时.只能存储一个元素.
 *
 * 最好存储的元素是不重复的. 没有测试这种情况. 不过理论上没有问题
 * <p>
 * LockFreeRingBufferWithWriteNotCheckNull.png 展示了 pWrite不check 当前位置是否为NULL造成的潜在问题.
 * </pre>
 *
 * @param <T> 缓存的数据类型.
 */
public class LockFreeRingBuffer<T> {

    public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LockFreeRingBuffer.class);

    private final AtomicReferenceArray<T> buffer;
    private final int bufferSize;
    private final long bufferSizeMask;

    private final AtomicLong writeIndex = new AtomicLong(0);
    private final AtomicLong readIndex = new AtomicLong(0);

    public LockFreeRingBuffer(int bufferSize) {
        // Check if bufferSize is positive
        if (bufferSize <= 1) {
            throw new IllegalArgumentException("bufferSize must be positive");
        }

        // Check if bufferSize is power of 2
        int zCnt = 0;
        int _bufferSize = bufferSize;
        while (_bufferSize > 0) {
            if ((_bufferSize & 1) == 1) {
                zCnt++;
            }
            if (zCnt > 1) {
                throw new IllegalArgumentException("bufferSize must be power of 2");
            }
            _bufferSize = _bufferSize >> 1;
        }

        // Initialize buffer and bufferSize
        this.buffer = new AtomicReferenceArray<>(bufferSize);
        this.bufferSize = bufferSize;
        this.bufferSizeMask = bufferSize - 1;
    }

    public int push(T value) {

        // 确保写入数据有效
        if (value == null) {
            return -1;
        }

        long pWrite, pRead;
        int loopCnt = 0;
        for (; ; ) {

            int _rIndex = makeIndex(pRead = readIndex.get());
            int _wIndex = makeIndex(pWrite = writeIndex.get()); // push 操作. _wIndex 后读. 以期读到最新的版本.

            if (nextIndex(pWrite) == _rIndex) {
                // buffer is full
                return -2;
            }

            // 确保当前的写指针指向的槽位是 NULL. 也就是可以写入的. (保证take 方已经把数据清理干净了)
            if (buffer.get(_wIndex) != null) {
                if ((++loopCnt) > 16) {
                    logger.trace("TRACE: push data retry [01] - buffer[{}] is not null, pRead: {}, pWrite: {} readIndex:{} writeIndex:{} loopCnt:{}",
                            _wIndex, pRead, pWrite, readIndex.get(), writeIndex.get(), loopCnt);
                    Thread.yield();
                }
                continue;
            }

            // 先更新指针, 再写值. 保证写所有权正确
            if (writeIndex.compareAndSet(pWrite, pWrite + 1)) {

                // 写值: 理论上这个位置一定是空待写的.
                if (buffer.compareAndSet(_wIndex, null, value)) {
                    // writeCnt.incrementAndGet();
                    return _wIndex;
                }
                // 这样就可以保证这里不会出现了.
                throw new RuntimeException("state error");
            }
        }
    }

    public T pop() {

        int loopCnt = 0;
        long pRead, pWrite;
        for (; ; ) {

            // P_w == P_r , 缓冲区为空
            int _rIndex = makeIndex(pRead = readIndex.get());
            int _wIndex = makeIndex(pWrite = writeIndex.get());

            if (_rIndex == _wIndex) {
                // buffer is empty
                return null;
            }

            T t = buffer.get(_rIndex); // 本来这里不需要判定null.但是由于是用的 pRead 的快照. 所以可能会出现null的情况.

            if (t == null) {
                if ((++loopCnt) > 16) {
                    logger.trace("TRACE: pop  data retry [20] - buffer[{}] is     null, pRead: {}, pWrite: {} readIndex:{} writeIndex:{} loopCnt:{}",
                            _rIndex, pRead, pWrite, readIndex.get(), writeIndex.get(), loopCnt);
                    Thread.yield();
                }
                continue;
            }

            /* ************************************************
             *              pWrite
             *              |
             *              v
             *  [] -> [] -> [] -> [] -> [] -> [] -> [] -> []
             *        ^
             *        |
             *        pRead
             *  ************************************************
             *  case: pRead = 1, pWrite = 1
             *        然后 pWrite = pWrite + 1 = 2
             *        但是还没有来得及写入数据. 这时候 pRead = 1, pWrite = 2. pRead 位置数据是空的.
             *        此时,上面的 t==null 会导致直接continue.
             *        多次循环后,pRead 处的值终于有效了. 然后这里也正常的把pRead的值+1.表明获取到了 pos_1 位置数据的所有权.
             *        在并发竞争下,只有一个线程可以获取到这个位置的数据的所有权.
             */
            if (readIndex.compareAndSet(pRead, pRead + 1)) {
                // 指针正确+1,
                // 表明可以安全的操作原指针位置的数据. 而且上面已经保证读取到了非NULL的值. 也就是 与push的竞争写入 已经完成了.
                //
                // set to null
                boolean compareAndSet = buffer.compareAndSet(_rIndex, t, null);

                // 正常情况一定会设置成功.
                if (compareAndSet) {
                    // readCnt.incrementAndGet();
                    // 竞争到当前结点, 读取成功. t值是有效的.
                    return t;
                }
                logger.error("ERROR: pop_data_error - set to null failed, pRead: {} ({}) , pWrite: {} ({})readIndex:{} writeIndex:{}",
                        pRead, _rIndex, pWrite, _wIndex, readIndex.get(), writeIndex.get());
                // 这样就可以保证这里不会出现了.
                throw new RuntimeException("state error");
            }
        }
    }

    /**
     * 有被内联的可能性.暂不做优化
     */
    private int nextIndex(long currentIndex) {
        return (int) ((currentIndex + 1) & bufferSizeMask);
    }

    /**
     * 有被内联的可能性.暂不做优化
     */
    private int makeIndex(long currentIndex) {
        return (int) (currentIndex & bufferSizeMask);
    }

    // ======================== get / setter =======================
    public long getReadCnt() {
        return readIndex.get();
    }

    public long getWriteCnt() {
        return writeIndex.get();
    }

    @Override
    public String toString() {
        return "LockFreeRingBuffer{" +
                "bufferSize=" + bufferSize +
                ", bufferSizeMask=" + bufferSizeMask +
                ", writeIndex=" + writeIndex +
                ", readIndex=" + readIndex +
                '}';
    }
}

