package top.guoziyang.mydb.backend.common;

/**
 * 模拟python和go的切片操作，实现一个共享内存的子数组，
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
