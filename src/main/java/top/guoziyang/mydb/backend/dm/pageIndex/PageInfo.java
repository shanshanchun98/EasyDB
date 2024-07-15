package top.guoziyang.mydb.backend.dm.pageIndex;

/**
 * 页面信息数据结构：页号 和 空闲大小
 */
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
