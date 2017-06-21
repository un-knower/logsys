package cn.whaley.bi.logsys.metadata.entity;

/**
 * Created by fj on 2017/6/20.
 * 包含自增器的实体
 */
public interface SeqEntity {

    /**
     * 自增器名称
     * @return
     */
    String getSeqName();

    /**
     * 设置当前自增器值
     * @param seq
     */
    void setSeq(Integer seq);

    /**
     * 获取当前自增器值
     * @return
     */
    Integer getSeq();
}
