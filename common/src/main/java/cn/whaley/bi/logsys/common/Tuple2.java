package cn.whaley.bi.logsys.common;

/**
 * Created by fj on 15/12/8.
 */
public  class Tuple2<T1,T2> {
    private T1 v1;
    private T2 v2;

    public Tuple2(){}

    public Tuple2(T1 v1, T2 v2){
        this.v1=v1;
        this.v2=v2;
    }

    public T1 getV1() {
        return v1;
    }

    public void setV1(T1 v1) {
        this.v1 = v1;
    }

    public T2 getV2() {
        return v2;
    }

    public void setV2(T2 v2) {
        this.v2 = v2;
    }
}
