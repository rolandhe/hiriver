package com.hiriver.streamsource;

/**
 * created by Yang Huawei (xander.yhw@alibaba-inc.com) on 2018/9/3 18:07
 */
public interface DbHostInfoSupplier {

    /**
     * <pre>
     * 提供可用的数据库源信息。
     * 如有多个数据源，不推荐多个候选数据源间随机选择，较佳的方式是按照优先级探活；
     * </pre>
     *
     * @return 可用的数据源
     */
    DbHostInfo available();

}
