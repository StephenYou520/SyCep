package cep.functions;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;

import cep.pattern.Pattern;

/**
 * @author StephenYou
 * Created on 2023-07-23
 * Description: 动态Pattern接口（用户调用API）不区分key
 */
public interface DynamicPatternFunction<T> extends Function, Serializable {

    /***
     * 初始化
     * @throws Exception
     */
    public void init() throws Exception;

    /**
     * 注入新的pattern
     * @return
     */
    public Pattern<T,T> inject() throws Exception;

    /**
     * 一个扫描周期:ms
     * @return
     */
    public long getPeriod() throws Exception;

    /**
     * 规则是否发生变更
     * @return
     */
    public boolean isChanged() throws Exception;
}
