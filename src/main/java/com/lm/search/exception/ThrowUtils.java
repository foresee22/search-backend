package com.lm.search.exception;

import com.lm.search.common.ErrorCode;

/**
 * 抛异常工具类
 *
 * @author lm
 * @from lm
 */
public class ThrowUtils {

    /**
     * 条件成立则抛异常
     *
     * @param condition
     * @param runtimeException
     */
    public static void throwIf(boolean condition, RuntimeException runtimeException) {
        if (condition) {
            throw runtimeException;
        }
    }

    // [编程导航学习圈](https://t.zsxq.com/0emozsIJh) 零基础快速入门编程，不走弯路！30+ 原创学习路线和专栏、1000+ 编程精华文章、500+ 编程学习指南、20T+ 编程资源汇总

    /**
     * 条件成立则抛异常
     *
     * @param condition
     * @param errorCode
     */
    public static void throwIf(boolean condition, ErrorCode errorCode) {
        throwIf(condition, new BusinessException(errorCode));
    }

    /**
     * 条件成立则抛异常
     *
     * @param condition
     * @param errorCode
     * @param message
     */
    public static void throwIf(boolean condition, ErrorCode errorCode, String message) {
        throwIf(condition, new BusinessException(errorCode, message));
    }
}
