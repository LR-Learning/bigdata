package com.flume.interceptor;

import com.alibaba.fastjson.JSONException;
import org.mortbay.util.ajax.JSON;

/**
 * @author LR
 * @create 2021-05-28:14:30
 */
public class JSONUtil {
    public static boolean validateJSON(String log){
        try {
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }

    }
}
