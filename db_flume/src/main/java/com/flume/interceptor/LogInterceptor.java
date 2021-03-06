package com.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.mortbay.util.ajax.JSON;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @author LR
 * @create 2021-05-28:14:27
 */
public class LogInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        if (JSONUtil.validateJSON(log)){
            return event;
        }else {
            return null;
        }

    }

    @Override
    public List<Event> intercept(List<Event> list) {
        //        while (iterator.hasNext()){
//           Event next = iterator.next();
//           if (intercept(next) == null){
//               iterator.remove();
//           }
//        }
        list.removeIf(next -> intercept(next) == null);
        return list;
    }

    @Override
    public void close() {


    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
