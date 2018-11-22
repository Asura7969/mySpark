package utils;

import com.alibaba.fastjson.JSONObject;

import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author gongwenzhou
 */
public class LogScanner {
    private final static Charset UTF8 = Charset.forName("utf-8");

    public static String scan(String log,List<String> keys) {

        byte[] s = log.getBytes(UTF8);

        int LENGTH = 512;
        List<byte[]> keyArray = keys.stream()
                .map(x -> x.getBytes(UTF8))
                .collect(Collectors.toList());

        int keysLength = keys.size();
        byte[][] valueArray = new byte[keysLength][LENGTH];
        int[] nextArray = new int[keysLength];
        int[] positionArray = new int[keysLength];


        int a = 0;//1:数组开始标记
        int k = 0;//1:匹配到key,需要匹配value
        int v = -1;
        int state = -1;
        int offset = 0;
        while (offset < s.length) {
            byte b = s[offset];

            if (state == -1 && k == 0) {
                for (int i = 0; i < keyArray.size(); i ++) {
                    int next = nextArray[i];
                    int len = keyArray.get(i).length;
                    byte[] key = keyArray.get(i);

                    if (next < len) {
                        if (key[next] == b) {
                            nextArray[i] ++;
                            if (nextArray[i] == len) {
                                if((s[offset + 1] == '\\' || s[offset + 1] == '"')){
                                    state = i;
                                    k = 1;//已经获取到一个key
                                    v = 0;//开始获取value
                                }else{
                                    nextArray[i] = 0;
                                }
                            }
                        } else {
                            nextArray[i] = 0;
                        }
                    }
                }
            } else {
                if(k == 1){
                    if(v == 0 && (b == ':' || b == '\\' ||  b == '"')){
                        //还没有匹配value,凡是带有 冒号,反斜杠,引号  直接跳过
                    }else if(v == 1 && (b == '\\' ||  b == '"')){
                        //匹配到的值，后面带有   反斜杠,引号  直接跳过
                    }else if(v == 1 && a == 1 && (b == ']' || b == '{')){
                        state = -1;
                        k = 0;
                        v = -1;
                        a = 0;
                    }else if(v == 1 && a == 0 && (b == ',' || b == '}')){
                        state = -1;
                        k = 0;
                        v = -1;
                    }else if (v == 0 && b == '[') {
                        a = 1;
                    }else if(v == 0 && b == '{'){
                        state = -1;
                        k = 0;
                        v = -1;
                    }else {
                        for (int i = 0; i < keyArray.size(); ++i) {
                            if (state == i) {
                                if (positionArray[i] < LENGTH) {
                                    valueArray[i][positionArray[i]++] = b;
                                }
                                if(a == 1){
                                    if(b == ']'){
                                        k = 0;
                                        v = -1;
                                        a = 0;
                                        state = -1;
                                    }else{
                                        v = 1;
                                    }
                                } else{
                                    v = 1;
                                }
                                break;
                            }
                        }
                    }
                }
            }
            offset ++;
        }

        JSONObject ret = new JSONObject();
        for (int i = 0; i < positionArray.length; i++) {
            if (positionArray[i] > 0) {
                ret.put(keys.get(i),new String(valueArray[i], 0, positionArray[i], UTF8));
            }else{
                ret.put(keys.get(i),"null");
            }
        }


        if(ret.size() == 0){
            return null;
        }

        return ret.toJSONString();
    }


    public static void main(String[] args) {
        String log = "ANALYSIS,2018-08-13 10:30:00.341,2,{\"condition\":{\"iface\":\"com.oss.dal.iface.BikeInfoIface\",\"peerIp\":{\"address\":\"10.111.61.193\",\"port\":34870},\"method\":\"getBikeCityInfo\",\"service\":\"AppOssDALService\",\"appid\":\"com.oss.dal\",\"rpcid\":\"1.1.1.1\",\"requestJson\":\"{\\\"arg0\\\":\\\"\\\\\\\"2100012152\\\\\\\"\\\"}\",\"reqid\":\"365b50f930f6483dafd7c36afcab9315\",\"timestamp\":1534127400341},\"entityMata\":{\"ipAddress\":\"10.81.68.66\",\"logValue\":0.0,\"metric\":\"REQUEST_SERVER\"}}";
        List<String> keys = new ArrayList<>();
        keys.add("metric");
        keys.add("arg0");
        keys.add("timestamp");
        System.out.println(scan(log, keys));
    }
}
