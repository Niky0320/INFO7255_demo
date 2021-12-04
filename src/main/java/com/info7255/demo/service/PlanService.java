package com.info7255.demo.service;


import org.json.JSONArray;
import org.json.JSONObject;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;

import java.util.*;


@Service
public class PlanService {
    private final Jedis jedis;
    private final ETagService eTagService;

    public PlanService(Jedis jedis, ETagService eTagService) {
        this.jedis = jedis;
        this.eTagService = eTagService;
    }

    public boolean isKeyPresent(String key) {
        Map<String, String> value = jedis.hgetAll(key);
        jedis.close();
        return !(value == null || value.isEmpty());
    }

    public String getETag(String key) {
        return jedis.hget(key, "eTag");
    }

    public String setETag(String key, JSONObject jsonObject) {
        String eTag = eTagService.getETag(jsonObject);
        jedis.hset(key, "eTag", eTag);
        return eTag;
    }

    public String createPlan(JSONObject plan, String key) {
        jsonToMap(plan);
        return setETag(key, plan);
    }

    public List<Object> jsonToList(JSONArray jsonArray) {
        List<Object> result = new ArrayList<>();
        for (Object value : jsonArray) {
            if (value instanceof JSONArray) value = jsonToList((JSONArray) value);
            else if (value instanceof JSONObject) value = jsonToMap((JSONObject) value);
            result.add(value);
        }
        return result;
    }
    

    public Map<String, Map<String, Object>> jsonToMap(JSONObject jsonObject) {
        Map<String, Map<String, Object>> map = new HashMap<>();
        Map<String, Object> contentMap = new HashMap<>();

        for (String key : jsonObject.keySet()) {
            String redisKey = jsonObject.get("objectType") + ":" + jsonObject.get("objectId");
            Object value = jsonObject.get(key);

            if (value instanceof JSONObject) {
                value = jsonToMap((JSONObject) value);
                jedis.sadd(redisKey + ":" + key, ((Map<String, Map<String, Object>>) value).entrySet().iterator().next().getKey());
            } else if (value instanceof JSONArray) {
            	value = jsonToList((JSONArray) value);
                List<Map<String, Map<String, Object>>> list = (List<Map<String, Map<String, Object>>>) value;
				
                for(Map<String, Map<String, Object>> entry: list){
					for(String listKey: entry.keySet()){
						jedis.sadd(redisKey +"_"+ key, listKey);
					}
				}
                
            } else {
                jedis.hset(redisKey, key, value.toString());
                contentMap.put(key, value);
                map.put(redisKey, contentMap);
            }
        }
        return map;
    }
    
    public Map<String, Object> getPlan(String key) {
        Map<String, Object> result = new HashMap<>();
        getFunc(key, result);
        return result;
    }


    private Map<String, Object> getFunc(String key, Map<String, Object> result) {
    	Set<String> keys = jedis.keys(key + "_*");
        keys.add(key);

        for (String k : keys) {
            if (k.equals(key)) {
                Map<String, String> object = jedis.hgetAll(k);
                for (String attrKey : object.keySet()) {
                    if (!attrKey.equals("etag")) {
                    	try {
                    		Integer.parseInt(object.get(attrKey));
                    		result.put(attrKey, Integer.parseInt(object.get(attrKey)));
                    	}
                    	catch(Exception e){
                    		result.put(attrKey,object.get(attrKey));
                    	}
                    }
                }
            }
            else {
                String newKey = k.substring((key + "_").length());
                Set<String> members = jedis.smembers(k);
                if (isNest(members, newKey)) {
                    List<Object> listObj = new ArrayList<>();
                    for (String member : members) {
                            Map<String, Object> listMap = new HashMap<>();
                            listObj.add(getFunc(member, listMap));
                    }
                	result.put(newKey, listObj);
                } 
                else {
                    Map<String, String> object = jedis.hgetAll(members.iterator().next());
                    Map<String, Object> innerMap = new HashMap<>();
                    for (String attrKey : object.keySet()) {
                    	try {
                    		Integer.parseInt(object.get(attrKey));
                    		innerMap.put(attrKey, Integer.parseInt(object.get(attrKey)));
                    	}
                    	catch(Exception e){
                    		innerMap.put(attrKey,object.get(attrKey));
                    	}
                    }
                    result.put(newKey, innerMap);
                }
            }
        }
        return result;
    }
    
    public void deletePlan(String key) {
    	Set<String> keys = jedis.keys(key + "_*");
        keys.add(key);

        for (String k : keys) {
            if (k.equals(key)) {
                jedis.del(new String[]{k});

            } else {
                String newKey = k.substring((key + "_").length());
                Set<String> members = jedis.smembers(k);
                if (isNest(members, newKey)) {
                    for (String member : members) {
                        	deletePlan(member);
                    }
                    jedis.del(new String[]{k});
                } else {
                        jedis.del(new String[]{members.iterator().next(), k});

                }
            }
        }
    }


    public boolean isNest(Set<String> s, String str) {
    	if(s.size() > 1 || str.equals("linkedPlanServices")){
    		return true;
    	}
    	return false;
    }
}
