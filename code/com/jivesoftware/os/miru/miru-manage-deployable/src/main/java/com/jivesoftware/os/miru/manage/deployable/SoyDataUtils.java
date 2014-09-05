/*
 * This class is adapted from code available at https://github.com/codedance/silken
 * which has been published with the below license information, and which contains
 * the following modifications:
 *
 *   1. Added enum support
 *
 * --------------------- Start original license information ---------------------
 *
 * (c) Copyright 2011-2013 PaperCut Software Int. Pty. Ltd. http://www.papercut.com/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SoyDataUtils {

    /**
     * Convert all data stored in a POJO or Map<String, Object> into a format compatible with Soy's DataMap.
     * This method will convert nested POJOs to a corresponding nested Maps.
     *
     * @param obj The Map or POJO who's data should be converted.
     * @return A Map of data compatible with Soy.
     */
    @SuppressWarnings("unchecked")
    public Map<String, ?> toSoyCompatibleMap(Object obj) {
        Object ret = toSoyCompatibleObjects(obj);
        if (!(ret instanceof Map)) {
            throw new IllegalArgumentException("Input should be a Map or POJO.");
        }
        return (Map<String, ?>) ret;
    }

    /**
     * Convert an object (or graph of objects) to types compatible with Soy (data able to be stored in SoyDataMap).
     * This will convert:
     * - POJOs to Maps
     * - Iterables to Lists
     * - all strings and primitives remain as is.
     *
     * @param obj The object to convert.
     * @return The object converted (in applicable).
     */
    private Object toSoyCompatibleObjects(Object obj) {
        if (obj == null) {
            return obj;
        }
        if (Primitives.isWrapperType(obj.getClass())
                || obj.getClass().isPrimitive()
                || obj instanceof String) {
            return obj;
        }
        if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) obj;
            Map<String, Object> newMap = new HashMap<>(map.size());
            for (String key : map.keySet()) {
                newMap.put(key, toSoyCompatibleObjects(map.get(key)));
            }
            return newMap;
        }
        if (obj instanceof Iterable<?>) {
            List<Object> list = Lists.newArrayList();
            for (Object subValue : ((Iterable<?>) obj)) {
                list.add(toSoyCompatibleObjects(subValue));
            }
            return list;
        }
        if (obj.getClass().isArray()) {
            return obj;
        }
        if (obj.getClass().isEnum()) {
            return ((Enum) obj).name();
        }

        // At this point we must assume it's a POJO so map-it.
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> pojoMap = (Map<String, Object>) pojoToMap(obj);
            Map<String, Object> newMap = new HashMap<>(pojoMap.size());
            for (String key : pojoMap.keySet()) {
                newMap.put(key, toSoyCompatibleObjects(pojoMap.get(key)));
            }
            return newMap;
        }
    }

    /**
     * Convert a Java POJO (aka Bean) to a Map<String, Object>.
     *
     * @param pojo The Java pojo object with standard getters and setters.
     * @return Pojo data as a Map.
     */
    private Map<String, ?> pojoToMap(Object pojo) {
        Map<String, Object> map = new HashMap<>();
        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(pojo.getClass());
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor pd : propertyDescriptors) {
                if (pd.getReadMethod() != null && !"class".equals(pd.getName())) {
                    map.put(pd.getName(), pd.getReadMethod().invoke(pojo));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return map;
    }

}
