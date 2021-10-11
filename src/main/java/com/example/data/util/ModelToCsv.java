package com.example.data.util;

import com.example.data.model.BaseModel;
import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ModelToCsv {

    public static String getCsv(Map<String, Field> fields, BaseModel dto) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        StringBuilder buffer = new StringBuilder();
        boolean firstFieldAdded = false;
        for (Map.Entry<String, Field> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            String methodName;
            if ("boolean".equals(entry.getValue().getType().getName())) {
                methodName = "is" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            } else {
                methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            }

            Method method = dto.getClass().getMethod(methodName);
            Object data = method.invoke(dto);
            if (firstFieldAdded) {
                buffer.append(",");
            }
            buffer.append(data != null ? data.toString() : "");

            firstFieldAdded = true;
        }
        return buffer.toString();
    }

    public static Map<String, Field> getFields(Class dtoClass) {
        return getFieldsUpTo(getFieldsUpTo(dtoClass, BaseModel.class));
    }

    public static String getColumnNamesCsv(Map<String, Field> fields) {
        StringBuilder buffer = new StringBuilder();
        boolean firstElementAdded = false;
        for (Map.Entry<String, Field> entry : fields.entrySet()) {
            if (firstElementAdded) {
                buffer.append(",");
            }

            buffer.append(entry.getKey());

            firstElementAdded = true;
        }
        return buffer.toString();
    }

    public static List<Field> getFieldsUpTo(Class<?> startClass,
                                            Class<?> exclusiveParent) {

        List<Field> currentClassFields = Lists.newArrayList(startClass.getDeclaredFields());
        Class<?> parentClass = startClass.getSuperclass();

        if (parentClass != null &&
                (!(parentClass.equals(exclusiveParent)))) {
            List<Field> parentClassFields =
                    (List<Field>) getFieldsUpTo(parentClass, exclusiveParent);
            currentClassFields.addAll(parentClassFields);
        }

        return currentClassFields;
    }

    private static TreeMap<String, Field> getFieldsUpTo(List<Field> fields) {
        TreeMap<String, Field> fieldsMap = new TreeMap<>();
        for (Field field : fields) {
            fieldsMap.put(field.getName(), field);
        }
        return fieldsMap;
    }
}
