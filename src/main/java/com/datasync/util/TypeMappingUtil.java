package com.datasync.util;

import java.util.HashMap;
import java.util.Map;

public class TypeMappingUtil {
    private static final Map<String, String> MAPPING = new HashMap<>();

    static {
        // 数字类型
        MAPPING.put("tinyint", "TINYINT");
        MAPPING.put("smallint", "SMALLINT");
        MAPPING.put("mediumint", "INT");
        MAPPING.put("int", "INT");
        MAPPING.put("integer", "INT");
        MAPPING.put("bigint", "BIGINT");
        MAPPING.put("float", "FLOAT");
        MAPPING.put("double", "DOUBLE");
        MAPPING.put("decimal", "DECIMAL");

        // 字符串
        MAPPING.put("char", "CHAR");
        MAPPING.put("varchar", "VARCHAR");
        MAPPING.put("tinytext", "STRING");
        MAPPING.put("text", "STRING");
        MAPPING.put("mediumtext", "STRING");
        MAPPING.put("longtext", "STRING");

        // 日期时间
        MAPPING.put("date", "DATE");
        MAPPING.put("time", "TIME");
        MAPPING.put("datetime", "DATETIME");
        MAPPING.put("timestamp", "DATETIME");

        // 其他
        MAPPING.put("json", "JSON");
        MAPPING.put("enum", "VARCHAR");
        MAPPING.put("set", "VARCHAR");
    }

    public static String mysqlToStarRocks(String mysqlType) {
        if (mysqlType == null) return "STRING";
        String lower = mysqlType.toLowerCase().trim();
        String baseType = lower.split("\\(")[0].trim();
        String mapped = MAPPING.get(baseType);
        if (mapped != null) {
            if (lower.contains("(")) {
                String params = lower.substring(lower.indexOf("(") + 1, lower.indexOf(")"));
                // DECIMAL 精度上限为 38（MySQL 允许到 65）
                if ("decimal".equals(baseType)) {
                    String[] parts = params.split(",");
                    int precision = Integer.parseInt(parts[0].trim());
                    int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
                    if (precision > 38) {
                        scale = Math.min(scale, 38);
                        return mapped + "(38," + scale + ")";
                    }
                }
                return mapped + "(" + params + ")";
            }
            return mapped;
        }
        return "STRING";
    }
}