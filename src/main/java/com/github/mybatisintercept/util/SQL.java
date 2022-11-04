package com.github.mybatisintercept.util;

import java.util.*;
import java.util.function.Function;

public class SQL {
    private static final String PLACEHOLDER_BEGIN = "{";
    private static final String PLACEHOLDER_END = "}";
    private final String sourceSql;
    private final String exprSql;
    private final Object[] args;
    private final Map<String, String> argNameAndDefaultValues;
    private final List<Placeholder> placeholders;
    private final Map<String, Object> argsMap = new LinkedHashMap<>();

    public SQL(String sourceSql, String exprSql, Object[] args, Map<String, String> argNameAndDefaultValues, List<Placeholder> placeholders) {
        this.sourceSql = sourceSql;
        this.exprSql = exprSql;
        this.args = args;
        this.argNameAndDefaultValues = argNameAndDefaultValues;
        this.placeholders = placeholders;
        for (Placeholder placeholder : placeholders) {
            argsMap.put(placeholder.argName, placeholder.value == null ? placeholder.argDefaultValue : placeholder.value);
        }
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public Object[] getArgs() {
        return args;
    }

    public Map<String, String> getArgNameAndDefaultValues() {
        return argNameAndDefaultValues;
    }

    public List<Placeholder> getPlaceholders() {
        return placeholders;
    }

    public Map<String, Object> getArgsMap() {
        return argsMap;
    }

    public String getExprSql() {
        return exprSql;
    }

    public static SQL compile(String expressionsSql, Map<String, Object> getter) {
        return compile(expressionsSql, getter::get);
    }

    /**
     * 将sql表达式与参数 转换为JDBC所需的sql对象
     *
     * @param expressionsSql sql表达式
     * @param getter         参数
     * @return JDBC所需的sql对象
     */
    public static SQL compile(String expressionsSql, Function<String, Object> getter) {
        String expressionsSqlCopy = expressionsSql;
        List<Placeholder> placeholderList = new ArrayList<>();
        Queue<Placeholder> placeholderQueue = getPlaceholderQueue(expressionsSqlCopy, '#');
        List<Object> argsList = new ArrayList<>();
        Map<String, String> argNameAndDefaultValues = new LinkedHashMap<>();
        StringBuilder sqlBuffer = new StringBuilder(expressionsSqlCopy);
        Placeholder placeholder;
        while ((placeholder = placeholderQueue.poll()) != null) {
            placeholderList.add(placeholder);
            Object value = getter.apply(placeholder.argName);
            placeholder.value = cast(value);
            int offset = expressionsSqlCopy.length() - sqlBuffer.length();
            int offsetBegin = placeholder.placeholder.getBegin() - PLACEHOLDER_BEGIN.length() - offset - 1;
            int offsetEnd = placeholder.placeholder.getEnd() + PLACEHOLDER_END.length() - offset;
            sqlBuffer.replace(offsetBegin, offsetEnd, "?");
            argNameAndDefaultValues.put(placeholder.argName, placeholder.argDefaultValue);
            argsList.add(value);
        }

        expressionsSqlCopy = sqlBuffer.toString();
        placeholderQueue = getPlaceholderQueue(expressionsSqlCopy, '$');
        while ((placeholder = placeholderQueue.poll()) != null) {
            placeholderList.add(placeholder);
            Object value = getter.apply(placeholder.argName);
            placeholder.value = cast(value);
            int offset = expressionsSqlCopy.length() - sqlBuffer.length();
            int offsetBegin = placeholder.placeholder.getBegin() - PLACEHOLDER_BEGIN.length() - offset - 1;
            int offsetEnd = placeholder.placeholder.getEnd() + PLACEHOLDER_END.length() - offset;
            String replace = value != null && !"".equals(value) ? value.toString() : Objects.toString(placeholder.argDefaultValue, "");
            sqlBuffer.replace(offsetBegin, offsetEnd, replace);

            argNameAndDefaultValues.put(placeholder.argName, placeholder.argDefaultValue);
        }
        return new SQL(expressionsSql, sqlBuffer.toString(), argsList.toArray(), argNameAndDefaultValues, placeholderList);
    }

    private static Object cast(Object value) {
        return value;
    }

    /**
     * 获取占位符
     *
     * @param str 表达式
     * @return 多个占位符
     */
    private static Queue<Placeholder> getPlaceholderQueue(String str, char symbol) {
        Queue<Placeholder> keys = new LinkedList<>();
        keys.clear();
        int charAt = 0;
        String begin = symbol + PLACEHOLDER_BEGIN;
        while (true) {
            charAt = str.indexOf(begin, charAt);
            if (charAt == -1) {
                break;
            }
            charAt = charAt + begin.length();
            Placeholder placeholder = new Placeholder(symbol, new Substring(str, charAt, str.indexOf(PLACEHOLDER_END, charAt)));
            keys.add(placeholder);
        }
        return keys;
    }

    @Override
    public String toString() {
        return exprSql;
    }

    public static class Placeholder {
        private final char symbol;
        private final Substring placeholder;
        private final String argName;
        private final String argDefaultValue;
        private Object value;

        Placeholder(char symbol, Substring placeholder) {
            this.symbol = symbol;
            this.placeholder = placeholder;
            String[] argNameSplit = placeholder.toString().split("\\|");
            this.argName = argNameSplit[0];
            this.argDefaultValue = argNameSplit.length > 1 ? argNameSplit[1] : null;
        }

        public char getSymbol() {
            return symbol;
        }

        public Substring getPlaceholder() {
            return placeholder;
        }

        public String getArgName() {
            return argName;
        }

        public String getArgDefaultValue() {
            return argDefaultValue;
        }

        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return symbol + "" + placeholder;
        }
    }

}