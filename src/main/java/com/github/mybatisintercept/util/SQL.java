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
    public static SQL compile(String expressionsSql, Function<String, Object> getter, boolean getterNotnull) {
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
            if (getterNotnull && value == null) {
                return null;
            }
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
            if (getterNotnull && value == null) {
                return null;
            }
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

    public static SQL compile(String expressionsSql, Function<String, Object> getter) {
        return compile(expressionsSql, getter, false);
    }

    public static String compileString(String expressionsSql, Function<String, Object> getter, boolean getterNotnull) {
        SQL sql = compile(expressionsSql, getter, getterNotnull);
        return sql == null ? null : sql.getExprSql();
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

        public CharSequence getPlaceholder() {
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

    /**
     * Zero memory copy String Method.
     *
     * @author wangzihaogithub
     */
    public static class Substring implements CharSequence, Comparable<CharSequence>, Cloneable {
        private String source;
        private int begin;
        private int end;
        private int hash;

        public Substring(String source, int begin, int end) {
            this.source = source;
            this.begin = begin;
            this.end = end;
        }

        public Substring(String source) {
            this.source = source;
            this.begin = 0;
            this.end = source.length();
        }

        public void setSource(String source) {
            this.source = source;
            this.hash = 0;
        }

        public void setBegin(int begin) {
            if (this.begin != begin) {
                this.hash = 0;
                this.begin = begin;
            }
        }

        public void setEnd(int end) {
            if (this.end < end) {
                if (this.hash != 0) {
                    int h = hash;
                    for (int i = this.end; i < end; i++) {
                        h = 31 * h + source.charAt(i);
                    }
                    this.hash = h;
                }
                this.end = end;
            } else if (this.end > end) {
                this.hash = 0;
                this.end = end;
            }
        }

        @Override
        public int hashCode() {
            int h = hash;
            if (h == 0 && end - begin > 0) {
                for (int i = begin; i < end; i++) {
                    h = 31 * h + source.charAt(i);
                }
                hash = h;
            }
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Substring) {
                Substring that = (Substring) obj;
                return equals(that.source, that.begin, that.end, source, begin, end);
            } else if (obj instanceof CharSequence) {
                CharSequence that = (CharSequence) obj;
                return equals(that, 0, that.length(), source, begin, end);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return source.substring(begin, end);
        }

        @Override
        public int compareTo(CharSequence that) {
            int len1 = this.length();
            int len2 = that.length();
            int min = Math.min(len1, len2);
            int k = 0;
            while (k < min) {
                char c1 = charAt(k);
                char c2 = that.charAt(k);
                if (c1 != c2) {
                    return c1 - c2;
                }
                k++;
            }
            return len1 - len2;
        }

        @Override
        public Substring clone() {
            Substring clone = new Substring(source, begin, end);
            clone.hash = this.hash;
            return clone;
        }

        private static boolean equals(CharSequence str1, int begin1, int end1, CharSequence str2, int begin2, int end2) {
            if (end2 - begin2 != end1 - begin1) {
                return false;
            }
            for (int i = begin1, j = begin2; i < end1; i++, j++) {
                char c1 = str2.charAt(j);
                char c2 = str1.charAt(i);
                if (c1 != c2) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int length() {
            return end - begin;
        }

        @Override
        public char charAt(int index) {
            return source.charAt(begin + index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new Substring(source, begin + start, begin + end);
        }

        public int getBegin() {
            return begin;
        }

        public int getEnd() {
            return end;
        }

        public String getSource() {
            return source;
        }
    }
}