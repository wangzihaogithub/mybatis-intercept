package com.github.mybatisintercept.util;

import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.scripting.xmltags.ForEachSqlNode;
import org.apache.ibatis.session.Configuration;

import java.lang.reflect.Method;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public class MybatisUtil {
    public final static int INDEX_MAPPED_STATEMENT = 0;
    public final static int INDEX_PARAMETER = 1;
    private static final Map<String, Method> MAPPED_STATEMENT_METHOD_LRU_MAP = new LinkedHashMap<String, Method>(64, 0.75F, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Method> eldest) {
            return size() > 200;
        }
    };

    public static boolean isInterceptPackage(Invocation invocation, Collection<String> interceptPackageNameSet) {
        if (interceptPackageNameSet == null || interceptPackageNameSet.isEmpty()) {
            return true;
        }
        Method mapperMethod = MybatisUtil.getMapperMethod(invocation);
        String packageName = Optional.ofNullable(mapperMethod).map(Method::getDeclaringClass).map(Class::getPackage).map(Package::getName).orElse(null);
        if (packageName == null) {
            return false;
        }
        for (String interceptPackageName : interceptPackageNameSet) {
            if (packageName.startsWith(interceptPackageName)) {
                return true;
            }
        }
        return false;
    }

    public static MappedStatement getMappedStatement(Invocation invocation) {
        return (MappedStatement) invocation.getArgs()[INDEX_MAPPED_STATEMENT];
    }

    public static void setMappedStatement(Invocation invocation, MappedStatement ms) {
        invocation.getArgs()[INDEX_MAPPED_STATEMENT] = ms;
    }

    public static String getBoundSqlString(Invocation invocation) {
        return getBoundSql(invocation).getSql();
    }

    public static Object getParameter(Invocation invocation) {
        return invocation.getArgs()[INDEX_PARAMETER];
    }

    public static boolean setBoundSql(Invocation invocation, BoundSql boundSql) {
        Object[] args = invocation.getArgs();
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg instanceof BoundSql) {
                args[i] = boundSql;
                return true;
            }
        }
        return false;
    }

    public static BoundSql getBoundSql(Invocation invocation) {
        try {
            Object[] args = invocation.getArgs();
            for (Object arg : args) {
                if (arg instanceof BoundSql) {
                    return (BoundSql) arg;
                }
            }
            MappedStatement statement = getMappedStatement(invocation);
            return statement.getBoundSql(getParameter(invocation));
        } catch (Exception e) {
            throw new IllegalStateException(String.format(
                    "MybatisUtil#getBoundSql(%s) fail. error = %s",
                    invocation.getMethod(), e), e);
        }
    }

    public static Class<?> getMapperClass(MappedStatement ms) {
        String mapperClassName = substringBeforeLast(ms.getId());
        try {
            return Class.forName(mapperClassName);
        } catch (ClassNotFoundException e) {
            PlatformDependentUtil.sneakyThrows(e);
            return null;
        }
    }

    public static Method getMapperMethod(Invocation invocation) {
        return getMapperMethod(getMappedStatement(invocation));
    }

    public static Method getMapperMethod(MappedStatement ms) {
        String id = ms.getId();
        return MAPPED_STATEMENT_METHOD_LRU_MAP.computeIfAbsent(id, o -> {
            Class<?> mapperClass = getMapperClass(ms);
            if (mapperClass == null) {
                return null;
            }
            String mapperMethodName = substringAfterLast(id);
            for (Method method : mapperClass.getMethods()) {
                // fix https://github.com/mybatis/mybatis-3/issues/237
                if (method.isBridge()) {
                    continue;
                }
                if (method.getName().equals(mapperMethodName)) {
                    return method;
                }
            }
            return null;
        });
    }

    public static void rewriteSql(Invocation invocation, String sql) {
        MappedStatement ms = getMappedStatement(invocation);
        BoundSql boundSql = getBoundSql(invocation);
        setMappedStatement(invocation, newMappedStatement(ms, boundSql, sql));
        setBoundSql(invocation, boundSql);
    }

    public static MappedStatement newMappedStatement(MappedStatement ms, BoundSql boundSql, String sql) {
        MappedStatement.Builder builder = new MappedStatement.Builder(ms.getConfiguration(), ms.getId(),
                new BoundSqlSqlSource(ms.getConfiguration(), boundSql, sql), ms.getSqlCommandType());
        builder.resource(ms.getResource());
        builder.parameterMap(ms.getParameterMap());
        builder.resultMaps(ms.getResultMaps());
        builder.fetchSize(ms.getFetchSize());
        builder.timeout(ms.getTimeout());
        builder.statementType(ms.getStatementType());
        builder.resultSetType(ms.getResultSetType());
        builder.cache(ms.getCache());
        builder.flushCacheRequired(ms.isFlushCacheRequired());
        builder.useCache(ms.isUseCache());
        builder.keyGenerator(ms.getKeyGenerator());
        builder.keyProperty(delimitedArrayToString(ms.getKeyProperties()));
        builder.keyColumn(delimitedArrayToString(ms.getKeyColumns()));
        builder.databaseId(ms.getDatabaseId());
        return builder.build();
    }

    public static void setParameter(Invocation invocation, Object parameter) {
        invocation.getArgs()[INDEX_PARAMETER] = parameter;
    }

    public static boolean isInstanceofKeyValue(Object parameter) {
        if (parameter == null) {
            // skip null
            return false;
        } else if (isBasicType(parameter)) {
            // skip 基本类型
            return false;
        } else if (parameter instanceof Collection) {
            // skip 集合
            return false;
        } else {
            // bean 或 MapperMethod.ParamMap 或 map
            return true;
        }
    }

    public static void setParameterValue(Invocation invocation, String name, Object value) {
        Object parameter = getParameter(invocation);
        if (!isInstanceofKeyValue(parameter)) {
            // skip 不支持key value的
        } else if (parameter instanceof MapperMethod.ParamMap) {
            // ParamMap
            Map<String, Object> paramMap = ((MapperMethod.ParamMap) parameter);
            putIfMiss(paramMap, name, value);
        } else if (parameter instanceof Map) {
            // Map
            Map<String, Object> copyMap = new LinkedHashMap<>((Map<String, Object>) parameter);
            if (putIfMiss(copyMap, name, value)) {
                setParameter(invocation, copyMap);
            }
        } else {
            // Bean
            if (!BeanMap.invokeSetter(parameter, name, value)) {
                BeanMap beanMap = new BeanMap(parameter);
                beanMap.set(name, value);
                setParameter(invocation, beanMap);
            }
        }

        BoundSql boundSql = getBoundSql(invocation);
        boundSql.setAdditionalParameter(name, value);
    }

    public static String getParameterMappingProperty(BoundSql boundSql, int columnParameterizedIndex) {
        ParameterMapping parameterMapping = boundSql.getParameterMappings().get(columnParameterizedIndex);
        return getParameterMappingProperty(parameterMapping);
    }

    public static String getParameterMappingProperty(ParameterMapping parameterMapping) {
        String property = parameterMapping.getProperty();
        if (property.startsWith(ForEachSqlNode.ITEM_PREFIX)) {
            String[] split = property.split("\\.");
            return split[split.length - 1];
        } else {
            return property;
        }
    }

    public static String getAdditionalParameterPropertyName(ParameterMapping parameterMapping) {
        String propertyName = parameterMapping.getProperty();
        if (propertyName.startsWith(ForEachSqlNode.ITEM_PREFIX)) {
            for (int i = propertyName.length() - 1; i >= 0; i--) {
                char c = propertyName.charAt(i);
                if (c == '.') {
                    return propertyName.substring(0, i);
                }
            }
        }
        return propertyName;
    }

    public static boolean isEqualsProperty(ParameterMapping parameterMapping, String property) {
        String parameterMappingProperty = parameterMapping.getProperty();
        if (parameterMappingProperty.startsWith(ForEachSqlNode.ITEM_PREFIX)) {
            return parameterMappingProperty.endsWith("." + property);
        } else {
            return Objects.equals(parameterMappingProperty, property);
        }
    }

    public static List<ParameterMapping> removeParameterMapping(BoundSql boundSql, String property) {
        List<ParameterMapping> removeList = new ArrayList<>();
        for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
            if (isEqualsProperty(parameterMapping, property)) {
                removeList.add(parameterMapping);
            }
        }
        if (!removeList.isEmpty()) {
            boundSql.getParameterMappings().removeAll(removeList);
        }
        return removeList;
    }

    public static boolean invokeParameterObjectSetter(BoundSql boundSql, String property, Object value) {
        int setterCount = 0;
        for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
            if (!MybatisUtil.isEqualsProperty(parameterMapping, property)) {
                continue;
            }

            Object parameterObject = boundSql.getAdditionalParameter(MybatisUtil.getAdditionalParameterPropertyName(parameterMapping));
            if (parameterObject == null || isBasicType(parameterObject)) {
                parameterObject = boundSql.getParameterObject();
            }
            if (parameterObject == null) {
                continue;
            }
            Class<?> javaType = parameterMapping.getJavaType();
            if (javaType != null) {
                value = TypeUtil.cast(value, javaType);
            }
            boolean setPropertyValueSuccess = setPropertyValue(parameterObject, property, value);
            if (setPropertyValueSuccess) {
                setterCount++;
            } else {
                return false;
            }
        }
        return setterCount > 0;
    }

    public static boolean setPropertyValue(Object parameterObject, String property, Object value) {
        Object existValue;
        if (parameterObject instanceof Map) {
            existValue = ((Map) parameterObject).containsKey(property) ? ((Map) parameterObject).get(property) : null;
        } else if (BeanMap.existProperty(parameterObject, property)) {
            existValue = BeanMap.invokeGetter(parameterObject, property);
        } else {
            // 用户实体类里没有这个属性
            return false;
        }

        if (existValue != null && !"".equals(existValue)) {
            // 用户自己赋值了, 不更改用户填的值
            return true;
        } else {
            try {
                // 用户没有赋值，自动回填至实体类
                return BeanMap.invokeSetter(parameterObject, property, value);
            } catch (UnsupportedOperationException | IllegalStateException e) {
                // 不可变Map
                return false;
            }
        }
    }

    private static boolean isBasicType(Object value) {
        if (value == null) {
            return false;
        }
        return value.getClass().isPrimitive()
                || value instanceof Number
                || value instanceof CharSequence
                || value instanceof Date
                || value instanceof TemporalAccessor
                || value instanceof Enum;
    }

    private static String delimitedArrayToString(String[] in) {
        if (in == null || in.length == 0) {
            return null;
        } else {
            StringBuilder builder = new StringBuilder();
            for (String str : in) {
                builder.append(str).append(",");
            }
            return builder.toString();
        }
    }

    private static String substringAfterLast(String str) {
        int pos = str.lastIndexOf(".");
        return pos != -1 && pos != str.length() - ".".length() ? str.substring(pos + ".".length()) : "";
    }

    private static String substringBeforeLast(String str) {
        int pos = str.lastIndexOf(".");
        return pos == -1 ? str : str.substring(0, pos);
    }

    private static boolean putIfMiss(Map<String, Object> map, String name, Object value) {
        if (map.containsKey(name)) {
            return false;
        } else {
            map.put(name, value);
            return true;
        }
    }

    static class BoundSqlSqlSource implements SqlSource {
        private final BoundSql prototype;
        private final Configuration configuration;
        private final String sql;
        private BoundSql boundSql;

        public BoundSqlSqlSource(Configuration configuration, BoundSql prototype, String sql) {
            this.configuration = configuration;
            this.prototype = prototype;
            this.sql = sql;
            if (Objects.equals(sql, prototype.getSql())) {
                this.boundSql = prototype;
            }
        }

        @Override
        public BoundSql getBoundSql(Object parameterObject) {
            if (parameterObject != prototype.getParameterObject()) {
                return newBoundSql(configuration, prototype, sql, parameterObject);
            } else if (boundSql != null) {
                return boundSql;
            } else {
                return boundSql = newBoundSql(configuration, prototype, sql, parameterObject);
            }
        }

        private BoundSql newBoundSql(Configuration configuration, BoundSql prototype, String rewriteSql, Object parameterObject) {
            BoundSql newBoundSql = new BoundSql(configuration, rewriteSql, prototype.getParameterMappings(), parameterObject);
            for (ParameterMapping mapping : prototype.getParameterMappings()) {
                String prop = mapping.getProperty();
                if (prototype.hasAdditionalParameter(prop)) {
                    newBoundSql.setAdditionalParameter(prop, prototype.getAdditionalParameter(prop));
                }
            }
            return newBoundSql;
        }
    }

}
