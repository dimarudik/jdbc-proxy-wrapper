package dev.humus.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

public class WrapperProxy implements InvocationHandler {
    private final Object target;
    private final List<ProxyPlugin> plugins;

    // Список интерфейсов, которые подлежат рекурсивному проксированию
    private static final Set<Class<?>> PROXY_INTERFACES = Set.of(
            Connection.class,
            Statement.class
            // Можно расширить: PreparedStatement.class, CallableStatement.class, ResultSet.class
    );

    private WrapperProxy(Object target, List<ProxyPlugin> plugins) {
        this.target = target;
        this.plugins = plugins;
    }

    /**
     * Создает прокси-объект для указанного интерфейса.
     */
    public static <T> T create(T target, Class<T> interfaceClass, List<ProxyPlugin> plugins) {
        return interfaceClass.cast(Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new WrapperProxy(target, plugins)
        ));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        PluginChain chain = new PluginChain(plugins);

        JdbcCallable<Object, Object> terminal = (t, a) -> {
            try {
                Object result = method.invoke(t, a);
                return wrapResultIfNeeded(result);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof SQLException sqlException) {
                    throw sqlException;
                }
                throw new SQLException(cause);
            } catch (IllegalAccessException e) {
                throw new SQLException("Access denied to method: " + method.getName(), e);
            }
        };

        // Теперь компилятор спокоен: terminal выбрасывает только SQLException
        return chain.proceed(target, method.getName(), terminal, args);
    }
    
    /**
     * Проверяет результат вызова. Если возвращен JDBC-объект, оборачивает его в прокси.
     */
    private Object wrapResultIfNeeded(Object result) {
        if (result == null) {
            return null;
        }

        for (Class<?> iface : PROXY_INTERFACES) {
            if (iface.isInstance(result)) {
                return createDynamicProxy(result, iface);
            }
        }
        return result;
    }

    /**
     * Хелпер-метод для безопасного приведения типов при создании прокси.
     */
    @SuppressWarnings("unchecked")
    private <T> T createDynamicProxy(Object target, Class<T> iface) {
        return WrapperProxy.create((T) target, iface, plugins);
    }
}
