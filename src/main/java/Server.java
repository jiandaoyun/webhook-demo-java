import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class Server {

    private static final String SECRET = "test-secret";
    private static final String DATA_CREATE = "data_create";
    private static final String DATA_UPDATE = "data_update";
    private static final String DATA_DELETE = "data_remove";

    // JDBC 驱动名及数据库 URL
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/webhook?serverTimezone=UTC";

    // 数据库的用户名与密码，需要根据自己的设置
    private static final String USER = "root";
    private static final String PASS = "1393199906";

    // 连接池一些配置
    private static final int INIT_POOL_SIZE = 10;
    private static final int MAX_IDLE_TIME = 30;
    private static final int MAX_POOL_SIZE = 100;
    private static final int MIN_POOL_SIZE = 10;

    private static ComboPooledDataSource dataSource;

    // 初始化连接池
    static {
        dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(JDBC_DRIVER);
            dataSource.setJdbcUrl(DB_URL);
            dataSource.setUser(USER);
            dataSource.setPassword(PASS);
            dataSource.setInitialPoolSize(INIT_POOL_SIZE);
            dataSource.setMaxIdleTime(MAX_IDLE_TIME);
            dataSource.setMaxPoolSize(MAX_POOL_SIZE);
            dataSource.setMinPoolSize(MIN_POOL_SIZE);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
    }

    // 获取数据库连接
    private static Connection getConnection () {
        try {
            return dataSource.getConnection();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 关闭数据连接
    private static void closeConnection (Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 生成签名信息
    private static String getSignature(String nonce, String payload, String secret, String timestamp) {
        return DigestUtils.sha1Hex(nonce + ":" + payload + ":" + secret + ":" + timestamp);
    }

    // 获取GET请求中的参数
    private static Map<String, String> parseParameter(String query) {
        Map<String, String> paramMap = new HashMap<String, String>();
        String[] params = query.split("&");
        for (String param : params) {
            String[] keyValue = param.split("=");
            paramMap.put(keyValue[0], keyValue[1]);
        }
        return paramMap;
    }

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(3100), 0);
        server.createContext("/callback", new HttpHandler() {
            @Override
            public void handle(HttpExchange httpExchange) throws IOException {
                String method = httpExchange.getRequestMethod();
                if (method.equalsIgnoreCase("post")) {
                    String payload = IOUtils.toString(httpExchange.getRequestBody(), "utf-8");
                    String jdy = httpExchange.getRequestHeaders().get("x-jdy-signature").get(0);
                    URI uri = httpExchange.getRequestURI();
                    Map<String, String> parameterMap = parseParameter(uri.getRawQuery());
                    String nonce = parameterMap.get("nonce");
                    String timestamp = parameterMap.get("timestamp");
                    String signature = Server.getSignature(nonce, payload, SECRET, timestamp);
                    OutputStream out = httpExchange.getResponseBody();
                    if (!signature.equals(jdy)) {
                        httpExchange.sendResponseHeaders(401, 0);
                        out.write("fail".getBytes());
                        out.close();
                        return;
                    }
                    httpExchange.sendResponseHeaders(200, 0);
                    out.write("success".getBytes());
                    out.close();
                    // 处理数据 - 入库出库等处理
                    handleData(payload);
                }
            }
        });

        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    /**
     * 处理推送来的数据
     * @param payload 推送的数据
     */
    private static void handleData (final String payload) {
        Runnable process = new Runnable() {
            @Override
            public void run() {
                // 解析为json字符串
                JSONObject payloadJSON = JSONObject.parseObject(payload);
                String op = (String)payloadJSON.get("op");
                JSONObject data = (JSONObject)payloadJSON.get("data");
                // 新数据提交
                if (DATA_CREATE.equals(op)) {
                    add(data);
                }
                // 数据修改
                if (DATA_UPDATE.equals(op)) {
                    update(data);
                }
                // 数据删除
                if (DATA_DELETE.equals(op)) {
                    delete(data);
                }
            }
        };
        new Thread(process).start();
    }

    // 处理array类型
    private static String handleArray (JSONArray array) {
        return array.toJSONString();
    }

    // 处理地址类型
    private static String handleAddress (JSONObject address) {
        return address.toJSONString();
    }

    // 处理子表单类型
    private static String handleSubform (JSONArray subform) {
        return subform.toJSONString();
    }

    private static void add (JSONObject data) {
        String sql = "insert into `order` values (?, ?, ?, ?, ?, ?)";
        Connection conn = getConnection();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, data.getString("_id"));
            ps.setString(2, data.getString("_widget_1515649885212"));
            ps.setString(3, handleArray(data.getJSONArray("_widget_1516945244833")));
            ps.setString(4, handleAddress(data.getJSONObject("_widget_1516945244846")));
            ps.setString(5, handleSubform(data.getJSONArray("_widget_1516945244887")));
            ps.setDouble(6, data.getDouble("_widget_1516945245257"));
            ps.execute();
            ps.close();
            closeConnection(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void update(JSONObject data) {
        String sql = "update `order` set time = ?, types = ?, address = ?, orderItems = ?, price = ? where id = ?";
        Connection conn = getConnection();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, data.getString("_widget_1515649885212"));
            ps.setString(2, handleArray(data.getJSONArray("_widget_1516945244833")));
            ps.setString(3, handleAddress(data.getJSONObject("_widget_1516945244846")));
            ps.setString(4, handleSubform(data.getJSONArray("_widget_1516945244887")));
            ps.setDouble(5, data.getDouble("_widget_1516945245257"));
            ps.setString(6, data.getString("_id"));
            ps.execute();
            ps.close();
            closeConnection(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void delete(JSONObject data) {
        String sql = "delete from `order` where id = ?";
        Connection conn = getConnection();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, data.getString("_id"));
            ps.execute();
            ps.close();
            closeConnection(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}