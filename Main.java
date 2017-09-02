package sql;
import java.sql.*;

public class Main {

    private static final String DB_CONN = "jdbc:mysql://localhost:3306/myorders";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "WIN72007@NAZAr";
    private static Connection conn;
    private static final String TB_GOODS = "CREATE TABLE IF NOT EXISTS GOODS " +
                                             "(Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                                             "GoodsName VARCHAR (150), " +
                                             "GoodsPrice DOUBLE)";
    private static final String TB_CLIENTS = "CREATE TABLE IF NOT EXISTS CLIENTS " +
                                               "(Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                                               "ClientName VARCHAR (55), " +
                                               "ClientSurname VARCHAR (55), " +
                                               "ClientAddress VARCHAR (55), " +
                                               "ClientPhone VARCHAR (55), " +
                                               "ClientEmail VARCHAR (55))";
    private static final String TB_ORDERS = "CREATE TABLE IF NOT EXISTS ORDERS " +
                                              "(Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                                              "ClientId INT, " +
                                              "GoodsId INT, " +
                                              "OrderNumber INT, " +
                                              "OrderQuantity INT)";

    private static void createTables() {
        try(Statement st = conn.createStatement()) {
            st.execute(TB_GOODS);
            st.execute(TB_CLIENTS);
            st.execute(TB_ORDERS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void addClient(String clientName, String clientSurname,
                                 String clientAddress, String clientPhone, String clientEmail){
        try(PreparedStatement ps = conn.prepareStatement("INSERT INTO CLIENTS " +
                                                                   "(ClientName, ClientSurname," +
                                                                   "ClientAddress, ClientPhone," +
                                                                   "ClientEmail) " +
                                                             "VALUES (?,?,?,?,?)")) {
            ps.setString(1,clientName);
            ps.setString(2,clientSurname);
            ps.setString(3,clientAddress);
            ps.setString(4,clientPhone);
            ps.setString(5,clientEmail);

            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void addOrder(Integer clientId, Integer goodsId,
                                 Integer orderNumber, Integer orderQuantity){
        try(PreparedStatement ps = conn.prepareStatement("INSERT INTO ORDERS " +
                                                                 "(ClientId, GoodsId," +
                                                                 "OrderNumber, OrderQuantity) " +
                                                                 "VALUES (?,?,?,?)")) {
            ps.setInt(1,clientId);
            ps.setInt(2,goodsId);
            ps.setInt(3,orderNumber);
            ps.setInt(4,orderQuantity);

            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void addGoods(String goodsName, Double goodsPrice){
        try(PreparedStatement ps = conn.prepareStatement("INSERT INTO GOODS " +
                                                                "(GoodsName, GoodsPrice) " +
                                                             "VALUES (?,?)")) {
            ps.setString(1,goodsName);
            ps.setDouble(2,goodsPrice);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void SelectOrderSumByClients(){
        try(PreparedStatement ps = conn.prepareStatement("Select ClientName, ClientSurname, " +
                                                         "sum(GoodsPrice*OrderQuantity) as OrderSum " +
                                                         "from orders inner join clients on orders.ClientId = clients.Id " +
                                                         "inner join goods on orders.GoodsId = goods.Id " +
                                                         "group by ClientName, ClientSurname " +
                                                         "order by 3 desc")) {
            getTable(ps);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void getTable(PreparedStatement ps) {
        try (ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData rsm = rs.getMetaData();

            for (int i = 1; i <= rsm.getColumnCount(); i++) {
                System.out.print(rsm.getColumnName(i) + "\t\t");
            }
            System.out.println();
            while (rs.next()) {
                for (int i = 1; i <= rsm.getColumnCount(); i++) {
                    System.out.print(rs.getString(i) + "\t\t");
                }
                System.out.println();
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException  {

        try {
            conn = DriverManager.getConnection(DB_CONN,DB_USER,DB_PASS);
            createTables();
          // fill table GOODS
            addGoods("Cellphone", 25000.00);
            addGoods("IPad", 35000.00);
            addGoods("LapTop", 75000.00);
            addGoods("TV", 55000.00);
            addGoods("Mouse", 1000.00);
          // fill table CLIENTS
            addClient("Nazar","Mykhailechko",
                      "Kyiv, Khreschatik,1","111111111",
                      "email1@com.ua");
            addClient("Kolya","Vystatkiv",
                      "Kyiv, E.Chavdar,3","222222222",
                      "email2@com.ua");
            addClient("Andriy","Tatarskiy",
                      "Kyiv, Lukyanivska,20","333333333",
                      "email3@com.ua");
          // fill table ORDERS
            addOrder(1,2,12345,2);
            addOrder(1,3,23456,1);
            addOrder(3,4,56789,1);
            addOrder(3,1,22333,3);
            addOrder(3,5,90871,1);
            addOrder(2,5,56781,1);
            addOrder(2,3,45321,2);
            addOrder(2,1,98765,1);
            addOrder(2,4,33444,1);
            SelectOrderSumByClients();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            conn.close();
        }
    }
}