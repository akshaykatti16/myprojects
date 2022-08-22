import java.sql.*;

public class HiveJDBCConnect {
    public static void main(String[] args) {
        Connection con = null;
        try {
            String conStr = "jdbc:hive2://AKDell5415:10000/emp";
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            con = DriverManager.getConnection(conStr, "", "");
            Statement stmt = con.createStatement();
            String queryString2="show tables";
            PreparedStatement ps = con.prepareStatement(queryString2);
            ResultSet res = ps.executeQuery();
            System.out.println("Executing query : "+queryString2);
            while (res.next()){
                System.out.println(res.getString(1));
                /*Executing query : show tables
                employee
                employee_tmp
                employee_trans
                similar
                zipcodeswithoutp
                zipcodeswithp*/
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch (Exception ex) {
            }
        }
    }
}

