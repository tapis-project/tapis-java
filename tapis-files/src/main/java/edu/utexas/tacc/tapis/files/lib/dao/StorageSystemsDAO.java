package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.db.DBConnectionPool;
import edu.utexas.tacc.tapis.files.lib.models.StorageSystem;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class StorageSystemsDAO implements IStorageSystemsDAO {

    public List<StorageSystem> listSystems(String username, String tenantId) throws TapisException, SQLException {
        Connection conn = DBConnectionPool.getConnection();

        try {
            conn.setSchema("files");

            String query = "SELECT * from files.systems where tenant_id = 'test_tenant'";
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            List<StorageSystem> systems = new ArrayList<>();
            while (rs.next()) {
                StorageSystem sys = new StorageSystem();
                sys.setProtocol(rs.getString("protocol"));
                sys.setName(rs.getString("name"));
                sys.setHost(rs.getString("host"));
                sys.setDescription(rs.getString("description"));
                sys.setId(rs.getString("id"));
                sys.setPort(rs.getLong("port"));
                systems.add(sys);

            }
            rs.close();
            st.close();
            return systems;
        } catch (SQLException e) {
            throw new TapisException("SQLException", e);
        } finally {
            conn.close();

        }

    }

    public StorageSystem getStorageSystem(String username, String tenantId, long systemId) {
        return null;
    }
}
