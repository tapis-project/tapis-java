package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.models.StorageSystem;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

import java.sql.SQLException;
import java.util.List;

public interface IStorageSystemsDAO {

    List<StorageSystem> listSystems(String username, String tenantId) throws TapisException, SQLException;
    StorageSystem getStorageSystem(String username, String tenantId, long systemId) throws TapisException, SQLException;;
}
