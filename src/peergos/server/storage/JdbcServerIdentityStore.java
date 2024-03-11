package peergos.server.storage;

import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import peergos.server.sql.*;
import peergos.server.util.Logging;

import java.sql.*;
import java.sql.Connection;
import java.util.*;
import java.util.function.*;
import java.util.logging.*;

public class JdbcServerIdentityStore implements ServerIdentityStore {
	private static final Logger LOG = Logging.LOG();

    private static final String SELECT_PEERIDS = "SELECT peerid FROM serverids ORDER BY id;";
    private static final String SELECT_PRIVATE = "SELECT private FROM serverids WHERE peerid=?;";
    private static final String GET_RECORD = "SELECT record FROM serverids WHERE peerid=?;";
    private static final String SET_RECORD = "UPDATE serverids SET record=? WHERE peerid = ?;";

    private Supplier<Connection> conn;
    private final SqlSupplier commands;
    private volatile boolean isClosed;

    public JdbcServerIdentityStore(Supplier<Connection> conn, SqlSupplier commands) {
        this.conn = conn;
        this.commands = commands;
        init(commands);
    }

    private Connection getConnection() {
        Connection connection = conn.get();
        try {
            connection.setAutoCommit(true);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void init(SqlSupplier commands) {
        if (isClosed)
            return;

        try (Connection conn = getConnection()) {
            commands.createTable(commands.createServerIdentitiesTableCommand(), conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addIdentity(PrivKey privateKey, byte[] signedIpnsRecord) {
        try (Connection conn = getConnection();
             PreparedStatement insert = conn.prepareStatement(commands.insertServerIdCommand())) {
            insert.clearParameters();
            insert.setBytes(1, PeerId.fromPubKey(privateKey.publicKey()).getBytes());
            insert.setBytes(2, privateKey.bytes());
            insert.setBytes(3, signedIpnsRecord);
            insert.executeUpdate();
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
        }
    }

    @Override
    public List<PeerId> getIdentities() {
        try (Connection conn = getConnection();
             PreparedStatement select = conn.prepareStatement(SELECT_PEERIDS)) {
            ResultSet qres = select.executeQuery();
            List<PeerId> res = new ArrayList<>();
            while (qres.next()) {
                res.add(new PeerId(qres.getBytes(1)));
            }
            return res;
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public byte[] getPrivateKey(PeerId peerId) {
        try (Connection conn = getConnection();
             PreparedStatement select = conn.prepareStatement(SELECT_PRIVATE)) {
            select.setBytes(1, peerId.getBytes());
            ResultSet qres = select.executeQuery();
            while (qres.next()) {
                return qres.getBytes(1);
            }
            throw new IllegalStateException("No id record for " + peerId);
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public byte[] getRecord(PeerId peerId) {
        try (Connection conn = getConnection();
             PreparedStatement select = conn.prepareStatement(GET_RECORD)) {
            select.setBytes(1, peerId.getBytes());
            ResultSet qres = select.executeQuery();
            while (qres.next()) {
                return qres.getBytes(1);
            }
            throw new IllegalStateException("No ipns record for " + peerId);
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public void setRecord(PeerId peerId, byte[] newRecord) {
        try (Connection conn = getConnection();
             PreparedStatement select = conn.prepareStatement(SET_RECORD)) {
            select.setBytes(1, newRecord);
            select.setBytes(2, peerId.getBytes());
            int updated = select.executeUpdate();
            if (updated != 1)
                throw new IllegalStateException("Set record failed for " + peerId);
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    public synchronized void close() {
        if (isClosed)
            return;
        isClosed = true;
    }

    public static JdbcServerIdentityStore build(Supplier<Connection> conn, SqlSupplier commands) {
        return new JdbcServerIdentityStore(conn, commands);
    }
}
