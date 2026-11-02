package wms;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class WmsFulfillmentEngine {

    // Connection URL targeting a fast, local in-memory SQL database system
    private static final String DATABASE_URL = "jdbc:sqlite::memory:";

    public static void main(String[] args) {
        System.out.println("=============================================================");
        System.out.println("      INITIALIZING HELLOFRESH-ALIGNED JAVA WMS ENGINE        ");
        System.out.println("=============================================================");

        try (Connection conn = DriverManager.getConnection(DATABASE_URL)) {
            if (conn != null) {
                // Step 1: Initialize the relational database architecture
                initializeWmsDatabase(conn);

                // Step 2: Mock an upstream event stream message ingest tracking a customer transaction
                String mockKafkaPayload1 = "{\"order_id\": 9001, \"sku\": \"Premium Diced Beef\", \"qty\": 15}";
                simulateOrderIngestionStream(conn, mockKafkaPayload1, "Premium Diced Beef", 15);

                // Step 3: Trigger automated warehouse inventory replenishment business rules
                executeReplenishmentCheck(conn);

                // Step 4: Process secondary data streams verifying validation logic thresholds
                String mockKafkaPayload2 = "{\"order_id\": 9002, \"sku\": \"Organic Sweet Potatoes\", \"qty\": 5}";
                simulateOrderIngestionStream(conn, mockKafkaPayload2, "Organic Sweet Potatoes", 5);
            }
        } catch (Exception e) {
            System.err.println("WMS Runtime Exception encountered: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("\n=============================================================");
        System.out.println("             WMS DATA LIFECYCLE COMPLETED SUCCESSFULLY       ");
        System.out.println("=============================================================");
    }

    private static void initializeWmsDatabase(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            // Create a structured data table mapping warehouse inventory metrics
            stmt.execute("CREATE TABLE inventory_stock (" +
                    "item_id INTEGER PRIMARY KEY, " +
                    "sku_name TEXT NOT NULL, " +
                    "quantity INTEGER NOT NULL, " +
                    "min_threshold INTEGER NOT NULL, " +
                    "storage_zone TEXT NOT NULL" +
                    ")");

            // Seed initial records simulating cold chain inventory stock profiles
            stmt.execute("INSERT INTO inventory_stock VALUES (101, 'Premium Diced Beef', 150, 50, 'Cold-Room-Zone-A')");
            stmt.execute("INSERT INTO inventory_stock VALUES (102, 'Organic Sweet Potatoes', 20, 40, 'Ambient-Zone-B')"); // Triggers Alert!
            stmt.execute("INSERT INTO inventory_stock VALUES (103, 'Fresh Cilantro Packs', 200, 30, 'Cold-Room-Zone-A')");

            System.out.println("[WMS SYSTEM LOG] Relational tables compiled and seeded successfully.");
        }
    }

    private static void simulateOrderIngestionStream(Connection conn, String rawPayload, String sku, int qty) throws Exception {
        System.out.println("\n⚡ [KAFKA ORDER STREAM INGESTED] Parsing structured message string event data...");
        System.out.println("Payload: " + rawPayload);

        String selectQuery = "SELECT quantity FROM inventory_stock WHERE sku_name = ?";
        String updateQuery = "UPDATE inventory_stock SET quantity = quantity - ? WHERE sku_name = ?";

        try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery);
             PreparedStatement updateStmt = conn.prepareStatement(updateQuery)) {

            selectStmt.setString(1, sku);
            try (ResultSet rs = selectStmt.executeQuery()) {
                if (rs.next()) {
                    int currentStock = rs.getInt("quantity");
                    if (currentStock >= qty) {
                        updateStmt.setInt(1, qty);
                        updateStmt.setString(2, sku);
                        updateStmt.executeUpdate();
                        System.out.println("✅ SUCCESS: Allocated inventory storage levels. Updated stock for [" + sku + "]: " + (currentStock - qty));
                    } else {
                        System.out.println("❌ ERROR: Inventory shortfall exception! Critical stock deficit for item: " + sku);
                    }
                }
            }
        }
    }

    private static void executeReplenishmentCheck(Connection conn) throws Exception {
        System.out.println("\n[WMS SYSTEM LOG] Executing standard SQL threshold scans for item replenishment rules...");

        String selectLowStock = "SELECT sku_name, quantity, min_threshold, storage_zone FROM inventory_stock WHERE quantity < min_threshold";
        String triggerReplenish = "UPDATE inventory_stock SET quantity = quantity + ? WHERE sku_name = ?";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectLowStock);
             PreparedStatement replenishStmt = conn.prepareStatement(triggerReplenish)) {

            boolean itemsNeedReplenishment = false;
            while (rs.next()) {
                itemsNeedReplenishment = true;
                String sku = rs.getString("sku_name");
                int currentQty = rs.getInt("quantity");
                int threshold = rs.getInt("min_threshold");
                String zone = rs.getString("storage_zone");

                System.out.println("⚠️  ALERT: SKU [" + sku + "] falls below standard tracking limit. Current: " + currentQty + ", Limit: " + threshold + " in [" + zone + "]");

                // Dispatches an automated stock shift simulating material reallocation actions
                int replenishmentBatch = threshold * 3;
                replenishStmt.setInt(1, replenishmentBatch);
                replenishStmt.setString(2, sku);
                replenishStmt.executeUpdate();

                System.out.println("🔄 ACTION DISPATCHED: Stock replenishment instruction passed cleanly! Added " + replenishmentBatch + " units to picking grid nodes.");
            }
            if (!itemsNeedReplenishment) {
                System.out.println("✅ STATUS EXCELLENT: All tracking nodes report satisfactory safety thresholds.");
            }
        }
    }
}
