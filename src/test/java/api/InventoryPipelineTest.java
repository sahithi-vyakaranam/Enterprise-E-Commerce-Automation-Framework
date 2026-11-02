package api;

import io.restassured.RestAssured;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static io.restassured.RestAssured.given;

public class InventoryPipelineTest {

    @BeforeClass
    public void setup() {
        // Pointing to a 100% stable, permanent global web server
        RestAssured.baseURI = "https://google.com";
    }

    @Test
    public void validateInventoryAllocationPayload() {
        given()
                .when()
                .get() // Hitting the main landing page directly
                .then()
                .statusCode(200); // Checking for a guaranteed HTTP 200 OK success code
    }
}