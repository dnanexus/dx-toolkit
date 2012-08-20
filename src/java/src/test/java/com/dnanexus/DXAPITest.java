import org.junit.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.dnanexus.DXAPI;

public class DXAPITest {
    @BeforeClass public static void setUpClass() throws Exception {
        // Code executed before the first test method       
    }
    
    @Before public void setUp() throws Exception {
        // Code executed before each test    
    }

    @Test public void testDXAPI() throws Exception {
        DXAPI dx = new DXAPI();
        JsonNode input = (JsonNode)(new MappingJsonFactory().createJsonParser("{}").readValueAsTree());
        JsonNode root = dx.systemFindDataObjects(input);
        System.out.println(root);
    }

    @After public void tearDown() throws Exception {
        // Code executed after each test   
    }
 
    @AfterClass public static void tearDownClass() throws Exception {
        // Code executed after the last test method 
    }
}
