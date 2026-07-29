package za.co.woolworths.itemkafka_poc.model;

import java.io.Serializable;

public class JwtResponse implements Serializable {

    private static final long serialVersionUID = -8091879091924046846L;
    private final String jwttoken;

    public JwtResponse(String jwttoken) {
        this.jwttoken = jwttoken;
    }

    public String getToken() {
        return this.jwttoken;
    }

    public void setMessage(String successful) {
    }

    public void setSuccess(boolean b) {
    }

    public void setInfo(String toString) {
    }
}
