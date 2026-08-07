package com.antontech.itemkafka_poc.model;

import java.io.Serializable;

/** Simple wrapper around a signed JWT string returned/used internally by {@link com.antontech.itemkafka_poc.util.JwtTokenUtil}. */
public class JwtResponse implements Serializable {

    private static final long serialVersionUID = -8091879091924046846L;
    private final String jwttoken;

    /** @param jwttoken the signed JWT compact string (may be blank if no key was configured). */
    public JwtResponse(String jwttoken) {
        this.jwttoken = jwttoken;
    }

    /** @return the signed JWT compact string. */
    public String getToken() {
        return this.jwttoken;
    }

    /** No-op placeholder retained for backward compatibility with existing call sites. */
    public void setMessage(String successful) {
    }

    /** No-op placeholder retained for backward compatibility with existing call sites. */
    public void setSuccess(boolean b) {
    }

    /** No-op placeholder retained for backward compatibility with existing call sites. */
    public void setInfo(String toString) {
    }
}

