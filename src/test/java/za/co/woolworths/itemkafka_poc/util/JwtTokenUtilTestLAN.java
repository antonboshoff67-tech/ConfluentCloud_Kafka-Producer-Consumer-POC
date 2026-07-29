package za.co.woolworths.itemkafka_poc.util;

import io.jsonwebtoken.Header;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.junit.jupiter.api.Test;
import java.security.Key;
import java.io.IOException;
import java.io.StringReader;
import java.security.KeyPair;
import java.security.Security;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
class JwtTokenUtilTestLAN {

  //private String privateKeyString = "MIIEogIBAAKCAQEAkcMgHHPzLwiIYg7raoyR6QXdgbNJwou9Tah1/3qTEgw3bV6PrpqSL3PdrOBaiCa6cGwNr3i4ZGWh2B8iQuYcUVxsuImH9+uu6X/K0uE0avKrGj5e1Xawxb+VxuhHXkngD/LrRqryNCBAIJ3Cf7f8WdNJtUvSPO/FcU8EeYlmTOY9MUIeXptIupCwQZ/7jawpiXj/lau8lf67BrDYdiqH/gjwAja//LiqElWF7Ud6aNUbLp0ETj2nhnqvk2pWSfye3H4m5vVlqeKAeuVtPQBIrccCKfZ6AAfnfIhWFlnxkwEVsLtT2iDeZ9QZQcyiMudjSE8OE8pTo/9k55ux+09n8wIDAQABAoIBABQCYbOWNTFlLfesDaQUROlknTozRvJWZXe2IvCdvnexRqCUZQv2ew/Zi3oyWCstidVp99z5KPLbQsLcz0K4vPVsRx5z/r7JUzsJiGm3cY8m3dcd7b3G69BRkvl5pGZspL37vlF2oQYH0VXI4kzPEGyyRQGGtYcY8RUzYC1g77KpiXPYpjx24UeH2Mw9/werJfvHVAhZoHXsoYE3gxS1UT2L8/OlYiNIqoxPGIPPknk+Ih2MCHeuZUen5Q/3LU7nRyoKcnb0WRuQH4A9c3zdgZaked5jrxRRZT3ML+1UPmGEZ4m76nqJI9DFVhh68vqKZenwmJYCy8fqUzvZac4EzRECgYEAuYxfXU89gsgRvu4NGdjzrBlKa5e+RccKWevRunfvm/Q9syH+VdAyxBqPavmxLolsbBOY41StbDqu7UoF3YS+L3yorr7ej3YygoiJPyLHv/mC9szGM11Qk7gJKg1ohkAfWFBVqRc/51CxTfxwxgxJNDwOcIuInhmmkbQCV2YdrEMCgYEAyRt4oIDxc0z+uh7HyXUIO3OzmUeQ4BBbY+mD1m94xYNr5cyXPBI8WHk6ZMvVD6bhBsHyNzTs7TLOmSiBXExntTWWp1awwDVU0ZamOqRt7p0EDDFsKVONuTwDDpGcKRmxPFr2pE2f3Pwn4IvxAryf6BlBnE49P192JOgWkWPZcpECgYBrByB/J1UY2iT6WEAwV6d+7vuxgk8b2ssKwW8xuzr4X3Mzn/kgML8HxTZTA4KDkHt8//ThtJdwZY0/jMJDW+2EYMjAO5MPd6N9Dhr00zAbeWZkzAvL1xjCjJakriVLwzMRcw1mQuX2nOIZVPMMGkwjmDzQCHyejbc6NnZfSaT2FwKBgDl0483XiSRMByrdyG7CRQvXTqSoUbfydGnjOYt2ZabomVcaUMsU2rDcUdaidcj3AKuRlcY4FMjgDy6q5+qa2bZoDRbaKb2Afy8yP2PZvp7BtXpWVHSqLn+Rqb3r1BXaBU0dIAJpOxdD45C/6qfBhF4neRtTLyR//Z0lwezf/ixBAoGAUQehXNOpGKV4jO9En9P5PPJOj2LZk4DDWh7op15MBsGMyr1IUUY42TRTzP+6unfgiLq8xYYPjlIeHNFjztjk7nj892R/ykiiUY3+u8w2nHJc0jfQTk4rTM8D2qmRAcrCE7haE8I6FCBA6pe3KOPspT1eYugA5GknNEQyDmoIWqI=";
  //private String privateKeyString = "MIIEpAIBAAKCAQEAxxtcsabCkYcldYuC5176+zjGxQ6M0Gx797ZDFi3pMi+5C/frG8iPCybc1piETPImqw5qOx5Q823yg88480TUIqjmhmGZYqjf0xUrI4NYNs9aYYmBjZgdVwExb94LZ1Pym9MJKJiNyqpkIF5kR8ZY6WGOZoivBILZqntVNi5sErWvltdbwhCXRRQE4L+L17aAePY5kDhTX0Vsvprc87JJK4LSc+Dggq5a2LPUYyvcPg70crjJjFPWc4isfw+9YzXDXJVJ0rb+PEuxgKv2XGkDWPlWlgP16PraAOtGEPA0An5FtsLinj7cuaN53XUvhe5VJfrkD3FbBYJh2ay2tBVw5wIDAQABAoIBAC6Vns7r4usUsTV60kJbDSLZKlxxpo2fTDPtX23hiQWWd9euhkImXx1vVs0Yux+bqmNsSHuTgMIrz8l6Iut8B5wiY2k+jZoxQ8kJ02GIOgv84LsgZoDf/cFdBJmVXJqs3/8IVcS7SLWJv0p5e0H1zNix3BTuVvCZwt6p44p9Owj3MjygwTIqBFH9uip0Mnf5NsTzSIrqy27GCU+XEncJJ+TK3RGDKjDOodGxBaNe8pKDDTzmiqVqs+vLcjBVF9pqVnqTWPGbyB0tg+KGIGHMgEnS6FDSdPL3xvKzGGSmC3alJA5kq9tEibOv5XXjm+e034luGsa00KiWKRvOwQ1bjAECgYEA+MUwElWaT/ILo2CmV0miHX3YMyk6sAC9KfhQGvkbPQ1ZsUHZza1IqdSXgUZlLifCUDPhopWjPMJsVeYRR8WVdONu8KwAPs0UBLRHqQDgZnaahlaUybjMkEGNYJjT7EKH0qyDVc7uj9sewe7QPK6L/e7cFBzujcC1DKyT3TA2TAECgYEAzOSvurQsgosFLDa5YgfWLwFTvihcRUW/lHJbhOFHFeb4MHDByLKj5Y0iDdYaTNyZBwzQ7q8Tv8Y1KAeBQ+4LixZsjWimG2bxVf989GFanYerQFh/esRGMohptPTX5vAgnETYda3kH1qFvASQd3kmd3dFNVvXVVA90tOpZsHG3OcCgYEAryU9pBrqyxBxyzQXyOrJnZPlbJfBfvr2M59k3qi12bUViev/6YPv3coUZOrn/6f0cb0lRe+ufu+vkIarcemVQTeC+yVOAukmB+3Zag547wrf3mcE2EUomQPeAr12rydkMxpMWVSYt2WhcQ6vQ5nsa/sIrYx1FA4yKdszO45fHAECgYEAk0llRZpVULYszr67/stRXVLf5xdOVQGxmT2nqlri5cy15uR3eRVpmg8dRsL8/vmpC5db7ehvEvGhrpfOhY1uQNqqx/BNT+9tVvuk0GvXXtxy73tukErdh1/mVQ2Y0ksVtiALVnTdQuiosCK70xaE32Yemx/dKFfZSV+cK+DqWksCgYAKyH2ytxSlC+WCZ1lJR2FZNk0q1tmmEH25jZzr5hFQQ+jWbrVrZw6FbASXf9COXBhOGyDoP0zkr6DE/beu0EwIcJEhComNBjtXPo/fM6AZIlQmO+595/VQExqtPK0yOI+3UjD+J/FGiLcKOIwgda8P9IPBguiImz9oaI/Nsqg3/g==";

  private String privateKeyString = "MIIEowIBAAKCAQEAtTkfHfLYeURl0diylK0V+AEGGIEcdJtde5gUv8inxt1wwulFlFOflRM1StEk5nI3ENsEKDp5xw5JGyP8xbXzQJ93tvUVLF/FmK4fzdaB05QKGwHaB1ZbLWELYlp3dc4R2/eANiMx24CB8q+6bb+qo2LPbs483jzg+jtsIaxyMRH1IezAGQcH00l8UOeNIQi+gyumd72aFpoINNRSiSrLL5DLN/NRk0wdobiqCS2cwAwh92HRf2PDORfRGjPkHC3qiYF1tvfpfM5QFHF68htDwViy7goCzkk8Y7Y6cyXQxTUG8TIyj3GIj2gJHO3qDmvRQGasEz+K5Pe+EYFPNzfKrwIDAQABAoIBAAlS9iEc/A0IqPHlkW17lp6PocgD+t7AvjnJ627O86Smp6UXpZzA30VwWzTNUvXIGQrHVEztSUqTYIRCnGTgDsVSfNXYgUjhsXGcGQb0iztVzl3e7uA1oCrlVUNmkJ4ztEesZBfb10iljO6ktPpbTUTrNqRLM8IZd+ohL0W5Hrz3XVMWCPXb6mK2ZKYhNzG+D0u3qaAVwWGWtkhUEB02R5xVGZBz734XBGtiUJcgycV5RWll1ctEUbkgh++J1Oz3HNjilL49DqukWP+yNl3JiKfV81DteDSLxieQYH78feNIxouA52hBbynrTcgZw3OmHU/cAgdfym4utLHvqy1Q3e0CgYEA2vF4Gx+p8F4n+PYZd6If8/924KlZcJ8J37ThHzOnjc5ImteCEO2yOHce4OIioL9+mEi5fa4xalElPV+KPWcrg1SJ3F4da5AmS+45HaWvb1QrrPTABotU1qwUjjxhDshgq9PIfveJRYABpX1IWtCOF7DQupEm81mu5xl+dGsEK/sCgYEA0+VJziL+dvebZJxDuz1hkUrcXHJ1xdUn3QrLRT3I2d3ClNUYTe/A5AY0ueADBpbqoCci3C9lJmaoFqOCGV+7Ig8yUGrCROiw1ucLGDR/VyPDW2IdOEws2z/zW+1VD3zD/dzfhXb4OOoFruPlbqPo4Jo9OjeEdp4896cfT56WCd0CgYBrP5OM/dK56vV7r9nDz4gFduNdo7crQsyMZ8dLCHuQLDdGmkCdWEJQBJH8qOk7gvJZWG58kSKlqMAsMyu9bldTSssJRpT0xrTrxvL1gd7RExMd8vpJemEBk2OyVQNaQW+RW/m/sDPu9ohX72nNCpJAKp3kdgv1cr4JeGSIVI/DlwKBgQCBPdffcD6vB2VzX1B8kvrAqN1xUar96H4w91V2PTIVmIoHR1ZUqwiGzDY6rSSLT196IYTEse/LFVAuxroiid5U7tnsbMFjUF4UJcYBlHubBcgxI83YubWeYQopJr/7+0h7vc1j1WWfKTYgLwaQjk8LN2hi6tvTkb5z+y5A3OR8zQKBgEvNUOK+WIphlE70Rsj/0x/3md2bgo1B4633uvKPK+5Z6btxEI2fyPNgU0oU3AGHZ+DRvAoyFlXWmuuW1SxfTrCSbRNEO+uHC7NmFwrfr2YlN13Tqa2D9+3LQUs+QfqzRV0nmjx1JzyEowXjMwTAgmTUeybIT4tVeauqNNT2Ws5m";


  private String issuer = "item-kafka";
  //private int expiry = 129600;
  private int expiry = 1380;
  //private String audience = "/api/hermes/sendmessage/v1";
  private String audience = "/item-kafka/app/send-items/call-back/v1";
  //private String audience  "/item-kafka/app/send-items/Please put Endpoint here/v1";


  @Test
  void buildPEPToken() {
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, expiry);
    Map<String, Object> header = new HashMap<>();
    header.put(Header.TYPE, Header.JWT_TYPE);
    header.put(JwsHeader.ALGORITHM, SignatureAlgorithm.RS256);
    header.put(JwsHeader.KEY_ID, issuer + ".1");
    String jti = UUID.randomUUID().toString();
    log.info("Build JWT");
    String jwtToken = "";
    jwtToken = Jwts.builder()
        .setHeader(header)
        .setId(jti)
        .setAudience(audience)
        .setIssuer(issuer)
        .setExpiration(calendar.getTime())
        .signWith(SignatureAlgorithm.RS256, this.getPrivatekey())
        .compact();
    log.info("JWT Token : {}", jwtToken);
  }

  private Key getPrivatekey() {
    try {
      Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
      StringBuilder sb = new StringBuilder();
      sb.append("-----BEGIN RSA PRIVATE KEY-----");
      sb.append(System.lineSeparator());
      sb.append(privateKeyString);
      sb.append(System.lineSeparator());
      sb.append("-----END RSA PRIVATE KEY-----");
      PEMParser pemParser = new PEMParser(new StringReader(sb.toString()));
      JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
      Object object = pemParser.readObject();
      KeyPair kp = converter.getKeyPair((PEMKeyPair) object);
      log.info("", kp.getPrivate());
      return kp.getPrivate();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
