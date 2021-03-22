# Mosquito TLS

## Certificate Authority

`openssl req -new -newkey rsa:2048 -x509 -keyout server/ca.key -out server/ca.crt -days 365 -subj "/CN=smallrye.io"`

## Server Certificate

1. Server Key

    `openssl genrsa -out server/tls.key 2048`

2. Server Certificate CN=localhost

    ```
    openssl req -out server/tls.csr -key server/tls.key -new
    openssl x509 -req -in server/tls.csr -CA server/ca.crt -CAkey server/ca.key -CAcreateserial -out server/tls.crt -days 10000
    ```

3. Client Truststore

    ```
    keytool -import -storepass password -file server/ca.crt -alias smallrye.io -keystore client/client.ts
    ```

# Mosquitto Mutual TLS

## Client Certificate

1. Client Key

    `keytool -genkeypair -storepass password -keyalg RSA -keysize 2048 -dname "CN=client" -alias client -keystore client/client.ks`

2. Certificate Signing Request (CSR)

    `keytool -certreq -storepass password -keyalg rsa -alias client -keystore client/client.ks -file client/client.csr`

3. Certificate Authority Sign

    ```
    openssl x509 -req -CA server/ca.crt -CAkey server/ca.key -in client/client.csr -out client/client.crt -days 10000 -CAcreateserial

    keytool -import -v -trustcacerts -alias root -file server/ca.crt -keystore client/client.ks
    keytool -import -v -trustcacerts -alias client -file client/client.crt -keystore client/client.ks
    ```