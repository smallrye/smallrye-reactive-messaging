# Mosquito TLS

## Certificate Authority

```
openssl req -new -newkey rsa:4096 -x509 -keyout server/ca.key -out server/ca.crt -days 10000 -subj "/CN=smallrye.io"
```

Use `password` as ... password

## Server Certificate

1. Server Key

```
openssl genrsa -out server/tls.key 4096
```

2. Server Certificate CN=localhost

```
openssl req -out server/tls.csr -key server/tls.key -new  # Use `password` as challenge password, localhost as common name
openssl x509 -req -in server/tls.csr -CA server/ca.crt -CAkey server/ca.key -CAcreateserial -out server/tls.crt -days 10000
```

`password` is the passphrase.

3. Client Truststore

```
keytool -delete -alias smallrye.io -keystore client/client.ts # `password` as passphrase
keytool -import -storepass password -file server/ca.crt -alias smallrye.io -keystore client/client.ts # trust the certificate
```

# Mosquitto Mutual TLS

## Client Certificate

1. Client Key

```
keytool -delete -alias client -keystore client/client.ks # `password` as passphrase
keytool -genkeypair -storepass password -keyalg RSA -keysize 4096 -dname "CN=client" -alias client -validity 10000 -keystore client/client.ks
```


2. Certificate Signing Request (CSR)

```
keytool -certreq -storepass password -keyalg rsa -alias client -keystore client/client.ks -file client/client.csr
```

3. Certificate Authority Sign

```
openssl x509 -req -CA server/ca.crt -CAkey server/ca.key -in client/client.csr -out client/client.crt -days 10000 -CAcreateserial #password as passphrase
keytool -delete -alias root -keystore client/client.ks # `password` as passphrase
keytool -import -v -trustcacerts -alias root -file server/ca.crt -keystore client/client.ks # `password` as passphrase and trust the certificate
keytool -import -v -trustcacerts -alias client -file client/client.crt -keystore client/client.ks
```
