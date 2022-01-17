#!/usr/bin/env bash

export CERT_FILE=localhost.crt

export KEY_SECRET=serverks
export KEY_JKS=server.keystore.jks
export KEY_PKCS12=server.keystore.p12

export TRUST_SECRET=clientts
export TRUST_JKS=client.truststore.jks
export TRUST_PKCS12=client.truststore.p12

keytool -genkey -alias kafka-test-store -keyalg RSA -keystore ${KEY_JKS} -keysize 2048 -dname CN=localhost -keypass ${KEY_SECRET} -storepass ${KEY_SECRET}
keytool -export -alias kafka-test-store -file ${CERT_FILE} -keystore ${KEY_JKS} -keypass ${KEY_SECRET} -storepass ${KEY_SECRET}
keytool -importkeystore -srckeystore ${KEY_JKS} -srcstorepass ${KEY_SECRET} -destkeystore ${KEY_PKCS12} -deststoretype PKCS12 -deststorepass ${KEY_SECRET}
keytool -keystore ${TRUST_JKS} -import -file ${CERT_FILE} -keypass ${KEY_SECRET} -storepass ${TRUST_SECRET} -noprompt
keytool -importkeystore -srckeystore ${TRUST_JKS} -srcstorepass ${TRUST_SECRET} -destkeystore ${TRUST_PKCS12} -deststoretype PKCS12 -deststorepass ${TRUST_SECRET}
rm -Rf localhost.crt client.truststore.jks server.keystore.jks
