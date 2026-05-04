/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import org.hyperledger.fabric.client.identity.Identities;
import org.hyperledger.fabric.client.identity.Identity;
import org.hyperledger.fabric.client.identity.Signer;
import org.hyperledger.fabric.client.identity.Signers;
import org.hyperledger.fabric.client.identity.X509Identity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.cert.CertificateException;
import java.util.Objects;

public final class Connections {
    public static final class GatewayProfile {
        private final String mspId;
        private final Path certDirPath;
        private final Path keyDirPath;
        private final Path tlsCertPath;
        private final String peerEndpoint;
        private final String overrideAuth;

        public GatewayProfile(final String mspId,
                              final Path certDirPath,
                              final Path keyDirPath,
                              final Path tlsCertPath,
                              final String peerEndpoint,
                              final String overrideAuth) {
            this.mspId = Objects.requireNonNull(mspId, "mspId is required");
            this.certDirPath = Objects.requireNonNull(certDirPath, "certDirPath is required");
            this.keyDirPath = Objects.requireNonNull(keyDirPath, "keyDirPath is required");
            this.tlsCertPath = Objects.requireNonNull(tlsCertPath, "tlsCertPath is required");
            this.peerEndpoint = Objects.requireNonNull(peerEndpoint, "peerEndpoint is required");
            this.overrideAuth = Objects.requireNonNull(overrideAuth, "overrideAuth is required");
        }

        public String getMspId() {
            return mspId;
        }

        public Path getCertDirPath() {
            return certDirPath;
        }

        public Path getKeyDirPath() {
            return keyDirPath;
        }

        public Path getTlsCertPath() {
            return tlsCertPath;
        }

        public String getPeerEndpoint() {
            return peerEndpoint;
        }

        public String getOverrideAuth() {
            return overrideAuth;
        }
    }

    private Connections() {
        // Private constructor to prevent instantiation
    }

    public static GatewayProfile defaultProfile() {
        Path cryptoPath = Paths.get("..", "..", "test-network", "organizations", "peerOrganizations", "org1.example.com");
        Path certDirPath = cryptoPath.resolve(Paths.get("users", "User1@org1.example.com", "msp", "signcerts"));
        Path keyDirPath = cryptoPath.resolve(Paths.get("users", "User1@org1.example.com", "msp", "keystore"));
        Path tlsCertPath = cryptoPath.resolve(Paths.get("peers", "peer0.org1.example.com", "tls", "ca.crt"));
        return new GatewayProfile(
                "Org1MSP",
                certDirPath,
                keyDirPath,
                tlsCertPath,
                "localhost:7051",
                "peer0.org1.example.com");
    }

    public static ManagedChannel newGrpcConnection(final GatewayProfile profile) throws IOException {
        var credentials = TlsChannelCredentials.newBuilder()
                .trustManager(profile.tlsCertPath.toFile())
                .build();
        return Grpc.newChannelBuilder(profile.peerEndpoint, credentials)
                .overrideAuthority(profile.overrideAuth)
                .build();
    }

    public static ManagedChannel newGrpcConnection() throws IOException {
        return newGrpcConnection(defaultProfile());
    }

    public static Identity newIdentity(final GatewayProfile profile) throws IOException, CertificateException {
        try (var certReader = Files.newBufferedReader(getFirstFilePath(profile.certDirPath))) {
            var certificate = Identities.readX509Certificate(certReader);
            return new X509Identity(profile.mspId, certificate);
        }
    }

    public static Identity newIdentity() throws IOException, CertificateException {
        return newIdentity(defaultProfile());
    }

    public static Signer newSigner(final GatewayProfile profile) throws IOException, InvalidKeyException {
        try (var keyReader = Files.newBufferedReader(getFirstFilePath(profile.keyDirPath))) {
            var privateKey = Identities.readPrivateKey(keyReader);
            return Signers.newPrivateKeySigner(privateKey);
        }
    }

    public static Signer newSigner() throws IOException, InvalidKeyException {
        return newSigner(defaultProfile());
    }

    private static Path getFirstFilePath(Path dirPath) throws IOException {
        try (var keyFiles = Files.list(dirPath)) {
            return keyFiles.findFirst().orElseThrow();
        }
    }
}
