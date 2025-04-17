package org.hiero.base.crypto;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class CryptoUtils {

    private CryptoUtils() {}

    /**
     * Decode a X509Certificate from a byte array that was previously obtained via X509Certificate.getEncoded().
     *
     * @param encoded a byte array with an encoded representation of a certificate
     * @return the certificate reconstructed from its encoded form
     */
    @NonNull
    public static X509Certificate decodeCertificate(@NonNull final byte[] encoded) {
        try (final InputStream in = new ByteArrayInputStream(encoded)) {
            final CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        } catch (CertificateException | IOException e) {
            throw new CryptographyException(e);
        }
    }


}
