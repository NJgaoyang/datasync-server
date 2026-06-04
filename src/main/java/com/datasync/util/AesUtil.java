package com.datasync.util;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * AES 加解密工具 — 用于加密 SeaTunnel 配置中的明文密码
 */
public class AesUtil {

    // 固定前缀，用于标识已加密的密码
    private static final String PREFIX = "AES:";

    /**
     * 加密明文密码，返回 "AES:base64密文"
     */
    public static String encrypt(String plainText, String secretKey) {
        if (plainText == null || plainText.isEmpty()) return plainText;
        if (isEncrypted(plainText)) return plainText; // 已加密不重复处理
        try {
            SecretKeySpec keySpec = buildKey(secretKey);
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
            byte[] encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            return PREFIX + Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            // 加密失败降级返回明文（不阻塞业务流程）
            return plainText;
        }
    }

    /**
     * 解密，返回明文密码。如果字符串不以 "AES:" 开头则原样返回
     */
    public static String decrypt(String cipherText, String secretKey) {
        if (cipherText == null || cipherText.isEmpty()) return cipherText;
        if (!isEncrypted(cipherText)) return cipherText;
        try {
            String b64 = cipherText.substring(PREFIX.length());
            SecretKeySpec keySpec = buildKey(secretKey);
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, keySpec);
            byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(b64));
            return new String(decrypted, StandardCharsets.UTF_8);
        } catch (Exception e) {
            // 解密失败返回原始字符串（旧数据可能未加密）
            return cipherText;
        }
    }

    public static boolean isEncrypted(String text) {
        return text != null && text.startsWith(PREFIX);
    }

    /**
     * 使用 SHA-256 将任意长度密钥转为 16 字节 AES-128 密钥
     */
    private static SecretKeySpec buildKey(String secretKey) throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        byte[] hash = sha.digest(secretKey.getBytes(StandardCharsets.UTF_8));
        byte[] key = new byte[16];
        System.arraycopy(hash, 0, key, 0, 16);
        return new SecretKeySpec(key, "AES");
    }
}
