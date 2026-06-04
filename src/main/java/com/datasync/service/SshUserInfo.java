package com.datasync.service;

import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

/**
 * SSH 认证信息（支持 password + keyboard-interactive）
 */
public class SshUserInfo implements UserInfo, UIKeyboardInteractive {
    private final String password;

    public SshUserInfo(String password) {
        this.password = password;
    }

    public String getPassword() { return password; }
    public boolean promptYesNo(String msg) { return true; }
    public String getPassphrase() { return null; }
    public boolean promptPassphrase(String msg) { return false; }
    public boolean promptPassword(String msg) { return true; }
    public void showMessage(String msg) {}

    public String[] promptKeyboardInteractive(String destination, String name,
            String instruction, String[] prompt, boolean[] echo) {
        String[] response = new String[prompt.length];
        for (int i = 0; i < prompt.length; i++) {
            response[i] = password;
        }
        return response;
    }
}
