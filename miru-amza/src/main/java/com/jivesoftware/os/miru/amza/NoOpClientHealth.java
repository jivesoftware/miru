package com.jivesoftware.os.miru.amza;

import com.jivesoftware.os.routing.bird.shared.ClientHealth;

public class NoOpClientHealth implements ClientHealth {

    @Override
    public void attempt(String s) {
    }

    @Override
    public void success(String s, long l) {
    }

    @Override
    public void markedDead() {
    }

    @Override
    public void connectivityError(String s) {
    }

    @Override
    public void fatalError(String s, Exception e) {
    }

    @Override
    public void stillDead() {
    }
}
