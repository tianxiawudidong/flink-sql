package com.ifchange.test;

public class Singleton {

    private static volatile Singleton instance = null;

    private Singleton() {
        System.out.println("constructor................");
    }

    public static Singleton getInstance() {
        if (instance == null) {
            synchronized (Singleton.class) {
                if (instance == null) {
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }


    public static void main(String[] args) {



        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                Singleton instance = Singleton.getInstance();
                System.out.println(Thread.currentThread().getName());
            }, String.valueOf(i)).start();
        }

    }


}
