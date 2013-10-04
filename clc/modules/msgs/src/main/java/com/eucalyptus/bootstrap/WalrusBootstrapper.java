package com.eucalyptus.bootstrap;

public class WalrusBootstrapper {

	public static void main(String[] args) {

		SystemBootstrapper singleton = SystemBootstrapper.getInstance();

		try {
			singleton.init();
			singleton.load();
			singleton.start();
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}

	}

}
