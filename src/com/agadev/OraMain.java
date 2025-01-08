package com.agadev;

public class OraMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(args.toString());
		String job = args[1].toString();
		if (job.equals("backup")) {
			OraBack.main(args);
		}
		else if (job.equals("restore")) {
			OraRestore.main(args);
		}

	}

}
