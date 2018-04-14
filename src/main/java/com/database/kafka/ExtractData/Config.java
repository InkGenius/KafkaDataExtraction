package com.database.kafka.ExtractData;

public class Config {
	// JDBC 驱动名及数据库 URL
	public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	public static final String DB_URL = "jdbc:mysql://192.168.56.122:3306/csdn";

	// 数据库的用户名与密码，需要根据自己的设置
	public static final String USER = "root";
	public static final String PASS = "cluster";

	public static final String articleInfoSQL = "insert into articleinfo values(?, ?, ?, ?, ?, ?, ?, ?)";
	public static final String userBasicSQL = "insert into userbasic values(?, ?, ?, ?, ?, ?)";
	public static final String userEduSQL = "insert into useredu values(?, ?, ?, ?)";
	public static final String userSkillSQL = "insert into userskill values(?, ?)";
	public static final String userInterestSQL = "insert into userinterest values(?, ?)";
	public static final String userBehaviorSQL = "insert into userbehavior values(?, ?, ?, ?)";

	public static enum Topics {
		USERBASIC("userbasic", userBasicSQL), USEREDU("useredu", userEduSQL), USERSKILL("userskill",
				userSkillSQL), USERINTEREST("userinterest", userInterestSQL), USERBEHAVIOR("userbehavior",
						userBehaviorSQL), ARTICLEINFO("articleinfo", articleInfoSQL);
		// 定义私有变量
		public  String topicName;
		public String insertSQL;

		// 构造函数，枚举类型只能为私有
		private Topics(String topicName, String insertSQL) {
			this.topicName = topicName;
			this.insertSQL = insertSQL;
		}
		
	}
}
