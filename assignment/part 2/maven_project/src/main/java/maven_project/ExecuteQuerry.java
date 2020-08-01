package maven_project;

import java.sql.*;

public class ExecuteQuerry {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			Connection con = ConnectDatabase.getConnection();

			Statement stmt = con.createStatement();

			String ques1, ques2, ques3,ques4;
			ques1 = "select ProjectTypes,LEEDSystemVersionDisplayName,count(*) as Count from demo1 where State == 'VA' group by ProjectTypes,LEEDSystemVersionDisplayName";
			ques2 = "select OwnerTypes,count(*) as Count from demo1 where State == 'VA' group by OwnerTypes";
			ques3 = "select sum(GrossSqFoot) as Total from demo1 where State == 'VA' and IsCertified == 'Yes'";
			ques4="select zipcode,max(No_Of_Project) from (select zipcode,count(ProjectName) as \"No_Of_Project\" "
					+ "from demo1 where State==\"VA\" groupby zipcode)";
			String ques[] = { ques1, ques2, ques3,ques4 };
			for (int i = 0; i < 4; i++) {
				ResultSet rs = stmt.executeQuery(ques[i]);
				ResultSetMetaData rmd = rs.getMetaData();
				int temp = rmd.getColumnCount();
				for (int j = 0; j < temp; j++) {
					System.out.println(rmd.getColumnName(j) + "\t");
				}
				while (rs.next()) {
					System.out.println();
					for (int j = 0; j < temp; j++) {
						System.out.println(rs.getString(j) + "\t");
					}
				}
			}

			// stmt.executeUpdate();

			con.close();

		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
