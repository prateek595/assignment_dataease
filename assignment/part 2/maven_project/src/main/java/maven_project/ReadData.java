package maven_project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class ReadData {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			Connection con = ConnectDatabase.getConnection();
			String sql = "INSERT INTO \"DEMO1\"( \"ID\", \"ISCONFIDENTIAL\", "
					+ "\"PROJECTNAME\", \"STREET\", \"CITY\", \"STATE\",\"ZIPCODE\", \"COUNTRY\","
					+ " \"LEEDSYSTEMVERSIONDISPLAYNAME\", \"POINTSACHIEVED\", \"CERTLEVEL\","
					+ " \"CERTDATE\", \"ISCERTIFIED\", \"OWNERTYPES\", \"GROSSSQFOOT\", "
					+ "\"TOTALPROPAREA\", \"PROJECTTYPES\", \"OWNERORGANIZATION\", "
					+ "\"REGISTRATIONDATE\" ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement stmt = con.prepareStatement(sql);
			BufferedReader lineReader = new BufferedReader(new FileReader("F:\\Dataeaze\\mycsv1.csv"));
			String lineText = null;
			int count = 0;
			int batchSize = 1000;
			lineReader.readLine(); // skip header line
			while ((lineText = lineReader.readLine()) != null) {
				String[] data = lineText.split(",");
				for(int i=0;i<=data.length;i++)
				{
					switch(i)
					{
					case 0 : String ID = data[1];stmt.setString(1, ID);break;
					case 1:	String Isconfidential = data[2];stmt.setString(2, Isconfidential);break;
					
					case 2:	String ProjectName = data[3];stmt.setString(3, ProjectName);break;
					
					case 3:String Street = data[4];stmt.setString(4, Street);break;
					
					case 4:String City = data[5];stmt.setString(5, City);break;
					
					case 5:String State = data[6];stmt.setString(6, State);break;
					
					case 6:String Zipcode = data[7];stmt.setString(7, Zipcode);break;
					
					case 7:String Country = data[8];stmt.setString(8, Country);break;
					
					case 8:String LEEDSystemVersionDisplayName = data[9];stmt.setString(9, LEEDSystemVersionDisplayName);break;
					
					case 9:String PointsAchieved = data[10];stmt.setString(10, PointsAchieved);break;
					
					case 10:String CertLevel = data[11];stmt.setString(11, CertLevel);break;
					
					case 11:String CertDate = data[12];stmt.setString(12, CertDate);break;
					
					case 12:String IsCertified = data[13];stmt.setString(13, IsCertified);break;
					
					case 13:String OwnerTypes = data[14];stmt.setString(14, OwnerTypes);break;
					
					case 14:String GrossSqFoot = data[15];stmt.setString(15, GrossSqFoot);break;
					
					case 15:String TotalPropArea = data[16];stmt.setString(16, TotalPropArea);break;
					
					case 16:String ProjectTypes = data[17];stmt.setString(17, ProjectTypes);break;
					
					case 17:String OwnerOrganization = data[18];stmt.setString(18, OwnerOrganization);break;
					
					case 18:String RegistrationDate = data[19];stmt.setString(19, RegistrationDate);break;
					default : break;
					}
				}
				
				count++;

				stmt.addBatch();
				if (count % batchSize == 0) {
					stmt.executeBatch();
				}
				//stmt.executeUpdate();
				
				

			}
			lineReader.close();
			// execute the remaining queries
			stmt.executeBatch();
			System.out.println(count);
			con.commit();
			con.close();

		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
