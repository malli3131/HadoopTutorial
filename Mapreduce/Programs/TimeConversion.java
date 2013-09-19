import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeConversion
{
	public static long getTime(String time) throws ParseException
	{
		long seconds = 0L;
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = dateformat.parse(time);
		seconds = date.getTime();
		return seconds;
	}
	public static String getDateString(Long seconds)
	{
		Date date = new Date(seconds);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateString = sdf.format(date);
		return dateString;
	}
}
