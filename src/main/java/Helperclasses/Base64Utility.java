package Helperclasses;

import java.util.Base64;

/**
 *
 * @author keerthy
 */

/**
 * Class used to encode and decode String to and from BASE-64
 * 
 * @author Riyas
 * 
 */
public class Base64Utility {
	/**
	 * Converting a string to base 64 values
	 * 
	 * @param text
	 * @return base64 string
	 */
	public static String encodeToBase64(byte[] data) {

		if (data == null || data.length == 0)
			return null;
		String base64 = Base64.getEncoder().encodeToString(data);


		return base64.trim();
	}

	/**
	 * Converting a base64 string to normal string
	 * 
	 * @param text
	 * @return normal string
	 */
	public static byte[] decodeFromBase64(String text) {

		byte[] data1 = null;

		if (text != null) {
			try {
				data1 = Base64.getDecoder().decode(text);

			} catch (IllegalArgumentException e) {

			}
		}

		return data1;
	}
}
