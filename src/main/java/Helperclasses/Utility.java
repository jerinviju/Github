package Helperclasses;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class Utility {
	
	static Utility mUtility;
	
	public static Utility getInstance() {

		if (mUtility == null) {

			mUtility = new Utility();
		}
		return mUtility;

	}
	
	/**
	 *   
	 * @param tempVal
	 * @return temperature
	 */
	public int getTemperature(int tempVal) {
		
		double tVal = 0.0;
		if(tempVal >= 214 && tempVal <= 768){
			tVal = (( 11246.08 * (tempVal) ) / 65536.0 )  - 46.85;
		}else{
			tVal = 0.0;
		}
		return (int)tVal;
	}
	
	/**
	 *   
	 * @param humidityVal
	 * @return humidity
	 */
	public int GetHumidity (int humidityVal)
	{
		double humVal = 0.0;
		/**
		 * check whether sent data is in bounds
		 */
		if(humidityVal >= 98 && humidityVal <= 1737){
			humVal = ((4000.0 * (humidityVal)) / 65536.0 ) - 6.0;
		}else{
			humVal = 0.0;
		}
		return (int)humVal;
	}
	
	/**
	 *   
	 * @param accel
	 * @return accelIntensity
	 */
	public int GetMotionAvgAcceleration (int accel)
	{
		int accelIntensity = 0;
		if((128 & accel) == 128){
			accelIntensity = (127 & accel);
			return accelIntensity;
		}else{
			return 0;
		}
	}	
	/**
	 *   
	 * @param 19 byte scanRecord
	 * @return 16byte encrypted packet
	 */

	public byte[] reverseMask(byte[] scanRecord) {
		
		byte hope=scanRecord[0];
		byte firstPadding=scanRecord[17];
		byte secondPadding=scanRecord[18];



		// 000 1(1) 1 111 CHECKING FOURTH BIT IS ONE IF YES MSB OF SECOND PADDING IS ONE
		if((hope&0x10)!=0)
		{
			secondPadding=(byte)(secondPadding|0x80);
		}

		// 000 1 (1) 111 CHECKING FIFTH BIT IS ONE IF YES MSB OF FIRST PADDING IS ONE


		if((hope&0x08)!=0)
		{
			firstPadding=(byte)(firstPadding|0x80);
		}
		int pad=128;

		
		byte[] result=new byte[16];
		int counter=0;
		for(int i=1;i<=8;i++)
		{
			byte individualByte=(byte)scanRecord[i];
			
			//
			if(((firstPadding)&pad)==pad)
			{
				individualByte=(byte)(individualByte|0x80);
			}
			
			pad=(byte)(pad>>1);
			
			result[counter++]=individualByte;
			
		}
		
		pad=128;

		for(int i=9;i<=16;i++)
		{
			byte individualByte=(byte)scanRecord[i];
			
			if(((secondPadding)&pad)==pad)
			{
				individualByte=(byte)(individualByte|0x80);
			}
			
			pad=(byte)(pad>>1);
			result[counter++]=individualByte;

		}
		
		return result;
	}
	
	/**
	 *   
	 * @param 19 byte packet, 16 byte key
	 * @return 16byte decrypted packet
	 */
	public byte[] decrypt(byte[] packet, byte[] key) throws Exception {
		SecretKeySpec skeySpec;
		Cipher cipher;
		skeySpec = new SecretKeySpec(key, "AES");
		cipher = Cipher.getInstance("AES/ECB/NoPadding");
		cipher.init(Cipher.DECRYPT_MODE, skeySpec);
		byte[] plaintext = cipher.doFinal(packet);
		if(plaintext==null){
			return null;
		}
		String decryptedDataString="Decrypted status packet data : ";
		for(int i=0;i<plaintext.length;i++)
		{
			decryptedDataString=decryptedDataString+" | "+String.format("%02X",(plaintext[i]&0xff));
		}
		return plaintext;
	}
	
	/**
	 *   
	 * @param 16 byte packet
	 * @return base64 encoded string
	 */
	public String byteToBase64(byte[] packet) {	
		Base64 codec = new Base64();
		String encodedData = new String(codec.encode(packet));
		return encodedData;
	}
	
	/**
	 *   
	 * @param encodedDAta
	 * @return byte array
	 */
	public byte[] base64ToByte(String encodedData) {
		Base64 codec = new Base64();
		byte[] decodedData = codec.decode(encodedData);
		return decodedData;
	}
	
	/**
	 *   
	 * @param commandByte, pwmValue
	 * @return boolean
	 */
	public boolean isAllowedPacket(int commandByte, int pwmValue) {
		
		boolean val = false;
		
        switch (commandByte){

        case Constants.ENABLE_DISABLE_MULTISENSOR_COMMAND_OLD :
            switch(pwmValue){
                case Constants.ENABLE_LISTENER_PWM:
                case Constants.DISABLE_LISTENER_PWM:

                case Constants.MULTI_SENSOR_ENABLE_NONE_PWM:
                case Constants.MULTI_SENSOR_LDR_ENABLE_PWM:
                case Constants.MULTI_SENSOR_LDR_MAX_PWM:
                case Constants.MULTI_SENSOR_LDR_MIN_PWM:
                case Constants.MULTI_SENSOR_PIR_ENABLE_PWM:
                case Constants.MULTI_SENSOR_PIR_INTERVAL_PWM:
                	val = true;
                default:
                	val = false;
            }
            break;
        case Constants.SHUTTER_COMMAND_OLD :
            switch(pwmValue){
                case Constants.SHUTTER_UP_PWM:
                case Constants.SHUTTER_DOWN_PWM:
                case Constants.SHUTTER_STOP_PWM:
                case Constants.SHUTTER_SPIKE_UP_PWM:
                case Constants.SHUTTER_SPIKE_DOWN_PWM:
                case Constants.SHUTTER_LOCK_PWM:
                case Constants.SHUTTER_UNLOCK_PWM:
                	val = true;
                default:
                	val = false;
            }
            break;
        case Constants.TIME_SYNC_FEEDBACK_COMMAND:
        case Constants.OPERATE_STATUS_COMMAND_NEW : //new operate with status
        case Constants.STATUS_COMMAND_NEW : //new status packet
        case Constants.GROUP_OPERATE_STATUS_COMMAND_NEW : //new group operate with status
        case Constants.OTA_UPDATE_STATUS_COMMAND:
        case Constants.DEVICE_POWER_CONSUMPTION_STATUS_COMMAND:

//        case Wise::Constants::STATUS_COMMAND_OLD : //old status
        case Constants.OPERATE_STATUS_COMMAND_OLD : //old operate with status
        case Constants.GROUP_OPERATE_STATUS_COMMAND_OLD : //old group operate with status
        	val = true;
        default:
        	val = false;

    }
    return val;
		
	}





}
