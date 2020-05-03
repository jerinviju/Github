package Helperclasses;/*
 * WISILICA CONFIDENTIAL
 * __________________
 *
 * [2013] - [2018] WiSilica Incorporated
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of WiSilica Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to WiSilica Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from WiSilica Incorporated.
 */

import java.util.Arrays;

public class ByteMasking {
	static final String TAG = "WiSe SDK : ByteMasking";
	static int currentHope= 5;

	public static void setHope(int currentHope) {
		ByteMasking.currentHope = currentHope;
	}

	public static byte[] mask(byte[] byteToMask) {
		short pad1 = 0;
		short mask1 = 128;

		short pad2 = 0;
		short mask2 = 128;
		//Logger.d(TAG, "BYTE MASKING INPUT LENGTH>>>>>>>>:" + byteToMask.length);

		for (int i = 0; i < byteToMask.length; i++) // 9-18 10 byte
													// _operation
													// sequence id
		{
			/*
			 * if (i != 7) byteToMask[i] = (byte) 0xff; else byteToMask[i] =
			 * (byte) 0x00;
			 */

			byte individualByte = (byte) ((byte) byteToMask[i] & 0xFF);

			// checking msb of each byte is zero or not
			if (msbValueOfByte(individualByte) != 0) {
				// removing msb
				// /int y = b & 0x7F;
				byteToMask[i] = (byte) removeMsb(individualByte);

				if (i < 8) {


					/* 
					
					 Changing pad1 according to position for the first time its looks like
					 0000 0000
					 1000 0000
					 ---------
					 1000 0000(After masking)
						
					*/
					
					
					pad1 = (short) (pad1 | mask1);

				} else {
				
					/* 
					
					 Changing pad2 according to position for the first time its looks like
					 0000 0000
					 1000 0000
					 ---------
					 1000 0000 (After masking)
						
					*/

					pad2 = (short) (pad2 | mask2);


				}

			}

			if (i < 8) {
				mask1 = (short) (mask1 / 2);

			} else {
				mask2 = (short) (mask2 / 2);


			}

		}

		byte paddingFirst = (byte) ((byte) pad1 & 0xFF);

		byte hope = (byte) currentHope;



		//checking msb !=0
		if ((paddingFirst & 0x80) != 0) {
			//removing msb
			paddingFirst = (byte) (paddingFirst & (byte) 0x7f);
			hope = (byte) (hope | 0x08);
		}
		//checking msb !=0
		byte paddingSecond = (byte) ((byte) pad2 & 0xFF);
		if ((paddingSecond & 0x80) != 0) {
			//removing msb
			paddingSecond = (byte) (paddingSecond & (byte) 0x7f);

			hope = (byte) (hope | 0x10);
		}




		// result( total of 19 bytes)==> 1 byte hope & rest of masked index + 16
		// byte masked byte + 2 byte masking index bytes

		int counter = 0;
		byte[] result = new byte[19];

		result[counter++] = (byte) hope;
		for (byte b : byteToMask)
			result[counter++] = b;

		result[counter++] = paddingFirst;
		result[counter++] = paddingSecond;
		System.out.print("result: "+Arrays.toString(result)+"\n");
		//Debugger.debugMaskedPacketAdvertising(result);



		return result;
	}
	
	
	
	
	
	
	/**
	 *   
	 * @param scanRecord
	 * @return
	 */

	public static byte[] reverseMask(byte[] scanRecord) {
		
		byte hope=scanRecord[12];
		byte firstPadding=scanRecord[29];
		byte secondPadding=scanRecord[30];



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
		for(int i=13;i<=20;i++)
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

		for(int i=21;i<=28;i++)
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
	 * Return the msb of the byte
	 * 
	 * @param b
	 * @return
	 */
	private static int msbValueOfByte(byte b) {
		return (b & 0x80);
	}

	/**
	 * removing msb from byte
	 * 
	 * @param b
	 * @return
	 */
	private static byte removeMsb(byte b) {
		return (byte) (b & 0x7F);
	}

}
