package Helperclasses;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class AesUtility {

	private SecretKeySpec skeySpec;
	private Cipher cipher;
	
	public AesUtility(byte[] key) throws Exception {
		byte[] bytesOfMessage =key;
		skeySpec = new SecretKeySpec(bytesOfMessage, "AES");
		cipher = Cipher.getInstance("AES/ECB/NoPadding");
	}

	public byte[] encrypt(byte[] plaintext) throws Exception {
		cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
		byte[] ciphertext = cipher.doFinal((plaintext));
		return ciphertext;
	}

	public byte[] decrypt(byte[] ciphertext) throws Exception {
		// returns byte array decrypted with key
		cipher.init(Cipher.DECRYPT_MODE, skeySpec);
		byte[] plaintext = cipher.doFinal(ciphertext);
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

}

