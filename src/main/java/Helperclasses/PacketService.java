package Helperclasses;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;

/**
 *
 * @author keerthy
 */
public class PacketService {

    public PacketService() {

    }

    public static void main(String args[]) throws Exception {

        int[] pwm = {0, 0, 0, 0};
        
        /*Packet creation */
        byte[] aesEncrypeData = PacketService.packetCreation(1066, "J9HYtlIZd/x9a+fg8oojrg==", 49, 16, 182, pwm, 501);

        /*Function to convert 16 byte array to 19 byte array*/
        byte[] maskedDataArray = ByteMasking.mask(aesEncrypeData);
        
        
        /*Function to create 24 byte data*/
        String fullBridgeData = bitShifting(maskedDataArray, 867, 1066);
        System.out.println(fullBridgeData);
//        
//       int i=0;
//        while(i<10){
//            i++;
////            Multi multi=new Multi("thraedd---jiji");
////            multi.start();
//
//        }
    }

    /*Function to create  16 byte packet*/
    public static byte[] packetCreation(int shortId, String securityKey, int commandByte, int maskingByte, int operationSequence, int[] pwmValue, int operationId) throws Exception {

    	
        int versionNumber = 1;
        int sourceId = 0;
        byte[] sequence_array;
        byte[] destinationId;
        int firstConstant = 0;
        int secondConstant = 0;
        if(commandByte==130 ){
            firstConstant=2;
            if(operationId==1013)
                secondConstant=1;
            else
                secondConstant=4;
        }


        /*conversion to byte array*/
        sequence_array = convertIntToByteArray(operationSequence, 2);

        destinationId = convertIntToByteArray(shortId, 2);

        /* packet Data without CRC 
        ############################################################################################################################################################################################
        #  VersionNumber   ##  sourceId    ##  F-ReserveByte    ##  destinationId    ##  SequenceNumber    ##  CommandByte   ##  f-contstant   ##  s-constant   ##  maskingByte   ##  pwmValues   ##
        ############################################################################################################################################################################################        
         */
        byte[] data = new byte[]{(byte) versionNumber, (byte) sourceId, (byte) destinationId[0], (byte) destinationId[1], (byte) sequence_array[0], (byte) sequence_array[1],
            (byte) commandByte, (byte) firstConstant, (byte) secondConstant, (byte) maskingByte, (byte) pwmValue[0], (byte) pwmValue[1], (byte) pwmValue[2], (byte) pwmValue[3]};

        System.out.println(Arrays.toString(data));
        
        StringBuffer result = new StringBuffer();
        for (byte b : data) {
            result.append(String.format("%02X ", b));
            result.append(" "); // delimiter
        }

        System.out.print(result.toString()+"\n");
        
        System.out.println("CRC");
        
        //CRC calculation
        byte[] crc = CrcGenerator.calculateCrc(data);
        
        StringBuffer result1 = new StringBuffer();
        for (byte b : crc) {
        	result1.append(String.format("%02X ", b));
        	result1.append(" "); // delimiter
        }
        
        System.out.print(result1.toString()+"\n");

        byte[] dataToEncrypt = new byte[16];

        //Generating 16 byte application data
        System.arraycopy(crc, 0, dataToEncrypt, 0, 2);
        System.arraycopy(data, 0, dataToEncrypt, 2, 14);
        
        System.out.println("BEFORE ENCRYPTION");
        
        StringBuffer result2 = new StringBuffer();
        for (byte b : dataToEncrypt) {
        	result2.append(String.format("%02X ", b));
        	result2.append(" "); // delimiter
        }
        
        System.out.println(result2);

        byte[] decodedSecurityKey = Base64Utility.decodeFromBase64(securityKey);

        //Aes encryption 
        AesUtility aesUtility = new AesUtility(decodedSecurityKey);
        byte[] encryptedData = aesUtility.encrypt(dataToEncrypt);

        String output = "Data to encrypt::";
        for (int i = 0; i < 16; i++) {
            output = output + " " + String.format("%2X", dataToEncrypt[i] & 0xFF);

        }
        //System.out.println(output);
        return encryptedData;

    }
    /*Function to create  24 byte packet*/
    public static String bitShifting(byte[] maskedData, int network_id, int destination) {

        int packetFlag = 1;
        String packetFormat = "13";
        String server_id = "00";
        String manufacture_id_m = "97";
        String manufacture_id_l = "01";

        byte[] network_id_arr = convertIntToByteArray(network_id, 2);
        String device_id = Integer.toHexString(destination & 127);
                
        if (device_id.length() == 1) {
            device_id = '0' + device_id;
        }

        String network_byte1 = Integer.toHexString(network_id_arr[0] & 255);
        String network_byte2 = Integer.toHexString(network_id_arr[1] & 255);

        if (network_byte1.length() == 1) {
            network_byte1 = '0' + network_byte1;
        }
        if (network_byte2.length() == 1) {
            network_byte2 = '0' + network_byte2;
        }
        
        String operation_data_bridge_device = manufacture_id_m + manufacture_id_l + packetFormat + network_byte1 + network_byte2 + server_id + device_id;

        for (int k = 0; k < maskedData.length; k++) {

            operation_data_bridge_device = operation_data_bridge_device + String.format("%02X", maskedData[k] & 0xFF);
        }

        //System.err.println("base64 encode" + operation_data_bridge_device);
        
        return operation_data_bridge_device;
                

    }

    public static void bitShiftingBack(byte[] valuesdata, int network_id, int destination) {

        //new 24 byte
        String s_string = "";
        String s_string1 = "";
        String temp_byt_hex = "";
        String hop = "";
        int packetFlag = 1;
        String packetFormat = "13";
        String server_id = "00";
        String manufacture_id_m = "";
        String manufacture_id_l = "";

        for (int j = 0; j < valuesdata.length; j++) {
            System.err.println("Encrypted Array" + valuesdata[j]);
            int value = (valuesdata[j] & 128);
            if (value == 128) {
                if (j == 0 || j == 8) {
                    s_string = s_string + '0';
                    s_string1 = '1' + s_string1;
                } else {
                    s_string = s_string + '1';
                }
                int masked = valuesdata[j] & 127;
                valuesdata[j] = (byte) masked;
            } else {
                s_string = s_string + '0';
                if (j == 0 || j == 8) {
                    s_string1 = '0' + s_string1;
                }
            }

            String str = Integer.toHexString(valuesdata[j] & 255);
            if (str.length() == 1) {
                temp_byt_hex = temp_byt_hex + '0' + str;
            } else {
                temp_byt_hex = temp_byt_hex + str;
            }
        }

        System.err.println("st_string::" + s_string);
        System.err.println("st_string1::" + s_string1);

        switch (s_string1) {
            case "00":
                hop = "00000101";
                break;
            case "01":
                hop = "00001101";
                break;
            case "10":
                hop = "00010101";
                break;
            case "11":
                hop = "00011101";
                break;
        }

        String bitshift_data1 = Integer.toHexString(Integer.parseInt(s_string.substring(0, 8), 2));

        String bitshift_data2 = Integer.toHexString(Integer.parseInt(s_string.substring(9, 16), 2));;
        if (bitshift_data1.length() == 1) {
            bitshift_data1 = '0' + bitshift_data1;
        }
        if (bitshift_data2.length() == 1) {
            bitshift_data2 = '0' + bitshift_data2;
        }
        String bitshift_data = bitshift_data1 + bitshift_data2;

        System.err.println("bit shift data::" + bitshift_data);

        String hop_data = Integer.toHexString(Integer.parseInt(hop, 2));

        if (packetFlag == 1) {
            manufacture_id_m = "97";
            manufacture_id_l = "01";
        }

        byte[] network_id_arr = convertIntToByteArray(network_id, 2);

        String device_id = Integer.toHexString(destination & 127);
        if (device_id.length() == 1) {
            device_id = '0' + device_id;
        }

        String network_byte1 = Integer.toHexString(network_id_arr[0] & 255);
        String network_byte2 = Integer.toHexString(network_id_arr[1] & 255);

        if (hop_data.length() == 1) {
            hop_data = '0' + hop_data;
        }
        if (network_byte1.length() == 1) {
            network_byte1 = '0' + network_byte1;
        }
        if (network_byte2.length() == 1) {
            network_byte2 = '0' + network_byte2;
        }

        String operation_data_bridge_device = manufacture_id_m + manufacture_id_l + packetFormat + network_byte1 + network_byte2 + server_id + device_id + hop_data + temp_byt_hex + bitshift_data;

        System.err.println("base64 encode" + operation_data_bridge_device);

    }

    /*Function to convert decimal to byte array*/
    public static byte[] convertIntToByteArray(int value, int length) {

        byte[] b = new byte[length];
        for (int i = 0; i < length; ++i) {
            b[i] = (byte) ((int) (value % 256L));
            value /= 256L;
        }

        byte[] b1 = new byte[length];
        int byteArrayLength = b.length - 1;
        for (int i = byteArrayLength; i >= 0; --i) {

            b1[byteArrayLength - i] = b[i];
        }
        return b1;
    }

    /*Function to encrypt the data*/
    public static String encrypt(byte[] crcArr, String strKey) throws Exception {
        String strData = "";
        System.out.println("Security key" + strKey.getBytes().length);
        try {
            SecretKeySpec skeyspec = new SecretKeySpec(strKey.getBytes(), "AES");
            Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, skeyspec);
            byte[] encrypted = cipher.doFinal(crcArr);
            strData = new String(encrypted);

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        }
        return strData;
    }

}
