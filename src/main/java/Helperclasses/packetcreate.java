package Helperclasses;

public class packetcreate {

    public String packetcreation(int networkid,int broadcast,String networkkey,int  root,int zone,int sequenceno,int devicetype){

        System.out.print("networkid--"+networkid+"\n");
        System.out.print("networkkey--"+networkkey+"\n");
        System.out.print("broacast--"+ broadcast+"\n");


        System.out.print("root--"+root+"\n");
        System.out.print("zone--"+zone+"\n");
        System.out.print("sequenceno--"+sequenceno+"\n");
        String root2=Integer.toHexString(root);
        String zone2=Integer.toHexString(zone);

        root2="0000"+root2;
        zone2="0000"+zone2;

        root2=root2.substring(root2.length()-4,root2.length());
        zone2=zone2.substring(zone2.length()-4,zone2.length());


        System.out.print(root2+"--"+zone2+"\n");

        String sendPacket="";

        int[] pwm=new int[4];

        pwm[0]=0;
        pwm[1]=0;
        pwm[2]=1;
        pwm[3]=1;

        if(broadcast==-1){
            pwm[3]=1;
            broadcast=127;
        }
        byte[] finalpacket=null;
        try {
            byte[] aesEncrypeData=null;
            if (devicetype==13002){
                aesEncrypeData = PacketService.packetCreation(broadcast, networkkey, 130, 2, sequenceno, pwm,1013 );

            }else{
                aesEncrypeData = PacketService.packetCreation(broadcast, networkkey, 130, 2, sequenceno, pwm,1015 );

            }
            byte[] maskedDataArray = ByteMasking.mask(aesEncrypeData);
            String fullBridgeData = PacketService.bitShifting(maskedDataArray, networkid, broadcast);

            sendPacket= "09001e"+root2+zone2+fullBridgeData;













        }catch(Exception e){
            System.out.print("error in publish");
            e.printStackTrace();
        }


        return sendPacket;
    }
}
