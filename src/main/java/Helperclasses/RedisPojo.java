package Helperclasses;

public class RedisPojo {
    String Key;
    String value;
    int expiry=-1;
    public RedisPojo(String Key,String Value){
        this.Key=Key;
        this.value=Value;
    }

    public RedisPojo(String Key,String Value,int expiry){
        this.Key=Key;
        this.value=Value;
        this.expiry=expiry;
    }

    public int getExpiry() {
        return expiry;
    }

    public void setExpiry(int expiry) {
        this.expiry = expiry;
    }

    public String getKey() {
        return Key;
    }

    public void setKey(String key) {
        Key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
