package Helperclasses;

public class SpPojo {

    String sp;
    String[] args;
    public SpPojo(String sp,String[] args){
        this.sp=sp;
        this.args=args;
    }

    public String getSp() {
        return sp;
    }

    public void setSp(String sp) {
        this.sp = sp;
    }

    public String[] getArgs() {
        return args;
    }

    public void setArgs(String[] args) {
        this.args = args;
    }
}
