public class FilesInfo {
    //get this value by exec: hadoop fs -count /data/wuxia_novels/
    private static final int TOTAL_FILE_NUM = 218;

    public static int getTotalFileNum(){
        return TOTAL_FILE_NUM;
    }

    public static double calcIDF(int fileNum){
        return Math.log((double)TOTAL_FILE_NUM / (double)(fileNum+1));
    }
}
