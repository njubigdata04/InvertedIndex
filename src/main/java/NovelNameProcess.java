public class NovelNameProcess {
    public static  String splitAuthorName(String novelname){
        for(int i = 0; i<novelname.length(); i++){
            if(novelname.charAt(i) >= '0' && novelname.charAt(i) <= '9'){
                return novelname.substring(0, i);
            }
        }
        return novelname;
    }
}
